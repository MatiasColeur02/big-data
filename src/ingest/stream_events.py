"""Bronze streaming: `usage_events_stream/*.jsonl` a Parquet con Structured Streaming.

    python -m src.ingest.stream_events                  # procesa lo disponible y termina (D18)
    python -m src.ingest.stream_events --reset          # borra Bronze de eventos y su checkpoint juntos
    python -m src.ingest.stream_events --demo 60        # trigger de 10 s durante 60 s (O7)

- Esquema explícito, `value` como string (D4, D5). `timestamp` pasa a llamarse `event_ts` y de ahí
  sale `event_date` en UTC (D15).
- `withWatermark("event_ts", "60 days")` + `dropDuplicatesWithinWatermark("event_id")` (D6).
- File sink de Parquet en modo append, particionado por `event_date`, con checkpoint: exactly-once
  (D13). Se reparticiona por `event_date` para escribir un archivo por fecha y por lote.
- `availableNow`: procesa todo lo pendiente respetando `maxFilesPerTrigger` y termina (D18).
- Las métricas salen de `recentProgress` (todos los lotes, no solo el último) y van al registro
  de corridas: lotes, filas, duplicados descartados y filas descartadas por el watermark.

El checkpoint y la salida se borran siempre juntos (D13). Si aparece uno sin el otro, el job se
niega a arrancar: de lo contrario el stream no escribiría nada o releería todo, sin ningún error.
"""

from __future__ import annotations

import argparse
import logging
import shutil
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common import schemas
from src.common.config import Config, load_config
from src.common.lake import path_stats, with_technical_columns
from src.common.logs import setup_logging
from src.common.run_log import track

log = logging.getLogger("pipeline")

TABLE = "usage_events_stream"
QUERY_NAME = "bronze_usage_events"


class InconsistentStreamState(RuntimeError):
    pass


def bronze_path(cfg: Config) -> str:
    return f"{cfg.zone('bronze')}/{TABLE}"


def checkpoint_path(cfg: Config) -> str:
    return f"{cfg.zone('checkpoints')}/{QUERY_NAME}"


def reset(cfg: Config) -> None:
    """Borra juntos la salida y el checkpoint (D13)."""
    for path in (bronze_path(cfg), checkpoint_path(cfg)):
        shutil.rmtree(path, ignore_errors=True)
    log.info("Bronze de eventos y su checkpoint borrados")


def check_state(cfg: Config) -> None:
    has_checkpoint = (Path(checkpoint_path(cfg)) / "offsets").is_dir()
    has_output = (Path(bronze_path(cfg)) / "_spark_metadata").is_dir()
    if has_checkpoint and not has_output:
        raise InconsistentStreamState(
            "existe el checkpoint pero no la salida de Bronze: el stream daría los archivos por "
            "leídos y Bronze quedaría vacío. Correr con --reset para empezar de cero (D13)"
        )
    if has_output and not has_checkpoint:
        raise InconsistentStreamState(
            "existe la salida de Bronze pero no el checkpoint: el sink saltearía los lotes ya "
            "registrados y no escribiría nada. Correr con --reset para empezar de cero (D13)"
        )


def source_stream(spark: SparkSession, cfg: Config) -> DataFrame:
    source = cfg.get("streaming.source_path")
    if not Path(source).is_dir():
        raise FileNotFoundError(f"no existe el directorio de eventos {source}. Ver data/README.md")
    return (
        spark.readStream.schema(schemas.USAGE_EVENTS)
        .option("maxFilesPerTrigger", int(cfg.get("streaming.max_files_per_trigger")))
        .json(source)
    )


def to_bronze(events: DataFrame, cfg: Config) -> DataFrame:
    """Transformación del stream, separada para poder probarla."""
    conformed = (
        events.withColumnRenamed("timestamp", "event_ts")
        .withColumn("event_date", F.to_date("event_ts"))
    )
    # Las columnas técnicas se calculan antes del shuffle: input_file_name() solo existe en el scan.
    return (
        with_technical_columns(conformed)
        .withWatermark("event_ts", cfg.get("streaming.watermark"))
        .dropDuplicatesWithinWatermark([cfg.get("streaming.dedup_key")])
        .repartition("event_date")
    )


def summarize_progress(progress: list[dict]) -> dict:
    # Con estado, Spark agrega al final lotes sin datos que solo avanzan el watermark: no se cuentan.
    data_batches = {p["batchId"] for p in progress if p.get("numInputRows", 0) > 0}
    dropped_late = dropped_dup = 0
    for p in progress:
        for op in p.get("stateOperators", []):
            dropped_late += op.get("numRowsDroppedByWatermark", 0)
            dropped_dup += op.get("customMetrics", {}).get("numDroppedDuplicateRows", 0)
    return {
        "batches": len(data_batches),
        "input_rows": sum(p.get("numInputRows", 0) for p in progress),
        "dropped_duplicates": dropped_dup,
        "dropped_by_watermark": dropped_late,
        "watermark": next((p["eventTime"]["watermark"] for p in reversed(progress) if "eventTime" in p and "watermark" in p["eventTime"]), None),
    }


def run(
    cfg: Config,
    spark: SparkSession | None = None,
    demo_seconds: int | None = None,
) -> dict:
    if spark is None:
        from src.common.spark_session import build_spark

        spark = build_spark(cfg, app_name="bronze_stream", serving=False)
    check_state(cfg)

    with track(cfg, job="bronze_stream", source="landing", target="bronze") as job:
        writer = (
            to_bronze(source_stream(spark, cfg), cfg)
            .writeStream.format("parquet")
            .outputMode("append")
            .partitionBy("event_date")
            .option("checkpointLocation", checkpoint_path(cfg))
            .queryName(QUERY_NAME)
        )
        if demo_seconds:
            writer = writer.trigger(processingTime=cfg.get("streaming.demo_trigger_interval"))
        else:
            writer = writer.trigger(availableNow=True)

        query = writer.start(bronze_path(cfg))
        if demo_seconds:
            query.awaitTermination(demo_seconds)
            query.stop()
        else:
            query.awaitTermination()
        if query.exception():
            raise RuntimeError(f"el stream terminó con error: {query.exception()}")

        metrics = summarize_progress(query.recentProgress)
        totals = bronze_totals(spark, cfg)
        job.update(
            rows_read=metrics["input_rows"],
            rows_written=metrics["input_rows"] - metrics["dropped_duplicates"] - metrics["dropped_by_watermark"],
            rows_rejected=metrics["dropped_duplicates"] + metrics["dropped_by_watermark"],
            partitions_written=totals["dates"],
        )
        job.extra.update(stream=metrics, bronze=totals)
        log.info(
            "  lotes=%d filas=%d duplicados=%d tardías descartadas=%d · watermark=%s",
            metrics["batches"], metrics["input_rows"], metrics["dropped_duplicates"],
            metrics["dropped_by_watermark"], metrics["watermark"],
        )
        log.info(
            "  Bronze total: %d filas, %d event_id distintos, %d fechas · %d archivos, %.1f KB",
            totals["rows"], totals["distinct_event_ids"], totals["dates"], totals["files"], totals["bytes"] / 1024,
        )
    return {"stream": metrics, "bronze": totals}


def bronze_totals(spark: SparkSession, cfg: Config) -> dict:
    """Conteos de Bronze leyendo desde la raíz, para que Spark use `_spark_metadata` (D13)."""
    path = bronze_path(cfg)
    stats = path_stats(path)
    if not stats["files"]:
        return {"rows": 0, "distinct_event_ids": 0, "dates": 0, "files": 0, "bytes": 0}
    row = spark.read.parquet(path).agg(
        F.count("*").alias("rows"),
        F.countDistinct("event_id").alias("ids"),
        F.countDistinct("event_date").alias("dates"),
    ).first()
    return {
        "rows": row["rows"],
        "distinct_event_ids": row["ids"],
        "dates": row["dates"],
        "files": stats["files"],
        "bytes": stats["bytes"],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--profile", help="local | cloud (por defecto PIPELINE_PROFILE o local)")
    parser.add_argument("--reset", action="store_true", help="borra Bronze de eventos y el checkpoint antes de correr")
    parser.add_argument("--demo", type=int, metavar="SEGUNDOS", help="trigger por tiempo durante SEGUNDOS (O7)")
    args = parser.parse_args()
    setup_logging()
    cfg = load_config(args.profile)
    if args.reset:
        reset(cfg)
    run(cfg, demo_seconds=args.demo)


if __name__ == "__main__":
    main()
