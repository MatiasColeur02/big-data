"""Job de Silver: Bronze → Silver (consigna §6.2).

    python -m src.silver.run

1. `dim_org` y `dim_resource` desde los maestros de Bronze.
2. Reglas de calidad sobre los eventos (`quality.events.validate_events`): Quarantine queda escrita.
3. Conformado y enriquecimiento de los eventos válidos → `silver/usage_events`, particionado por
   `event_date`, un archivo por fecha (es donde se compacta Bronze, D13).
4. Control `bronze[d] = silver[d] + quarantine[d]` para cada fecha (O4). Si no cierra, el job falla.
5. Features diarias → `silver/usage_daily_features`, particionado por `usage_date` (D21).

Escribe con overwrite dinámico por partición (D10) y recalcula todas las fechas: re-ejecutarlo
deja el mismo resultado (O6).
"""

from __future__ import annotations

import argparse
import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.common.config import Config, load_config
from src.common.lake import path_stats
from src.common.logs import setup_logging
from src.common.run_log import track
from src.ingest.stream_events import bronze_path
from src.quality.events import BalanceError, check_balance, read_quarantine, validate_events
from src.silver.conform_events import conform, enrich
from src.silver.dims import dim_path, write_dims
from src.silver.features import GRAIN, daily_features

log = logging.getLogger("pipeline")


def events_path(cfg: Config) -> str:
    return f"{cfg.zone('silver')}/usage_events"


def features_path(cfg: Config) -> str:
    return f"{cfg.zone('silver')}/usage_daily_features"


def run(cfg: Config, spark: SparkSession | None = None) -> dict:
    if spark is None:
        from src.common.spark_session import build_spark

        spark = build_spark(cfg, app_name="silver", serving=False)

    with track(cfg, job="silver", source="bronze", target="silver") as job:
        dims = write_dims(spark, cfg)
        valid, quality = validate_events(spark, cfg)
        events = enrich(
            conform(valid),
            spark.read.parquet(dim_path(cfg, "dim_org")),
            spark.read.parquet(dim_path(cfg, "dim_resource")),
        )
        events.repartition("event_date").write.mode("overwrite").partitionBy("event_date").parquet(events_path(cfg))
        valid.unpersist()

        silver = spark.read.parquet(events_path(cfg))
        unbalanced = check_balance(spark.read.parquet(bronze_path(cfg)), silver, read_quarantine(spark, cfg))
        if unbalanced:
            raise BalanceError(f"bronze != silver + quarantine en {len(unbalanced)} fecha(s): {unbalanced[:5]}")

        (
            daily_features(silver).repartition("usage_date")
            .write.mode("overwrite").partitionBy("usage_date").parquet(features_path(cfg))
        )
        features = spark.read.parquet(features_path(cfg))

        stats = silver.agg(
            F.count("*").alias("rows"),
            F.countDistinct("event_date").alias("dates"),
            F.sum(F.col("orphan_org").cast("int")).alias("orphan_org"),
            F.sum(F.col("orphan_resource").cast("int")).alias("orphan_resource"),
            F.sum(F.array_contains("dq_flags", "resource_mismatch").cast("int")).alias("resource_mismatch"),
        ).first().asDict()
        fstats = features.agg(
            F.count("*").alias("rows"),
            F.countDistinct(*GRAIN).alias("grain_keys"),
            F.round(F.sum("daily_cost_usd"), 2).alias("total_cost_usd"),
            F.sum(F.col("requests").isNull().cast("int")).alias("requests_not_measured"),
        ).first().asDict()
        report = {
            "dims": dims,
            "quality": {k: quality[k] for k in ("bronze", "valid", "quarantine", "blocked_by_rule", "flagged_by_rule")},
            "usage_events": {**stats, **_files(events_path(cfg))},
            "usage_daily_features": {**fstats, **_files(features_path(cfg))},
            "unbalanced_dates": unbalanced,
        }
        job.update(rows_read=quality["bronze"], rows_written=stats["rows"], rows_rejected=quality["quarantine"],
                   partitions_written=stats["dates"])
        job.extra["silver"] = report
        log.info("  dims: %s", dims)
        log.info("  usage_events: %d filas, %d fechas, huérfanos org/recurso=%d/%d, %d archivos",
                 stats["rows"], stats["dates"], stats["orphan_org"], stats["orphan_resource"],
                 report["usage_events"]["files"])
        log.info("  usage_daily_features: %d filas (%d claves), costo total %.2f USD, requests no medidos=%d",
                 fstats["rows"], fstats["grain_keys"], fstats["total_cost_usd"], fstats["requests_not_measured"])
        log.info("  balance bronze = silver + quarantine: %d fechas sin cerrar", len(unbalanced))
    return report


def _files(path: str) -> dict:
    s = path_stats(path)
    return {"files": s["files"], "bytes": s["bytes"]}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--profile", help="local | cloud (por defecto PIPELINE_PROFILE o local)")
    args = parser.parse_args()
    setup_logging()
    run(load_config(args.profile))


if __name__ == "__main__":
    main()
