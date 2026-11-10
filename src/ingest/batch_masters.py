"""Bronze batch: los 7 maestros CSV de Landing a Parquet (consigna §6.2, D3, D11, D12).

    python -m src.ingest.batch_masters                      # los 7 maestros
    python -m src.ingest.batch_masters --sources users,billing_monthly

Por fuente:
1. Lee el CSV con su esquema explícito en modo FAILFAST y valida que la cabecera coincida con el
   esquema: un valor que no encaja en su tipo o una columna movida hacen fallar el job en vez de
   convertirse en null o en datos corridos sin aviso (D4).
2. Agrega `ingest_ts` y `source_file`.
3. Separa las filas con clave primaria nula o repetida y las manda a Quarantine; de cada clave
   repetida conserva la primera aparición en el archivo.
4. Escribe Bronze reemplazando la carga anterior (snapshot, D12): sin particionar y en un solo
   archivo, salvo `billing_monthly`, particionado por `month` (D3).
5. Verifica `landing = bronze + quarantine` por fuente (O3) y lo deja en el registro de corridas.

Re-ejecutarlo produce el mismo resultado: cada carga reemplaza la anterior completa.
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from src.common import schemas
from src.common.config import Config, load_config
from src.common.lake import path_stats, with_technical_columns
from src.common.logs import setup_logging
from src.common.run_log import track

log = logging.getLogger("pipeline")

QUARANTINE_PARTITION = "dq_rule"
RULE_PK_NULL = "pk_null"
RULE_PK_DUPLICATE = "pk_duplicate"


class LandingFileMissing(FileNotFoundError):
    pass


class CountMismatch(RuntimeError):
    pass


def landing_file(cfg: Config, name: str) -> str:
    return f"{cfg.zone('landing')}/{name}.csv"


def read_master(spark: SparkSession, cfg: Config, name: str) -> DataFrame:
    path = landing_file(cfg, name)
    if not Path(path).is_file():
        raise LandingFileMissing(f"falta {name}.csv en Landing ({path}). Ver data/README.md para poblarlo")
    return (
        spark.read.option("header", True)
        .option("mode", "FAILFAST")
        .option("enforceSchema", False)   # la cabecera tiene que coincidir con el esquema
        # Comillas escapadas como "" (RFC 4180), no con \ como espera Spark por defecto: sin esto,
        # 211 de las 400 filas de resources.csv (tags_json con varias etiquetas) se corren de columna.
        .option("escape", '"')
        .option("dateFormat", "yyyy-MM-dd")
        .schema(schemas.MASTERS[name]["schema"])
        .csv(path)
    )


def split_by_key(df: DataFrame, key: list[str]) -> tuple[DataFrame, DataFrame]:
    """Devuelve (válidas, rechazadas). Rechaza clave nula y apariciones repetidas de una clave."""
    ordered = df.withColumn("_row", F.monotonically_increasing_id())
    null_key = F.greatest(*[F.col(c).isNull() for c in key]) if len(key) > 1 else F.col(key[0]).isNull()
    rank = F.row_number().over(Window.partitionBy(*key).orderBy("_row"))
    tagged = ordered.withColumn(
        QUARANTINE_PARTITION,
        F.when(null_key, F.lit(RULE_PK_NULL)).when(rank > 1, F.lit(RULE_PK_DUPLICATE)),
    ).drop("_row")
    valid = tagged.filter(F.col(QUARANTINE_PARTITION).isNull()).drop(QUARANTINE_PARTITION)
    rejected = tagged.filter(F.col(QUARANTINE_PARTITION).isNotNull()).withColumn(
        "rejected_ts", F.current_timestamp()
    )
    return valid, rejected


def ingest_master(spark: SparkSession, cfg: Config, name: str) -> dict:
    spec = schemas.MASTERS[name]
    raw = read_master(spark, cfg, name)
    staged = with_technical_columns(raw).cache()
    valid, rejected = split_by_key(staged, spec["key"])

    bronze_path = f"{cfg.zone('bronze')}/{name}"
    writer = valid.coalesce(1).write.mode("overwrite").option("partitionOverwriteMode", "static")
    if spec["partition_by"]:
        writer = writer.partitionBy(spec["partition_by"])
    writer.parquet(bronze_path)

    quarantine_path = f"{cfg.zone('quarantine')}/{name}"
    (
        rejected.coalesce(1).write.mode("overwrite").option("partitionOverwriteMode", "static")
        .partitionBy(QUARANTINE_PARTITION).parquet(quarantine_path)
    )

    landing_rows = staged.count()
    staged.unpersist()
    bronze_rows = spark.read.parquet(bronze_path).count()
    quarantine_rows = _count_quarantine(spark, quarantine_path)
    if landing_rows != bronze_rows + quarantine_rows:
        raise CountMismatch(
            f"{name}: landing={landing_rows} != bronze={bronze_rows} + quarantine={quarantine_rows}"
        )
    stats = path_stats(bronze_path)
    log.info(
        "  %-18s landing=%5d bronze=%5d quarantine=%d · %d archivo(s), %d partición(es), %.1f KB",
        name, landing_rows, bronze_rows, quarantine_rows, stats["files"], stats["partitions"], stats["bytes"] / 1024,
    )
    return {
        "landing": landing_rows,
        "bronze": bronze_rows,
        "quarantine": quarantine_rows,
        "bronze_files": stats["files"],
        "bronze_bytes": stats["bytes"],
        "bronze_partitions": stats["partitions"],
    }


def _count_quarantine(spark: SparkSession, path: str) -> int:
    # Sin rechazos Spark escribe solo _SUCCESS: no hay Parquet del que leer el esquema (D20).
    if path_stats(path)["files"] == 0:
        return 0
    return spark.read.parquet(path).count()


def run(cfg: Config, spark: SparkSession | None = None, sources: list[str] | None = None) -> dict:
    sources = sources or sorted(schemas.MASTERS)
    unknown = sorted(set(sources) - set(schemas.MASTERS))
    if unknown:
        raise ValueError(f"fuentes desconocidas: {', '.join(unknown)}. Válidas: {', '.join(sorted(schemas.MASTERS))}")
    if spark is None:
        from src.common.spark_session import build_spark

        spark = build_spark(cfg, app_name="bronze_masters", serving=False)

    with track(cfg, job="bronze_masters", source="landing", target="bronze") as job:
        results = {name: ingest_master(spark, cfg, name) for name in sources}
        job.update(
            rows_read=sum(r["landing"] for r in results.values()),
            rows_written=sum(r["bronze"] for r in results.values()),
            rows_rejected=sum(r["quarantine"] for r in results.values()),
            partitions_written=sum(max(r["bronze_partitions"], 1) for r in results.values()),
        )
        job.extra["sources"] = results
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--profile", help="local | cloud (por defecto PIPELINE_PROFILE o local)")
    parser.add_argument("--sources", help="lista separada por comas; por defecto los 7 maestros")
    args = parser.parse_args()
    setup_logging()
    cfg = load_config(args.profile)
    run(cfg, sources=args.sources.split(",") if args.sources else None)


if __name__ == "__main__":
    main()
