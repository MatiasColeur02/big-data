"""Dimensiones conformadas de Silver: `dim_org` (SCD tipo 1) y `dim_resource`.

Cada carga reemplaza la anterior completa (D12): los maestros son una foto, sin historia de
cambios, por eso no hay SCD tipo 2 (decisión cerrada en la segunda entrega).
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.config import Config


def _clean(df: DataFrame, columns: list[str]) -> DataFrame:
    """Normaliza categorías: sin espacios y en minúsculas. El dataset ya viene conformado (perfilado),
    pero se aplica igual para que una variante tipo `US-East ` no abra un grupo nuevo."""
    for c in columns:
        df = df.withColumn(c, F.lower(F.trim(F.col(c))))
    return df


def build_dim_org(customers: DataFrame, nps_range: tuple[float, float]) -> DataFrame:
    """D7: `nps_score` fuera de [-100, 100] se marca y se anula; el original queda en `nps_score_raw`."""
    low, high = nps_range
    out_of_range = F.col("nps_score_raw").isNotNull() & ~F.col("nps_score_raw").between(low, high)
    return (
        _clean(customers, ["industry", "hq_region", "plan_tier", "sales_rep", "lifecycle_stage", "marketing_source"])
        .withColumn("org_name", F.trim("org_name"))
        .withColumn("nps_score_raw", F.col("nps_score"))
        .withColumn("dq_flags", F.filter(F.array(F.when(out_of_range, F.lit("nps_out_of_range"))), lambda x: x.isNotNull()))
        .withColumn("nps_score", F.when(out_of_range, F.lit(None).cast("double")).otherwise(F.col("nps_score_raw")))
    )


def build_dim_resource(resources: DataFrame) -> DataFrame:
    """`tags_json` se conserva y además se parsea a `tags` (array) para poder filtrar por etiqueta."""
    return (
        _clean(resources, ["service", "region", "state"])
        .withColumn("tags", F.from_json("tags_json", "array<string>"))
    )


def dim_path(cfg: Config, name: str) -> str:
    return f"{cfg.zone('silver')}/{name}"


def write_dims(spark: SparkSession, cfg: Config) -> dict:
    bronze = cfg.zone("bronze")
    nps = tuple(float(x) for x in cfg.get("quality.nps_valid_range"))
    dims = {
        "dim_org": build_dim_org(spark.read.parquet(f"{bronze}/customers_orgs"), nps),
        "dim_resource": build_dim_resource(spark.read.parquet(f"{bronze}/resources")),
    }
    counts = {}
    for name, df in dims.items():
        df.coalesce(1).write.mode("overwrite").option("partitionOverwriteMode", "static").parquet(dim_path(cfg, name))
        counts[name] = spark.read.parquet(dim_path(cfg, name)).count()
    return counts
