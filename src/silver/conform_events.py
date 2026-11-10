"""Conformado y enriquecimiento de eventos para Silver (consigna §4.2, D5, D7, D19).

- `value` pasa a double con el casteo de `rules.value_as_double`; el string original queda en
  `value_raw`. Los nulos se conservan sin imputar (D19).
- Categorías normalizadas (minúsculas, sin espacios).
- Compatibilidad v1/v2 (D5): `carbon_kg` y `genai_tokens` quedan null en v1 y `schema_version`
  se conserva. No se imputan: el esquema v1 no los medía.
- Una columna por métrica (`requests`, `cpu_hours`, `storage_gb_hours`) con el valor del evento
  cuando corresponde a esa métrica: es la base de las features diarias.
- Left join con `dim_resource` y `dim_org`: ningún evento se pierde por el join. Los huérfanos y
  los eventos cuyo servicio, región u organización no coinciden con su recurso se marcan en
  `dq_flags` (el perfilado midió 0 casos: el join funciona como control de calidad).
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.quality.rules import UNIT_BY_METRIC, value_as_double

METRIC_COLUMNS = list(UNIT_BY_METRIC)   # requests, cpu_hours, storage_gb_hours


def conform(valid: DataFrame) -> DataFrame:
    df = valid.withColumn("value_raw", F.col("value")).withColumn("value", value_as_double("value_raw"))
    for c in ("service", "region", "metric", "unit"):
        df = df.withColumn(c, F.lower(F.trim(F.col(c))))
    for metric in METRIC_COLUMNS:
        df = df.withColumn(metric, F.when(F.col("metric") == metric, F.col("value")))
    return df


def enrich(events: DataFrame, dim_org: DataFrame, dim_resource: DataFrame) -> DataFrame:
    resource = dim_resource.select(
        "resource_id",
        F.col("org_id").alias("_res_org"),
        F.col("service").alias("_res_service"),
        F.col("region").alias("_res_region"),
        F.col("state").alias("resource_state"),
        F.lit(True).alias("_res_found"),
    )
    org = dim_org.select(
        "org_id", "org_name", "industry", "plan_tier", "is_enterprise",
        F.col("hq_region").alias("org_hq_region"),
        F.lit(True).alias("_org_found"),
    )
    joined = events.join(F.broadcast(resource), "resource_id", "left").join(F.broadcast(org), "org_id", "left")

    orphan_resource = F.col("_res_found").isNull()
    orphan_org = F.col("_org_found").isNull()
    mismatch = ~orphan_resource & (
        (F.col("service") != F.col("_res_service"))
        | (F.col("region") != F.col("_res_region"))
        | (F.col("org_id") != F.col("_res_org"))
    )
    new_flags = F.filter(
        F.array(
            F.when(orphan_org, F.lit("orphan_org")),
            F.when(orphan_resource, F.lit("orphan_resource")),
            F.when(mismatch, F.lit("resource_mismatch")),
        ),
        lambda x: x.isNotNull(),
    )
    return (
        joined.withColumn("dq_flags", F.concat("dq_flags", new_flags))
        .withColumn("orphan_org", orphan_org)
        .withColumn("orphan_resource", orphan_resource)
        .drop("_res_org", "_res_service", "_res_region", "_res_found", "_org_found")
    )
