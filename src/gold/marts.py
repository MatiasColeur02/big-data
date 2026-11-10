"""Marts FinOps de Gold a partir de `silver/usage_daily_features` (D21: Gold no reagrega eventos).

| Mart                          | Grano                         | Consulta (§7.4) |
|-------------------------------|-------------------------------|-----------------|
| `org_daily_usage_by_service`  | `org_id, usage_date, service` | P1              |
| `org_service_cost_14d`        | `org_id, service`             | P2 (Top-N)      |
"""

from __future__ import annotations

from datetime import date, timedelta

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

DAILY_COLUMNS = [
    "org_id", "usage_date", "service", "daily_cost_usd", "requests", "cpu_hours", "storage_gb_hours",
    "genai_tokens", "carbon_kg", "events", "value_null_events", "cost_anomaly_events",
]
TOP_N_WINDOW_DAYS = 14


def org_daily_usage_by_service(features: DataFrame) -> DataFrame:
    return features.select(*DAILY_COLUMNS).withColumn("processed_ts", F.current_timestamp())


def resolve_as_of_date(features: DataFrame, configured: str | date | None) -> date:
    """D15: la fecha de la config, o el máximo `usage_date` de los datos. Nunca `current_date()`."""
    if configured:
        return configured if isinstance(configured, date) else date.fromisoformat(str(configured))
    return features.agg(F.max("usage_date")).first()[0]


def org_service_cost_14d(features: DataFrame, as_of: date, days: int = TOP_N_WINDOW_DAYS) -> DataFrame:
    """Costo acumulado por org y servicio en `[as_of − (days−1), as_of]`, con su posición en la org.
    En Cassandra el total es clustering key descendente (D8); la recarga es TRUNCATE + carga (D14)."""
    start = as_of - timedelta(days=days - 1)
    window = features.filter(F.col("usage_date").between(F.lit(start), F.lit(as_of)))
    totals = window.groupBy("org_id", "service").agg(
        F.round(F.sum("daily_cost_usd"), 4).alias("total_cost_usd"),
        F.sum("requests").alias("total_requests"),
        F.countDistinct("usage_date").alias("active_days"),
    )
    rank = F.row_number().over(Window.partitionBy("org_id").orderBy(F.desc("total_cost_usd"), "service"))
    return (
        totals.withColumn("rank", rank)
        .withColumn("window_start", F.lit(start))
        .withColumn("as_of_date", F.lit(as_of))
        .withColumn("processed_ts", F.current_timestamp())
    )
