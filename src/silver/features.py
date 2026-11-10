"""Features diarias por `(org_id, usage_date, service)` (consigna §4.4 y §6.2, D21).

| Feature             | Cálculo                                                               |
|---------------------|-----------------------------------------------------------------------|
| `daily_cost_usd`    | Suma de `cost_usd_increment`, incluidos los negativos marcados (D7)   |
| `requests`          | Suma de `value` de los eventos `requests`                             |
| `cpu_hours`         | Suma de `value` de los eventos `cpu_hours`                            |
| `storage_gb_hours`  | Suma de `value` de los eventos `storage_gb_hours`                     |
| `genai_tokens`      | Suma de `genai_tokens` (solo genai en v2; null antes del 18/07, D5)   |
| `carbon_kg`         | Suma de `carbon_kg` (solo v2; null antes del 18/07, D5)               |

Métricas de uso (D19): 0 si el grupo no tuvo eventos de esa métrica; null si los tuvo pero todos
llegaron con `value` nulo ("no medido" no es "cero"). `value_null_events` dice cuántos eventos
del grupo no traían medición.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from src.silver.conform_events import METRIC_COLUMNS

GRAIN = ["org_id", "usage_date", "service"]


def _metric_sum(metric: str) -> Column:
    events_of_metric = F.sum(F.when(F.col("metric") == metric, 1).otherwise(0))
    return F.when(events_of_metric == 0, F.lit(0.0)).otherwise(F.sum(metric)).alias(metric)


def daily_features(events: DataFrame) -> DataFrame:
    return events.groupBy("org_id", F.col("event_date").alias("usage_date"), "service").agg(
        F.sum("cost_usd_increment").alias("daily_cost_usd"),
        *[_metric_sum(m) for m in METRIC_COLUMNS],
        F.sum("genai_tokens").alias("genai_tokens"),
        F.sum("carbon_kg").alias("carbon_kg"),
        F.count("*").alias("events"),
        F.sum(F.col("value").isNull().cast("int")).alias("value_null_events"),
        F.sum(F.col("cost_anomaly").cast("int")).alias("cost_anomaly_events"),
    )
