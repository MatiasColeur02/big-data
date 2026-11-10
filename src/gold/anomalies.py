"""Anomalías de costo diario con MAD (D9): componente analítico de la entrega (§6.2).

Por grupo `(org_id, service)` sobre `daily_cost_usd`:

    score = 0.6745 · (x − mediana) / MAD        anomalía si |score| > 3.5

Mediana y MAD son robustas: no las contaminan los mismos spikes que se buscan, a diferencia de
media y desvío (z-score). Si MAD = 0 (grupo con costo casi constante), el score no está definido
y se cae a percentil 99 del grupo: anomalía si x > p99. `method` registra cuál se usó.
Percentiles exactos (`percentile`), no aproximados: los grupos tienen como mucho 60 días.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

GROUP = ["org_id", "service"]
MAD_CONSTANT = 0.6745


def cost_anomalies(features: DataFrame, threshold: float = 3.5) -> DataFrame:
    x = F.col("daily_cost_usd")
    med = features.groupBy(*GROUP).agg(
        F.expr("percentile(daily_cost_usd, 0.5)").alias("median_cost_usd"),
        F.expr("percentile(daily_cost_usd, 0.99)").alias("p99_cost_usd"),
        F.count("*").alias("group_days"),
    )
    dev = features.join(med, GROUP).withColumn("_absdev", F.abs(x - F.col("median_cost_usd")))
    mad = dev.groupBy(*GROUP).agg(F.expr("percentile(_absdev, 0.5)").alias("mad_cost_usd"))
    scored = dev.join(mad, GROUP).drop("_absdev")

    use_mad = F.col("mad_cost_usd") > 0
    score = F.when(use_mad, F.lit(MAD_CONSTANT) * (x - F.col("median_cost_usd")) / F.col("mad_cost_usd"))
    return (
        scored.withColumn("anomaly_score", F.round(score, 4))
        .withColumn("method", F.when(use_mad, F.lit("mad")).otherwise(F.lit("p99")))
        .withColumn(
            "is_anomaly",
            F.when(use_mad, F.abs(F.col("anomaly_score")) > F.lit(threshold)).otherwise(x > F.col("p99_cost_usd")),
        )
        .withColumn("threshold", F.lit(threshold))
        .select(
            "org_id", "usage_date", "service", "daily_cost_usd", "median_cost_usd", "mad_cost_usd",
            "p99_cost_usd", "group_days", "anomaly_score", "method", "is_anomaly", "threshold",
            "cost_anomaly_events",
        )
        .withColumn("processed_ts", F.current_timestamp())
    )
