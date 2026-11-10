"""Reglas de calidad (D7, D16, D19). Funciones puras de DataFrame: no leen ni escriben.

Eventos (Bronze → Silver):

| Regla                 | Acción          | Condición                                                        |
|-----------------------|-----------------|------------------------------------------------------------------|
| `event_id_null`       | BLOQUEA         | `event_id` nulo                                                  |
| `event_id_duplicate`  | BLOQUEA         | `event_id` repetido: queda la fila con menor `ingest_ts` (D16)   |
| `value_not_castable`  | BLOQUEA         | `value` no nulo que no se puede castear a double (D19)           |
| `cost_negative`       | MARCA           | `cost_usd_increment < -0.01`, con `cost_anomaly = true`          |
| `unit_imputed`        | MARCA e imputa  | `unit` nula con `value`: se deduce de la métrica                 |
| `value_null`          | MARCA           | `value` nulo: se conserva sin imputar (D19)                      |

Billing:

| `fx_usd_forced`       | CORRIGE         | `currency = 'USD'` con `exchange_rate_to_usd != 1`               |

Una fila bloqueada entra a Quarantine con una sola regla, la primera en el orden de la tabla
(D16). Las marcas se acumulan en `dq_flags`. El valor original de lo que se corrige o imputa se
conserva en una columna `_raw` (D12).
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

BLOCK, FLAG, FIX = "BLOQUEA", "MARCA", "CORRIGE"


@dataclass(frozen=True)
class Rule:
    id: str
    action: str
    description: str


EVENT_RULES = (
    Rule("event_id_null", BLOCK, "event_id no nulo"),
    Rule("event_id_duplicate", BLOCK, "event_id único"),
    Rule("value_not_castable", BLOCK, "value casteable a double cuando no es nulo"),
    Rule("cost_negative", FLAG, "cost_usd_increment >= -0.01; si no, flag de anomalía"),
    Rule("unit_imputed", FLAG, "unit no nula cuando hay value; se imputa según la métrica"),
    Rule("value_null", FLAG, "value nulo: se conserva sin imputar y se cuenta aparte"),
)
BILLING_RULES = (Rule("fx_usd_forced", FIX, "currency = 'USD' implica exchange_rate_to_usd = 1"),)
BLOCKING_EVENT_RULES = [r.id for r in EVENT_RULES if r.action == BLOCK]

# D7: la unidad se deduce de la métrica sin una sola excepción en los 43.200 eventos.
UNIT_BY_METRIC = {"requests": "count", "cpu_hours": "hours", "storage_gb_hours": "gb_hours"}


def value_as_double(col: str = "value") -> Column:
    """Casteo de `value` con fallback controlado: null si no es numérico (D4). Única definición,
    la usan la regla de bloqueo y el casteo de Silver."""
    return F.expr(f"try_cast({col} AS DOUBLE)")


def blocking_rule(df: DataFrame) -> DataFrame:
    """Agrega `dq_rule` con la primera regla BLOQUEA que viola la fila, o null."""
    first_seen = Window.partitionBy("event_id").orderBy(F.col("ingest_ts").asc(), F.col("source_file").asc())
    return df.withColumn("_rank", F.row_number().over(first_seen)).withColumn(
        "dq_rule",
        F.when(F.col("event_id").isNull(), F.lit("event_id_null"))
        .when(F.col("_rank") > 1, F.lit("event_id_duplicate"))
        .when(F.col("value").isNotNull() & value_as_double().isNull(), F.lit("value_not_castable")),
    ).drop("_rank")


def flag_and_impute(df: DataFrame, cost_min: float) -> DataFrame:
    """Aplica las reglas MARCA: `dq_flags`, `cost_anomaly` e imputación de `unit` (con `unit_raw`)."""
    mapping = F.create_map(*[F.lit(x) for pair in UNIT_BY_METRIC.items() for x in pair])
    cost_negative = F.col("cost_usd_increment") < F.lit(cost_min)
    unit_missing = F.col("unit").isNull() & F.col("value").isNotNull()
    flags = F.array(
        F.when(cost_negative, F.lit("cost_negative")),
        F.when(unit_missing, F.lit("unit_imputed")),
        F.when(F.col("value").isNull(), F.lit("value_null")),
    )
    return (
        df.withColumn("dq_flags", F.filter(flags, lambda x: x.isNotNull()))
        .withColumn("cost_anomaly", F.coalesce(cost_negative, F.lit(False)))
        .withColumn("unit_raw", F.col("unit"))
        .withColumn("unit", F.when(unit_missing, mapping[F.col("metric")]).otherwise(F.col("unit")))
    )


def apply_event_rules(bronze: DataFrame, cost_min: float) -> tuple[DataFrame, DataFrame]:
    """Separa los eventos de Bronze en (válidos con marcas, rechazados para Quarantine).

    Los rechazados conservan las columnas de Bronze (incluida `event_date`, D16) y suman
    `dq_rule` y `rejected_ts`. Los válidos suman `dq_flags`, `cost_anomaly` y `unit_raw`.
    """
    tagged = blocking_rule(bronze)
    rejected = tagged.filter(F.col("dq_rule").isNotNull()).withColumn("rejected_ts", F.current_timestamp())
    valid = flag_and_impute(tagged.filter(F.col("dq_rule").isNull()).drop("dq_rule"), cost_min)
    return valid, rejected


def apply_billing_rules(billing: DataFrame) -> DataFrame:
    """D7: una factura en USD tiene tipo de cambio 1 por definición. Conserva el original."""
    # La condición se evalúa sobre el valor original: después de corregirlo siempre vale 1.
    usd_wrong = (F.col("currency") == "USD") & (F.col("exchange_rate_raw") != 1.0)
    return (
        billing.withColumn("exchange_rate_raw", F.col("exchange_rate_to_usd"))
        .withColumn("dq_flags", F.filter(F.array(F.when(usd_wrong, F.lit("fx_usd_forced"))), lambda x: x.isNotNull()))
        .withColumn("exchange_rate_to_usd", F.when(usd_wrong, F.lit(1.0)).otherwise(F.col("exchange_rate_raw")))
    )
