"""Esquemas explícitos de las 8 fuentes de Landing (D11: el esquema vive en el código).

Ninguna fuente se lee con inferencia de tipos. Los nombres de columna son los del archivo; los
renombres (por ejemplo `timestamp` → `event_ts`) los hace cada job de ingesta.
"""

from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def _schema(*fields: tuple) -> StructType:
    # Todo nullable: Spark ignora `nullable=False` al leer archivos. La clave no nula se controla
    # en la ingesta y en las reglas de calidad (D7).
    return StructType([StructField(name, dtype, True) for name, dtype in fields])


# Eventos: esquema único v1+v2 (D5). `value` como string (D4): llega como número, como string
# numérico o null, y un DoubleType convertiría los strings en null sin avisar. `carbon_kg` y
# `genai_tokens` solo existen en v2 y quedan null en v1.
USAGE_EVENTS = _schema(
    ("event_id", StringType()),
    ("timestamp", TimestampType()),
    ("org_id", StringType()),
    ("resource_id", StringType()),
    ("service", StringType()),
    ("region", StringType()),
    ("metric", StringType()),
    ("value", StringType()),
    ("unit", StringType()),
    ("cost_usd_increment", DoubleType()),
    ("schema_version", IntegerType()),
    ("carbon_kg", DoubleType()),
    ("genai_tokens", LongType()),
)

CUSTOMERS_ORGS = _schema(
    ("org_id", StringType()),
    ("org_name", StringType()),
    ("industry", StringType()),
    ("hq_region", StringType()),
    ("plan_tier", StringType()),
    ("is_enterprise", BooleanType()),
    ("signup_date", DateType()),
    ("sales_rep", StringType()),
    ("lifecycle_stage", StringType()),
    ("marketing_source", StringType()),
    ("nps_score", DoubleType()),
)

USERS = _schema(
    ("user_id", StringType()),
    ("org_id", StringType()),
    ("email", StringType()),
    ("role", StringType()),
    ("active", BooleanType()),
    ("created_at", DateType()),
    ("last_login", DateType()),
)

RESOURCES = _schema(
    ("resource_id", StringType()),
    ("org_id", StringType()),
    ("service", StringType()),
    ("region", StringType()),
    ("created_at", DateType()),
    ("state", StringType()),
    ("tags_json", StringType()),
)

BILLING_MONTHLY = _schema(
    ("invoice_id", StringType()),
    ("org_id", StringType()),
    ("month", DateType()),
    ("subtotal", DoubleType()),
    ("credits", DoubleType()),
    ("taxes", DoubleType()),
    ("currency", StringType()),
    ("exchange_rate_to_usd", DoubleType()),
)

SUPPORT_TICKETS = _schema(
    ("ticket_id", StringType()),
    ("org_id", StringType()),
    ("category", StringType()),
    ("severity", StringType()),
    ("created_at", DateType()),
    ("resolved_at", DateType()),
    ("csat", DoubleType()),
    ("sla_breached", BooleanType()),
)

MARKETING_TOUCHES = _schema(
    ("touch_id", StringType()),
    ("org_id", StringType()),
    ("campaign", StringType()),
    ("channel", StringType()),
    ("timestamp", DateType()),   # en el archivo es una fecha, sin hora
    ("clicked", BooleanType()),
    ("converted", BooleanType()),
)

NPS_SURVEYS = _schema(
    ("org_id", StringType()),
    ("survey_date", DateType()),
    ("nps_score", DoubleType()),
    ("comment", StringType()),
)


# Maestros CSV: esquema, clave primaria para deduplicar y columna de partición (D3).
# `nps_surveys` no tiene clave propia: una organización puede responder varias veces.
MASTERS = {
    "customers_orgs":    {"schema": CUSTOMERS_ORGS,    "key": ["org_id"],                 "partition_by": None},
    "users":             {"schema": USERS,             "key": ["user_id"],                "partition_by": None},
    "resources":         {"schema": RESOURCES,         "key": ["resource_id"],            "partition_by": None},
    "billing_monthly":   {"schema": BILLING_MONTHLY,   "key": ["invoice_id"],             "partition_by": "month"},
    "support_tickets":   {"schema": SUPPORT_TICKETS,   "key": ["ticket_id"],              "partition_by": None},
    "marketing_touches": {"schema": MARKETING_TOUCHES, "key": ["touch_id"],               "partition_by": None},
    "nps_surveys":       {"schema": NPS_SURVEYS,       "key": ["org_id", "survey_date"],  "partition_by": None},
}
