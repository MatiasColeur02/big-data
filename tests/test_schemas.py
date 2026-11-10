"""Los esquemas explícitos leen el dataset real completo sin perder ni convertir nada en silencio.

Se lee en modo FAILFAST: si un valor no encaja en el tipo declarado, Spark falla en vez de
dejarlo en null (el problema que describe D4).
"""

import pytest
from pyspark.sql import functions as F

from src.common import schemas
from src.ingest.batch_masters import read_master

pytestmark = pytest.mark.spark

EXPECTED_ROWS = {
    "customers_orgs": 80,
    "users": 800,
    "resources": 400,
    "billing_monthly": 240,
    "support_tickets": 1000,
    "marketing_touches": 1500,
    "nps_surveys": 92,
}


@pytest.mark.parametrize("name", sorted(schemas.MASTERS))
def test_maestro_lee_completo_con_su_esquema(spark, cfg, name):
    spec = schemas.MASTERS[name]
    df = read_master(spark, cfg, name)
    # count() solo no alcanza: Spark poda las columnas y no las parsea. Contar cada columna obliga a
    # parsear todas, que es cuando FAILFAST detecta un valor o una fila mal formados.
    counts = df.select(F.count(F.lit(1)).alias("_filas"), *[F.count(c).alias(c) for c in df.columns]).first()
    assert counts["_filas"] == EXPECTED_ROWS[name]
    assert df.select(spec["key"]).distinct().count() == EXPECTED_ROWS[name]   # 0 duplicados de PK


def test_comillas_escapadas_rfc4180_en_resources(spark, cfg):
    df = read_master(spark, cfg, "resources")
    tags = df.filter(F.col("tags_json").isNotNull())
    assert tags.count() == 317
    # Todas las etiquetas quedan como un array JSON válido (con varias etiquetas incluidas).
    assert tags.filter(F.from_json("tags_json", "array<string>").isNull()).count() == 0
    assert df.filter(F.col("state").isin("running", "stopped", "terminated")).count() == 400


def test_booleanos_y_nulos_de_csv(spark, cfg):
    df = read_master(spark, cfg, "billing_monthly")
    assert df.filter(F.col("credits").isNull()).count() == 137   # vacío → null (57 %)
    users = read_master(spark, cfg, "users")
    assert users.filter(F.col("active")).count() == 728          # "True" → true


def test_eventos_con_esquema_unificado_v1_v2(spark, cfg):
    df = (
        spark.read.option("mode", "FAILFAST").schema(schemas.USAGE_EVENTS)
        .json(cfg.get("streaming.source_path"))
    ).cache()
    stats = df.agg(
        F.count("*").alias("filas"),
        F.countDistinct("event_id").alias("ids"),
        F.count("timestamp").alias("ts"),
        F.count("value").alias("value"),
        F.count("carbon_kg").alias("carbon"),
        F.count("genai_tokens").alias("tokens"),
        F.countDistinct(F.to_date("timestamp")).alias("fechas"),
        F.min(F.to_date("timestamp")).alias("desde"),
        F.max(F.to_date("timestamp")).alias("hasta"),
    ).first()
    assert stats["filas"] == stats["ids"] == stats["ts"] == 43_200
    assert stats["value"] == 43_200 - 877          # D4: los 1.309 strings numéricos no se pierden
    assert stats["carbon"] == 32_400               # solo v2 (D5)
    assert stats["tokens"] == 3_132                # solo genai en v2 (D5)
    # D15: con la sesión en UTC hay exactamente 60 fechas, del 03/07 al 31/08.
    assert stats["fechas"] == 60
    assert (str(stats["desde"]), str(stats["hasta"])) == ("2025-07-03", "2025-08-31")
    # value se conserva tal cual llega: número serializado o string numérico.
    assert df.filter(F.col("value").isNotNull() & F.expr("try_cast(value AS DOUBLE)").isNull()).count() == 0
    df.unpersist()
