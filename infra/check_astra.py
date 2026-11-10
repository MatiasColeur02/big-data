"""Verificación de AstraDB (fase 2, 2.9): la misma prueba que B3 pero contra el perfil cloud.

1. Config cloud válida: bundle existente y `ASTRA_DB_TOKEN` definido.
2. cassandra-driver conecta con el bundle y el keyspace existe (se crea desde la consola de Astra).
3. Spark escribe y lee con el connector; escribir dos veces el mismo lote no duplica (upsert).
4. cassandra-driver hace TRUNCATE (lo necesita D14).

Trabaja sobre una tabla descartable (`astra_check`) y la borra al terminar. Reutiliza `src.common`,
así que prueba exactamente el mismo camino de conexión que usa el pipeline.

    make check-astra        # o: docker compose run --rm spark python infra/check_astra.py
"""

import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.common import cassandra  # noqa: E402
from src.common.config import ConfigError, load_config  # noqa: E402
from src.common.spark_session import build_spark  # noqa: E402

TABLE = "astra_check"
ROWS = [
    ("org_a", date(2025, 8, 31), "compute", 10.0),
    ("org_a", date(2025, 8, 30), "storage", 2.5),
    ("org_b", date(2025, 8, 31), "genai", 7.25),
]


def ok(msg):
    print(f"  OK     {msg}")


def fail(msg):
    print(f"  FALLA  {msg}")
    sys.exit(1)


def main():
    print("AstraDB")
    try:
        cfg = load_config("cloud")
        if cassandra.provider(cfg) != "astradb":
            fail("serving.provider del perfil cloud no es `astradb`")
        cassandra.spark_conf(cfg)   # valida bundle y token antes de abrir nada
        ok("config cloud: bundle encontrado y token definido")
        cluster, session = cassandra.connect(cfg)
    except (ConfigError, ConnectionError) as exc:
        fail(str(exc))

    ks = cassandra.keyspace(cfg)
    target = f"{ks}.{TABLE}"
    ok(f"cassandra-driver conectado a AstraDB, keyspace `{ks}` presente")
    session.execute(
        f"CREATE TABLE IF NOT EXISTS {target} ("
        " org_id text, usage_date date, service text, cost_usd double,"
        " PRIMARY KEY ((org_id), usage_date, service)"
        ") WITH CLUSTERING ORDER BY (usage_date DESC, service ASC)"
    )
    spark = None
    try:
        spark = build_spark(cfg, app_name="astra_check")
        ok("SparkSession con el connector y el bundle")

        df = spark.createDataFrame(
            ROWS, "org_id string, usage_date date, service string, cost_usd double"
        )
        options = {"keyspace": ks, "table": TABLE}
        for _ in range(2):
            df.write.format("org.apache.spark.sql.cassandra").options(**options).mode("append").save()

        n = spark.read.format("org.apache.spark.sql.cassandra").options(**options).load().count()
        if n != len(ROWS):
            fail(f"se escribieron {len(ROWS)} filas dos veces y se leyeron {n}: el upsert duplicó")
        ok(f"connector: 2 escrituras de {len(ROWS)} filas, {n} leídas (upsert por clave primaria)")

        session.execute(f"TRUNCATE {target}")
        n = spark.read.format("org.apache.spark.sql.cassandra").options(**options).load().count()
        if n != 0:
            fail(f"después del TRUNCATE quedan {n} filas")
        ok("cassandra-driver: TRUNCATE deja la tabla vacía (D14)")
    finally:
        if spark is not None:
            spark.stop()
        session.execute(f"DROP TABLE IF EXISTS {target}")
        cluster.shutdown()
    print("AstraDB listo.")


if __name__ == "__main__":
    main()
