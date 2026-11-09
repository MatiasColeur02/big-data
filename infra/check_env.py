"""Verificación del entorno: bloqueantes B2 y B3 del plan de la segunda entrega.

1. Versiones: Python 3.11, Java 17 y PySpark 3.5 (D17).
2. Spark escribe y lee en Cassandra con el connector. Se escribe dos veces el mismo lote para
   comprobar que el upsert por clave primaria no duplica (base de O6).
3. cassandra-driver hace TRUNCATE, que es lo que necesita la carga del Top-N (D14).

Trabaja sobre una tabla descartable (`env_check`) y la borra al terminar.

    docker compose run --rm spark python infra/check_env.py     # o: make check
"""

import os
import re
import subprocess
import sys
from datetime import date

HOST = os.environ.get("CASSANDRA_HOST", "localhost")
PORT = int(os.environ.get("CASSANDRA_PORT", "9042"))
KEYSPACE = os.environ.get("CASSANDRA_KEYSPACE", "cloud_analytics")
PACKAGE = os.environ.get(
    "SPARK_CASSANDRA_PACKAGE", "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1"
)
TABLE = "env_check"
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


def check_versions():
    print("Versiones")
    py = sys.version_info
    if (py.major, py.minor) != (3, 11):
        fail(f"Python {py.major}.{py.minor}: se requiere 3.11 (PySpark 3.5 no soporta 3.12+ oficialmente)")
    ok(f"Python {py.major}.{py.minor}.{py.micro}")

    try:
        out = subprocess.run(["java", "-version"], capture_output=True, text=True).stderr
    except FileNotFoundError:
        fail("no se encontró `java` en el PATH: se requiere Java 17")
    match = re.search(r'version "(\d+)', out)
    if not match or match.group(1) != "17":
        fail(f"Java {match.group(1) if match else '?'}: se requiere 17")
    ok(f"Java {match.group(1)}")

    import pyspark

    if not pyspark.__version__.startswith("3.5."):
        fail(f"PySpark {pyspark.__version__}: se requiere 3.5.x")
    ok(f"PySpark {pyspark.__version__}")


def connect_driver():
    from cassandra.cluster import Cluster, NoHostAvailable

    cluster = Cluster([HOST], port=PORT)
    try:
        session = cluster.connect()
    except NoHostAvailable:
        fail(
            f"no hay conexión con Cassandra en {HOST}:{PORT}. "
            "¿Está levantada? `docker compose up -d` y esperar a que quede healthy"
        )
    if KEYSPACE not in cluster.metadata.keyspaces:
        fail(f"no existe el keyspace `{KEYSPACE}`: lo crea el servicio cassandra-init al hacer `docker compose up`")
    ok(f"cassandra-driver conectado a {HOST}:{PORT}, keyspace `{KEYSPACE}` presente")
    return cluster, session


def build_spark():
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.master("local[2]")
        .appName("env_check")
        .config("spark.jars.packages", PACKAGE)
        .config("spark.cassandra.connection.host", HOST)
        .config("spark.cassandra.connection.port", str(PORT))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
    )
    if os.environ.get("SPARK_IVY_DIR"):
        builder = builder.config("spark.jars.ivy", os.environ["SPARK_IVY_DIR"])
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def check_cassandra():
    print("Cassandra")
    cluster, session = connect_driver()
    target = f"{KEYSPACE}.{TABLE}"
    session.execute(
        f"CREATE TABLE IF NOT EXISTS {target} ("
        " org_id text, usage_date date, service text, cost_usd double,"
        " PRIMARY KEY ((org_id), usage_date, service)"
        ") WITH CLUSTERING ORDER BY (usage_date DESC, service ASC)"
    )
    spark = None
    try:
        spark = build_spark()
        ok(f"SparkSession con {PACKAGE}")

        df = spark.createDataFrame(
            ROWS, "org_id string, usage_date date, service string, cost_usd double"
        )
        cassandra = {"keyspace": KEYSPACE, "table": TABLE}
        for _ in range(2):
            df.write.format("org.apache.spark.sql.cassandra").options(**cassandra).mode("append").save()

        read = spark.read.format("org.apache.spark.sql.cassandra").options(**cassandra).load()
        n = read.count()
        if n != len(ROWS):
            fail(f"se escribieron {len(ROWS)} filas dos veces y se leyeron {n}: el upsert duplicó")
        ok(f"connector: 2 escrituras de {len(ROWS)} filas, {n} leídas (upsert por clave primaria)")

        session.execute(f"TRUNCATE {target}")
        n = session.execute(f"SELECT count(*) FROM {target}").one()[0]
        if n != 0:
            fail(f"después del TRUNCATE quedan {n} filas")
        ok("cassandra-driver: TRUNCATE deja la tabla vacía (D14)")
    finally:
        if spark is not None:
            spark.stop()
        session.execute(f"DROP TABLE IF EXISTS {target}")
        cluster.shutdown()


if __name__ == "__main__":
    check_versions()
    check_cassandra()
    print("Entorno listo.")
