"""Serving: Gold → Cassandra/AstraDB con el Spark Cassandra Connector (consigna §6.2, D8, D14, D17).

    python -m src.serving.load_cassandra                 # perfil local: Cassandra en Docker
    python -m src.serving.load_cassandra --profile cloud # AstraDB (bundle + ASTRA_DB_TOKEN)

1. Verifica que existan los marts de Gold antes de abrir cualquier conexión.
2. Aplica `cql/02_tables.cql` con cassandra-driver (idempotente): en Astra no hay cassandra-init.
3. `org_daily_usage_by_service`: upsert por clave natural `(org_id, usage_date, service)`, sin
   escribir nulls (no generan tombstones; ver `write`).
4. `org_service_cost_14d`: materializa el Top-N, hace `TRUNCATE` y recién ahí escribe (D14).
   Si el cálculo falla, la tabla queda como estaba.
5. Verifica `filas en Cassandra == filas en Gold` por tabla, leyendo con el connector.
6. Opcional: corre `cql/03_queries.cql` y guarda la evidencia (`src.serving.queries`).

Es independiente del provider: la conexión sale de `src.common.cassandra` (D17).
Re-ejecutarlo deja las mismas filas (O6).
"""

from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common import cassandra
from src.common.config import REPO_ROOT, Config, load_config
from src.common.lake import path_stats
from src.common.logs import setup_logging
from src.common.run_log import track
from src.serving.cql_scripts import CQL_DIR, statements, table_columns

log = logging.getLogger("pipeline")

CASSANDRA_FORMAT = "org.apache.spark.sql.cassandra"
TABLES_CQL = CQL_DIR / "02_tables.cql"


class GoldMissing(FileNotFoundError):
    pass


class LoadMismatch(RuntimeError):
    pass


@dataclass(frozen=True)
class Target:
    table: str          # tabla en Cassandra
    mart: str           # mart en Gold
    truncate: bool      # D14: la clave incluye una métrica → TRUNCATE + recarga


TARGETS = (
    Target("org_daily_usage_by_service", "org_daily_usage_by_service", truncate=False),
    Target("org_service_cost_14d", "org_service_cost_14d", truncate=True),
)


def gold_path(cfg: Config, mart: str) -> str:
    return f"{cfg.zone('gold')}/{mart}"


def check_gold(cfg: Config) -> None:
    missing = [t.mart for t in TARGETS if not path_stats(gold_path(cfg, t.mart))["files"]]
    if missing:
        raise GoldMissing(f"no hay datos en Gold para {', '.join(missing)}: correr antes `python -m src.gold.run`")


def apply_schema(session, cfg: Config) -> int:
    """Ejecuta 02_tables.cql en el keyspace de la config. Devuelve cuántas sentencias aplicó."""
    session.set_keyspace(cassandra.keyspace(cfg))
    stmts = statements(TABLES_CQL)
    for stmt in stmts:
        session.execute(stmt.cql)
    return len(stmts)


def prepare(spark: SparkSession, cfg: Config, target: Target, columns: list[str]) -> DataFrame:
    """Lee el mart y deja exactamente las columnas de la tabla, con los tipos enteros del DDL."""
    df = spark.read.parquet(gold_path(cfg, target.mart))
    missing = sorted(set(columns) - set(df.columns))
    if missing:
        raise LoadMismatch(f"{target.mart}: faltan en Gold las columnas {missing} que pide {TABLES_CQL.name}")
    df = df.select(*columns)
    for name in ("active_days", "rank"):   # long/int en Spark → int en CQL
        if name in df.columns:
            df = df.withColumn(name, F.col(name).cast("int"))
    return df


def write(df: DataFrame, cfg: Config, table: str) -> None:
    # ignoreNulls: un null escrito es un tombstone en Cassandra. Las métricas que no aplican
    # (genai_tokens fuera de genai, carbon_kg en v1) generarían miles por carga y degradan las
    # lecturas. Contra: si una métrica pasara de valor a null en un reproceso, quedaría el valor
    # viejo; con Gold determinista no ocurre, y un TRUNCATE de la tabla lo corrige (D14).
    (
        df.write.format(CASSANDRA_FORMAT)
        .options(keyspace=cassandra.keyspace(cfg), table=table)
        .option("spark.cassandra.output.ignoreNulls", "true")
        .mode("append")   # en Cassandra todo INSERT es upsert por clave primaria
        .save()
    )


def count_table(spark: SparkSession, cfg: Config, table: str) -> int:
    # Con el connector y no con `SELECT count(*)`: recorre por rangos de token y no hace timeout en Astra.
    return spark.read.format(CASSANDRA_FORMAT).options(keyspace=cassandra.keyspace(cfg), table=table).load().count()


def load_target(spark: SparkSession, session, cfg: Config, target: Target, columns: list[str]) -> dict:
    df = prepare(spark, cfg, target, columns).cache()
    gold_rows = df.count()   # materializa antes del TRUNCATE (D14)
    if target.truncate:
        session.execute(f"TRUNCATE {target.table}")
    write(df, cfg, target.table)
    df.unpersist()

    served = count_table(spark, cfg, target.table)
    if served != gold_rows:
        hint = "" if target.truncate else " (¿quedaron filas de una carga anterior con otras claves?)"
        raise LoadMismatch(f"{target.table}: gold={gold_rows} != cassandra={served}{hint}")
    log.info("  %-28s gold=%5d cassandra=%5d · %s", target.table, gold_rows, served,
             "TRUNCATE + carga (D14)" if target.truncate else "upsert por clave natural")
    return {"gold": gold_rows, "cassandra": served, "mode": "truncate+load" if target.truncate else "upsert"}


def run(cfg: Config, spark: SparkSession | None = None, evidence_dir: Path | None = None) -> dict:
    check_gold(cfg)
    if spark is None:
        from src.common.spark_session import build_spark

        spark = build_spark(cfg, app_name="serving", serving=True)

    columns = table_columns(TABLES_CQL)
    with track(cfg, job="serving", source="gold", target="serving") as job:
        cluster, session = cassandra.connect(cfg)
        try:
            applied = apply_schema(session, cfg)
            log.info("  %s: esquema aplicado (%d sentencias) en `%s` · provider=%s",
                     TABLES_CQL.name, applied, cassandra.keyspace(cfg), cassandra.provider(cfg))
            tables = {t.table: load_target(spark, session, cfg, t, columns[t.table]) for t in TARGETS}
        finally:
            cluster.shutdown()
        job.update(
            rows_read=sum(t["gold"] for t in tables.values()),
            rows_written=sum(t["cassandra"] for t in tables.values()),
            rows_rejected=0,
        )
        job.extra.update(provider=cassandra.provider(cfg), keyspace=cassandra.keyspace(cfg), tables=tables)

    report = {"provider": cassandra.provider(cfg), "keyspace": cassandra.keyspace(cfg), "tables": tables}
    if evidence_dir:
        from src.serving import queries

        report["queries"] = queries.run(cfg, evidence_dir)
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--profile", help="local | cloud (por defecto PIPELINE_PROFILE o local)")
    parser.add_argument("--evidence", default="evidence/entrega2/serving", help="carpeta de evidencia (relativa al repo)")
    args = parser.parse_args()
    setup_logging()
    run(load_config(args.profile), evidence_dir=REPO_ROOT / args.evidence)


if __name__ == "__main__":
    main()
