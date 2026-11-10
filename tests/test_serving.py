"""Serving (2.7): scripts CQL, alineación DDL ↔ Gold y errores claros antes de conectar."""

import pytest

from src.gold.marts import DAILY_COLUMNS
from src.serving import load_cassandra
from src.serving.cql_scripts import CQL_DIR, statements, table_columns

TABLES = CQL_DIR / "02_tables.cql"
QUERIES = CQL_DIR / "03_queries.cql"


def test_statements_skip_use_and_header(tmp_path):
    script = tmp_path / "x.cql"
    script.write_text(
        "-- cabecera del archivo\n\nUSE ks;\n\n-- P9 · título\n-- detalle\nSELECT * FROM t\nWHERE a = 1;\n",
        encoding="utf-8",
    )
    [stmt] = statements(script)
    assert stmt.title == "P9 · título"
    assert stmt.cql == "SELECT * FROM t\nWHERE a = 1"


def test_tables_script_defines_one_table_per_query():
    assert set(table_columns(TABLES)) == {t.table for t in load_cassandra.TARGETS}
    assert all(s.cql.startswith("CREATE TABLE IF NOT EXISTS") for s in statements(TABLES))


def test_daily_table_matches_gold_mart():
    # Todas las columnas de la tabla salen del mart; si alguien agrega una al DDL o la saca de
    # Gold, este test falla antes que la carga.
    assert set(table_columns(TABLES)["org_daily_usage_by_service"]) == set(DAILY_COLUMNS) | {"processed_ts"}


def test_queries_are_p1_and_p2_without_allow_filtering():
    stmts = statements(QUERIES)
    assert [s.title.split(" ")[0] for s in stmts] == ["P1", "P2"]
    assert all(s.cql.upper().startswith("SELECT") and "ALLOW FILTERING" not in s.cql.upper() for s in stmts)
    assert all("WHERE org_id =" in s.cql for s in stmts)   # una sola partición (D8)


def test_missing_gold_fails_before_connecting(cfg):
    with pytest.raises(load_cassandra.GoldMissing, match="correr antes"):
        load_cassandra.run(cfg, spark=object())


@pytest.mark.spark
def test_prepare_selects_ddl_columns_with_int_types(spark, cfg):
    from datetime import date

    df = spark.createDataFrame(
        [("o1", "compute", 10.0, 5.0, 3, 1, date(2025, 8, 18), date(2025, 8, 31), None, "extra")],
        "org_id string, service string, total_cost_usd double, total_requests double, active_days long, "
        "rank int, window_start date, as_of_date date, processed_ts timestamp, sobra string",
    )
    df.write.parquet(load_cassandra.gold_path(cfg, "org_service_cost_14d"))
    target = load_cassandra.TARGETS[1]
    columns = table_columns(TABLES)[target.table]
    out = load_cassandra.prepare(spark, cfg, target, columns)
    assert out.columns == columns
    assert dict(out.dtypes)["active_days"] == "int"
