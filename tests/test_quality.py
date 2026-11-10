import json
from datetime import date, datetime

import pytest
from pyspark.sql import functions as F

from src.quality import events, sandbox
from src.quality.rules import apply_billing_rules, apply_event_rules

pytestmark = pytest.mark.spark

COLUMNS = ("event_id string, event_ts timestamp, event_date date, org_id string, service string, "
           "metric string, value string, unit string, cost_usd_increment double, "
           "ingest_ts timestamp, source_file string")
T0, T1 = datetime(2025, 9, 1, 10), datetime(2025, 9, 1, 11)
D = date(2025, 8, 10)


def row(event_id, value="1.0", unit="hours", cost=1.0, metric="cpu_hours", ingest=T0, source="a.jsonl"):
    return (event_id, datetime(2025, 8, 10, 12), D, "org", "compute", metric, value, unit, cost, ingest, source)


def split(spark, rows, cost_min=-0.01):
    valid, rejected = apply_event_rules(spark.createDataFrame(rows, COLUMNS), cost_min)
    return {r.event_id: r for r in valid.collect()}, rejected.collect()


def test_bloqueos_y_una_sola_regla_por_fila(spark):
    valid, rejected = split(spark, [
        row(None, value="abc"),          # viola event_id_null y value_not_castable: gana la primera
        row("e1", value="1,5"),
        row("e2"),
    ])
    assert {(r.event_id, r.dq_rule) for r in rejected} == {("e1", "value_not_castable"), (None, "event_id_null")}
    assert all(r.rejected_ts is not None and r.event_date == D for r in rejected)   # D16
    assert set(valid) == {"e2"}


def test_duplicado_conserva_el_de_menor_ingest_ts(spark):
    valid, rejected = split(spark, [row("e1", cost=2.0, ingest=T1), row("e1", cost=1.0, ingest=T0)])
    assert valid["e1"].cost_usd_increment == 1.0
    assert [(r.dq_rule, r.cost_usd_increment) for r in rejected] == [("event_id_duplicate", 2.0)]


def test_value_nulo_no_bloquea_y_se_marca(spark):
    """D19: value nulo pasa con su costo; solo bloquea un value no nulo que no castea."""
    valid, rejected = split(spark, [row("e1", value=None, unit=None, cost=3.0)])
    assert rejected == []
    assert valid["e1"].dq_flags == ["value_null"]
    assert valid["e1"].unit is None              # sin value no hay unidad que imputar
    assert valid["e1"].cost_usd_increment == 3.0


def test_marcas_costo_negativo_e_imputacion_de_unit(spark):
    valid, _ = split(spark, [
        row("neg", cost=-5.0),
        row("borde", cost=-0.01),                # en el límite: no es anomalía
        row("u1", unit=None, metric="requests"),
        row("u2", unit=None, metric="storage_gb_hours"),
        row("ok"),
    ])
    assert valid["neg"].dq_flags == ["cost_negative"] and valid["neg"].cost_anomaly
    assert valid["borde"].dq_flags == [] and not valid["borde"].cost_anomaly
    assert (valid["u1"].unit, valid["u1"].unit_raw, valid["u1"].dq_flags) == ("count", None, ["unit_imputed"])
    assert valid["u2"].unit == "gb_hours"
    assert (valid["ok"].unit, valid["ok"].unit_raw, valid["ok"].dq_flags) == ("hours", "hours", [])


def test_fx_usd_forzado_a_uno(spark):
    df = spark.createDataFrame(
        [("i1", "USD", 0.97), ("i2", "USD", 1.0), ("i3", "ARS", 0.0015)],
        "invoice_id string, currency string, exchange_rate_to_usd double",
    )
    out = {r.invoice_id: r for r in apply_billing_rules(df).collect()}
    assert (out["i1"].exchange_rate_to_usd, out["i1"].exchange_rate_raw, out["i1"].dq_flags) == (1.0, 0.97, ["fx_usd_forced"])
    assert out["i2"].dq_flags == []
    assert out["i3"].exchange_rate_to_usd == 0.0015   # ARS es real, no se toca (D7)


def test_balance_detecta_filas_perdidas(spark):
    bronze = spark.createDataFrame([row("e1"), row("e2")], COLUMNS)
    valid = bronze.filter("event_id = 'e1'")
    assert events.check_balance(bronze, valid, None) == [
        {"event_date": "2025-08-10", "bronze": 2, "valid": 1, "quarantine": 0}
    ]


def test_sandbox_reproduce_la_tabla_de_d20(spark, tmp_path):
    s = sandbox.run(root=tmp_path / "sandbox", evidence_dir=tmp_path / "evidence", spark=spark)
    assert s["stream_initial"]["dropped_duplicates"] == 1          # evt_nwlh5boz2guf repetido
    assert s["stream_late"]["input_rows"] == 2
    assert s["stream_late"]["dropped_by_watermark"] == 1           # el del 15/06
    assert s["bronze"]["rows"] == 1800 + 6 + 1

    q = s["quality"]
    assert q["blocked_by_rule"] == {"event_id_null": 1, "event_id_duplicate": 0, "value_not_castable": 2}
    assert q["quarantine"] == 3 and q["valid"] == s["bronze"]["rows"] - 3
    assert q["unbalanced_dates"] == []
    assert q["flagged_by_rule"]["cost_negative"] >= 1 and q["cost_anomalies"] == q["flagged_by_rule"]["cost_negative"]

    report = json.loads((tmp_path / "evidence" / "quality_report.json").read_text())
    assert report["quarantine"] == 3
    samples = (tmp_path / "evidence" / "quarantine_samples.md").read_text()
    assert "fx_value_texto" in samples and "fx_value_coma" in samples


def test_lake_principal_sin_bloqueos(spark, cfg):
    """En el dataset real las reglas BLOQUEA tienen 0 violaciones (D7, O5): Quarantine vacía."""
    from src.ingest import stream_events

    stream_events.run(cfg, spark)
    _, report = events.validate_events(spark, cfg)
    assert report["bronze"] == report["valid"] == 43_200 and report["quarantine"] == 0
    assert report["flagged_by_rule"] == {"cost_negative": 211, "unit_imputed": 2038, "value_null": 877}
    assert report["unbalanced_dates"] == []
    # Re-ejecutar no cambia nada (O6).
    assert events.validate_events(spark, cfg)[1] == report
