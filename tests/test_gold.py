from datetime import date, timedelta

import pytest

from src.common.config import load_config
from src.gold import run as gold
from src.gold.anomalies import cost_anomalies
from src.gold.marts import org_service_cost_14d, resolve_as_of_date
from src.ingest import batch_masters, stream_events
from src.silver import run as silver

pytestmark = pytest.mark.spark

FEATURES = "org_id string, usage_date date, service string, daily_cost_usd double, requests double, cost_anomaly_events int"
START = date(2025, 8, 1)


def days(org, service, costs, requests=1.0):
    return [(org, START + timedelta(days=i), service, float(c), requests, 0) for i, c in enumerate(costs)]


def test_mad_detecta_el_spike_y_no_el_ruido(spark):
    costs = [10, 11, 9, 10, 12, 10, 9, 11, 10, 200]                # un spike
    out = {r.usage_date: r for r in cost_anomalies(spark.createDataFrame(days("o", "compute", costs), FEATURES)).collect()}
    flagged = [d for d, r in out.items() if r.is_anomaly]
    assert flagged == [START + timedelta(days=9)]
    spike = out[START + timedelta(days=9)]
    assert spike.method == "mad" and spike.median_cost_usd == 10.0 and spike.mad_cost_usd == 1.0
    assert spike.anomaly_score == pytest.approx(0.6745 * 190)


def test_mad_cero_cae_a_p99(spark):
    costs = [5.0] * 9 + [50.0]                                     # MAD = 0
    rows = cost_anomalies(spark.createDataFrame(days("o", "storage", costs), FEATURES)).collect()
    assert {r.method for r in rows} == {"p99"}
    assert [r.daily_cost_usd for r in rows if r.is_anomaly] == [50.0]
    assert all(r.anomaly_score is None for r in rows)


def test_top_n_ventana_de_14_dias_inclusiva(spark):
    rows = days("o1", "compute", [1.0] * 20) + days("o1", "genai", [5.0] * 20) + days("o2", "db", [2.0] * 20)
    df = spark.createDataFrame(rows, FEATURES)
    as_of = resolve_as_of_date(df, None)
    assert as_of == START + timedelta(days=19)                    # D15: máximo de los datos
    top = {(r.org_id, r.service): r for r in org_service_cost_14d(df, as_of).collect()}
    assert top[("o1", "genai")].total_cost_usd == 70.0 and top[("o1", "genai")].rank == 1
    assert top[("o1", "compute")].total_cost_usd == 14.0 and top[("o1", "compute")].rank == 2
    assert top[("o1", "compute")].window_start == as_of - timedelta(days=13)
    assert resolve_as_of_date(df, "2025-08-05") == date(2025, 8, 5)   # valor fijo de la config


def test_gold_completo_sobre_el_dataset_real(spark, cfg, tmp_path):
    batch_masters.run(cfg, spark)
    stream_events.run(cfg, spark)
    silver.run(cfg, spark)
    first = gold.run(cfg, spark, evidence_dir=tmp_path / "ev")

    assert first["as_of_date"] == "2025-08-31"
    assert first["rows"] == {"org_daily_usage_by_service": 11_050, "org_service_cost_14d": 262, "cost_anomaly_mart": 11_050}
    assert 0 < first["anomalies"]["anomalies"] < first["anomalies"]["evaluated"]
    top = spark.read.parquet(gold.mart_path(cfg, "org_service_cost_14d"))
    assert top.filter("rank = 1").count() == 80                     # un primer puesto por org
    assert (tmp_path / "ev" / "anomalies.md").is_file()

    second = gold.run(cfg, spark)                                   # O6
    assert second["rows"] == first["rows"] and second["anomalies"] == first["anomalies"]


def test_as_of_date_de_la_config(spark, cfg):
    fixed = load_config("local", overrides={"datalake.root": cfg.get("datalake.root"),
                                            "datalake.zones.landing": cfg.zone("landing"),
                                            "analysis.as_of_date": "2025-08-15"})
    batch_masters.run(fixed, spark)
    stream_events.run(fixed, spark)
    silver.run(fixed, spark)
    assert gold.run(fixed, spark)["as_of_date"] == "2025-08-15"
