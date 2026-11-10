from datetime import date, datetime

import pytest
from pyspark.sql import functions as F

from src.ingest import batch_masters, stream_events
from src.silver import run as silver
from src.silver.conform_events import conform, enrich
from src.silver.dims import build_dim_org
from src.silver.features import daily_features

pytestmark = pytest.mark.spark

VALID = ("event_id string, event_ts timestamp, event_date date, org_id string, resource_id string, "
         "service string, region string, metric string, value string, unit string, "
         "cost_usd_increment double, genai_tokens long, carbon_kg double, dq_flags array<string>, cost_anomaly boolean")
D = date(2025, 8, 10)


def ev(eid, metric="requests", value="10", cost=1.0, org="o1", res="r1", service="Compute ", tokens=None, carbon=None):
    return (eid, datetime(2025, 8, 10, 12), D, org, res, service, "us-east", metric, value, "count",
            cost, tokens, carbon, [], cost < -0.01)


def test_conform_castea_value_y_separa_metricas(spark):
    df = conform(spark.createDataFrame([ev("e1", value="12.5"), ev("e2", metric="cpu_hours", value=None)], VALID))
    rows = {r.event_id: r for r in df.collect()}
    assert (rows["e1"].value, rows["e1"].value_raw, rows["e1"].requests, rows["e1"].cpu_hours) == (12.5, "12.5", 12.5, None)
    assert rows["e1"].service == "compute"                      # normalizado
    assert rows["e2"].value is None and rows["e2"].cpu_hours is None


def test_enrich_marca_huerfanos_y_no_pierde_filas(spark):
    events = conform(spark.createDataFrame([ev("e1"), ev("e2", org="o_x", res="r_x"), ev("e3", service="storage")], VALID))
    dim_org = spark.createDataFrame([("o1", "Org 1", "tech", "pro", True, "us-east")],
                                    "org_id string, org_name string, industry string, plan_tier string, is_enterprise boolean, hq_region string")
    dim_res = spark.createDataFrame([("r1", "o1", "compute", "us-east", "running")],
                                    "resource_id string, org_id string, service string, region string, state string")
    rows = {r.event_id: r for r in enrich(events, dim_org, dim_res).collect()}
    assert len(rows) == 3
    assert rows["e1"].dq_flags == [] and rows["e1"].org_name == "Org 1" and rows["e1"].resource_state == "running"
    assert rows["e2"].dq_flags == ["orphan_org", "orphan_resource"] and rows["e2"].orphan_org
    assert rows["e3"].dq_flags == ["resource_mismatch"]          # servicio distinto al del recurso


def test_features_distinguen_cero_de_no_medido(spark):
    events = conform(spark.createDataFrame([
        ev("a1", value="10", cost=1.0), ev("a2", value="5", cost=-2.0),          # grupo compute
        ev("b1", metric="cpu_hours", value="3", service="db"),                  # db: sin eventos de requests
        ev("c1", value=None, service="net"),                                     # net: requests no medido
        ev("g1", metric="requests", value="1", service="genai", tokens=100, carbon=0.5),
        ev("g2", metric="requests", value="1", service="genai", tokens=50, carbon=0.25),
    ], VALID))
    f = {r.service: r for r in daily_features(events).collect()}
    assert (f["compute"].daily_cost_usd, f["compute"].requests, f["compute"].cost_anomaly_events) == (-1.0, 15.0, 1)
    assert f["db"].requests == 0.0 and f["db"].cpu_hours == 3.0
    assert f["net"].requests is None and f["net"].value_null_events == 1      # D19
    assert (f["genai"].genai_tokens, f["genai"].carbon_kg) == (150, 0.75)
    assert f["compute"].genai_tokens is None                                  # no aplica
    assert f["compute"].usage_date == D and f["compute"].events == 2


def test_dim_org_anula_nps_fuera_de_rango(spark):
    df = spark.createDataFrame([("o1", " Org ", " Tech", "PRO", 101.0), ("o2", "B", "x", "free", -3.0)],
                               "org_id string, org_name string, industry string, plan_tier string, nps_score double")
    df = df.withColumn("hq_region", F.lit("us-east")).withColumn("sales_rep", F.lit("rep_a")) \
           .withColumn("lifecycle_stage", F.lit("active")).withColumn("marketing_source", F.lit("ads"))
    rows = {r.org_id: r for r in build_dim_org(df, (-100, 100)).collect()}
    assert (rows["o1"].nps_score, rows["o1"].nps_score_raw, rows["o1"].dq_flags) == (None, 101.0, ["nps_out_of_range"])
    assert (rows["o1"].org_name, rows["o1"].industry, rows["o1"].plan_tier) == ("Org", "tech", "pro")
    assert rows["o2"].nps_score == -3.0 and rows["o2"].dq_flags == []


def test_silver_completo_sobre_el_dataset_real(spark, cfg):
    batch_masters.run(cfg, spark)
    stream_events.run(cfg, spark)
    first = silver.run(cfg, spark)

    assert first["dims"] == {"dim_org": 80, "dim_resource": 400}
    ev_ = first["usage_events"]
    assert (ev_["rows"], ev_["dates"], ev_["files"]) == (43_200, 60, 60)     # 1 archivo por fecha (D13)
    assert (ev_["orphan_org"], ev_["orphan_resource"], ev_["resource_mismatch"]) == (0, 0, 0)
    f = first["usage_daily_features"]
    assert f["rows"] == f["grain_keys"] == 11_050                              # grano único (D8)
    assert f["total_cost_usd"] == 147_433.98                                   # ningún costo se pierde
    assert first["unbalanced_dates"] == []                                     # O4: 60 de 60

    nps = spark.read.parquet(f"{cfg.zone('silver')}/dim_org").filter(F.array_contains("dq_flags", "nps_out_of_range"))
    assert nps.count() == 1                                                    # D7: el 101.0

    second = silver.run(cfg, spark)                                            # O6
    assert second["usage_events"] == first["usage_events"]
    assert second["usage_daily_features"] == first["usage_daily_features"]
