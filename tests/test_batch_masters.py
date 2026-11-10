from pathlib import Path

import pytest
from pyspark.sql.utils import AnalysisException

from src.common.config import load_config
from src.common.run_log import read_runs
from src.ingest import batch_masters
from src.ingest.batch_masters import LandingFileMissing

pytestmark = pytest.mark.spark


def test_carga_completa_e_idempotente(spark, cfg):
    first = batch_masters.run(cfg, spark)
    assert {n: r["bronze"] for n, r in first.items()} == {
        "billing_monthly": 240,
        "customers_orgs": 80,
        "marketing_touches": 1500,
        "nps_surveys": 92,
        "resources": 400,
        "support_tickets": 1000,
        "users": 800,
    }
    assert sum(r["landing"] for r in first.values()) == 4112          # O3
    assert all(r["quarantine"] == 0 for r in first.values())

    # D3: billing particionado por mes (3 meses), los demás en un solo archivo sin particionar.
    assert first["billing_monthly"]["bronze_partitions"] == 3
    assert first["users"]["bronze_files"] == 1 and first["users"]["bronze_partitions"] == 0

    users = spark.read.parquet(f"{cfg.zone('bronze')}/users")
    assert {"ingest_ts", "source_file"} <= set(users.columns)
    assert {r.source_file for r in users.select("source_file").distinct().collect()} == {"users.csv"}

    second = batch_masters.run(cfg, spark)                              # O6: re-ejecución
    assert {n: r["bronze"] for n, r in second.items()} == {n: r["bronze"] for n, r in first.items()}
    runs = [r for r in read_runs(cfg) if r["job"] == "bronze_masters"]
    assert [r["status"] for r in runs] == ["ok", "ok"]
    assert runs[-1]["rows_read"] == runs[-1]["rows_written"] == 4112


def _landing(tmp_path: Path, name: str, text: str):
    landing = tmp_path / "landing_fixture"
    landing.mkdir(exist_ok=True)
    (landing / f"{name}.csv").write_text(text, encoding="utf-8")
    return landing


def test_clave_nula_y_repetida_van_a_quarantine(spark, tmp_path):
    landing = _landing(
        tmp_path,
        "users",
        "user_id,org_id,email,role,active,created_at,last_login\n"
        "u1,o1,a@x.com,admin,True,2025-07-01,\n"
        "u2,o1,b@x.com,admin,False,2025-07-02,2025-08-01\n"
        "u1,o2,c@x.com,devops,True,2025-07-03,\n"
        ",o3,d@x.com,devops,True,2025-07-04,\n",
    )
    cfg = load_config("local", overrides={"datalake.root": str(tmp_path / "lake"), "datalake.zones.landing": str(landing)})
    result = batch_masters.run(cfg, spark, sources=["users"])["users"]
    assert (result["landing"], result["bronze"], result["quarantine"]) == (4, 2, 2)

    bronze = spark.read.parquet(f"{cfg.zone('bronze')}/users")
    assert bronze.filter("user_id = 'u1'").first().email == "a@x.com"      # queda la primera aparición
    rules = {r.dq_rule: r["count"] for r in spark.read.parquet(f"{cfg.zone('quarantine')}/users").groupBy("dq_rule").count().collect()}
    assert rules == {"pk_null": 1, "pk_duplicate": 1}


def test_valor_que_no_encaja_en_el_tipo_falla(spark, tmp_path):
    landing = _landing(
        tmp_path,
        "users",
        "user_id,org_id,email,role,active,created_at,last_login\n"
        "u1,o1,a@x.com,admin,quizas,2025-07-01,\n",
    )
    cfg = load_config("local", overrides={"datalake.root": str(tmp_path / "lake"), "datalake.zones.landing": str(landing)})
    with pytest.raises(Exception, match="(?i)malformed|FAILFAST"):
        batch_masters.run(cfg, spark, sources=["users"])
    assert read_runs(cfg)[-1]["status"] == "failed"


def test_cabecera_distinta_al_esquema_falla(spark, tmp_path):
    landing = _landing(
        tmp_path,
        "users",
        "org_id,user_id,email,role,active,created_at,last_login\n"      # columnas invertidas
        "o1,u1,a@x.com,admin,True,2025-07-01,\n",
    )
    cfg = load_config("local", overrides={"datalake.root": str(tmp_path / "lake"), "datalake.zones.landing": str(landing)})
    with pytest.raises((AnalysisException, Exception), match="(?i)header|schema"):
        batch_masters.run(cfg, spark, sources=["users"])


def test_archivo_faltante_da_mensaje_claro(spark, tmp_path):
    cfg = load_config("local", overrides={"datalake.root": str(tmp_path / "lake"), "datalake.zones.landing": str(tmp_path / "vacio")})
    with pytest.raises(LandingFileMissing, match="falta users.csv"):
        batch_masters.run(cfg, spark, sources=["users"])


def test_fuente_desconocida(spark, cfg):
    with pytest.raises(ValueError, match="desconocidas"):
        batch_masters.run(cfg, spark, sources=["clientes"])
