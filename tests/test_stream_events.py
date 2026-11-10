"""Bronze streaming sobre un Landing chico armado en un directorio temporal (3 archivos reales).

Cubre D6 (watermark y dedupe), D13 (file sink, checkpoint, estado inconsistente) y D18
(availableNow con varios micro-lotes), y verifica las afirmaciones de D20 sobre late data.
"""

import json
import shutil
from pathlib import Path

import pytest

from src.common.config import load_config
from src.common.run_log import read_runs
from src.ingest import stream_events
from src.ingest.stream_events import InconsistentStreamState

pytestmark = pytest.mark.spark

REAL_FILES = ["events_part_0000.jsonl", "events_part_0001.jsonl", "events_part_0002.jsonl"]


def event(event_id, ts, **extra):
    base = {
        "event_id": event_id, "timestamp": ts, "org_id": "org_test", "resource_id": "res_test",
        "service": "compute", "region": "us-east", "metric": "cpu_hours", "value": 1.0,
        "unit": "hours", "cost_usd_increment": 0.5, "schema_version": 2, "carbon_kg": 0.1,
    }
    return {**base, **extra}


def write_jsonl(path: Path, events: list[dict]) -> None:
    path.write_text("\n".join(json.dumps(e) for e in events) + "\n", encoding="utf-8")


@pytest.fixture
def stream_cfg(tmp_path):
    real = load_config("local")
    source = tmp_path / "landing" / "usage_events_stream"
    source.mkdir(parents=True)
    for name in REAL_FILES:
        shutil.copy(Path(real.get("streaming.source_path")) / name, source / name)
    return load_config(
        "local",
        overrides={
            "datalake.root": str(tmp_path / "lake"),
            "datalake.zones.landing": str(tmp_path / "landing"),
            "streaming.max_files_per_trigger": 1,
        },
    )


def source_dir(cfg) -> Path:
    return Path(cfg.get("streaming.source_path"))


def test_ingesta_inicial_y_reejecucion_idempotente(spark, stream_cfg):
    first = stream_events.run(stream_cfg, spark)
    assert first["stream"]["batches"] == 3                        # 1 archivo por lote
    assert first["bronze"]["rows"] == first["bronze"]["distinct_event_ids"] == 1080
    assert first["stream"]["dropped_by_watermark"] == 0           # D6: 60 días no descarta nada

    bronze = spark.read.parquet(stream_events.bronze_path(stream_cfg))
    assert {"event_ts", "event_date", "ingest_ts", "source_file"} <= set(bronze.columns)
    assert "timestamp" not in bronze.columns
    files = {r.source_file for r in bronze.select("source_file").distinct().collect()}
    assert files == {f"usage_events_stream/{n}" for n in REAL_FILES}
    # D13: un archivo por fecha y por lote como máximo.
    assert first["bronze"]["files"] <= first["bronze"]["dates"] * first["stream"]["batches"]

    second = stream_events.run(stream_cfg, spark)                 # O6: nada nuevo que leer
    assert second["stream"]["input_rows"] == 0
    assert second["bronze"]["rows"] == 1080
    assert [r["status"] for r in read_runs(stream_cfg)] == ["ok", "ok"]


def test_archivo_nuevo_duplicados_y_late_data(spark, stream_cfg):
    stream_events.run(stream_cfg, spark)
    existing = spark.read.parquet(stream_events.bronze_path(stream_cfg)).select("event_id").first().event_id

    write_jsonl(
        source_dir(stream_cfg) / "events_part_9000.jsonl",
        [
            event("evt_nuevo_1", "2025-08-20T10:00:00Z"),
            event(existing, "2025-08-20T10:00:00Z"),          # duplicado de un evento ya ingerido
            event("evt_tarde_ok", "2025-07-15T10:00:00Z"),    # tardío dentro del watermark
            event("evt_tarde_fuera", "2025-06-15T10:00:00Z"), # tardío fuera del watermark (< 02/07)
        ],
    )
    result = stream_events.run(stream_cfg, spark)
    assert result["stream"]["input_rows"] == 4
    assert result["stream"]["dropped_duplicates"] == 1
    assert result["stream"]["dropped_by_watermark"] == 1
    assert result["bronze"]["rows"] == result["bronze"]["distinct_event_ids"] == 1080 + 2

    ids = {r.event_id for r in spark.read.parquet(stream_events.bronze_path(stream_cfg)).select("event_id").collect()}
    assert {"evt_nuevo_1", "evt_tarde_ok"} <= ids
    assert "evt_tarde_fuera" not in ids


def test_event_id_nulos_se_agrupan_como_una_clave(spark, stream_cfg):
    """D20: la deduplicación trata todos los event_id nulos como la misma clave."""
    write_jsonl(
        source_dir(stream_cfg) / "events_part_9001.jsonl",
        [event(None, "2025-08-20T10:00:00Z"), event(None, "2025-08-21T10:00:00Z")],
    )
    stream_events.run(stream_cfg, spark)
    nulls = spark.read.parquet(stream_events.bronze_path(stream_cfg)).filter("event_id IS NULL").count()
    assert nulls == 1


def test_estado_inconsistente_no_arranca(spark, stream_cfg):
    stream_events.run(stream_cfg, spark)
    shutil.rmtree(stream_events.bronze_path(stream_cfg))
    with pytest.raises(InconsistentStreamState, match="--reset"):
        stream_events.run(stream_cfg, spark)

    stream_events.reset(stream_cfg)
    assert stream_events.run(stream_cfg, spark)["bronze"]["rows"] == 1080


def test_directorio_de_eventos_faltante(spark, tmp_path):
    cfg = load_config("local", overrides={"datalake.root": str(tmp_path / "lake"), "datalake.zones.landing": str(tmp_path / "nada")})
    with pytest.raises(FileNotFoundError, match="data/README.md"):
        stream_events.run(cfg, spark)
