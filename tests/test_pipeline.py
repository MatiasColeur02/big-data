"""Orquestador (2.8): selección de etapas, limpieza segura (D13) y corte ante fallas."""

from pathlib import Path

import pytest

from src import pipeline
from src.common.config import ConfigError, load_config
from src.common.run_log import read_runs


def test_default_steps_in_pipeline_order():
    assert pipeline.select_steps() == ["bronze_masters", "bronze_stream", "quality", "silver", "gold", "serving"]


def test_steps_keep_pipeline_order_and_skip():
    assert pipeline.select_steps(steps=["gold", "silver"]) == ["silver", "gold"]
    assert "serving" not in pipeline.select_steps(skip=["serving"])
    assert pipeline.select_steps(with_sandbox=True)[-1] == "sandbox"


def test_unknown_step_is_rejected():
    with pytest.raises(ValueError, match="desconocidas: bronce"):
        pipeline.select_steps(steps=["bronce"])
    with pytest.raises(ValueError, match="desconocidas: x"):
        pipeline.select_steps(skip=["x"])


def _populate(cfg):
    for zone in (*pipeline.GENERATED_ZONES, "metadata"):
        path = Path(cfg.zone(zone)) / "tabla"
        path.mkdir(parents=True)
        (path / "part-0.parquet").write_bytes(b"x")


def test_clean_removes_generated_zones_together_and_keeps_landing(cfg):
    _populate(cfg)
    deleted = pipeline.clean(cfg)
    assert sorted(Path(p).name for p in deleted) == sorted(Path(cfg.zone(z)).name for z in pipeline.GENERATED_ZONES)
    assert all(not Path(cfg.zone(z)).exists() for z in pipeline.GENERATED_ZONES)
    assert Path(cfg.zone("metadata")).exists()           # el registro de corridas se conserva
    assert any(Path(cfg.zone("landing")).iterdir())      # Landing intacta


def test_clean_with_metadata(cfg):
    _populate(cfg)
    pipeline.clean(cfg, include_metadata=True)
    assert not Path(cfg.zone("metadata")).exists()


def test_clean_is_idempotent(cfg):
    assert pipeline.clean(cfg) == []


def test_clean_refuses_zone_outside_lake(tmp_path):
    cfg = load_config("local", overrides={
        "datalake.root": str(tmp_path / "lake"),
        "datalake.zones.gold": str(tmp_path / "otro"),
    })
    with pytest.raises(ConfigError, match="no está dentro de datalake.root"):
        pipeline.clean(cfg)


def test_clean_refuses_zone_overlapping_landing(tmp_path):
    lake = tmp_path / "lake"
    cfg = load_config("local", overrides={
        "datalake.root": str(lake),
        "datalake.zones.landing": str(lake / "landing"),
        "datalake.zones.bronze": str(lake / "landing" / "bronze"),
    })
    with pytest.raises(ConfigError, match="se superpone con Landing"):
        pipeline.clean(cfg)


def test_connector_only_when_serving_runs():
    assert pipeline.needs_connector(pipeline.select_steps()) is True
    assert pipeline.needs_connector(pipeline.select_steps(skip=["serving"])) is False


def test_failed_step_stops_pipeline_and_is_logged(cfg, monkeypatch):
    calls = []

    def boom(cfg, spark):
        raise RuntimeError("falla simulada")

    monkeypatch.setitem(pipeline.STEPS, "bronze_masters",
                        pipeline.Step("bronze_masters", "falla", boom))
    monkeypatch.setitem(pipeline.STEPS, "bronze_stream",
                        pipeline.Step("bronze_stream", "no debería correr", lambda c, s: calls.append(1) or {}))
    with pytest.raises(RuntimeError, match="falla simulada"):
        pipeline.run(cfg, spark=object(), steps=["bronze_masters", "bronze_stream"])
    assert calls == []
    last = read_runs(cfg)[-1]
    assert last["job"] == "pipeline" and last["status"] == "failed"
