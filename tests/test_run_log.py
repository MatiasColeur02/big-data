import pytest

from src.common.run_log import read_runs, runs_file, track


def test_corrida_exitosa_escribe_una_linea(cfg):
    with track(cfg, job="job_ok", source="landing", target="bronze") as run:
        run.update(rows_read=10, rows_written=9, rows_rejected=1, partitions_written=2)
        run.extra["detalle"] = {"a": 1}
    runs = read_runs(cfg)
    assert len(runs) == 1
    r = runs[0]
    assert r["status"] == "ok" and r["job"] == "job_ok"
    assert (r["rows_read"], r["rows_written"], r["rows_rejected"]) == (10, 9, 1)
    assert r["extra"] == {"detalle": {"a": 1}}
    assert r["duration_s"] >= 0
    assert runs_file(cfg).parent.name == "runs"


def test_corrida_fallida_se_registra_y_propaga(cfg):
    with pytest.raises(ValueError):
        with track(cfg, job="job_falla", source="bronze", target="silver"):
            raise ValueError("archivo faltante")
    r = read_runs(cfg)[-1]
    assert r["status"] == "failed"
    assert r["error"] == "ValueError: archivo faltante"


def test_corridas_se_acumulan(cfg):
    for i in range(3):
        with track(cfg, job=f"j{i}", source="a", target="b"):
            pass
    assert [r["job"] for r in read_runs(cfg)] == ["j0", "j1", "j2"]


def test_campo_desconocido(cfg):
    with pytest.raises(AttributeError, match="run.extra"):
        with track(cfg, job="j", source="a", target="b") as run:
            run.update(filas=1)
