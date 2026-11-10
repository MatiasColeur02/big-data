"""Registro de corridas (D11): una línea JSON por job ejecutado en `_metadata/runs/runs.jsonl`.

    with track(cfg, job="bronze_masters", source="landing", target="bronze") as run:
        ...
        run.update(rows_read=4112, rows_written=4112, partitions_written=3)
        run.extra["por_fuente"] = {...}

Si el bloque lanza una excepción, la línea se escribe igual con `status="failed"` y el error, y la
excepción se propaga. Es JSON Lines y no Parquet para leerlo sin Spark y copiarlo a `evidence/`.
"""

from __future__ import annotations

import json
import logging
import time
import uuid
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from src.common.config import Config

log = logging.getLogger("pipeline")


@dataclass
class Run:
    job: str
    source_zone: str
    target_zone: str
    profile: str
    run_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    started_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat(timespec="seconds"))
    duration_s: float | None = None
    status: str = "running"
    error: str | None = None
    rows_read: int | None = None
    rows_written: int | None = None
    rows_rejected: int | None = None
    partitions_written: int | None = None
    extra: dict[str, Any] = field(default_factory=dict)

    def update(self, **values: Any) -> None:
        for key, value in values.items():
            if not hasattr(self, key) or key in ("job", "run_id", "started_at", "status"):
                raise AttributeError(f"`{key}` no es un campo del registro de corridas; usar run.extra")
            setattr(self, key, value)


def runs_file(cfg: Config) -> Path:
    return Path(cfg.zone("metadata")) / "runs" / "runs.jsonl"


@contextmanager
def track(cfg: Config, job: str, source: str, target: str) -> Iterator[Run]:
    run = Run(job=job, source_zone=source, target_zone=target, profile=cfg.profile)
    start = time.monotonic()
    log.info("[%s] %s: %s → %s", run.run_id, job, source, target)
    try:
        yield run
        run.status = "ok"
    except BaseException as exc:
        run.status = "failed"
        run.error = f"{type(exc).__name__}: {exc}"
        raise
    finally:
        run.duration_s = round(time.monotonic() - start, 2)
        _append(runs_file(cfg), asdict(run))
        log.info(
            "[%s] %s %s en %.1f s · leídas=%s escritas=%s rechazadas=%s",
            run.run_id, job, run.status, run.duration_s, run.rows_read, run.rows_written, run.rows_rejected,
        )


def read_runs(cfg: Config) -> list[dict]:
    path = runs_file(cfg)
    if not path.is_file():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _append(path: Path, record: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")
