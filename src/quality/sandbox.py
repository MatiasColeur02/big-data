"""Sandbox de calidad y late data (D20): mismo código que el pipeline, otro lake.

    python -m src.quality.sandbox

1. Arma `data/sandbox/landing/usage_events_stream/` con 5 archivos reales y `fixtures_calidad.jsonl`.
2. Corre el stream (availableNow): el watermark queda en 31/08 − 60 días.
3. Agrega `fixtures_tardios.jsonl` y vuelve a correr el stream: el archivo nuevo se evalúa contra
   el watermark guardado en el checkpoint.
4. Corre las reglas de calidad y exporta la evidencia a `evidence/entrega2/quality_sandbox/`.

Cada ejecución borra el sandbox y empieza de cero. El lake principal no se toca.
"""

from __future__ import annotations

import logging
import shutil
from pathlib import Path

from src.common.config import REPO_ROOT, Config, load_config
from src.common.logs import setup_logging
from src.ingest import stream_events
from src.quality import events

log = logging.getLogger("pipeline")

SANDBOX_ROOT = REPO_ROOT / "data" / "sandbox"
FIXTURES = REPO_ROOT / "data" / "fixtures"
REAL_FILES = 5
EVIDENCE = REPO_ROOT / "evidence" / "entrega2" / "quality_sandbox"


def sandbox_config(root: Path = SANDBOX_ROOT) -> Config:
    return load_config(overrides={"datalake.root": str(root)})


def build_landing(cfg: Config, real: Config) -> Path:
    source = Path(cfg.get("streaming.source_path"))
    source.mkdir(parents=True, exist_ok=True)
    for path in sorted(Path(real.get("streaming.source_path")).glob("*.jsonl"))[:REAL_FILES]:
        shutil.copy(path, source / path.name)
    # copy (no copy2): mtime nuevo, así el file source los ve después de los reales.
    shutil.copy(FIXTURES / "fixtures_calidad.jsonl", source / "fixtures_calidad.jsonl")
    return source


def run(root: Path = SANDBOX_ROOT, evidence_dir: Path = EVIDENCE, spark=None) -> dict:
    shutil.rmtree(root, ignore_errors=True)
    cfg = sandbox_config(root)
    source = build_landing(cfg, load_config())
    if spark is None:
        from src.common.spark_session import build_spark

        spark = build_spark(cfg, app_name="quality_sandbox", serving=False)

    log.info("Sandbox · corrida 1: %d archivos reales + fixtures de calidad", REAL_FILES)
    first = stream_events.run(cfg, spark)
    log.info("Sandbox · corrida 2: llegan los eventos tardíos")
    shutil.copy(FIXTURES / "fixtures_tardios.jsonl", source / "fixtures_tardios.jsonl")
    late = stream_events.run(cfg, spark)
    log.info("Sandbox · reglas de calidad")
    quality = events.run(cfg, spark, evidence_dir=evidence_dir)

    summary = {
        "stream_initial": first["stream"],
        "stream_late": late["stream"],
        "bronze": late["bronze"],
        "quality": quality,
    }
    _write_summary(evidence_dir, summary)
    return summary


def _write_summary(out: Path, s: dict) -> None:
    lines = [
        "# Sandbox de calidad y late data (D20)", "",
        f"Generado por `python -m src.quality.sandbox`: {REAL_FILES} archivos reales + `data/fixtures/`.", "",
        "| Paso | Resultado |", "|---|---|",
        f"| Corrida 1 del stream | {s['stream_initial']['input_rows']} filas leídas, "
        f"{s['stream_initial']['dropped_duplicates']} duplicado(s) descartado(s), watermark `{s['stream_initial']['watermark']}` |",
        f"| Corrida 2 (tardíos) | {s['stream_late']['input_rows']} filas leídas, "
        f"{s['stream_late']['dropped_by_watermark']} descartada(s) por watermark |",
        f"| Bronze | {s['bronze']['rows']} filas, {s['bronze']['distinct_event_ids']} `event_id` distintos |",
        f"| Calidad | {s['quality']['valid']} válidos, {s['quality']['quarantine']} en Quarantine |",
        f"| Bloqueos por regla | {s['quality']['blocked_by_rule']} |",
        f"| Marcas por regla | {s['quality']['flagged_by_rule']} |",
        f"| Fechas sin balance (O4) | {len(s['quality']['unbalanced_dates'])} |",
        "", "Muestras: [`quarantine_samples.md`](quarantine_samples.md) · conteos: [`quality_report.json`](quality_report.json)",
    ]
    (out / "README.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


if __name__ == "__main__":
    setup_logging()
    run()
