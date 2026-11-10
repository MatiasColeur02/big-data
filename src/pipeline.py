"""Orquestador del pipeline: Landing → Bronze → Silver → Gold → Serving en una sola corrida (2.8).

    python -m src.pipeline                        # todas las etapas, en orden
    python -m src.pipeline --clean                # borra las zonas generadas y corre todo de cero
    python -m src.pipeline --clean-only           # solo limpia (con --all borra también _metadata)
    python -m src.pipeline --steps silver,gold    # solo esas etapas (siempre en el orden del pipeline)
    python -m src.pipeline --skip serving         # todas menos serving
    python -m src.pipeline --with-sandbox         # agrega el sandbox de calidad y late data (D20)

El notebook `notebooks/02_pipeline.ipynb` llama a estas mismas funciones: acá vive la lógica y el
notebook solo importa y llama (D17).

| Etapa            | Qué hace                                                      | Módulo                     |
|------------------|---------------------------------------------------------------|----------------------------|
| `bronze_masters` | 7 maestros CSV → Bronze, PK nula/repetida → Quarantine        | `src.ingest.batch_masters` |
| `bronze_stream`  | `usage_events_stream/*.jsonl` → Bronze con Structured Streaming | `src.ingest.stream_events` |
| `quality`        | Reglas de eventos → Quarantine + evidencia                    | `src.quality.events`       |
| `silver`         | Dimensiones, eventos conformados y features diarias           | `src.silver.run`           |
| `gold`           | Mart diario, Top-N 14 días y anomalías MAD                    | `src.gold.run`             |
| `serving`        | Gold → Cassandra/AstraDB y consultas P1/P2 + evidencia        | `src.serving.load_cassandra` |
| `sandbox`        | Fixtures de calidad y late data en un lake aparte (opcional)  | `src.quality.sandbox`      |

Todas las etapas comparten una SparkSession. Cada una deja su línea en el registro de corridas
(D11) y la corrida completa deja otra con `job="pipeline"`. Si una etapa falla, el pipeline se
detiene ahí y propaga el error: las siguientes dependen de su salida.

**Limpieza (D13):** `clean` borra juntas Bronze, Silver, Gold, Quarantine y los checkpoints. Nunca
toca Landing. El registro de corridas se conserva salvo que se pida `include_metadata=True`, porque
es la evidencia de idempotencia (O6). Cassandra no se limpia: la carga es upsert por clave natural
y el Top-N hace `TRUNCATE` antes de cargar (D14), así que recargar no duplica.
"""

from __future__ import annotations

import argparse
import logging
import shutil
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from pyspark.sql import SparkSession

from src.common.config import REPO_ROOT, Config, ConfigError, load_config
from src.common.lake import path_stats
from src.common.logs import setup_logging
from src.common.run_log import track

log = logging.getLogger("pipeline")

EVIDENCE = REPO_ROOT / "evidence" / "entrega2"
# Zonas que genera el pipeline y que `clean` borra juntas (D13). Landing nunca está acá.
GENERATED_ZONES = ("bronze", "silver", "gold", "quarantine", "checkpoints")
# Zonas con tablas Parquet, en el orden en que se promueven los datos.
TABLE_ZONES = ("bronze", "quarantine", "silver", "gold")


@dataclass(frozen=True)
class Step:
    name: str
    description: str
    run: Callable[[Config, SparkSession], dict]
    default: bool = True
    needs_connector: bool = False


# --- Etapas -----------------------------------------------------------------------------------
# Imports dentro de cada función: importar el orquestador no carga todos los jobs ni pide
# cassandra-driver si no se corre el serving.

def _bronze_masters(cfg: Config, spark: SparkSession) -> dict:
    from src.ingest import batch_masters

    return batch_masters.run(cfg, spark)


def _bronze_stream(cfg: Config, spark: SparkSession) -> dict:
    from src.ingest import stream_events

    return stream_events.run(cfg, spark)


def _quality(cfg: Config, spark: SparkSession) -> dict:
    from src.quality import events

    return events.run(cfg, spark, evidence_dir=EVIDENCE / "quality")


def _silver(cfg: Config, spark: SparkSession) -> dict:
    from src.silver import run as silver

    return silver.run(cfg, spark)


def _gold(cfg: Config, spark: SparkSession) -> dict:
    from src.gold import run as gold

    return gold.run(cfg, spark, evidence_dir=EVIDENCE / "gold")


def _serving(cfg: Config, spark: SparkSession) -> dict:
    from src.serving import load_cassandra

    return load_cassandra.run(cfg, spark, evidence_dir=EVIDENCE / "serving")


def _sandbox(cfg: Config, spark: SparkSession) -> dict:
    from src.quality import sandbox

    # Usa su propio lake (data/sandbox); el lake principal no se toca (D20).
    return sandbox.run(spark=spark)


STEPS: dict[str, Step] = {
    s.name: s
    for s in (
        Step("bronze_masters", "Maestros CSV → Bronze", _bronze_masters),
        Step("bronze_stream", "Eventos JSONL → Bronze (streaming)", _bronze_stream),
        Step("quality", "Reglas de calidad → Quarantine", _quality),
        Step("silver", "Bronze → Silver", _silver),
        Step("gold", "Silver → Gold", _gold),
        Step("serving", "Gold → Cassandra/AstraDB", _serving, needs_connector=True),
        Step("sandbox", "Fixtures de calidad y late data (D20)", _sandbox, default=False),
    )
}


# --- Selección de etapas ----------------------------------------------------------------------

def select_steps(
    steps: list[str] | None = None,
    skip: list[str] | None = None,
    with_sandbox: bool = False,
) -> list[str]:
    """Devuelve las etapas a correr, siempre en el orden del pipeline."""
    unknown = sorted((set(steps or []) | set(skip or [])) - set(STEPS))
    if unknown:
        raise ValueError(f"etapas desconocidas: {', '.join(unknown)}. Válidas: {', '.join(STEPS)}")
    wanted = set(steps) if steps else {n for n, s in STEPS.items() if s.default}
    if with_sandbox:
        wanted.add("sandbox")
    wanted -= set(skip or [])
    return [name for name in STEPS if name in wanted]


def needs_connector(steps: list[str]) -> bool:
    """La SparkSession lleva el connector de Cassandra solo si se va a cargar el serving: así el
    resto del pipeline corre sin Cassandra ni credenciales de Astra (`--skip serving`)."""
    return any(STEPS[s].needs_connector for s in steps)


# --- Ejecución --------------------------------------------------------------------------------

def run_step(name: str, cfg: Config, spark: SparkSession) -> dict:
    """Corre una etapa y devuelve `{step, status, duration_s, result}`. Si falla, propaga el error."""
    if name not in STEPS:
        raise ValueError(f"etapa desconocida `{name}`. Válidas: {', '.join(STEPS)}")
    step = STEPS[name]
    log.info("━━ %s · %s", name, step.description)
    start = time.monotonic()
    try:
        result = step.run(cfg, spark)
    except Exception:
        log.error("  %s falló después de %.1f s; el pipeline se detiene acá", name, time.monotonic() - start)
        raise
    return {"step": name, "status": "ok", "duration_s": round(time.monotonic() - start, 2), "result": result}


def run(
    cfg: Config,
    spark: SparkSession | None = None,
    steps: list[str] | None = None,
    skip: list[str] | None = None,
    with_sandbox: bool = False,
    clean_first: bool = False,
) -> list[dict]:
    """Corre las etapas en orden con una sola SparkSession. Devuelve un resumen por etapa."""
    selected = select_steps(steps, skip, with_sandbox)
    if clean_first:
        clean(cfg)
    own_session = spark is None
    if own_session:
        from src.common.spark_session import build_spark

        spark = build_spark(cfg, app_name="pipeline", serving=needs_connector(selected))

    summary: list[dict] = []
    try:
        with track(cfg, job="pipeline", source="landing", target=selected[-1] if selected else "-") as job:
            job.extra["steps"] = selected
            for name in selected:
                summary.append(run_step(name, cfg, spark))
            job.extra["summary"] = [{k: v for k, v in s.items() if k != "result"} for s in summary]
        log.info("━━ pipeline terminado: %s",
                 " · ".join(f"{s['step']} {s['status']} ({s['duration_s']:.0f} s)" for s in summary))
    finally:
        if own_session:
            spark.stop()
    return summary


# --- Limpieza y reinicio ----------------------------------------------------------------------

def clean(cfg: Config, include_metadata: bool = False) -> list[str]:
    """Borra juntas las zonas generadas y los checkpoints (D13). Devuelve las rutas borradas.

    Antes de borrar verifica que cada ruta quede dentro de `datalake.root` y fuera de Landing: una
    config mal escrita no puede terminar borrando los datos crudos ni algo fuera del lake.
    """
    root = Path(cfg.get("datalake.root")).resolve()
    landing = Path(cfg.zone("landing")).resolve()
    zones = list(GENERATED_ZONES) + (["metadata"] if include_metadata else [])

    targets = []
    for zone in zones:
        path = Path(cfg.zone(zone)).resolve()
        if path == root or root not in path.parents:
            raise ConfigError(f"la zona `{zone}` ({path}) no está dentro de datalake.root ({root}): no se borra")
        if path == landing or landing in path.parents or path in landing.parents:
            raise ConfigError(f"la zona `{zone}` ({path}) se superpone con Landing: no se borra")
        targets.append(path)

    deleted = []
    for path in targets:
        if path.exists():
            shutil.rmtree(path)
            deleted.append(str(path))
    log.info("limpieza: %s", ", ".join(Path(p).name for p in deleted) if deleted else "no había nada que borrar")
    return deleted


# --- Evidencia --------------------------------------------------------------------------------

def lake_inventory(cfg: Config, spark: SparkSession | None = None) -> list[dict]:
    """Una fila por tabla de Bronze, Quarantine, Silver y Gold: archivos, bytes, particiones y,
    con una SparkSession, filas. Es la tabla de conteos por zona para la prueba de idempotencia (O6)."""
    rows = []
    for zone in TABLE_ZONES:
        base = Path(cfg.zone(zone))
        if not base.is_dir():
            continue
        for table in sorted(p for p in base.iterdir() if p.is_dir() and not p.name.startswith(("_", "."))):
            stats = path_stats(table)
            row = {
                "zone": zone,
                "table": table.name,
                "files": stats["files"],
                "kb": round(stats["bytes"] / 1024, 1),
                "partitions": stats["partitions"],
            }
            if spark is not None:
                # Sin archivos (Quarantine vacía, D20) no hay esquema que leer: son 0 filas.
                row["rows"] = spark.read.parquet(str(table)).count() if stats["files"] else 0
            rows.append(row)
    return rows


def _print_summary(summary: list[dict]) -> None:
    print("\n| Etapa | Estado | Duración (s) |\n|---|---|---|")
    for s in summary:
        print(f"| {s['step']} | {s['status']} | {s['duration_s']:.1f} |")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--profile", help="local | cloud (por defecto PIPELINE_PROFILE o local)")
    parser.add_argument("--steps", help=f"etapas separadas por comas. Válidas: {', '.join(STEPS)}")
    parser.add_argument("--skip", help="etapas a saltear, separadas por comas")
    parser.add_argument("--with-sandbox", action="store_true", help="agrega el sandbox de fixtures (D20)")
    parser.add_argument("--clean", action="store_true", help="borra las zonas generadas y los checkpoints antes de correr")
    parser.add_argument("--clean-only", action="store_true", help="solo limpia y termina")
    parser.add_argument("--all", action="store_true", help="con --clean-only, borra también el registro de corridas")
    args = parser.parse_args()

    setup_logging()
    cfg = load_config(args.profile)
    if args.clean_only:
        clean(cfg, include_metadata=args.all)
        return
    split = lambda value: [s.strip() for s in value.split(",") if s.strip()] if value else None  # noqa: E731
    summary = run(cfg, steps=split(args.steps), skip=split(args.skip),
                  with_sandbox=args.with_sandbox, clean_first=args.clean)
    _print_summary(summary)


if __name__ == "__main__":
    main()
