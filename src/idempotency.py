"""Prueba de idempotencia del pipeline completo (O6, consigna §6.2 y §6.4).

    python -m src.idempotency                    # perfil local: Cassandra en Docker
    python -m src.idempotency --profile cloud    # AstraDB

Corre el pipeline tres veces sobre el mismo lake y el mismo serving, y toma una foto (filas por
tabla del lake, duplicados y filas en Cassandra) antes de empezar y después de cada corrida:

| Corrida | Qué hace                                    | Qué demuestra                                         |
|---------|---------------------------------------------|-------------------------------------------------------|
| 1       | `clean` + pipeline                          | La carga desde cero sobre un serving ya poblado       |
| 2       | pipeline sin limpiar                        | Re-ejecución: checkpoint, overwrite dinámico y upserts |
| 3       | `clean` + pipeline                          | Reprocesamiento total: mismo resultado desde Landing   |

Las fotos de las corridas 1, 2 y 3 tienen que ser idénticas. La foto previa es informativa: puede
venir de una corrida anterior. Si algo difiere, el proceso termina con código 1.

Escribe `evidence/entrega2/idempotencia/<provider>.md` y `.json`. No toca Landing ni `_metadata/`.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src import pipeline
from src.common import cassandra
from src.common.config import REPO_ROOT, Config, load_config
from src.common.logs import setup_logging
from src.serving import load_cassandra

log = logging.getLogger("pipeline")

OUT_DIR = REPO_ROOT / "evidence" / "entrega2" / "idempotencia"

# (zona, tabla, columnas que identifican una fila). Duplicados = filas - claves distintas.
KEYS = (
    ("bronze", "usage_events_stream", ["event_id"]),
    ("silver", "usage_events", ["event_id"]),
    ("silver", "usage_daily_features", ["org_id", "usage_date", "service"]),
    ("gold", "org_daily_usage_by_service", ["org_id", "usage_date", "service"]),
    ("gold", "org_service_cost_14d", ["org_id", "service"]),
)
# Tablas de Cassandra con la métrica que se suma para detectar valores distintos con el mismo conteo.
SERVED = (
    ("org_daily_usage_by_service", "daily_cost_usd"),
    ("org_service_cost_14d", "total_cost_usd"),
)


def snapshot(cfg: Config, spark: SparkSession) -> dict:
    """Foto del estado: filas por tabla del lake, duplicados por clave y filas/suma en Cassandra."""
    rows = {f"{r['zone']}.{r['table']}": r["rows"] for r in pipeline.lake_inventory(cfg, spark)}

    duplicates = {}
    for zone, table, key in KEYS:
        path = f"{cfg.zone(zone)}/{table}"
        if not Path(path).is_dir():
            continue
        df = spark.read.parquet(path)
        duplicates[f"{zone}.{table}"] = df.count() - df.select(*key).distinct().count()

    served = {}
    for table, metric in SERVED:
        df = (
            spark.read.format(load_cassandra.CASSANDRA_FORMAT)
            .options(keyspace=cassandra.keyspace(cfg), table=table)
            .load()
        )
        total = df.agg(F.count("*").alias("n"), F.sum(metric).alias("s")).first()
        served[table] = {"rows": total["n"], f"sum_{metric}": round(total["s"] or 0.0, 4)}
    return {"lake_rows": rows, "duplicates": duplicates, "serving": served}


def comparable(snap: dict) -> dict:
    return {"lake_rows": snap["lake_rows"], "duplicates": snap["duplicates"], "serving": snap["serving"]}


def flatten(snap: dict) -> dict[str, object]:
    flat = {f"lake · {k} · filas": v for k, v in snap["lake_rows"].items()}
    flat |= {f"duplicados · {k}": v for k, v in snap["duplicates"].items()}
    for table, values in snap["serving"].items():
        flat |= {f"cassandra · {table} · {k}": v for k, v in values.items()}
    return flat


def render(cfg: Config, snaps: list[tuple[str, dict]], verdicts: list[tuple[str, bool]]) -> str:
    flats = [(name, flatten(s)) for name, s in snaps]
    metrics = list(dict.fromkeys(k for _, f in flats for k in f))
    stamp = datetime.now(timezone.utc).isoformat(timespec="seconds")
    lines = [
        "# Prueba de idempotencia (O6)", "",
        f"Provider `{cassandra.provider(cfg)}` · keyspace `{cassandra.keyspace(cfg)}` · perfil `{cfg.profile}` · {stamp}", "",
        "Generado por `python -m src.idempotency`. Corre el pipeline completo tres veces sobre el mismo "
        "lake y el mismo serving: (1) `clean` + pipeline, (2) pipeline sin limpiar, (3) `clean` + pipeline. "
        "La foto previa es informativa.", "",
        "| Métrica | " + " | ".join(name for name, _ in flats) + " |",
        "|---|" + "---:|" * len(flats),
    ]
    for m in metrics:
        lines.append(f"| {m} | " + " | ".join(str(f.get(m, "—")) for _, f in flats) + " |")
    lines += ["", "## Veredicto", ""]
    lines += [f"- {'OK' if ok else 'FALLA'} · {text}" for text, ok in verdicts]
    return "\n".join(lines) + "\n"


def run(cfg: Config, spark: SparkSession | None = None, out_dir: Path = OUT_DIR) -> bool:
    own = spark is None
    if own:
        from src.common.spark_session import build_spark

        spark = build_spark(cfg, app_name="idempotency", serving=True)
    try:
        snaps = [("Antes", snapshot(cfg, spark))]
        plan = (("Corrida 1 · desde cero", True), ("Corrida 2 · sin limpiar", False), ("Corrida 3 · reproceso total", True))
        for name, clean_first in plan:
            log.info("══ %s", name)
            pipeline.run(cfg, spark, clean_first=clean_first)
            snaps.append((name, snapshot(cfg, spark)))

        first = comparable(snaps[1][1])
        same_2 = comparable(snaps[2][1]) == first
        same_3 = comparable(snaps[3][1]) == first
        no_dups = all(v == 0 for _, s in snaps[1:] for v in s["duplicates"].values())
        verdicts = [
            ("la corrida 2 (sin limpiar) deja las mismas filas que la 1", same_2),
            ("la corrida 3 (reprocesamiento total) deja las mismas filas que la 1", same_3),
            ("0 claves duplicadas en Bronze, Silver, Gold y en el Top-N", no_dups),
        ]
        out_dir.mkdir(parents=True, exist_ok=True)
        stem = out_dir / cassandra.provider(cfg)
        stem.with_suffix(".md").write_text(render(cfg, snaps, verdicts), encoding="utf-8")
        stem.with_suffix(".json").write_text(json.dumps(dict(snaps), indent=2, default=str), encoding="utf-8")
        for text, ok in verdicts:
            log.info("%s · %s", "OK   " if ok else "FALLA", text)
        log.info("evidencia en %s", stem.with_suffix(".md").relative_to(REPO_ROOT))
        return all(ok for _, ok in verdicts)
    finally:
        if own:
            spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--profile", help="local | cloud (por defecto PIPELINE_PROFILE o local)")
    args = parser.parse_args()
    setup_logging()
    sys.exit(0 if run(load_config(args.profile)) else 1)


if __name__ == "__main__":
    main()
