"""Consultas P1 y P2 sobre el serving, con evidencia reproducible (consigna §6.2, §7.4).

    python -m src.serving.queries                    # perfil local
    python -m src.serving.queries --profile cloud    # AstraDB

Ejecuta cada sentencia de `cql/03_queries.cql` con cassandra-driver y escribe
`evidence/entrega2/serving/queries.md` con el CQL, la justificación query-first y el resultado.
Usa los mismos archivos que `cqlsh -f` (`make queries`), así la evidencia vale para los dos perfiles.
"""

from __future__ import annotations

import argparse
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from src.common import cassandra
from src.common.config import REPO_ROOT, Config, load_config
from src.common.logs import setup_logging
from src.serving.cql_scripts import CQL_DIR, statements

log = logging.getLogger("pipeline")

QUERIES_CQL = CQL_DIR / "03_queries.cql"

# Por qué cada consulta se resuelve con una sola partición (D8). Indexado por el prefijo del título.
QUERY_FIRST = {
    "P1": "Partition key `org_id` y clustering `usage_date DESC, service`: el rango de fechas es un "
          "slice contiguo dentro de una sola partición. Sin `ALLOW FILTERING` ni índices secundarios.",
    "P2": "Spark pre-agrega la ventana de 14 días y el total va como clustering key `DESC`: "
          "`LIMIT N` devuelve el Top-N ya ordenado desde una sola partición (D8, D14, D15).",
}


def execute(session, cfg: Config) -> list[dict]:
    session.set_keyspace(cassandra.keyspace(cfg))
    results = []
    for stmt in statements(QUERIES_CQL):
        rows = session.execute(stmt.cql)
        frame = pd.DataFrame([r._asdict() for r in rows], columns=rows.column_names)
        # Todo a texto: así el markdown muestra los 4 decimales y las fechas del driver como YYYY-MM-DD.
        frame = frame.map(lambda v: f"{v:.4f}".rstrip("0").rstrip(".") if isinstance(v, float) else (None if v is None else str(v)))
        key = stmt.title.split(" ")[0]
        results.append({"id": key, "title": stmt.title, "cql": stmt.cql, "rows": len(frame), "frame": frame})
        log.info("  %s: %d fila(s)", stmt.title, len(frame))
    return results


def export(results: list[dict], cfg: Config, out_dir: Path) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).isoformat(timespec="seconds")
    lines = [
        "# Consultas sobre el serving · segunda entrega", "",
        f"Provider `{cassandra.provider(cfg)}` · keyspace `{cassandra.keyspace(cfg)}` · perfil `{cfg.profile}` · {stamp}", "",
        f"Generado por `python -m src.serving.queries` a partir de [`03_queries.cql`]"
        "(../../../src/serving/cql/03_queries.cql). La salida de `cqlsh` está en "
        "[`cqlsh_queries.txt`](cqlsh_queries.txt) (`make queries`).",
    ]
    for r in results:
        lines += ["", f"## {r['title']}", ""]
        if r["id"] in QUERY_FIRST:
            lines += [f"**Query-first:** {QUERY_FIRST[r['id']]}", ""]
        lines += ["```sql", r["cql"] + ";", "```", "", f"**Resultado:** {r['rows']} fila(s)", ""]
        lines.append(r["frame"].to_markdown(index=False) if r["rows"] else "_Sin filas._")
    path = out_dir / "queries.md"
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def run(cfg: Config, evidence_dir: Path | None = None) -> dict:
    cluster, session = cassandra.connect(cfg)
    try:
        results = execute(session, cfg)
    finally:
        cluster.shutdown()
    summary = {r["id"]: r["rows"] for r in results}
    if any(n == 0 for n in summary.values()):
        log.warning("  hay consultas sin filas: %s. ¿Se cargó el serving?", summary)
    if evidence_dir:
        path = export(results, cfg, evidence_dir)
        log.info("  evidencia en %s", path.relative_to(REPO_ROOT) if path.is_relative_to(REPO_ROOT) else path)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--profile", help="local | cloud (por defecto PIPELINE_PROFILE o local)")
    parser.add_argument("--evidence", default="evidence/entrega2/serving", help="carpeta de evidencia (relativa al repo)")
    args = parser.parse_args()
    setup_logging()
    print(json.dumps(run(load_config(args.profile), evidence_dir=REPO_ROOT / args.evidence), indent=2))


if __name__ == "__main__":
    main()
