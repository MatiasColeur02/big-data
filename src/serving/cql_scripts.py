"""Lectura de los scripts de `src/serving/cql/` para ejecutarlos con cassandra-driver.

Los mismos archivos que aplica `cqlsh -f` en Docker se ejecutan desde Python en cualquier perfil
(D17): en AstraDB no hay cassandra-init, así que el DDL y las consultas pasan por el driver.

    for stmt in statements(CQL_DIR / "02_tables.cql"):
        stmt.title   # primera línea del comentario que precede a la sentencia
        stmt.cql     # la sentencia, sin comentarios ni `;`

Las sentencias `USE` se descartan: quien ejecuta fija el keyspace de la config con `set_keyspace`.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

CQL_DIR = Path(__file__).resolve().parent / "cql"


@dataclass(frozen=True)
class Statement:
    title: str
    cql: str


def statements(path: Path) -> list[Statement]:
    if not path.is_file():
        raise FileNotFoundError(f"no existe el script CQL {path}")
    # Línea por línea y no `split(";")`: los comentarios pueden tener `;`. Una sentencia termina en
    # la línea de código que cierra con `;`.
    result, comments, code = [], [], []
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped:
            if not code:
                comments = []   # solo vale el bloque de comentarios pegado a la sentencia
        elif stripped.startswith("--"):
            if not code:
                comments.append(stripped[2:].strip())
        else:
            code.append(line.rstrip())
            if stripped.endswith(";"):
                cql = "\n".join(code).strip().rstrip(";").rstrip()
                if not re.match(r"(?i)^USE\s", cql):
                    result.append(Statement(title=comments[0] if comments else "", cql=cql))
                comments, code = [], []
    if code:
        raise ValueError(f"{path.name}: la última sentencia no termina en `;`")
    return result


def table_columns(path: Path) -> dict[str, list[str]]:
    """Columnas de cada `CREATE TABLE` del script, en orden. Sirve para elegir qué columnas de
    Gold se escriben y para el test que verifica que el DDL y la carga no se desalinean."""
    tables = {}
    for stmt in statements(path):
        match = re.match(r"(?is)CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:\w+\.)?(\w+)\s*\((.*)\)", stmt.cql)
        if not match:
            continue
        body = re.split(r"(?i)\bPRIMARY\s+KEY\b", match.group(2))[0]
        tables[match.group(1)] = [
            line.split()[0] for line in (part.strip() for part in body.split(",")) if line
        ]
    return tables
