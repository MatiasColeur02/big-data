"""Utilidades compartidas por batch y streaming (D1): columnas técnicas y evidencia de rutas."""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


def source_file_col() -> Column:
    """Ruta del archivo de origen relativa a Landing (`users.csv`,
    `usage_events_stream/events_part_0000.jsonl`). Relativa para que el linaje sea el mismo en
    el perfil local y en Colab (D11)."""
    return F.regexp_replace(F.input_file_name(), r"^.*/landing/", "")


def with_technical_columns(df: DataFrame) -> DataFrame:
    """Agrega `ingest_ts` y `source_file` (consigna §4.2, D11)."""
    return df.withColumn("ingest_ts", F.current_timestamp()).withColumn("source_file", source_file_col())


def path_stats(path: str | Path) -> dict:
    """Archivos Parquet, bytes y particiones bajo una ruta: evidencia de tamaños (§4.4 Performance)."""
    root = Path(path)
    if not root.exists():
        return {"path": str(root), "exists": False, "files": 0, "bytes": 0, "partitions": 0}
    files = [p for p in root.rglob("*.parquet") if "_spark_metadata" not in p.parts]
    partitions = {p.parent for p in files if "=" in p.parent.name}
    return {
        "path": str(root),
        "exists": True,
        "files": len(files),
        "bytes": sum(p.stat().st_size for p in files),
        "partitions": len(partitions),
    }
