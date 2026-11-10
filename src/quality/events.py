"""Calidad de eventos: Bronze → (válidos, Quarantine), con control por fecha y evidencia.

    python -m src.quality.events     # valida Bronze, escribe Quarantine y exporta evidencia

`validate_events` es lo que llama Silver (2.5): devuelve los válidos ya marcados y deja escrita
la Quarantine. Silver recalcula todas las fechas en cada corrida (43.200 filas, segundos), así
que Quarantine se reemplaza completa y la re-ejecución da el mismo resultado (O6).
"""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.config import REPO_ROOT, Config, load_config
from src.common.lake import path_stats
from src.common.logs import setup_logging
from src.common.run_log import track
from src.ingest.stream_events import bronze_path
from src.quality.rules import BLOCKING_EVENT_RULES, EVENT_RULES, apply_event_rules

log = logging.getLogger("pipeline")

TABLE = "usage_events_stream"
SAMPLE_COLUMNS = ["dq_rule", "event_id", "event_ts", "event_date", "org_id", "service", "metric",
                  "value", "unit", "cost_usd_increment", "source_file", "rejected_ts"]


class BalanceError(RuntimeError):
    pass


def quarantine_path(cfg: Config) -> str:
    return f"{cfg.zone('quarantine')}/{TABLE}"


def read_quarantine(spark: SparkSession, cfg: Config) -> DataFrame | None:
    path = quarantine_path(cfg)
    return spark.read.parquet(path) if path_stats(path)["files"] else None


def validate_events(spark: SparkSession, cfg: Config) -> tuple[DataFrame, dict]:
    bronze = spark.read.parquet(bronze_path(cfg))
    valid, rejected = apply_event_rules(bronze, float(cfg.get("quality.cost_increment_min")))

    # Sin rechazos Spark escribe solo _SUCCESS; se lee con esquema conocido o se cuenta 0 (D20).
    (
        rejected.coalesce(1).write.mode("overwrite").option("partitionOverwriteMode", "static")
        .partitionBy("dq_rule").parquet(quarantine_path(cfg))
    )
    quarantine = read_quarantine(spark, cfg)

    valid = valid.cache()
    report = {
        "bronze": bronze.count(),
        "valid": valid.count(),
        "quarantine": quarantine.count() if quarantine is not None else 0,
        "blocked_by_rule": _counts(quarantine, "dq_rule", BLOCKING_EVENT_RULES),
        "flagged_by_rule": _counts(valid.select(F.explode("dq_flags").alias("flag")), "flag",
                                   [r.id for r in EVENT_RULES if r.id not in BLOCKING_EVENT_RULES]),
        "cost_anomalies": valid.filter("cost_anomaly").count(),
    }
    report["unbalanced_dates"] = check_balance(bronze, valid, quarantine)
    if report["unbalanced_dates"]:
        raise BalanceError(f"bronze != válidos + quarantine en {len(report['unbalanced_dates'])} fecha(s): "
                           f"{report['unbalanced_dates'][:5]}")
    return valid, report


def check_balance(bronze: DataFrame, valid: DataFrame, quarantine: DataFrame | None) -> list[dict]:
    """O4 / D16: `bronze[d] = válidos[d] + quarantine[d]` para cada fecha, con la fecha nula como
    una fecha más (join null-safe). Devuelve las fechas que no cierran."""
    def per_date(df, name):
        return df.groupBy("event_date").agg(F.count("*").alias(name))

    totals =per_date(bronze, "b").alias("b").join(
        per_date(valid, "v").alias("v"), F.col("b.event_date").eqNullSafe(F.col("v.event_date")), "full"
    ).select(F.coalesce("b.event_date", "v.event_date").alias("event_date"), "b", "v")
    if quarantine is not None:
        totals = totals.alias("t").join(
            per_date(quarantine, "q").alias("q"), F.col("t.event_date").eqNullSafe(F.col("q.event_date")), "full"
        ).select(F.coalesce("t.event_date", "q.event_date").alias("event_date"), "b", "v", "q")
    else:
        totals = totals.withColumn("q", F.lit(0))
    bad = totals.na.fill(0, ["b", "v", "q"]).filter(F.col("b") != F.col("v") + F.col("q"))
    return [{"event_date": str(r.event_date), "bronze": r.b, "valid": r.v, "quarantine": r.q} for r in bad.collect()]


def _counts(df: DataFrame | None, column: str, expected: list[str]) -> dict:
    found = {} if df is None else {r[column]: r["count"] for r in df.groupBy(column).count().collect()}
    return {rule: found.get(rule, 0) for rule in expected}


def export_evidence(spark: SparkSession, cfg: Config, report: dict, out_dir: Path, per_rule: int = 5) -> Path:
    """Escribe `quality_report.json` y `quarantine_samples.md` (muestras por regla) en out_dir."""
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "quality_report.json").write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    lines = ["# Muestras de Quarantine · eventos", "",
             f"Perfil `{cfg.profile}` · lake `{Path(cfg.get('datalake.root')).name}`", "",
             "| Regla | Acción | Filas |", "|---|---|---|"]
    counts = {**report["blocked_by_rule"], **report["flagged_by_rule"]}
    lines += [f"| `{r.id}` | {r.action} | {counts.get(r.id, 0)} |" for r in EVENT_RULES]
    quarantine = read_quarantine(spark, cfg)
    for rule in BLOCKING_EVENT_RULES:
        lines += ["", f"## `{rule}`", ""]
        if quarantine is None or not report["blocked_by_rule"][rule]:
            lines.append("Sin filas: ningún evento viola esta regla en este lake.")
            continue
        sample = (quarantine.filter(F.col("dq_rule") == rule).select(*SAMPLE_COLUMNS)
                  .orderBy("event_date", "event_id").limit(per_rule).toPandas())
        lines.append(sample.to_markdown(index=False))
    (out_dir / "quarantine_samples.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return out_dir


def run(cfg: Config, spark: SparkSession | None = None, evidence_dir: Path | None = None) -> dict:
    if spark is None:
        from src.common.spark_session import build_spark

        spark = build_spark(cfg, app_name="quality_events", serving=False)
    with track(cfg, job="quality_events", source="bronze", target="quarantine") as job:
        valid, report = validate_events(spark, cfg)
        valid.unpersist()
        job.update(rows_read=report["bronze"], rows_written=report["valid"], rows_rejected=report["quarantine"])
        job.extra["quality"] = report
        log.info("  bronze=%d válidos=%d quarantine=%d · bloqueos=%s · marcas=%s",
                 report["bronze"], report["valid"], report["quarantine"],
                 report["blocked_by_rule"], report["flagged_by_rule"])
    if evidence_dir:
        export_evidence(spark, cfg, report, evidence_dir)
        log.info("  evidencia en %s", evidence_dir.relative_to(REPO_ROOT) if evidence_dir.is_relative_to(REPO_ROOT) else evidence_dir)
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--profile", help="local | cloud (por defecto PIPELINE_PROFILE o local)")
    parser.add_argument("--evidence", default="evidence/entrega2/quality", help="carpeta de evidencia (relativa al repo)")
    args = parser.parse_args()
    setup_logging()
    run(load_config(args.profile), evidence_dir=REPO_ROOT / args.evidence)


if __name__ == "__main__":
    main()
