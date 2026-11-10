"""Job de Gold: Silver → marts FinOps + anomalías (consigna §6.2, D9, D14, D15, D21).

    python -m src.gold.run

| Salida                             | Partición    |
|------------------------------------|--------------|
| `gold/org_daily_usage_by_service`  | `usage_date` |
| `gold/org_service_cost_14d`        | ninguna (262 filas) |
| `gold/cost_anomaly_mart`           | `usage_date` |

Recalcula todo en cada corrida con overwrite: re-ejecutarlo da el mismo resultado (O6).
Exporta el reporte de anomalías a `evidence/entrega2/gold/anomalies.md`.
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
from src.gold.anomalies import cost_anomalies
from src.gold.marts import org_daily_usage_by_service, org_service_cost_14d, resolve_as_of_date
from src.silver.run import features_path

log = logging.getLogger("pipeline")

MARTS = ("org_daily_usage_by_service", "org_service_cost_14d", "cost_anomaly_mart")


def mart_path(cfg: Config, name: str) -> str:
    return f"{cfg.zone('gold')}/{name}"


def _write(df: DataFrame, path: str, partition: str | None) -> None:
    if partition:
        df.repartition(partition).write.mode("overwrite").partitionBy(partition).parquet(path)
    else:
        df.coalesce(1).write.mode("overwrite").option("partitionOverwriteMode", "static").parquet(path)


def run(cfg: Config, spark: SparkSession | None = None, evidence_dir: Path | None = None) -> dict:
    if spark is None:
        from src.common.spark_session import build_spark

        spark = build_spark(cfg, app_name="gold", serving=False)

    with track(cfg, job="gold", source="silver", target="gold") as job:
        features = spark.read.parquet(features_path(cfg)).cache()
        as_of = resolve_as_of_date(features, cfg.get("analysis.as_of_date", None))
        threshold = float(cfg.get("anomaly.modified_zscore_threshold"))

        _write(org_daily_usage_by_service(features), mart_path(cfg, MARTS[0]), "usage_date")
        _write(org_service_cost_14d(features, as_of), mart_path(cfg, MARTS[1]), None)
        _write(cost_anomalies(features, threshold), mart_path(cfg, MARTS[2]), "usage_date")
        features.unpersist()

        counts = {m: spark.read.parquet(mart_path(cfg, m)).count() for m in MARTS}
        anomalies = anomaly_report(spark.read.parquet(mart_path(cfg, MARTS[2])))
        report = {
            "as_of_date": str(as_of),
            "rows": counts,
            "files": {m: path_stats(mart_path(cfg, m))["files"] for m in MARTS},
            "anomalies": anomalies,
        }
        job.update(rows_read=counts[MARTS[0]], rows_written=sum(counts.values()), rows_rejected=0)
        job.extra["gold"] = report
        log.info("  as_of_date=%s · filas: %s", as_of, counts)
        log.info("  anomalías: %d de %d días-servicio (%.2f %%) · métodos: %s",
                 anomalies["anomalies"], anomalies["evaluated"], anomalies["anomaly_rate_pct"], anomalies["by_method"])
    if evidence_dir:
        export_evidence(report, spark.read.parquet(mart_path(cfg, MARTS[2])), evidence_dir)
    return report


def anomaly_report(mart: DataFrame) -> dict:
    total = mart.count()
    flagged = mart.filter("is_anomaly")
    n = flagged.count()
    by_method = {r.method: {"groups_days": r["count"], "anomalies": r.anomalies}
                 for r in mart.groupBy("method").agg(F.count("*").alias("count"),
                                                     F.sum(F.col("is_anomaly").cast("int")).alias("anomalies")).collect()}
    return {
        "evaluated": total,
        "anomalies": n,
        "anomaly_rate_pct": round(100 * n / total, 2) if total else 0.0,
        "positive": flagged.filter(F.col("daily_cost_usd") > F.col("median_cost_usd")).count(),
        "negative": flagged.filter(F.col("daily_cost_usd") < F.col("median_cost_usd")).count(),
        "by_method": by_method,
        "groups": mart.select("org_id", "service").distinct().count(),
        "groups_with_anomaly": flagged.select("org_id", "service").distinct().count(),
    }


def export_evidence(report: dict, mart: DataFrame, out_dir: Path, examples: int = 10) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "gold_report.json").write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    a = report["anomalies"]
    top = (mart.filter("is_anomaly").orderBy(F.desc(F.abs(F.coalesce("anomaly_score", F.lit(0.0)))))
           .select("org_id", "usage_date", "service", "daily_cost_usd", "median_cost_usd", "mad_cost_usd",
                   "anomaly_score", "method").limit(examples).toPandas())
    lines = [
        "# Anomalías de costo diario · MAD (D9)", "",
        f"`as_of_date` {report['as_of_date']} · umbral `|0.6745·(x−mediana)/MAD| > 3.5` por `(org_id, service)`; "
        "si MAD = 0, `x > p99` del grupo.", "",
        "| Métrica | Valor |", "|---|---|",
        f"| Días-servicio evaluados | {a['evaluated']} |",
        f"| Anomalías | {a['anomalies']} ({a['anomaly_rate_pct']} %) |",
        f"| Por encima / por debajo de la mediana | {a['positive']} / {a['negative']} |",
        f"| Grupos `(org, servicio)` con al menos una | {a['groups_with_anomaly']} de {a['groups']} |",
        *[f"| Método `{m}` | {v['groups_days']} días-servicio, {v['anomalies']} anomalías |" for m, v in sorted(a["by_method"].items())],
        "", f"## Las {examples} de mayor score", "", top.to_markdown(index=False), "",
        "**Limitaciones:** sin etiquetas de verdad no hay precisión ni recall; el umbral 3.5 es el de "
        "Iglewicz y Hoaglin. Con pocos días por grupo, el fallback p99 marca a lo sumo el máximo.",
    ]
    (out_dir / "anomalies.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--profile", help="local | cloud (por defecto PIPELINE_PROFILE o local)")
    parser.add_argument("--evidence", default="evidence/entrega2/gold", help="carpeta de evidencia (relativa al repo)")
    args = parser.parse_args()
    setup_logging()
    run(load_config(args.profile), evidence_dir=REPO_ROOT / args.evidence)


if __name__ == "__main__":
    main()
