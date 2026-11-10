"""SparkSession a partir de la config (D17).

    spark = build_spark(cfg)                    # con el connector de Cassandra
    spark = build_spark(cfg, serving=False)     # sin connector: tests y jobs que no tocan el serving
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession

from src.common import cassandra
from src.common.config import Config


def build_spark(cfg: Config, app_name: str | None = None, serving: bool = True) -> SparkSession:
    builder = SparkSession.builder.master(cfg.get("spark.master")).appName(
        app_name or cfg.get("spark.app_name")
    )
    for key, value in (cfg.get("spark.conf", {}) or {}).items():
        builder = builder.config(key, str(value))

    if serving:
        builder = builder.config("spark.jars.packages", cfg.get("spark.packages"))
        # La imagen de Docker trae el connector ya descargado en este directorio.
        if os.environ.get("SPARK_IVY_DIR"):
            builder = builder.config("spark.jars.ivy", os.environ["SPARK_IVY_DIR"])
        for key, value in cassandra.spark_conf(cfg).items():
            builder = builder.config(key, value)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
