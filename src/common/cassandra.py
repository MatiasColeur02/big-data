"""Único punto que sabe conectarse al serving, según `serving.provider` (D17).

    spark_conf(cfg)      opciones `spark.cassandra.*` para la SparkSession (connector)
    connect(cfg)         (cluster, session) de cassandra-driver, para TRUNCATE y DDL (D14)

El resto del código no distingue entre Cassandra local y AstraDB.
"""

from __future__ import annotations

import os
from pathlib import Path

from src.common.config import Config, ConfigError

PROVIDERS = ("cassandra", "astradb")


def provider(cfg: Config) -> str:
    name = cfg.get("serving.provider")
    if name not in PROVIDERS:
        raise ConfigError(f"serving.provider = `{name}`: usar uno de {', '.join(PROVIDERS)}")
    return name


def keyspace(cfg: Config) -> str:
    return cfg.get("serving.keyspace")


def spark_conf(cfg: Config) -> dict[str, str]:
    if provider(cfg) == "cassandra":
        host, port = _host_port(cfg)
        return {
            "spark.cassandra.connection.host": host,
            "spark.cassandra.connection.port": str(port),
        }
    # El connector no acepta una ruta local suelta: pide una URL o el nombre de un archivo
    # distribuido con `spark.files`, así que se distribuye el bundle y se lo cita por su nombre.
    bundle = _bundle(cfg)
    return {
        "spark.files": bundle,
        "spark.cassandra.connection.config.cloud.path": Path(bundle).name,
        "spark.cassandra.auth.username": "token",
        "spark.cassandra.auth.password": astra_token(cfg),
    }


def connect(cfg: Config):
    """Abre una sesión de cassandra-driver. Quien llama cierra con `cluster.shutdown()`."""
    from cassandra.auth import PlainTextAuthProvider
    from cassandra.cluster import Cluster, NoHostAvailable

    if provider(cfg) == "cassandra":
        host, port = _host_port(cfg)
        cluster = Cluster([host], port=port)
        target = f"Cassandra en {host}:{port}"
        hint = "¿Está levantada? `docker compose up -d` y esperar a que quede healthy"
    else:
        cluster = Cluster(
            cloud={"secure_connect_bundle": _bundle(cfg)},
            auth_provider=PlainTextAuthProvider("token", astra_token(cfg)),
        )
        target = "AstraDB"
        hint = "Revisar el secure connect bundle, el token y que la base no esté hibernada"
    try:
        session = cluster.connect()
    except NoHostAvailable as exc:
        cluster.shutdown()
        raise ConnectionError(f"no hay conexión con {target}. {hint}") from exc
    ks = keyspace(cfg)
    if ks not in cluster.metadata.keyspaces:
        cluster.shutdown()
        raise ConnectionError(
            f"no existe el keyspace `{ks}` en {target}: en Docker lo crea `cassandra-init`; "
            "en AstraDB se crea desde la consola"
        )
    return cluster, session


def astra_token(cfg: Config) -> str:
    env = cfg.get("serving.astradb.token_env")
    token = os.environ.get(env)
    if not token:
        raise ConfigError(f"falta la variable de entorno `{env}` con el token de AstraDB")
    return token


def _host_port(cfg: Config) -> tuple[str, int]:
    host = cfg.get("serving.cassandra.host")
    try:
        port = int(cfg.get("serving.cassandra.port"))
    except ValueError as exc:
        raise ConfigError("serving.cassandra.port debe ser un número") from exc
    return host, port


def _bundle(cfg: Config) -> str:
    path = cfg.get("serving.astradb.secure_connect_bundle")
    if not path:
        raise ConfigError("falta serving.astradb.secure_connect_bundle (ruta al .zip de AstraDB)")
    if not Path(path).is_file():
        raise ConfigError(f"no existe el secure connect bundle en {path}")
    return path
