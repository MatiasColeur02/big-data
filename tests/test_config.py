from pathlib import Path

import pytest

from src.common import cassandra
from src.common.config import REPO_ROOT, ConfigError, load_config


def write(path: Path, text: str) -> Path:
    path.write_text(text, encoding="utf-8")
    return path


@pytest.fixture
def config_dir(tmp_path):
    d = tmp_path / "config"
    d.mkdir()
    write(
        d / "pipeline.base.yaml",
        """
datalake:
  root: "data/datalake"
  zones:
    bronze: "${datalake.root}/bronze"
serving:
  provider: cassandra
  keyspace: ks
  cassandra:
    host: "${TEST_CASSANDRA_HOST:-localhost}"
    port: "9042"
  astradb:
    secure_connect_bundle: null
    token_env: TEST_ASTRA_TOKEN
""",
    )
    write(d / "pipeline.local.example.yaml", "serving:\n  keyspace: ks_local\n")
    return d


def test_perfil_por_defecto_y_merge_sobre_la_base(config_dir, monkeypatch):
    monkeypatch.delenv("PIPELINE_PROFILE", raising=False)
    cfg = load_config(config_dir=config_dir)
    assert cfg.profile == "local"
    assert cfg.get("serving.keyspace") == "ks_local"     # lo pisa el perfil
    assert cfg.get("serving.provider") == "cassandra"    # viene de la base


def test_rutas_relativas_contra_la_raiz_del_repo(config_dir, monkeypatch):
    monkeypatch.chdir(config_dir)   # el directorio de trabajo no debe importar
    cfg = load_config("local", config_dir=config_dir)
    assert cfg.zone("bronze") == str(REPO_ROOT / "data" / "datalake" / "bronze")


def test_override_cambia_la_raiz_y_las_zonas_derivadas(config_dir, tmp_path):
    cfg = load_config("local", overrides={"datalake.root": str(tmp_path / "sandbox")}, config_dir=config_dir)
    assert cfg.zone("bronze") == str(tmp_path / "sandbox" / "bronze")


def test_override_de_diccionario_se_combina(config_dir):
    write(config_dir / "pipeline.local.yaml", 'spark:\n  conf:\n    "a.b": "1"\n    "c.d": "2"\n')
    cfg = load_config("local", overrides={"spark.conf": {"a.b": "9"}}, config_dir=config_dir)
    assert cfg.get("spark.conf") == {"a.b": "9", "c.d": "2"}


def test_variable_de_entorno_con_y_sin_valor(config_dir, monkeypatch):
    monkeypatch.delenv("TEST_CASSANDRA_HOST", raising=False)
    assert load_config("local", config_dir=config_dir).get("serving.cassandra.host") == "localhost"
    monkeypatch.setenv("TEST_CASSANDRA_HOST", "cassandra")
    assert load_config("local", config_dir=config_dir).get("serving.cassandra.host") == "cassandra"


def test_variable_obligatoria_ausente(config_dir, monkeypatch):
    monkeypatch.delenv("TEST_REQUIRED", raising=False)
    write(config_dir / "pipeline.local.yaml", 'extra:\n  x: "${TEST_REQUIRED}"\n')
    with pytest.raises(ConfigError, match="TEST_REQUIRED"):
        load_config("local", config_dir=config_dir)


def test_copia_local_tiene_prioridad_sobre_la_plantilla(config_dir):
    write(config_dir / "pipeline.local.yaml", "serving:\n  keyspace: ks_copia\n")
    cfg = load_config("local", config_dir=config_dir)
    assert cfg.get("serving.keyspace") == "ks_copia"
    assert cfg.sources[-1].name == "pipeline.local.yaml"


def test_rechaza_token_escrito_en_el_yaml(config_dir):
    write(config_dir / "pipeline.local.yaml", "serving:\n  astradb:\n    token: AstraCS:abc123\n")
    with pytest.raises(ConfigError, match="variable de entorno"):
        load_config("local", config_dir=config_dir)


def test_acepta_token_como_referencia_a_variable(config_dir, monkeypatch):
    monkeypatch.setenv("SOME_TOKEN", "x")
    write(config_dir / "pipeline.local.yaml", 'serving:\n  astradb:\n    token: "${SOME_TOKEN}"\n')
    load_config("local", config_dir=config_dir)


def test_referencia_inexistente_y_circular(config_dir):
    write(config_dir / "pipeline.local.yaml", 'a: "${no.existe}"\n')
    with pytest.raises(ConfigError, match="no existe"):
        load_config("local", config_dir=config_dir)
    write(config_dir / "pipeline.local.yaml", 'a: "${b}"\nb: "${a}"\n')
    with pytest.raises(ConfigError, match="circular"):
        load_config("local", config_dir=config_dir)


def test_perfil_desconocido():
    with pytest.raises(ConfigError, match="desconocido"):
        load_config("produccion")


def test_opciones_de_conexion_por_provider(config_dir, monkeypatch, tmp_path):
    cfg = load_config("local", config_dir=config_dir)
    assert cassandra.spark_conf(cfg) == {
        "spark.cassandra.connection.host": "localhost",
        "spark.cassandra.connection.port": "9042",
    }

    bundle = write(tmp_path / "bundle.zip", "")
    astra = load_config(
        "local",
        overrides={"serving.provider": "astradb", "serving.astradb.secure_connect_bundle": str(bundle)},
        config_dir=config_dir,
    )
    monkeypatch.delenv("TEST_ASTRA_TOKEN", raising=False)
    with pytest.raises(ConfigError, match="TEST_ASTRA_TOKEN"):
        cassandra.spark_conf(astra)
    monkeypatch.setenv("TEST_ASTRA_TOKEN", "AstraCS:secreto")
    conf = cassandra.spark_conf(astra)
    assert conf["spark.cassandra.connection.config.cloud.path"] == str(bundle)
    assert conf["spark.cassandra.auth.password"] == "AstraCS:secreto"


def test_configs_reales_cargan():
    """Las plantillas versionadas son válidas y tienen las claves que usa el pipeline."""
    local = load_config("local")
    assert local.get("spark.conf")["spark.sql.session.timeZone"] == "UTC"
    assert local.zone("landing").endswith("data/datalake/landing")
    assert local.get("streaming.source_path") == local.zone("landing") + "/usage_events_stream"
    cloud = load_config("cloud")
    assert cloud.get("serving.provider") == "astradb"
    assert cloud.zone("bronze") == "/content/datalake/bronze"
