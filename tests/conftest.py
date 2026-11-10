import pytest

from src.common.config import load_config


@pytest.fixture
def cfg(tmp_path):
    """Config local con el lake en un directorio temporal: los tests no tocan data/datalake."""
    lake = tmp_path / "datalake"
    real = load_config("local")
    # Landing sigue apuntando al dataset real (solo lectura).
    return load_config(
        "local",
        overrides={"datalake.root": str(lake), "datalake.zones.landing": real.zone("landing")},
    )


@pytest.fixture(scope="session")
def spark():
    from src.common.spark_session import build_spark

    session = build_spark(
        load_config(
            "local",
            overrides={"spark.master": "local[2]", "spark.conf": {"spark.sql.shuffle.partitions": "2"}},
        ),
        app_name="tests",
        serving=False,
    )
    yield session
    session.stop()
