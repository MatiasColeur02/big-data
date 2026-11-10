"""Higiene de los notebooks versionados (plan 2.9, consigna §7.6).

Las salidas pueden mostrar rutas locales, el entorno de Colab o, por error, el token de AstraDB:
los notebooks se commitean siempre sin salidas. Para limpiarlos:

    jupyter nbconvert --clear-output --inplace notebooks/*.ipynb
"""

import json
import re

import pytest

from src.common.config import REPO_ROOT

NOTEBOOKS = sorted((REPO_ROOT / "notebooks").glob("*.ipynb"))
# Un token real de Astra: "AstraCS:" seguido de la parte secreta. El texto "AstraCS:..." de las
# instrucciones no matchea.
ASTRA_TOKEN = re.compile(r"AstraCS:[A-Za-z0-9]{8,}")


@pytest.mark.parametrize("path", NOTEBOOKS, ids=lambda p: p.name)
def test_notebook_has_no_outputs(path):
    cells = json.loads(path.read_text(encoding="utf-8"))["cells"]
    dirty = [i for i, c in enumerate(cells) if c.get("outputs") or c.get("execution_count")]
    assert not dirty, f"{path.name}: celdas con salida {dirty}. Limpiar antes de commitear (ver docstring)"


@pytest.mark.parametrize("path", NOTEBOOKS, ids=lambda p: p.name)
def test_notebook_has_no_astra_token(path):
    assert not ASTRA_TOKEN.search(path.read_text(encoding="utf-8")), f"{path.name} contiene un token de AstraDB"
