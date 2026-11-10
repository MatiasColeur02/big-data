"""Carga de la configuración por perfil (D17).

    cfg = load_config()                      # perfil de PIPELINE_PROFILE, o "local"
    cfg = load_config("cloud")
    cfg = load_config(overrides={"datalake.root": "data/sandbox"})   # lake de fixtures (D20)

    cfg.get("streaming.watermark")           # "60 days"
    cfg.zone("bronze")                       # ruta absoluta de la zona

Combina `config/pipeline.base.yaml` con el perfil, aplica los overrides, resuelve la interpolación
`${...}` y convierte las rutas relativas en absolutas contra la raíz del repo, para que el
notebook (que corre en notebooks/) y pytest (que corre en la raíz) vean el mismo lake.
"""

from __future__ import annotations

import copy
import os
import re
from pathlib import Path
from typing import Any

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
CONFIG_DIR = REPO_ROOT / "config"
PROFILES = ("local", "cloud")
DEFAULT_PROFILE = "local"

# Claves que contienen rutas del sistema de archivos y se vuelven absolutas.
_PATH_KEYS = ("datalake.root", "serving.astradb.secure_connect_bundle")
# Claves que nunca pueden tener un valor literal en el YAML: credenciales solo por entorno.
_SECRET_KEY = re.compile(r"(token|password|secret)$", re.IGNORECASE)
_REF = re.compile(r"\$\{([^}]+)\}")
_ENV_NAME = re.compile(r"^[A-Z_][A-Z0-9_]*$")


class ConfigError(Exception):
    """Configuración inválida o incompleta. El mensaje dice qué falta y dónde."""


class Config:
    def __init__(self, data: dict, profile: str, sources: list[Path]):
        self._data = data
        self.profile = profile
        self.sources = sources

    def get(self, key: str, default: Any = ...) -> Any:
        node: Any = self._data
        for part in key.split("."):
            if not isinstance(node, dict) or part not in node:
                if default is ...:
                    raise ConfigError(f"falta la clave `{key}` en la config del perfil `{self.profile}`")
                return default
            node = node[part]
        return node

    def zone(self, name: str) -> str:
        return self.get(f"datalake.zones.{name}")

    def as_dict(self) -> dict:
        return copy.deepcopy(self._data)

    def __repr__(self) -> str:
        files = ", ".join(p.name for p in self.sources)
        return f"Config(profile={self.profile!r}, sources=[{files}])"


def load_config(
    profile: str | None = None,
    overrides: dict[str, Any] | None = None,
    config_dir: Path | None = None,
) -> Config:
    config_dir = Path(config_dir) if config_dir else CONFIG_DIR
    profile = profile or os.environ.get("PIPELINE_PROFILE") or DEFAULT_PROFILE
    if profile not in PROFILES:
        raise ConfigError(f"perfil `{profile}` desconocido: usar uno de {', '.join(PROFILES)}")

    base_file = config_dir / "pipeline.base.yaml"
    profile_file = _profile_file(config_dir, profile)
    data = _read_yaml(base_file)
    _deep_merge(data, _read_yaml(profile_file))
    for key, value in (overrides or {}).items():
        current = _get(data, key)
        if isinstance(value, dict) and isinstance(current, dict):
            _deep_merge(current, value)   # {"spark.conf": {...}} agrega claves, no reemplaza
        else:
            _set(data, key, value)

    _reject_literal_secrets(data)
    for key in _PATH_KEYS:
        value = _get(data, key)
        if isinstance(value, str) and not _REF.search(value):
            _set(data, key, _absolute(value))
    data = _interpolate(data, data, stack=())
    return Config(data, profile, [base_file, profile_file])


def _profile_file(config_dir: Path, profile: str) -> Path:
    """La copia local tiene prioridad; si no existe, se usa la plantilla versionada."""
    for name in (f"pipeline.{profile}.yaml", f"pipeline.{profile}.example.yaml"):
        if (config_dir / name).is_file():
            return config_dir / name
    raise ConfigError(f"no existe config/pipeline.{profile}.yaml ni config/pipeline.{profile}.example.yaml")


def _read_yaml(path: Path) -> dict:
    if not path.is_file():
        raise ConfigError(f"no existe el archivo de configuración {path}")
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError as exc:
        raise ConfigError(f"{path.name} no es YAML válido: {exc}") from exc
    if not isinstance(data, dict):
        raise ConfigError(f"{path.name} debe ser un mapa de claves en el nivel superior")
    return data


def _deep_merge(base: dict, extra: dict) -> None:
    for key, value in extra.items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            _deep_merge(base[key], value)
        else:
            base[key] = value


def _get(data: dict, key: str) -> Any:
    node: Any = data
    for part in key.split("."):
        if not isinstance(node, dict) or part not in node:
            return None
        node = node[part]
    return node


def _set(data: dict, key: str, value: Any) -> None:
    *parents, last = key.split(".")
    node = data
    for part in parents:
        node = node.setdefault(part, {})
    node[last] = value


def _absolute(path: str) -> str:
    p = Path(os.path.expanduser(path))
    return str(p if p.is_absolute() else (REPO_ROOT / p).resolve())


def _reject_literal_secrets(node: Any, prefix: str = "") -> None:
    if not isinstance(node, dict):
        return
    for key, value in node.items():
        dotted = f"{prefix}{key}"
        if _SECRET_KEY.search(str(key)) and value not in (None, "") and not _is_env_ref(value):
            raise ConfigError(
                f"`{dotted}` tiene un valor escrito en el YAML. Las credenciales van solo por "
                "variable de entorno (D17): usar `${NOMBRE_VARIABLE}`"
            )
        _reject_literal_secrets(value, f"{dotted}.")


def _is_env_ref(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    match = re.fullmatch(r"\$\{([^}:]+)(:-[^}]*)?\}", value.strip())
    return bool(match and _ENV_NAME.match(match.group(1)))


def _interpolate(node: Any, root: dict, stack: tuple[str, ...]) -> Any:
    if isinstance(node, dict):
        return {k: _interpolate(v, root, stack) for k, v in node.items()}
    if isinstance(node, list):
        return [_interpolate(v, root, stack) for v in node]
    if not isinstance(node, str) or "${" not in node:
        return node
    return _REF.sub(lambda m: _resolve(m.group(1), root, stack), node)


def _resolve(expr: str, root: dict, stack: tuple[str, ...]) -> str:
    name, has_default, default = expr.partition(":-")
    name = name.strip()
    if _ENV_NAME.match(name):
        value = os.environ.get(name)
        if value is None or value == "":
            if has_default:
                return default
            raise ConfigError(f"falta la variable de entorno `{name}`, que pide la config")
        return value
    if name in stack:
        raise ConfigError(f"referencia circular en la config: {' -> '.join(stack + (name,))}")
    value = _get(root, name)
    if value is None:
        raise ConfigError(f"`${{{name}}}` apunta a una clave que no existe en la config")
    return str(_interpolate(value, root, stack + (name,)))
