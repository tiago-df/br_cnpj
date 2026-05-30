"""Loads config.yaml once and exposes typed helpers."""

from functools import lru_cache
from pathlib import Path

import yaml

_ROOT = Path(__file__).parent.parent


@lru_cache(maxsize=1)
def get_config() -> dict:
    config_path = _ROOT / "config.yaml"
    with open(config_path, "r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def resolve_path(key: str) -> Path:
    """Return an absolute path for a key under config.paths.*"""
    cfg = get_config()
    rel = cfg["paths"][key]
    p = _ROOT / rel
    p.mkdir(parents=True, exist_ok=True)
    return p
