"""Nạp cấu hình cho pipeline sentiment (configs/ingestion/sentiment.yaml)."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml

DEFAULT_CONFIG_ENV = "TLCN_SENTIMENT_CONFIG"
DEFAULT_CONFIG_PATH = "configs/ingestion/sentiment.yaml"
PROJECT_ROOT_ENV = "TLCN_PROJECT_ROOT"

# ingestion/sentiment/config.py -> parents[2] = repo root
_DEFAULT_ROOT = Path(__file__).resolve().parents[2]


def project_root() -> Path:
    """Repo root; cho phép override bằng ENV cho môi trường container."""
    env = os.environ.get(PROJECT_ROOT_ENV)
    if env:
        return Path(env).expanduser().resolve()
    return _DEFAULT_ROOT


def resolve_path(value: str | Path) -> Path:
    """Đường dẫn tương đối được tính từ repo root."""
    path = Path(value).expanduser()
    return path if path.is_absolute() else (project_root() / path)


def load_config(path: str | Path | None = None) -> dict[str, Any]:
    """ENV > tham số > mặc định."""
    if path is None:
        path = os.environ.get(DEFAULT_CONFIG_ENV) or DEFAULT_CONFIG_PATH
    config_path = resolve_path(path)
    if not config_path.exists():
        raise FileNotFoundError(f"Không tìm thấy config: {config_path}")
    with open(config_path, "r", encoding="utf-8") as handle:
        cfg = yaml.safe_load(handle) or {}
    if not isinstance(cfg, dict):
        raise ValueError(f"Config phải là mapping: {config_path}")
    cfg.setdefault("_meta", {})
    cfg["_meta"]["config_path"] = str(config_path)
    cfg["_meta"]["project_root"] = str(project_root())
    return cfg


def get(cfg: dict[str, Any], dotted: str, default: Any = None) -> Any:
    """Lấy giá trị lồng nhau: get(cfg, "model.batch_size")."""
    node: Any = cfg
    for part in dotted.split("."):
        if not isinstance(node, dict) or part not in node:
            return default
        node = node[part]
    return default if node is None else node
