"""Aspect Dictionary Loader.

Nạp từ điển aspect từ configs/ingestion/aspect_keywords.yaml.
Cung cấp tra cứu keyword -> aspect để dùng cho aspect detection (SP3).
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml

from ingestion.sentiment.config import get, load_config


@lru_cache(maxsize=1)
def load_aspect_keywords(path: str | Path | None = None) -> dict[str, list[str]]:
    """Nạp từ điển aspect từ file YAML.

    Args:
        path: Đường dẫn file YAML. Nếu None, dùng config pipeline.

    Returns:
        Dict mapping aspect_name -> list[keywords].
    """
    if path is None:
        cfg = load_config()
        path = get(cfg, "aspects.keywords_path", "configs/ingestion/aspect_keywords.yaml")

    config_path = Path(path)
    if not config_path.is_absolute():
        # Resolve relative to project root
        from ingestion.sentiment.config import project_root
        config_path = project_root() / config_path

    if not config_path.exists():
        raise FileNotFoundError(f"Không tìm thấy aspect keywords: {config_path}")

    with open(config_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    aspects = data.get("aspects", {})
    if not isinstance(aspects, dict):
        raise ValueError("Key 'aspects' phải là mapping")

    # Flatten: aspect -> list of keywords (lowercase)
    result: dict[str, list[str]] = {}
    for aspect, keywords in aspects.items():
        if isinstance(keywords, list):
            result[aspect] = [str(k).strip().lower() for k in keywords if str(k).strip()]
        else:
            result[aspect] = []

    return result


def build_aspect_keyword_index(keywords: dict[str, list[str]] | None = None) -> dict[str, str]:
    """Xây dựng index keyword -> aspect (để tra cứu O(1)).

    Nếu 1 keyword thuộc nhiều aspect, ưu tiên aspect đầu tiên trong dict.
    """
    if keywords is None:
        keywords = load_aspect_keywords()

    index: dict[str, str] = {}
    for aspect, kw_list in keywords.items():
        for kw in kw_list:
            if kw not in index:
                index[kw] = aspect
    return index


def get_all_aspects(keywords: dict[str, list[str]] | None = None) -> list[str]:
    """Trả về danh sách tất cả aspect names (có sắp xếp để deterministic)."""
    if keywords is None:
        keywords = load_aspect_keywords()
    return sorted(keywords.keys())


def get_aspect_keywords(aspect: str, keywords: dict[str, list[str]] | None = None) -> list[str]:
    """Trả về danh sách keywords của 1 aspect cụ thể."""
    if keywords is None:
        keywords = load_aspect_keywords()
    return keywords.get(aspect, [])


__all__ = [
    "load_aspect_keywords",
    "build_aspect_keyword_index",
    "get_all_aspects",
    "get_aspect_keywords",
]