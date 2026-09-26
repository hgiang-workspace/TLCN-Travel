"""I/O local (jsonl) cho pipeline sentiment.

Không dùng Spark/pyarrow - chỉ dùng stdlib + json.
Dùng cho test, smoke, và môi trường không có cụm.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> int:
    """Ghi list dict ra file JSONL (append). Trả về số dòng ghi."""
    path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with open(path, "a", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
            count += 1
    return count


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    """Đọc file JSONL thành list dict."""
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return rows


def read_jsonl_iter(path: Path):
    """Iterator đọc từng dòng (tiết kiệm RAM)."""
    if not path.exists():
        return
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue


__all__ = ["write_jsonl", "read_jsonl", "read_jsonl_iter"]