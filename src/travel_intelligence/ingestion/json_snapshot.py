"""Read-only JSON snapshot loading with safe structural errors."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterator


class SnapshotReadError(ValueError):
    """Raised when a snapshot cannot be decoded or has the wrong shape."""


class DuplicateJSONKeyError(ValueError):
    """Raised when a JSON object contains a duplicate key."""


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise DuplicateJSONKeyError(f"duplicate JSON object key: {key}")
        result[key] = value
    return result


def read_json_snapshot(path: str | Path) -> tuple[list[dict[str, Any]], bytes]:
    """Load a top-level JSON array of objects and return records plus raw bytes."""
    source = Path(path)
    try:
        raw = source.read_bytes()
    except OSError as exc:
        raise SnapshotReadError(f"cannot read snapshot {source}: {exc}") from exc
    try:
        value = json.loads(
            raw.decode("utf-8"), object_pairs_hook=_reject_duplicate_keys
        )
    except (DuplicateJSONKeyError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SnapshotReadError(f"invalid UTF-8/JSON snapshot {source}: {exc}") from exc
    if not isinstance(value, list):
        raise SnapshotReadError("snapshot top level must be a JSON array")
    if any(not isinstance(record, dict) for record in value):
        raise SnapshotReadError("snapshot array must contain only JSON objects")
    return value, raw


def iter_json_snapshot(path: str | Path) -> Iterator[dict[str, Any]]:
    """Yield records from a validated JSON snapshot."""
    records, _ = read_json_snapshot(path)
    yield from records

