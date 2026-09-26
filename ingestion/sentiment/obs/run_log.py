"""Ghi run log của pipeline (JSONL, append 1 dòng / lần chạy job)."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from ingestion.sentiment.paths import SentimentPaths

RUN_LOG_FILE = "run_log.jsonl"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def write_run_log(
    paths: SentimentPaths,
    *,
    job: str,
    run_id: str,
    status: str,
    rows_in: int = 0,
    rows_out: int = 0,
    started_at: str = "",
    finished_at: str = "",
    params: dict[str, Any] | None = None,
    metrics: dict[str, Any] | None = None,
    error: str = "",
) -> dict[str, Any]:
    """Append 1 dòng JSON vào <root>/_reports/run_log.jsonl. Trả về record đã ghi."""
    record: dict[str, Any] = {
        "run_id": run_id,
        "job": job,
        "status": status,
        "rows_in": int(rows_in),
        "rows_out": int(rows_out),
        "started_at": started_at,
        "finished_at": finished_at or utc_now_iso(),
        "duration_seconds": _duration_seconds(started_at, finished_at),
        "params": params or {},
        "metrics": metrics or {},
        "error": error,
        "run_date": (finished_at or utc_now_iso())[:10],
    }
    paths.reports.mkdir(parents=True, exist_ok=True)
    with open(paths.run_log, "a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, ensure_ascii=False) + "\n")
        handle.flush()
    return record


def read_run_log(paths: SentimentPaths, run_id: str | None = None) -> list[dict[str, Any]]:
    if not paths.run_log.exists():
        return []
    records: list[dict[str, Any]] = []
    with open(paths.run_log, "r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                item = json.loads(line)
            except json.JSONDecodeError:
                continue
            if run_id is None or item.get("run_id") == run_id:
                records.append(item)
    return records


def _duration_seconds(started_at: str, finished_at: str) -> float:
    if not started_at or not finished_at:
        return 0.0
    try:
        start = datetime.fromisoformat(started_at)
        end = datetime.fromisoformat(finished_at)
    except ValueError:
        return 0.0
    return round((end - start).total_seconds(), 3)
