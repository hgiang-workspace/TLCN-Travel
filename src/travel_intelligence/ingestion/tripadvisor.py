"""TripAdvisor source-aligned Bronze adapter.

This module performs validation and materializes in-memory Bronze rows only;
it deliberately has no object-store or Parquet side effects.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import rfc8785

from .json_snapshot import read_json_snapshot

CANONICAL_JSON_VERSION = "canonical-json-v1"
CONTRACT_VERSION = "bronze-contract-v2"
ADAPTER_VERSION = "tripadvisor-adapter-v1"
SOURCE_ID = "tripadvisor"
RECORD_TYPE = "place"
REQUIRED_FIELDS = frozenset({"record_id", "source_url", "reviews"})


@dataclass(frozen=True)
class ValidationIssue:
    code: str
    message: str
    record_position: int


@dataclass(frozen=True)
class BronzeRow:
    source_row_index: int
    source_id: str
    record_type: str
    source_record_id: str
    source_uri: str | None
    batch_id: str
    ingested_at: str
    source_collected_at_raw: str | None
    source_file: str
    payload_hash: str
    payload_json: str


@dataclass(frozen=True)
class TripAdvisorDryRun:
    input_records: int
    accepted_records: int
    rejected_records: int
    nested_review_count: int
    warnings: tuple[ValidationIssue, ...]
    errors: tuple[ValidationIssue, ...]
    batch_id: str
    input_sha256: str
    rows: tuple[BronzeRow, ...]


def canonical_json(value: Any) -> str:
    """Return RFC 8785 canonical JSON as a Unicode string."""
    return rfc8785.dumps(value).decode("utf-8")


def payload_hash(payload: dict[str, Any]) -> str:
    return hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()


def batch_id_for(
    raw_input: bytes,
    source_id: str,
    adapter_version: str,
    *,
    contract_version: str = CONTRACT_VERSION,
) -> str:
    parts = (raw_input, CANONICAL_JSON_VERSION, contract_version, adapter_version)
    digest_input = b"\n".join(
        part if isinstance(part, bytes) else part.encode("utf-8") for part in parts
    )
    digest = hashlib.sha256(digest_input).hexdigest()
    return f"b_{source_id}_{digest}"


def batch_id(raw_input: bytes) -> str:
    return batch_id_for(raw_input, SOURCE_ID, ADAPTER_VERSION)


def _issue(code: str, message: str, position: int) -> ValidationIssue:
    return ValidationIssue(code, message, position)


def adapt_tripadvisor(
    path: str | Path,
    *,
    ingested_at: datetime | None = None,
) -> TripAdvisorDryRun:
    """Validate and adapt a TripAdvisor snapshot without writing anywhere."""
    records, raw = read_json_snapshot(path)
    batch = batch_id(raw)
    timestamp = (ingested_at or datetime.now(timezone.utc)).astimezone(timezone.utc)
    ingested_text = timestamp.isoformat(timespec="microseconds").replace("+00:00", "Z")
    warnings: list[ValidationIssue] = []
    errors: list[ValidationIssue] = []
    rows: list[BronzeRow] = []
    seen_ids: dict[str, int] = {}
    nested_count = 0

    for position, record in enumerate(records):
        missing = sorted(REQUIRED_FIELDS - record.keys())
        if missing:
            errors.append(
                _issue("missing_required_field", ", ".join(missing), position)
            )
            continue
        record_id = record["record_id"]
        source_url = record["source_url"]
        reviews = record["reviews"]
        if not isinstance(record_id, str) or not record_id:
            errors.append(
                _issue(
                    "invalid_record_id", "record_id must be non-empty string", position
                )
            )
            continue
        if not isinstance(source_url, str) or not source_url:
            errors.append(
                _issue(
                    "invalid_source_url",
                    "source_url must be non-empty string",
                    position,
                )
            )
            continue
        if not isinstance(reviews, list) or any(
            not isinstance(item, dict) for item in reviews
        ):
            errors.append(
                _issue(
                    "invalid_reviews", "reviews must be an array of objects", position
                )
            )
            continue
        if record_id in seen_ids:
            warnings.append(
                _issue(
                    "duplicate_record_id",
                    f"duplicates position {seen_ids[record_id]}",
                    position,
                )
            )
        else:
            seen_ids[record_id] = position
        if record.get("status") in {"partial", "blocked"}:
            warnings.append(_issue("crawler_status", str(record["status"]), position))
        nested_count += len(reviews)
        rows.append(
            BronzeRow(
                source_row_index=position,
                source_id=SOURCE_ID,
                record_type=RECORD_TYPE,
                source_record_id=record_id,
                source_uri=source_url,
                batch_id=batch,
                ingested_at=ingested_text,
                source_collected_at_raw=record.get("scraped_at"),
                source_file=str(Path(path)),
                payload_hash=payload_hash(record),
                payload_json=canonical_json(record),
            )
        )

    return TripAdvisorDryRun(
        input_records=len(records),
        accepted_records=len(rows),
        rejected_records=len(errors),
        nested_review_count=nested_count,
        warnings=tuple(warnings),
        errors=tuple(errors),
        batch_id=batch,
        input_sha256=hashlib.sha256(raw).hexdigest(),
        rows=tuple(rows),
    )


def format_dry_run(result: TripAdvisorDryRun) -> str:
    """Create a sanitized dry-run report without source text or personal data."""
    warning_counts: dict[str, int] = {}
    for issue in result.warnings:
        warning_counts[issue.code] = warning_counts.get(issue.code, 0) + 1
    error_counts: dict[str, int] = {}
    for issue in result.errors:
        error_counts[issue.code] = error_counts.get(issue.code, 0) + 1
    return "\n".join(
        [
            f"input_records={result.input_records}",
            f"accepted_records={result.accepted_records}",
            f"rejected_records={result.rejected_records}",
            f"nested_review_count={result.nested_review_count}",
            f"warning_counts={warning_counts}",
            f"error_counts={error_counts}",
            f"batch_id={result.batch_id}",
            f"input_sha256={result.input_sha256}",
        ]
    )


__all__ = [
    "BronzeRow",
    "TripAdvisorDryRun",
    "adapt_tripadvisor",
    "batch_id",
    "batch_id_for",
    "canonical_json",
    "format_dry_run",
    "payload_hash",
]

