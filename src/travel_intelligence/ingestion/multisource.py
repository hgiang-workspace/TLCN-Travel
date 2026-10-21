"""Foody, TikTok and Vexere source-aligned Bronze adapters."""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from pathlib import Path

from .json_snapshot import read_json_snapshot
from .tripadvisor import (
    BronzeRow,
    ValidationIssue,
    batch_id_for,
    canonical_json,
    payload_hash,
)


class SourceDryRun:
    def __init__(
        self,
        source_id,
        record_type,
        nested_field,
        path,
        records,
        raw,
        rows,
        warnings,
        errors,
        nested_count,
    ):
        self.source_id = source_id
        self.record_type = record_type
        self.nested_field = nested_field
        self.input_records = len(records)
        self.accepted_records = len(rows)
        self.rejected_records = len(errors)
        self.nested_review_count = nested_count
        self.nested_record_count = nested_count
        self.warnings = tuple(warnings)
        self.errors = tuple(errors)
        self.rows = tuple(rows)
        self.input_sha256 = hashlib.sha256(raw).hexdigest()
        self.batch_id = batch_id_for(raw, source_id, f"{source_id}-adapter-v1")
        self.source_file = str(Path(path))


def _run(path, source_id, record_type, required, nested_field, identity, collected):
    records, raw = read_json_snapshot(path)
    batch = batch_id_for(raw, source_id, f"{source_id}-adapter-v1")
    ingested = (
        datetime.now(timezone.utc)
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z")
    )
    warnings, errors, rows = [], [], []
    seen = {}
    nested_count = 0
    for position, record in enumerate(records):
        missing = sorted(required - record.keys())
        if missing:
            errors.append(
                ValidationIssue("missing_required_field", ", ".join(missing), position)
            )
            continue
        nested = record.get(nested_field)
        if nested_field and (
            not isinstance(nested, list) or any(not isinstance(x, dict) for x in nested)
        ):
            errors.append(
                ValidationIssue(
                    "invalid_nested_records",
                    f"{nested_field} must be an array of objects",
                    position,
                )
            )
            continue
        source_key, key_warnings = identity(record, position)
        warnings.extend(key_warnings)
        if source_key is None:
            errors.append(
                ValidationIssue(
                    "missing_source_identity", "no usable source identity", position
                )
            )
            continue
        if source_key in seen:
            warnings.append(
                ValidationIssue(
                    "duplicate_source_id",
                    f"duplicates position {seen[source_key]}",
                    position,
                )
            )
        else:
            seen[source_key] = position
        warnings.extend(collected(record, position))
        nested_count += len(nested) if nested_field else 0
        rows.append(
            BronzeRow(
                source_row_index=position,
                source_id=source_id,
                record_type=record_type,
                source_record_id=source_key,
                source_uri=record.get("source_url"),
                batch_id=batch,
                ingested_at=ingested,
                source_collected_at_raw=record.get(
                    "crawl_time", record.get("crawl_date")
                ),
                source_file=str(Path(path)),
                payload_hash=payload_hash(record),
                payload_json=canonical_json(record),
            )
        )
    return SourceDryRun(
        source_id,
        record_type,
        nested_field,
        path,
        records,
        raw,
        rows,
        warnings,
        errors,
        nested_count,
    )


def _foody_identity(record, position):
    url = record.get("source_url")
    if isinstance(url, str) and url:
        return url, []
    fallback = hashlib.sha256(
        canonical_json(
            {k: record.get(k) for k in ("name", "address", "province")}
        ).encode()
    ).hexdigest()
    return fallback, [
        ValidationIssue(
            "identity_fallback", "source_url missing; stable-field hash used", position
        )
    ]


def adapt_foody(path):
    def warnings(record, position):
        result = []
        for field in ("rating_avg", "rating_price", "price_min", "price_max"):
            value = record.get(field)
            if isinstance(value, (int, float)) and value < 0:
                result.append(ValidationIssue("suspicious_value", field, position))
        return result

    return _run(path, "foody", "restaurant", {"name"}, None, _foody_identity, warnings)


def _tiktok_identity(record, position):
    value = record.get("video_id")
    if not isinstance(value, str) or not value:
        return None, [ValidationIssue("missing_video_id", "video_id missing", position)]
    return value, []


def adapt_tiktok(path):
    def warnings(record, position):
        result = []
        comments = record.get("comments", [])
        if record.get("comment_count") != len(comments):
            result.append(
                ValidationIssue(
                    "comment_count_mismatch",
                    "source count differs from collected array",
                    position,
                )
            )
        for comment in comments:
            if not comment.get("comment_id"):
                result.append(
                    ValidationIssue(
                        "missing_comment_id", "nested comment ID missing", position
                    )
                )
        return result

    return _run(
        path,
        "tiktok",
        "video",
        {"video_id", "comments"},
        "comments",
        _tiktok_identity,
        warnings,
    )


def _vexere_identity(record, position):
    fields = (
        "origin",
        "destination",
        "operator_name",
        "transport_type",
        "vehicle_type",
        "departure_time",
        "arrival_time",
        "crawl_date",
        "price_vnd",
        "source_url",
    )
    value = hashlib.sha256(
        canonical_json({key: record.get(key) for key in fields}).encode()
    ).hexdigest()
    return value, []


def adapt_vexere(path):
    def warnings(record, position):
        result = []
        if (
            isinstance(record.get("duration_min"), (int, float))
            and record["duration_min"] <= 0
        ):
            result.append(
                ValidationIssue(
                    "suspicious_duration", "duration_min is non-positive", position
                )
            )
        if (
            isinstance(record.get("price_vnd"), (int, float))
            and record["price_vnd"] <= 0
        ):
            result.append(
                ValidationIssue(
                    "suspicious_price", "price_vnd is non-positive", position
                )
            )
        return result

    required = {
        "origin",
        "destination",
        "operator_name",
        "departure_time",
        "arrival_time",
        "crawl_date",
    }
    return _run(
        path, "vexere", "transport_offer", required, None, _vexere_identity, warnings
    )
