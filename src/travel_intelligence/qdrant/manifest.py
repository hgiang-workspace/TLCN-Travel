"""Deterministic multi-run input manifests for Qdrant publication."""

from __future__ import annotations

import hashlib
import io
import json
from collections.abc import Mapping, Sequence
from typing import Any


class ManifestError(ValueError):
    """Raised when committed enrichment runs cannot form one input manifest."""


def _canonical_bytes(value: Mapping[str, Any]) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")


def manifest_id(
    *,
    source_id: str,
    source_release_id: str,
    runs: Sequence[Mapping[str, Any]],
) -> str:
    members = [
        {
            "processing_run_id": str(run["processing_run_id"]),
            "source_release_id": str(run["source_release_id"]),
            "model_fingerprint": str(run["model_fingerprint"]),
            "config_fingerprint": str(run["config_fingerprint"]),
            "expected_review_count": int(run["expected_review_count"]),
            "actual_review_count": int(run["actual_review_count"]),
            "expected_aspect_count": int(run["expected_aspect_count"]),
            "actual_aspect_count": int(run["actual_aspect_count"]),
        }
        for run in runs
    ]
    raw = _canonical_bytes(
        {
            "source_id": source_id,
            "source_release_id": source_release_id,
            "runs": members,
        }
    )
    return "qdrant-input-" + hashlib.sha256(raw).hexdigest()


def build_manifest(
    *,
    source_id: str,
    source_release_id: str,
    runs: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    if not runs:
        raise ManifestError("manifest requires at least one committed enrichment run")
    ordered = sorted(runs, key=lambda row: str(row["processing_run_id"]))
    fingerprints = {
        (str(row["model_fingerprint"]), str(row["config_fingerprint"]))
        for row in ordered
    }
    releases = {str(row["source_release_id"]) for row in ordered}
    if releases != {source_release_id}:
        raise ManifestError("manifest contains mixed source releases")
    if len(fingerprints) != 1:
        raise ManifestError("manifest contains mixed model/config fingerprints")
    processing_ids = [str(row["processing_run_id"]) for row in ordered]
    if len(set(processing_ids)) != len(processing_ids):
        raise ManifestError("manifest contains duplicate processing_run_id")
    for row in ordered:
        if str(row.get("status")) != "committed":
            raise ManifestError("manifest contains a non-committed run")
        for prefix in ("review", "aspect"):
            if int(row[f"expected_{prefix}_count"]) != int(
                row[f"actual_{prefix}_count"]
            ):
                raise ManifestError(
                    f"{row['processing_run_id']} has unreconciled {prefix} count"
                )
    model_fp, config_fp = next(iter(fingerprints))
    members = [
        {
            "member_index": index,
            "processing_run_id": str(row["processing_run_id"]),
            "source_release_id": str(row["source_release_id"]),
            "input_fingerprint": str(row["input_fingerprint"]),
            "model_fingerprint": str(row["model_fingerprint"]),
            "config_fingerprint": str(row["config_fingerprint"]),
            "expected_review_count": int(row["expected_review_count"]),
            "actual_review_count": int(row["actual_review_count"]),
            "expected_aspect_count": int(row["expected_aspect_count"]),
            "actual_aspect_count": int(row["actual_aspect_count"]),
        }
        for index, row in enumerate(ordered, start=1)
    ]
    value = {
        "manifest_id": manifest_id(
            source_id=source_id,
            source_release_id=source_release_id,
            runs=ordered,
        ),
        "source_id": source_id,
        "source_release_id": source_release_id,
        "model_fingerprint": model_fp,
        "config_fingerprint": config_fp,
        "input_run_count": len(members),
        "expected_input_review_count": sum(
            item["expected_review_count"] for item in members
        ),
        "expected_input_aspect_count": sum(
            item["expected_aspect_count"] for item in members
        ),
        "processing_runs": members,
    }
    digest = hashlib.sha256(_canonical_bytes(value)).hexdigest()
    return {**value, "manifest_sha256": "sha256:" + digest}


def write_manifest(
    client: Any,
    *,
    bucket: str,
    prefix: str,
    manifest: Mapping[str, Any],
) -> tuple[str, str]:
    body = _canonical_bytes(manifest)
    object_name = f"{prefix.rstrip('/')}/{manifest['manifest_id']}.json"
    client.put_object(
        bucket,
        object_name,
        io.BytesIO(body),
        length=len(body),
        content_type="application/json",
    )
    return f"s3://{bucket}/{object_name}", str(manifest["manifest_sha256"])


__all__ = ["ManifestError", "build_manifest", "manifest_id", "write_manifest"]
