"""Deterministic Gold input manifests and candidate run-state contracts."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from travel_intelligence.gold.contracts import GOLD_CONTRACT_VERSION

REQUIRED_SILVER_SNAPSHOTS = (
    "source_record",
    "place",
    "place_rating",
    "review",
    "source_entity_mapping",
    "normalization_run",
)
RUN_STATES = ("planned", "building", "validated", "committed", "partial", "failed")
LEGAL_TRANSITIONS = {
    "planned": frozenset({"building", "failed"}),
    "building": frozenset({"validated", "partial", "failed"}),
    "partial": frozenset({"building", "failed"}),
    "validated": frozenset({"committed", "failed"}),
    "committed": frozenset(),
    "failed": frozenset(),
}


class GoldManifestError(ValueError):
    """Raised when upstream rows cannot form one eligible Gold input."""


class GoldRunStateError(ValueError):
    """Raised for an illegal candidate-run transition."""


def _canonical_bytes(value: Mapping[str, Any]) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")


def _sha(value: Mapping[str, Any]) -> str:
    return "sha256:" + hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _required_text(row: Mapping[str, Any], field_name: str) -> str:
    value = str(row.get(field_name) or "").strip()
    if not value:
        raise GoldManifestError(f"missing required field: {field_name}")
    return value


def _validated_normalization(
    normalization: Mapping[str, Any], *, source_id: str, source_release_id: str
) -> dict[str, Any]:
    if normalization.get("status") != "committed":
        raise GoldManifestError("normalization release is not committed")
    if _required_text(normalization, "run_id") != source_release_id:
        raise GoldManifestError("normalization run_id does not match source release")
    if _required_text(normalization, "source_id") != source_id:
        raise GoldManifestError("normalization source does not match Gold scope")
    refs = normalization.get("table_snapshot_refs")
    if not isinstance(refs, Mapping):
        raise GoldManifestError("normalization snapshot references are missing")
    missing = [
        name for name in REQUIRED_SILVER_SNAPSHOTS if not str(refs.get(name) or "")
    ]
    if missing:
        raise GoldManifestError(
            "normalization snapshot references are incomplete: " + ", ".join(missing)
        )
    return {
        "run_id": source_release_id,
        "bronze_batch_id": _required_text(normalization, "bronze_batch_id"),
        "normalization_version": _required_text(normalization, "normalization_version"),
        "accepted_row_count": int(normalization["accepted_row_count"]),
        "nested_input_count": int(normalization["nested_input_count"]),
        "table_snapshot_refs": {
            name: str(refs[name]) for name in sorted(REQUIRED_SILVER_SNAPSHOTS)
        },
    }


def _validated_enrichment_runs(
    runs: Sequence[Mapping[str, Any]],
    *,
    source_id: str,
    source_release_id: str,
) -> tuple[list[dict[str, Any]], tuple[str, str, str]]:
    if not runs:
        raise GoldManifestError("at least one enrichment run is required")
    seen: set[str] = set()
    result: list[dict[str, Any]] = []
    lineage: set[tuple[str, str, str]] = set()
    for row in runs:
        run_id = _required_text(row, "processing_run_id")
        if run_id in seen:
            raise GoldManifestError(f"duplicate enrichment run: {run_id}")
        seen.add(run_id)
        if row.get("status") != "committed":
            raise GoldManifestError(f"enrichment run is not committed: {run_id}")
        if _required_text(row, "source_id") != source_id:
            raise GoldManifestError(f"enrichment source mismatch: {run_id}")
        if _required_text(row, "source_release_id") != source_release_id:
            raise GoldManifestError(f"enrichment release mismatch: {run_id}")
        expected_reviews = int(row["expected_review_count"])
        actual_reviews = int(row["actual_review_count"])
        expected_aspects = int(row["expected_aspect_count"])
        actual_aspects = int(row["actual_aspect_count"])
        if expected_reviews != actual_reviews or expected_aspects != actual_aspects:
            raise GoldManifestError(f"enrichment count mismatch: {run_id}")
        model_id = _required_text(row, "model_id")
        model_fp = _required_text(row, "model_fingerprint")
        config_fp = _required_text(row, "config_fingerprint")
        lineage.add((model_id, model_fp, config_fp))
        result.append(
            {
                "processing_run_id": run_id,
                "input_fingerprint": _required_text(row, "input_fingerprint"),
                "model_id": model_id,
                "model_fingerprint": model_fp,
                "config_fingerprint": config_fp,
                "review_count": actual_reviews,
                "aspect_count": actual_aspects,
                "review_snapshot_id": _required_text(row, "review_snapshot_id"),
                "aspect_snapshot_id": _required_text(row, "aspect_snapshot_id"),
                "started_at": _required_text(row, "started_at"),
            }
        )
    if len(lineage) != 1:
        raise GoldManifestError("enrichment runs contain mixed model/config lineage")
    result.sort(key=lambda row: (row["started_at"], row["processing_run_id"]))
    return result, next(iter(lineage))


def build_gold_manifest(
    normalization: Mapping[str, Any],
    enrichment_runs: Sequence[Mapping[str, Any]],
    *,
    source_id: str,
    source_release_id: str,
    contract_fingerprint: str,
    config_fingerprint: str,
    qdrant_generation: Mapping[str, Any] | None = None,
    deduplication: Mapping[str, Any] | None = None,
    geography_snapshot: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a canonical manifest without writing it or creating Gold objects."""
    if not contract_fingerprint.startswith("sha256:"):
        raise GoldManifestError("contract fingerprint must be SHA-256")
    if not config_fingerprint.startswith("sha256:"):
        raise GoldManifestError("config fingerprint must be SHA-256")
    normalized = _validated_normalization(
        normalization, source_id=source_id, source_release_id=source_release_id
    )
    runs, (model_id, model_fp, enrichment_config_fp) = _validated_enrichment_runs(
        enrichment_runs, source_id=source_id, source_release_id=source_release_id
    )
    review_count = sum(row["review_count"] for row in runs)
    aspect_count = sum(row["aspect_count"] for row in runs)
    deduplication_payload = None
    effective_review_count = review_count
    effective_aspect_count = aspect_count
    if deduplication is not None:
        required_deduplication = {
            "raw_candidate_review_count",
            "raw_candidate_aspect_count",
            "effective_review_count",
            "effective_aspect_count",
            "deduplication_policy_version",
            "deduplication_policy_fingerprint",
            "review_supersessions",
            "aspect_supersessions",
            "superseded_generation_counts",
        }
        missing = sorted(required_deduplication - deduplication.keys())
        if missing:
            raise GoldManifestError(
                "deduplication evidence is incomplete: " + ", ".join(missing)
            )
        if int(deduplication["raw_candidate_review_count"]) != review_count:
            raise GoldManifestError(
                "deduplication raw review count does not match runs"
            )
        if int(deduplication["raw_candidate_aspect_count"]) != aspect_count:
            raise GoldManifestError(
                "deduplication raw aspect count does not match runs"
            )
        effective_review_count = int(deduplication["effective_review_count"])
        effective_aspect_count = int(deduplication["effective_aspect_count"])
        if effective_review_count < 0 or effective_review_count > review_count:
            raise GoldManifestError("deduplication effective review count is invalid")
        if effective_aspect_count < 0 or effective_aspect_count > aspect_count:
            raise GoldManifestError("deduplication effective aspect count is invalid")
        if not str(deduplication["deduplication_policy_version"]):
            raise GoldManifestError("deduplication policy version is required")
        if not str(deduplication["deduplication_policy_fingerprint"]).startswith(
            "sha256:"
        ):
            raise GoldManifestError("deduplication policy fingerprint must be SHA-256")
        deduplication_payload = {
            key: deduplication[key] for key in sorted(required_deduplication)
        }
    geography_payload = None
    if geography_snapshot is not None:
        required_geography = {
            "table",
            "snapshot_id",
            "taxonomy_version",
            "taxonomy_fingerprint",
        }
        missing = sorted(required_geography - geography_snapshot.keys())
        if missing:
            raise GoldManifestError(
                "geography snapshot evidence is incomplete: " + ", ".join(missing)
            )
        if geography_snapshot["table"] != "travel.silver.place":
            raise GoldManifestError("geography snapshot must pin travel.silver.place")
        if not str(geography_snapshot["snapshot_id"]):
            raise GoldManifestError("geography snapshot ID is required")
        if not str(geography_snapshot["taxonomy_version"]):
            raise GoldManifestError("geography taxonomy version is required")
        if not str(geography_snapshot["taxonomy_fingerprint"]).startswith("sha256:"):
            raise GoldManifestError("geography taxonomy fingerprint must be SHA-256")
        geography_payload = {
            key: geography_snapshot[key] for key in sorted(required_geography)
        }
    qdrant = None
    if qdrant_generation is not None:
        if qdrant_generation.get("status") != "committed":
            raise GoldManifestError("Qdrant generation is not committed")
        if _required_text(qdrant_generation, "source_release_id") != source_release_id:
            raise GoldManifestError("Qdrant source release mismatch")
        if (
            int(qdrant_generation["expected_input_review_count"])
            != effective_review_count
        ):
            raise GoldManifestError("Qdrant review input count mismatch")
        if (
            int(qdrant_generation["expected_input_aspect_count"])
            != effective_aspect_count
        ):
            raise GoldManifestError("Qdrant aspect input count mismatch")
        qdrant = {
            "index_generation": _required_text(qdrant_generation, "index_generation"),
            "input_manifest_id": _required_text(qdrant_generation, "input_manifest_id"),
            "input_manifest_sha256": _required_text(
                qdrant_generation, "input_manifest_sha256"
            ),
            "review_point_count": int(qdrant_generation["actual_review_point_count"]),
            "aspect_point_count": int(qdrant_generation["actual_aspect_point_count"]),
            "effective_review_count": effective_review_count,
            "effective_aspect_count": effective_aspect_count,
        }
    content: dict[str, Any] = {
        "contract_version": GOLD_CONTRACT_VERSION,
        "source_id": source_id,
        "source_release_id": source_release_id,
        "contract_fingerprint": contract_fingerprint,
        "config_fingerprint": config_fingerprint,
        "normalization": normalized,
        "enrichment": {
            "runs": runs,
            "run_count": len(runs),
            "review_count": review_count,
            "aspect_count": aspect_count,
            "raw_candidate_review_count": review_count,
            "raw_candidate_aspect_count": aspect_count,
            "effective_review_count": effective_review_count,
            "effective_aspect_count": effective_aspect_count,
            "model_id": model_id,
            "model_fingerprint": model_fp,
            "config_fingerprint": enrichment_config_fp,
        },
        "deduplication": deduplication_payload,
        "geography_snapshot": geography_payload,
        "qdrant_generation": qdrant,
    }
    digest = hashlib.sha256(_canonical_bytes(content)).hexdigest()
    return {
        **content,
        "manifest_id": f"gold-input-{digest}",
        "manifest_sha256": f"sha256:{digest}",
    }


def candidate_ids(manifest: Mapping[str, Any]) -> tuple[str, str]:
    manifest_sha = _required_text(manifest, "manifest_sha256")
    contract_fp = _required_text(manifest, "contract_fingerprint")
    config_fp = _required_text(manifest, "config_fingerprint")
    value = {
        "manifest_sha256": manifest_sha,
        "contract_fingerprint": contract_fp,
        "config_fingerprint": config_fp,
    }
    digest = hashlib.sha256(_canonical_bytes(value)).hexdigest()
    return f"gold-run-{digest}", f"gold-release-{digest}"


def transition_status(current: str, target: str) -> str:
    if current not in LEGAL_TRANSITIONS:
        raise GoldRunStateError(f"unknown current Gold run state: {current}")
    if target not in RUN_STATES:
        raise GoldRunStateError(f"unknown target Gold run state: {target}")
    if target not in LEGAL_TRANSITIONS[current]:
        raise GoldRunStateError(f"illegal Gold run transition: {current} -> {target}")
    return target


def committed_replay_is_noop(
    existing: Mapping[str, Any] | None,
    *,
    manifest: Mapping[str, Any],
    required_tables: Sequence[str],
    snapshot_exists: Any,
) -> bool:
    if existing is None:
        return False
    gold_run_id, release_id = candidate_ids(manifest)
    expected = {
        "gold_run_id": gold_run_id,
        "release_id": release_id,
        "input_manifest_id": manifest["manifest_id"],
        "input_manifest_sha256": manifest["manifest_sha256"],
        "contract_fingerprint": manifest["contract_fingerprint"],
        "config_fingerprint": manifest["config_fingerprint"],
    }
    conflicts = [key for key, value in expected.items() if existing.get(key) != value]
    if conflicts:
        raise GoldManifestError(
            "existing Gold run conflicts with candidate: " + ", ".join(conflicts)
        )
    if existing.get("status") != "committed":
        return False
    refs = existing.get("output_snapshot_refs")
    if not isinstance(refs, Mapping):
        raise GoldManifestError("committed Gold run has no output snapshots")
    missing = [table for table in required_tables if not str(refs.get(table) or "")]
    if missing:
        raise GoldManifestError(
            "committed Gold run is missing snapshots: " + ", ".join(missing)
        )
    absent = [
        table for table in required_tables if not snapshot_exists(table, refs[table])
    ]
    if absent:
        raise GoldManifestError(
            "committed Gold run references absent snapshots: " + ", ".join(absent)
        )
    return True


@dataclass
class CandidateRun:
    manifest: Mapping[str, Any]
    status: str = "planned"
    output_snapshot_refs: dict[str, str] = field(default_factory=dict)

    @property
    def identities(self) -> tuple[str, str]:
        return candidate_ids(self.manifest)

    def transition(self, target: str) -> None:
        if target == "committed":
            raise GoldRunStateError(
                "use commit() so required output snapshots are revalidated"
            )
        self.status = transition_status(self.status, target)

    def record_snapshot(self, table: str, snapshot_id: str) -> None:
        if self.status not in {"building", "partial", "validated"}:
            raise GoldRunStateError("snapshots can only be recorded during a build")
        if not table or not snapshot_id:
            raise GoldRunStateError("table and snapshot ID are required")
        self.output_snapshot_refs[table] = snapshot_id

    def validate(self, required_tables: Sequence[str]) -> None:
        missing = [
            table
            for table in required_tables
            if not self.output_snapshot_refs.get(table)
        ]
        if missing:
            raise GoldRunStateError(
                "candidate outputs are incomplete: " + ", ".join(missing)
            )
        self.transition("validated")

    def commit(self, required_tables: Sequence[str]) -> None:
        """Commit only a validated candidate whose required snapshots remain present."""
        if self.status != "validated":
            raise GoldRunStateError("only a validated candidate can be committed")
        missing = [
            table
            for table in required_tables
            if not self.output_snapshot_refs.get(table)
        ]
        if missing:
            raise GoldRunStateError(
                "candidate outputs are incomplete: " + ", ".join(missing)
            )
        self.status = transition_status(self.status, "committed")


__all__ = [
    "CandidateRun",
    "GoldManifestError",
    "GoldRunStateError",
    "LEGAL_TRANSITIONS",
    "REQUIRED_SILVER_SNAPSHOTS",
    "RUN_STATES",
    "build_gold_manifest",
    "candidate_ids",
    "committed_replay_is_noop",
    "transition_status",
]
