"""Read-only TripAdvisor inventory, eligibility, and coverage contracts.

This module deliberately separates deterministic inventory decisions from the
runtime adapters that obtain MinIO, Iceberg, and Qdrant rows.  It does not
write to any external system and does not start a processing stage.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

TRIPADVISOR_SOURCE_ID = "tripadvisor"
BRONZE_CONTRACT_VERSION = "bronze-contract-v2"
BRONZE_SCHEMA_VERSION = "bronze-v2"
TRIPADVISOR_REQUIRED_RELEASE_TABLES = (
    "source_record",
    "place",
    "place_rating",
    "review",
    "source_entity_mapping",
    "normalization_run",
)
CATALOG_FILES = (
    "README.md",
    "architecture.md",
    "bronze.md",
    "silver.md",
    "enrichment.md",
    "gold.md",
    "qdrant.md",
    "data-lineage.md",
)


def _text(value: Any) -> str:
    return "" if value is None else str(value)


def _nonempty(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _as_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if hasattr(value, "asDict"):
        return value.asDict(recursive=True)
    raise TypeError(f"expected mapping-like row, received {type(value).__name__}")


def _json_default(value: Any) -> str:
    """Serialize runtime scalar values without losing their stable text form."""
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def catalog_fingerprints(root: str | Path) -> tuple[dict[str, str], tuple[str, ...]]:
    """Return deterministic catalog hashes and missing relative paths."""
    base = Path(root)
    hashes: dict[str, str] = {}
    missing: list[str] = []
    for relative in CATALOG_FILES:
        path = base / relative
        if not path.is_file():
            missing.append(relative)
            continue
        hashes[relative] = "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()
    return dict(sorted(hashes.items())), tuple(missing)


def classify_provenance(
    manifest: Mapping[str, Any],
    *,
    synthetic_input_hashes: Sequence[str] = (),
) -> str:
    """Classify only from stored manifest evidence; otherwise return unknown."""
    explicit = next(
        (
            manifest.get(key)
            for key in ("provenance_class", "provenance", "data_origin", "dataset_type")
            if manifest.get(key) is not None
        ),
        None,
    )
    value = _text(explicit).strip().lower()
    if value in {"synthetic", "fixture", "test", "mock"}:
        return "synthetic"
    if value in {"real", "crawler", "production", "live"}:
        return "real_crawler"
    source_file = _text(manifest.get("source_file")).lower()
    if any(
        marker in source_file for marker in ("tests/fixtures", "fixture", "synthetic")
    ):
        return "synthetic"
    if any(marker in source_file for marker in ("crawler", "production", "live")):
        return "real_crawler"
    input_hash = _text(manifest.get("input_sha256")).removeprefix("sha256:")
    known_hashes = {value.removeprefix("sha256:") for value in synthetic_input_hashes}
    if input_hash and input_hash in known_hashes:
        return "synthetic"
    return "unknown"


def required_snapshot_tables(source_id: str) -> tuple[str, ...]:
    if source_id == TRIPADVISOR_SOURCE_ID:
        return TRIPADVISOR_REQUIRED_RELEASE_TABLES
    return ()


def snapshot_completeness(
    release: Mapping[str, Any],
    required: Sequence[str] = TRIPADVISOR_REQUIRED_RELEASE_TABLES,
) -> tuple[bool, tuple[str, ...]]:
    refs = release.get("table_snapshot_refs")
    if not isinstance(refs, Mapping):
        return False, tuple(required)
    missing = tuple(table for table in required if not _nonempty(refs.get(table)))
    return not missing, missing


def bronze_manifest_reasons(manifest: Mapping[str, Any]) -> tuple[str, ...]:
    reasons: list[str] = []
    if manifest.get("source_id") != TRIPADVISOR_SOURCE_ID:
        reasons.append("source_not_tripadvisor")
    if manifest.get("status") != "success":
        reasons.append("bronze_manifest_not_success")
    if manifest.get("contract_version") != BRONZE_CONTRACT_VERSION:
        reasons.append("bronze_contract_ineligible")
    if manifest.get("schema_version") != BRONZE_SCHEMA_VERSION:
        reasons.append("bronze_schema_ineligible")
    hashes = manifest.get("payload_hashes")
    row_count = manifest.get("row_count")
    if not isinstance(hashes, list) or row_count != len(hashes):
        reasons.append("bronze_row_hash_count_mismatch")
    if not _nonempty(manifest.get("batch_id")):
        reasons.append("missing_batch_id")
    return tuple(reasons)


@dataclass(frozen=True)
class ReviewOccurrence:
    source_id: str
    batch_id: str
    source_row_index: int
    source_record_sk: str
    review_id: str
    review_ordinal: int
    place_id: str | None
    review_text_hash: str | None = None

    @property
    def occurrence_key(self) -> tuple[str, str, int, str, int]:
        return (
            self.source_id,
            self.source_record_sk,
            self.source_row_index,
            self.review_id,
            self.review_ordinal,
        )

    @property
    def logical_review_key(self) -> str:
        return self.review_id


@dataclass(frozen=True)
class EligibilityDecision:
    occurrence: ReviewOccurrence
    eligible: bool
    reasons: tuple[str, ...]
    provenance_class: str


@dataclass
class InventoryReport:
    status: str
    catalog: dict[str, Any]
    policy: dict[str, Any]
    counts: dict[str, int]
    bronze_batches: list[dict[str, Any]] = field(default_factory=list)
    normalization_releases: list[dict[str, Any]] = field(default_factory=list)
    review_occurrences: list[dict[str, Any]] = field(default_factory=list)
    enrichment_generations: list[dict[str, Any]] = field(default_factory=list)
    index_generations: list[dict[str, Any]] = field(default_factory=list)
    qdrant_collections: list[dict[str, Any]] = field(default_factory=list)
    exclusions: list[dict[str, Any]] = field(default_factory=list)
    eligible_work_units: list[dict[str, Any]] = field(default_factory=list)
    reconciliation: dict[str, Any] = field(default_factory=dict)
    blockers: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def to_json(self) -> str:
        return (
            json.dumps(
                self.to_dict(),
                default=_json_default,
                ensure_ascii=False,
                sort_keys=True,
                indent=2,
            )
            + "\n"
        )


def _occurrence_from_row(row: Mapping[str, Any]) -> ReviewOccurrence:
    return ReviewOccurrence(
        source_id=_text(row.get("source_id")),
        batch_id=_text(row.get("batch_id")),
        source_row_index=int(row["source_row_index"]),
        source_record_sk=_text(row.get("source_record_sk")),
        review_id=_text(row.get("review_id")),
        review_ordinal=int(row["review_ordinal"]),
        place_id=row.get("place_id"),
        review_text_hash=row.get("review_text_hash"),
    )


def _normalization_for_batch(
    releases: Iterable[Mapping[str, Any]], batch_id: str
) -> list[dict[str, Any]]:
    return [
        _as_dict(row)
        for row in releases
        if _text(row.get("bronze_batch_id")) == batch_id
    ]


def evaluate_occurrences(
    reviews: Iterable[Mapping[str, Any]],
    source_records: Iterable[Mapping[str, Any]],
    releases: Iterable[Mapping[str, Any]],
    manifests: Iterable[Mapping[str, Any]],
    *,
    permitted_provenance: Sequence[str] = ("synthetic",),
    synthetic_input_hashes: Sequence[str] = (),
) -> tuple[tuple[EligibilityDecision, ...], tuple[dict[str, Any], ...]]:
    """Evaluate occurrence eligibility without collapsing logical reviews."""
    records = {
        _text(row.get("source_record_sk")): _as_dict(row) for row in source_records
    }
    release_rows = [_as_dict(row) for row in releases]
    manifest_rows = {_text(row.get("batch_id")): _as_dict(row) for row in manifests}
    seen: set[tuple[str, str, int, str, int]] = set()
    decisions: list[EligibilityDecision] = []
    exclusions: list[dict[str, Any]] = []
    for raw in sorted(
        (_as_dict(row) for row in reviews),
        key=lambda item: json.dumps(item, sort_keys=True, default=str),
    ):
        reasons: list[str] = []
        try:
            occurrence = _occurrence_from_row(raw)
        except (KeyError, TypeError, ValueError):
            fallback = ReviewOccurrence(
                source_id=_text(raw.get("source_id")),
                batch_id=_text(raw.get("batch_id")),
                source_row_index=-1,
                source_record_sk=_text(raw.get("source_record_sk")),
                review_id=_text(raw.get("review_id")),
                review_ordinal=-1,
                place_id=raw.get("place_id"),
            )
            decisions.append(
                EligibilityDecision(
                    fallback, False, ("invalid_occurrence_identity",), "unknown"
                )
            )
            continue
        manifest = manifest_rows.get(occurrence.batch_id)
        provenance = classify_provenance(
            manifest or {}, synthetic_input_hashes=synthetic_input_hashes
        )
        if occurrence.source_id != TRIPADVISOR_SOURCE_ID:
            reasons.append("source_not_tripadvisor")
        if not _nonempty(occurrence.batch_id):
            reasons.append("missing_batch_id")
        if (
            not _nonempty(occurrence.source_record_sk)
            or occurrence.source_row_index < 0
        ):
            reasons.append("missing_source_occurrence_lineage")
        if not _nonempty(occurrence.review_id) or occurrence.review_ordinal < 0:
            reasons.append("missing_review_identity")
        record = records.get(occurrence.source_record_sk)
        if record is None:
            reasons.append("missing_source_record")
        else:
            if record.get("source_id") != TRIPADVISOR_SOURCE_ID:
                reasons.append("source_record_source_mismatch")
            if _text(record.get("batch_id")) != occurrence.batch_id:
                reasons.append("source_record_batch_mismatch")
            if record.get("source_row_index") != occurrence.source_row_index:
                reasons.append("source_record_row_mismatch")
        matching_releases = _normalization_for_batch(release_rows, occurrence.batch_id)
        if not matching_releases:
            reasons.append("normalization_release_missing")
        else:
            committed_complete = False
            for release in matching_releases:
                complete, _ = snapshot_completeness(release)
                if (
                    release.get("source_id") == TRIPADVISOR_SOURCE_ID
                    and release.get("status") == "committed"
                    and complete
                ):
                    committed_complete = True
                    break
            if not committed_complete:
                reasons.append("normalization_release_not_committed_or_incomplete")
        if manifest is None:
            reasons.append("bronze_manifest_missing")
        else:
            reasons.extend(bronze_manifest_reasons(manifest))
        if provenance not in permitted_provenance:
            reasons.append("provenance_not_permitted")
        if not _nonempty(raw.get("review_text")) and not _nonempty(
            raw.get("detail_review")
        ):
            reasons.append("empty_review_text")
        if occurrence.occurrence_key in seen:
            reasons.append("duplicate_source_occurrence")
        seen.add(occurrence.occurrence_key)
        unique_reasons = tuple(dict.fromkeys(reasons))
        decision = EligibilityDecision(
            occurrence, not unique_reasons, unique_reasons, provenance
        )
        decisions.append(decision)
        if unique_reasons:
            exclusions.append(
                {
                    "occurrence_key": list(occurrence.occurrence_key),
                    "logical_review_key": occurrence.logical_review_key,
                    "reasons": list(unique_reasons),
                    "provenance_class": provenance,
                }
            )
    return tuple(decisions), tuple(exclusions)


def generation_coverage(
    eligible: Sequence[EligibilityDecision],
    sentiment_rows: Iterable[Mapping[str, Any]],
    enrichment_runs: Iterable[Mapping[str, Any]],
    index_runs: Iterable[Mapping[str, Any]],
    aspect_rows: Iterable[Mapping[str, Any]] = (),
) -> dict[str, Any]:
    """Reconcile committed generation coverage using exact lineage keys."""
    sentiment_rows_list = [_as_dict(row) for row in sentiment_rows]
    aspect_rows_list = [_as_dict(row) for row in aspect_rows]
    eligible_keys = {
        item.occurrence.occurrence_key for item in eligible if item.eligible
    }
    committed_enrichment = {
        _text(row.get("processing_run_id")): _as_dict(row)
        for row in enrichment_runs
        if row.get("status") == "committed"
    }
    sentiment_by_key: dict[tuple[str, str, str, int], list[dict[str, Any]]] = {}
    for item in sentiment_rows_list:
        key = (
            _text(item.get("source_record_sk")),
            _text(item.get("batch_id")),
            _text(item.get("review_id")),
            int(item["source_row_index"])
            if item.get("source_row_index") is not None
            else -1,
        )
        sentiment_by_key.setdefault(key, []).append(item)
    covered: set[tuple[str, str, str, int]] = set()
    inconsistent: list[dict[str, Any]] = []
    for key, rows in sentiment_by_key.items():
        valid = [
            row
            for row in rows
            if _text(row.get("processing_run_id")) in committed_enrichment
        ]
        if valid:
            covered.add(key)
        else:
            inconsistent.append(
                {"key": list(key), "reason": "no_committed_enrichment_run"}
            )
    committed_indexes = [
        _as_dict(row)
        for row in index_runs
        if row.get("status") == "committed"
        and row.get("expected_review_point_count")
        == row.get("actual_review_point_count")
        and row.get("expected_aspect_point_count")
        == row.get("actual_aspect_point_count")
    ]
    enrichment_reconciliation = []
    for run_id, run in sorted(committed_enrichment.items()):
        review_count = sum(
            _text(row.get("processing_run_id")) == run_id for row in sentiment_rows_list
        )
        aspect_count = sum(
            _text(row.get("processing_run_id")) == run_id for row in aspect_rows_list
        )
        enrichment_reconciliation.append(
            {
                "processing_run_id": run_id,
                "expected_review_count": run.get("expected_review_count"),
                "actual_review_count_ledger": run.get("actual_review_count"),
                "actual_review_count_observed": review_count,
                "expected_aspect_count": run.get("expected_aspect_count"),
                "actual_aspect_count_ledger": run.get("actual_aspect_count"),
                "actual_aspect_count_observed": aspect_count,
                "counts_reconcile": (
                    run.get("expected_review_count")
                    == review_count
                    == run.get("actual_review_count")
                    and run.get("expected_aspect_count")
                    == aspect_count
                    == run.get("actual_aspect_count")
                ),
            }
        )
    index_generation_ids = sorted(
        _text(row.get("index_generation")) for row in committed_indexes
    )
    eligible_lineage_keys = {
        (
            item.occurrence.source_record_sk,
            item.occurrence.batch_id,
            item.occurrence.review_id,
            item.occurrence.source_row_index,
        )
        for item in eligible
        if item.eligible
    }
    return {
        "eligible_occurrence_count": len(eligible_keys),
        "eligible_logical_review_count": len(
            {item.occurrence.logical_review_key for item in eligible if item.eligible}
        ),
        "sentiment_covered_occurrence_count": len(covered & eligible_lineage_keys),
        "sentiment_uncovered_eligible_count": len(eligible_lineage_keys - covered),
        "committed_enrichment_run_count": len(committed_enrichment),
        "committed_index_generation_count": len(committed_indexes),
        "committed_index_generations": index_generation_ids,
        "enrichment_reconciliation": enrichment_reconciliation,
        "inconsistent_sentiment_lineage_count": len(inconsistent),
        "inconsistent_sentiment_lineage": sorted(
            inconsistent, key=lambda item: json.dumps(item, sort_keys=True)
        ),
    }


def build_inventory_report(
    *,
    catalog_root: str | Path,
    manifests: Iterable[Mapping[str, Any]] = (),
    normalization_releases: Iterable[Mapping[str, Any]] = (),
    source_records: Iterable[Mapping[str, Any]] = (),
    reviews: Iterable[Mapping[str, Any]] = (),
    sentiment_rows: Iterable[Mapping[str, Any]] = (),
    aspect_rows: Iterable[Mapping[str, Any]] = (),
    enrichment_runs: Iterable[Mapping[str, Any]] = (),
    index_runs: Iterable[Mapping[str, Any]] = (),
    qdrant_collections: Iterable[Mapping[str, Any]] = (),
    runtime_status: str = "not_connected",
    blockers: Sequence[str] = (),
    synthetic_input_hashes: Sequence[str] = (),
) -> InventoryReport:
    manifest_list = [_as_dict(row) for row in manifests]
    releases = [_as_dict(row) for row in normalization_releases]
    records = [_as_dict(row) for row in source_records]
    review_rows = [_as_dict(row) for row in reviews]
    sentiment = [_as_dict(row) for row in sentiment_rows]
    aspects = [_as_dict(row) for row in aspect_rows]
    enrich = [_as_dict(row) for row in enrichment_runs]
    indexes = [_as_dict(row) for row in index_runs]
    qdrant = [_as_dict(row) for row in qdrant_collections]
    decisions, exclusions = evaluate_occurrences(
        review_rows,
        records,
        releases,
        manifest_list,
        synthetic_input_hashes=synthetic_input_hashes,
    )
    eligible = [item for item in decisions if item.eligible]
    provenance_counts: dict[str, int] = {}
    for manifest in manifest_list:
        provenance = classify_provenance(
            manifest, synthetic_input_hashes=synthetic_input_hashes
        )
        provenance_counts[provenance] = provenance_counts.get(provenance, 0) + 1
    release_summary = []
    for release in sorted(
        releases, key=lambda item: json.dumps(item, sort_keys=True, default=str)
    ):
        complete, missing = snapshot_completeness(release)
        release_summary.append(
            {
                "run_id": release.get("run_id"),
                "source_id": release.get("source_id"),
                "bronze_batch_id": release.get("bronze_batch_id"),
                "status": release.get("status"),
                "snapshot_complete": complete,
                "missing_snapshot_tables": list(missing),
            }
        )
    coverage = generation_coverage(eligible, sentiment, enrich, indexes, aspects)
    counts = {
        "bronze_manifest_count": len(manifest_list),
        "tripadvisor_bronze_manifest_count": sum(
            item.get("source_id") == TRIPADVISOR_SOURCE_ID for item in manifest_list
        ),
        "normalization_release_count": len(releases),
        "tripadvisor_normalization_release_count": sum(
            item.get("source_id") == TRIPADVISOR_SOURCE_ID for item in releases
        ),
        "source_record_count": len(records),
        "review_row_count": len(review_rows),
        "eligible_occurrence_count": coverage["eligible_occurrence_count"],
        "eligible_logical_review_count": coverage["eligible_logical_review_count"],
        "excluded_occurrence_count": len(exclusions),
        "sentiment_row_count": len(sentiment),
        "enrichment_run_count": len(enrich),
        "index_run_count": len(indexes),
        "qdrant_collection_count": len(qdrant),
    }
    catalog_hashes, missing_catalog = catalog_fingerprints(catalog_root)
    all_blockers = list(blockers)
    if missing_catalog:
        all_blockers.append("missing_catalog_files:" + ",".join(missing_catalog))
    if runtime_status != "verified":
        all_blockers.append("runtime_metadata_not_verified")
    # Unknown provenance is an explicit inventory classification. It remains
    # visible in counts and reconciliation, but does not invalidate a
    # read-only runtime verification when those batches are not eligible.
    return InventoryReport(
        status="IMPLEMENTED / NOT VERIFIED" if all_blockers else "IMPLEMENTED",
        catalog={
            "root": str(Path(catalog_root)),
            "files": catalog_hashes,
            "missing_files": list(missing_catalog),
            "runtime_status": runtime_status,
        },
        policy={
            "source_id": TRIPADVISOR_SOURCE_ID,
            "permitted_provenance": ["synthetic"],
            "required_snapshot_tables": list(TRIPADVISOR_REQUIRED_RELEASE_TABLES),
            "occurrence_identity": [
                "source_id",
                "source_record_sk",
                "source_row_index",
                "review_id",
                "review_ordinal",
            ],
            "logical_review_identity": "review_id",
        },
        counts={**counts, "provenance_manifest_counts": provenance_counts},
        bronze_batches=sorted(
            manifest_list,
            key=lambda item: (
                _text(item.get("source_id")),
                _text(item.get("batch_id")),
            ),
        ),
        normalization_releases=release_summary,
        review_occurrences=[
            {
                "occurrence_key": list(item.occurrence.occurrence_key),
                "logical_review_key": item.occurrence.logical_review_key,
                "eligible": item.eligible,
                "reasons": list(item.reasons),
                "provenance_class": item.provenance_class,
            }
            for item in sorted(
                decisions, key=lambda item: item.occurrence.occurrence_key
            )
        ],
        enrichment_generations=sorted(
            enrich, key=lambda item: _text(item.get("processing_run_id"))
        ),
        index_generations=sorted(
            indexes, key=lambda item: _text(item.get("index_generation"))
        ),
        qdrant_collections=sorted(qdrant, key=lambda item: _text(item.get("name"))),
        exclusions=list(exclusions),
        eligible_work_units=[
            {
                "source_id": item.occurrence.source_id,
                "batch_id": item.occurrence.batch_id,
                "source_record_sk": item.occurrence.source_record_sk,
                "source_row_index": item.occurrence.source_row_index,
                "review_id": item.occurrence.review_id,
                "review_ordinal": item.occurrence.review_ordinal,
            }
            for item in sorted(
                eligible, key=lambda item: item.occurrence.occurrence_key
            )
        ],
        reconciliation={
            "coverage": coverage,
            "provenance_manifest_counts": provenance_counts,
        },
        blockers=sorted(set(all_blockers)),
    )


def human_summary(report: InventoryReport) -> str:
    counts = report.counts
    lines = [
        "TASK-005A TripAdvisor historical inventory",
        f"status: {report.status}",
        f"runtime: {report.catalog.get('runtime_status', 'see report blockers')}",
        (
            f"Bronze manifests: {counts['bronze_manifest_count']} total / "
            f"{counts['tripadvisor_bronze_manifest_count']} TripAdvisor"
        ),
        (
            f"Normalization releases: {counts['normalization_release_count']} total / "
            f"{counts['tripadvisor_normalization_release_count']} TripAdvisor"
        ),
        (
            f"Review rows: {counts['review_row_count']}; "
            f"eligible occurrences: {counts['eligible_occurrence_count']}; "
            f"eligible logical reviews: {counts['eligible_logical_review_count']}"
        ),
        f"Excluded occurrences: {counts['excluded_occurrence_count']}",
        (
            f"Sentiment rows/runs: {counts['sentiment_row_count']}/"
            f"{counts['enrichment_run_count']}; index runs: "
            f"{counts['index_run_count']}; Qdrant collections: "
            f"{counts['qdrant_collection_count']}"
        ),
        "provenance: "
        + json.dumps(counts.get("provenance_manifest_counts", {}), sort_keys=True),
    ]
    if report.blockers:
        lines.append("blockers: " + "; ".join(report.blockers))
    return "\n".join(lines) + "\n"


__all__ = [
    "CATALOG_FILES",
    "EligibilityDecision",
    "InventoryReport",
    "ReviewOccurrence",
    "TRIPADVISOR_REQUIRED_RELEASE_TABLES",
    "build_inventory_report",
    "catalog_fingerprints",
    "classify_provenance",
    "evaluate_occurrences",
    "generation_coverage",
    "human_summary",
    "snapshot_completeness",
]
