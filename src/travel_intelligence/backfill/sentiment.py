"""Read-only planning and guarded execution for TASK-005D.

This module validates enrichment coverage at the occurrence level.  It does
not run a model or write Iceberg rows; execution is delegated to the existing
TASK-004 driver by the runtime adapter.
"""

from __future__ import annotations

import json
import subprocess
from dataclasses import asdict, dataclass
from typing import Any, Callable, Iterable, Mapping, Sequence

from travel_intelligence.sentiment.contracts import text_hash
from travel_intelligence.silver.tripadvisor_source import (
    TRIPADVISOR_SOURCE_ID,
    TripAdvisorReviewOccurrence,
    validate_tripadvisor_release,
)

from .inventory import bronze_manifest_reasons, classify_provenance

VISOBERT_MODEL_ID = "5CD-AI/Vietnamese-Sentiment-visobert"
VISOBERT_MODEL_REVISION = "d7738b85200272f10ba8b048d80236c8c582b581"


@dataclass(frozen=True)
class SentimentDecision:
    occurrence_key: tuple[str, str, str, int, int]
    source_release_id: str
    review_id: str
    batch_id: str
    source_record_sk: str
    status: str
    reasons: tuple[str, ...] = ()
    compatible_processing_run_id: str | None = None


@dataclass(frozen=True)
class SentimentPlan:
    model_id: str
    model_revision: str
    config_fingerprint: str
    decisions: tuple[SentimentDecision, ...]
    invalid_historical_runs: tuple[dict[str, Any], ...] = ()
    max_occurrences: int | None = None
    selected_occurrence_keys: tuple[tuple[str, str, str, int, int], ...] = ()

    @property
    def missing(self) -> tuple[SentimentDecision, ...]:
        selected = set(self.selected_occurrence_keys)
        return tuple(
            item
            for item in self.decisions
            if item.status == "missing" and item.occurrence_key in selected
        )

    def to_dict(self) -> dict[str, Any]:
        decisions = [asdict(item) for item in self.decisions]
        for item in decisions:
            item["occurrence_key"] = list(item["occurrence_key"])
        return {
            "model_id": self.model_id,
            "model_revision": self.model_revision,
            "config_fingerprint": self.config_fingerprint,
            "max_occurrences": self.max_occurrences,
            "selected_occurrence_keys": [
                list(item) for item in self.selected_occurrence_keys
            ],
            "decisions": decisions,
            "counts": {
                "eligible_occurrence_count": sum(
                    item.status in {"already_enriched", "missing"}
                    for item in self.decisions
                ),
                "already_enriched_count": sum(
                    item.status == "already_enriched" for item in self.decisions
                ),
                "missing_enrichment_count": sum(
                    item.status == "missing" for item in self.decisions
                ),
                "excluded_occurrence_count": sum(
                    item.status == "excluded" for item in self.decisions
                ),
            },
            "selected_missing_count": len(self.missing),
            "invalid_historical_runs": list(self.invalid_historical_runs),
            "invalid_historical_generation_count": len(self.invalid_historical_runs),
        }

    def to_json(self) -> str:
        return (
            json.dumps(self.to_dict(), ensure_ascii=False, sort_keys=True, indent=2)
            + "\n"
        )


@dataclass(frozen=True)
class SentimentExecutionResult:
    status: str
    planned_occurrence_keys: tuple[tuple[str, str, str, int, int], ...]
    completed_occurrence_keys: tuple[tuple[str, str, str, int, int], ...] = ()
    failed_occurrence_keys: tuple[tuple[str, str, str, int, int], ...] = ()
    errors: tuple[str, ...] = ()


def _occurrence_key(
    occurrence: TripAdvisorReviewOccurrence,
) -> tuple[str, str, str, int, int]:
    return (
        occurrence.source_record_sk,
        occurrence.batch_id,
        occurrence.review_id,
        occurrence.source_row_index,
        occurrence.review_ordinal,
    )


def _text(value: Any) -> str:
    return "" if value is None else str(value)


def _run_is_compatible(
    run: Mapping[str, Any],
    *,
    release_id: str,
    model_id: str,
    config_fingerprint: str,
    sentiment_rows: Sequence[Mapping[str, Any]],
    aspect_rows: Sequence[Mapping[str, Any]],
    model_fingerprint: str | None,
) -> tuple[bool, tuple[str, ...]]:
    reasons: list[str] = []
    if run.get("source_id") != TRIPADVISOR_SOURCE_ID:
        reasons.append("enrichment_run_source_not_tripadvisor")
    if run.get("source_release_id") != release_id:
        reasons.append("enrichment_run_release_mismatch")
    if run.get("status") != "committed":
        reasons.append("enrichment_run_not_committed")
    if run.get("model_id") != model_id:
        reasons.append("model_id_mismatch")
    if run.get("config_fingerprint") != config_fingerprint:
        reasons.append("config_fingerprint_mismatch")
    if (
        model_fingerprint is not None
        and run.get("model_fingerprint") != model_fingerprint
    ):
        reasons.append("model_fingerprint_mismatch")
    if run.get("expected_review_count") != run.get("actual_review_count"):
        reasons.append("review_count_mismatch")
    if run.get("expected_aspect_count") != run.get("actual_aspect_count"):
        reasons.append("aspect_count_mismatch")
    if not _text(run.get("review_snapshot_id")):
        reasons.append("missing_review_snapshot")
    if not _text(run.get("aspect_snapshot_id")):
        reasons.append("missing_aspect_snapshot")
    run_id = _text(run.get("processing_run_id"))
    observed_reviews = {
        _text(row.get("review_sentiment_id"))
        for row in sentiment_rows
        if _text(row.get("processing_run_id")) == run_id
    }
    observed_aspects = {
        _text(row.get("aspect_sentiment_id"))
        for row in aspect_rows
        if _text(row.get("processing_run_id")) == run_id
    }
    if len(observed_reviews) != run.get("actual_review_count"):
        reasons.append("review_result_count_mismatch")
    if len(observed_aspects) != run.get("actual_aspect_count"):
        reasons.append("aspect_result_count_mismatch")
    return not reasons, tuple(dict.fromkeys(reasons))


def plan_sentiment_backfill(
    occurrences: Iterable[TripAdvisorReviewOccurrence],
    *,
    manifests: Iterable[Mapping[str, Any]],
    normalization_runs: Iterable[Mapping[str, Any]],
    enrichment_runs: Iterable[Mapping[str, Any]],
    sentiment_rows: Iterable[Mapping[str, Any]],
    aspect_rows: Iterable[Mapping[str, Any]],
    model_id: str = VISOBERT_MODEL_ID,
    model_revision: str = VISOBERT_MODEL_REVISION,
    config_fingerprint: str,
    permitted_provenance: Sequence[str] = ("synthetic",),
    synthetic_input_hashes: Sequence[str] = (),
    model_fingerprint: str | None = None,
    max_occurrences: int | None = None,
    snapshot_exists: Callable[[str, str], bool] | None = None,
) -> SentimentPlan:
    if model_id != VISOBERT_MODEL_ID or model_revision != VISOBERT_MODEL_REVISION:
        raise ValueError("TASK-005D requires the pinned ViSoBERT model and revision")
    if max_occurrences is not None and max_occurrences < 1:
        raise ValueError("max_occurrences must be positive")
    manifest_by_batch = {_text(row.get("batch_id")): row for row in manifests}
    release_by_id = {_text(row.get("run_id")): row for row in normalization_runs}
    enrichment = list(enrichment_runs)
    run_by_id = {_text(row.get("processing_run_id")): row for row in enrichment}
    sentiment = list(sentiment_rows)
    aspects = list(aspect_rows)
    invalid_historical_runs: list[dict[str, Any]] = []
    for run in enrichment:
        if run.get("source_id") != TRIPADVISOR_SOURCE_ID:
            continue
        compatible, reasons = _run_is_compatible(
            run,
            release_id=_text(run.get("source_release_id")),
            model_id=model_id,
            config_fingerprint=config_fingerprint,
            sentiment_rows=sentiment,
            aspect_rows=aspects,
            model_fingerprint=model_fingerprint,
        )
        if not compatible:
            invalid_historical_runs.append(
                {
                    "processing_run_id": _text(run.get("processing_run_id")),
                    "source_release_id": _text(run.get("source_release_id")),
                    "reasons": list(reasons),
                }
            )
    rows_by_key: dict[tuple[str, str, str, int], list[Mapping[str, Any]]] = {}
    for row in sentiment:
        key = (
            _text(row.get("source_record_sk")),
            _text(row.get("batch_id")),
            _text(row.get("review_id")),
            int(row["source_row_index"])
            if row.get("source_row_index") is not None
            else -1,
        )
        rows_by_key.setdefault(key, []).append(row)
    decisions: list[SentimentDecision] = []
    seen: set[tuple[str, str, str, int, int]] = set()
    ordered = sorted(occurrences, key=_occurrence_key)
    for occurrence in ordered:
        key = _occurrence_key(occurrence)
        reasons: list[str] = []
        if key in seen:
            reasons.append("duplicate_source_occurrence")
        seen.add(key)
        if occurrence.source_id != TRIPADVISOR_SOURCE_ID:
            reasons.append("source_not_tripadvisor")
        manifest = manifest_by_batch.get(occurrence.batch_id)
        provenance = classify_provenance(
            manifest or {}, synthetic_input_hashes=synthetic_input_hashes
        )
        if manifest is None:
            reasons.append("bronze_manifest_missing")
        else:
            reasons.extend(bronze_manifest_reasons(manifest))
            if provenance not in permitted_provenance:
                reasons.append("provenance_not_permitted")
        release = release_by_id.get(occurrence.source_release_id)
        if release is None:
            reasons.append("normalization_release_missing")
        else:
            try:
                validate_tripadvisor_release(release, snapshot_exists=snapshot_exists)
            except ValueError:
                reasons.append("normalization_release_not_committed_or_incomplete")
            if release.get("bronze_batch_id") != occurrence.batch_id:
                reasons.append("normalization_release_batch_mismatch")
        compatible_run_id: str | None = None
        coverage_rows = rows_by_key.get(key[:4], [])
        invalid_coverage = False
        for row in coverage_rows:
            run_id = _text(row.get("processing_run_id"))
            run = run_by_id.get(run_id)
            compatible = False
            if run is not None:
                compatible, run_reasons = _run_is_compatible(
                    run,
                    release_id=occurrence.source_release_id,
                    model_id=model_id,
                    config_fingerprint=config_fingerprint,
                    sentiment_rows=sentiment,
                    aspect_rows=aspects,
                    model_fingerprint=model_fingerprint,
                )
                if run_reasons:
                    invalid_coverage = True
                    reasons.extend(run_reasons)
            else:
                invalid_coverage = True
                reasons.append("enrichment_run_missing")
            expected_hash = text_hash(occurrence.review_text)
            row_valid = (
                row.get("source_id") == TRIPADVISOR_SOURCE_ID
                and row.get("source_record_sk") == occurrence.source_record_sk
                and row.get("batch_id") == occurrence.batch_id
                and row.get("source_row_index") == occurrence.source_row_index
                and row.get("review_id") == occurrence.review_id
                and row.get("source_release_id") == occurrence.source_release_id
                and row.get("review_text_hash") == expected_hash
                and row.get("model_id") == model_id
                and row.get("config_fingerprint") == config_fingerprint
            )
            if compatible and row_valid:
                compatible_run_id = run_id
                break
            invalid_coverage = True
            if not row_valid:
                reasons.append("enrichment_result_lineage_or_text_mismatch")
        if reasons:
            status = "excluded"
        elif compatible_run_id:
            status = "already_enriched"
        else:
            status = "missing"
        if invalid_coverage and not reasons:
            reasons.append("invalid_historical_enrichment_generation")
            status = "missing"
        decisions.append(
            SentimentDecision(
                occurrence_key=key,
                source_release_id=occurrence.source_release_id,
                review_id=occurrence.review_id,
                batch_id=occurrence.batch_id,
                source_record_sk=occurrence.source_record_sk,
                status=status,
                reasons=tuple(dict.fromkeys(reasons)),
                compatible_processing_run_id=compatible_run_id,
            )
        )
    missing = [item for item in decisions if item.status == "missing"]
    selected = tuple(item.occurrence_key for item in missing[:max_occurrences])
    if max_occurrences is None:
        selected = tuple(item.occurrence_key for item in missing)
    return SentimentPlan(
        model_id=model_id,
        model_revision=model_revision,
        config_fingerprint=config_fingerprint,
        decisions=tuple(decisions),
        invalid_historical_runs=tuple(
            sorted(
                invalid_historical_runs,
                key=lambda item: (
                    item["source_release_id"],
                    item["processing_run_id"],
                ),
            )
        ),
        max_occurrences=max_occurrences,
        selected_occurrence_keys=selected,
    )


def execute_sentiment_plan(
    plan: SentimentPlan,
    *,
    confirm_writes: bool,
    runner: Callable[[SentimentDecision], Any],
) -> SentimentExecutionResult:
    if not confirm_writes:
        raise PermissionError(
            "write execution requires --execute together with --confirm-writes"
        )
    completed: list[tuple[str, str, str, int, int]] = []
    failed: list[tuple[str, str, str, int, int]] = []
    errors: list[str] = []
    for item in plan.missing:
        try:
            runner(item)
        except Exception as error:  # noqa: BLE001 - isolate occurrence failures
            failed.append(item.occurrence_key)
            errors.append(f"{item.review_id}: {type(error).__name__}: {error}")
            continue
        completed.append(item.occurrence_key)
    return SentimentExecutionResult(
        status="failed" if failed else "completed",
        planned_occurrence_keys=tuple(item.occurrence_key for item in plan.missing),
        completed_occurrence_keys=tuple(completed),
        failed_occurrence_keys=tuple(failed),
        errors=tuple(errors),
    )


def occurrence_argument(decision: SentimentDecision) -> str:
    return "|".join(
        (
            decision.source_record_sk,
            decision.batch_id,
            decision.review_id,
            str(decision.occurrence_key[3]),
            str(decision.occurrence_key[4]),
        )
    )


def existing_enrichment_command(
    decision: SentimentDecision,
    *,
    script: str,
    python: str = "python",
) -> list[str]:
    return [
        python,
        script,
        "--source-release-id",
        decision.source_release_id,
        "--processing-run-id",
        _processing_run_id(decision),
        "--occurrence-key",
        occurrence_argument(decision),
    ]


def _processing_run_id(decision: SentimentDecision) -> str:
    import hashlib

    value = "|".join(str(item) for item in decision.occurrence_key)
    return hashlib.sha256(
        f"t05d|{decision.source_release_id}|{value}".encode()
    ).hexdigest()


def subprocess_runner(
    *, script: str, python: str = "python"
) -> Callable[[SentimentDecision], Any]:
    def run(decision: SentimentDecision) -> Any:
        return subprocess.run(
            existing_enrichment_command(decision, script=script, python=python),
            check=True,
        )

    return run


def human_summary(plan: SentimentPlan, *, mode: str = "dry-run") -> str:
    counts = plan.to_dict()["counts"]
    return (
        "TASK-005D TripAdvisor sentiment backfill\n"
        f"mode: {mode}; model: {plan.model_id}@{plan.model_revision}\n"
        f"eligible: {counts['eligible_occurrence_count']}; "
        f"already enriched: {counts['already_enriched_count']}; "
        f"missing: {counts['missing_enrichment_count']}; "
        f"excluded: {counts['excluded_occurrence_count']}\n"
        f"selected missing: {len(plan.missing)}\n"
    )


__all__ = [
    "SentimentDecision",
    "SentimentExecutionResult",
    "SentimentPlan",
    "VISOBERT_MODEL_ID",
    "VISOBERT_MODEL_REVISION",
    "execute_sentiment_plan",
    "existing_enrichment_command",
    "human_summary",
    "plan_sentiment_backfill",
    "subprocess_runner",
]
