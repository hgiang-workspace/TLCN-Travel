"""Planning and guarded execution for the TripAdvisor normalization backfill.

The planner is intentionally independent from Spark and MinIO.  Runtime
adapters provide persisted manifests and normalization ledger rows; execution
delegates to the existing TASK-003 manifest driver rather than implementing a
second normalizer here.
"""

from __future__ import annotations

import json
import subprocess
from dataclasses import asdict, dataclass
from typing import Any, Callable, Iterable, Mapping, Sequence

from travel_intelligence.silver.foundation import NORMALIZATION_VERSION

from .inventory import (
    TRIPADVISOR_SOURCE_ID,
    bronze_manifest_reasons,
    classify_provenance,
    snapshot_completeness,
)


@dataclass(frozen=True)
class BatchDecision:
    batch_id: str
    manifest_key: str
    ingested_at: str
    provenance_class: str
    status: str
    reasons: tuple[str, ...] = ()
    row_count: int | None = None
    nested_review_count: int | None = None
    normalization_run_ids: tuple[str, ...] = ()


@dataclass(frozen=True)
class BackfillPlan:
    source_id: str
    normalization_version: str
    decisions: tuple[BatchDecision, ...]
    max_batches: int | None = None
    selected_batch_ids: tuple[str, ...] = ()

    @property
    def pending(self) -> tuple[BatchDecision, ...]:
        return tuple(item for item in self.decisions if item.status == "pending")

    def to_dict(self) -> dict[str, Any]:
        decisions = [asdict(item) for item in self.decisions]
        eligible_count = sum(
            item.status in {"already_committed", "pending"}
            for item in self.decisions
        )
        return {
            "source_id": self.source_id,
            "normalization_version": self.normalization_version,
            "max_batches": self.max_batches,
            "selected_batch_ids": list(self.selected_batch_ids),
            "decisions": decisions,
            "counts": {
                "eligible": eligible_count,
                **{
                    status: sum(item.status == status for item in self.decisions)
                    for status in (
                        "already_committed",
                        "pending",
                        "failed_or_incomplete",
                        "excluded",
                    )
                },
            },
            "discovered": len(self.decisions),
            "eligible": eligible_count,
            "pending_batch_ids": [item.batch_id for item in self.pending],
        }

    def to_json(self) -> str:
        return (
            json.dumps(self.to_dict(), ensure_ascii=False, sort_keys=True, indent=2)
            + "\n"
        )


@dataclass(frozen=True)
class ExecutionResult:
    status: str
    planned_batch_ids: tuple[str, ...]
    completed_batch_ids: tuple[str, ...] = ()
    failed_batch_ids: tuple[str, ...] = ()
    errors: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _text(value: Any) -> str:
    return "" if value is None else str(value)


def _sort_key(manifest: Mapping[str, Any]) -> tuple[str, str, str]:
    return (
        _text(manifest.get("ingested_at")),
        _text(manifest.get("batch_id")),
        _text(manifest.get("manifest_key")),
    )


def _release_rows(
    releases: Iterable[Mapping[str, Any]], batch_id: str
) -> list[Mapping[str, Any]]:
    return [row for row in releases if _text(row.get("bronze_batch_id")) == batch_id]


def _release_state(
    rows: Sequence[Mapping[str, Any]],
) -> tuple[str, tuple[str, ...], tuple[str, ...]]:
    run_ids = tuple(
        sorted(_text(row.get("run_id")) for row in rows if row.get("run_id"))
    )
    if not rows:
        return "pending", (), run_ids
    complete_committed = [
        row
        for row in rows
        if row.get("source_id") == TRIPADVISOR_SOURCE_ID
        and row.get("normalization_version", NORMALIZATION_VERSION)
        == NORMALIZATION_VERSION
        and row.get("status") == "committed"
        and snapshot_completeness(row)[0]
    ]
    if complete_committed:
        return "already_committed", (), run_ids
    statuses = {_text(row.get("status")) for row in rows}
    reasons = ["normalization_release_not_committed_or_incomplete"]
    if statuses & {"failed", "partial", "started"}:
        reasons.insert(0, "normalization_release_failed_or_incomplete")
    return "failed_or_incomplete", tuple(reasons), run_ids


def plan_backfill(
    manifests: Iterable[Mapping[str, Any]],
    normalization_releases: Iterable[Mapping[str, Any]],
    *,
    synthetic_input_hashes: Sequence[str] = (),
    batch_ids: Sequence[str] = (),
    max_batches: int | None = None,
) -> BackfillPlan:
    """Build a deterministic, read-only plan from persisted metadata."""
    if max_batches is not None and max_batches < 1:
        raise ValueError("max_batches must be positive")
    selected = set(batch_ids)
    releases = list(normalization_releases)
    decisions: list[BatchDecision] = []
    for manifest in sorted(manifests, key=_sort_key):
        batch_id = _text(manifest.get("batch_id"))
        if selected and batch_id not in selected:
            continue
        reasons = list(bronze_manifest_reasons(manifest))
        provenance = classify_provenance(
            manifest, synthetic_input_hashes=synthetic_input_hashes
        )
        if provenance != "synthetic":
            reasons.append("provenance_not_verified_synthetic")
        if not _text(manifest.get("manifest_key")):
            reasons.append("missing_manifest_key")
        release_status, release_reasons, run_ids = _release_state(
            _release_rows(releases, batch_id)
        )
        reasons.extend(release_reasons)
        if reasons:
            status = "excluded"
            if not bronze_manifest_reasons(manifest) and provenance == "synthetic":
                status = release_status
        else:
            status = release_status
        decisions.append(
            BatchDecision(
                batch_id=batch_id,
                manifest_key=_text(manifest.get("manifest_key")),
                ingested_at=_text(manifest.get("ingested_at")),
                provenance_class=provenance,
                status=status,
                reasons=tuple(dict.fromkeys(reasons)),
                row_count=manifest.get("row_count"),
                nested_review_count=manifest.get("nested_review_count"),
                normalization_run_ids=run_ids,
            )
        )
    pending = [item for item in decisions if item.status == "pending"]
    if max_batches is not None:
        selected_pending = tuple(item.batch_id for item in pending[:max_batches])
    else:
        selected_pending = tuple(item.batch_id for item in pending)
    return BackfillPlan(
        source_id=TRIPADVISOR_SOURCE_ID,
        normalization_version=NORMALIZATION_VERSION,
        decisions=tuple(decisions),
        max_batches=max_batches,
        selected_batch_ids=selected_pending,
    )


def execute_plan(
    plan: BackfillPlan,
    *,
    confirm_writes: bool,
    runner: Callable[[BatchDecision], Any],
) -> ExecutionResult:
    """Run only the bounded pending portion after explicit authorization."""
    if not confirm_writes:
        raise PermissionError(
            "write execution requires --execute together with --confirm-writes"
        )
    selected = set(plan.selected_batch_ids)
    work = tuple(item for item in plan.pending if item.batch_id in selected)
    completed: list[str] = []
    failed: list[str] = []
    errors: list[str] = []
    for item in work:
        try:
            runner(item)
        except Exception as error:  # noqa: BLE001 - preserve per-batch recovery
            failed.append(item.batch_id)
            errors.append(f"{item.batch_id}: {type(error).__name__}: {error}")
            continue
        completed.append(item.batch_id)
    return ExecutionResult(
        status="failed" if failed else "completed",
        planned_batch_ids=tuple(item.batch_id for item in work),
        completed_batch_ids=tuple(completed),
        failed_batch_ids=tuple(failed),
        errors=tuple(errors),
    )


def existing_normalizer_command(
    decision: BatchDecision,
    *,
    script: str,
    python: str = "python",
    bucket: str = "travel-lakehouse",
) -> list[str]:
    """Build the existing TASK-003 driver invocation without executing it."""
    return [
        python,
        script,
        "--manifest-key",
        decision.manifest_key,
        "--bucket",
        bucket,
    ]


def subprocess_runner(
    *,
    script: str,
    python: str = "python",
    bucket: str = "travel-lakehouse",
) -> Callable[[BatchDecision], Any]:
    def run(decision: BatchDecision) -> Any:
        command = existing_normalizer_command(
            decision, script=script, python=python, bucket=bucket
        )
        return subprocess.run(command, check=True)

    return run


def human_summary(plan: BackfillPlan, *, mode: str = "dry-run") -> str:
    counts = plan.to_dict()["counts"]
    eligible = sum(
        item.status in {"already_committed", "pending"}
        for item in plan.decisions
    )
    return (
        "TASK-005C TripAdvisor normalization backfill\n"
        f"mode: {mode}\n"
        f"discovered: {len(plan.decisions)}; eligible: {eligible}; "
        f"pending: {counts['pending']}; excluded: {counts['excluded']}; "
        f"failed/incomplete: {counts['failed_or_incomplete']}\n"
        f"planned order: {', '.join(plan.selected_batch_ids) or '(none)'}\n"
    )


__all__ = [
    "BackfillPlan",
    "BatchDecision",
    "ExecutionResult",
    "existing_normalizer_command",
    "execute_plan",
    "human_summary",
    "plan_backfill",
    "subprocess_runner",
]
