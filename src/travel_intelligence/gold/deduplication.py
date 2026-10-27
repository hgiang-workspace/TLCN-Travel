"""Fail-closed deduplication for Gold input generations.

The Silver enrichment ledger retains every committed attempt.  Gold keeps that
lineage in its input manifest while selecting at most one identical prediction
per logical review/aspect identity for analytical facts.
"""

from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from collections.abc import Iterable, Mapping
from typing import Any

DEDUPLICATION_POLICY_VERSION = "gold-dedup-v1"


class GoldDeduplicationError(ValueError):
    """Raised when duplicate committed generations do not agree exactly."""


def _canonical(value: Any) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str
    ).encode("utf-8")


def deduplication_policy_fingerprint() -> str:
    policy = {
        "version": DEDUPLICATION_POLICY_VERSION,
        "review_identity": [
            "source_id",
            "source_release_id",
            "source_record_sk",
            "review_id",
        ],
        "review_equivalence": [
            "review_text_hash",
            "sentiment_label",
            "confidence",
            "model_id",
            "model_fingerprint",
            "config_fingerprint",
            "source_id",
            "source_release_id",
            "source_record_sk",
            "review_id",
        ],
        "aspect_identity": [
            "source_id",
            "source_release_id",
            "source_record_sk",
            "review_id",
            "segment_ordinal",
            "segment_start_char",
            "segment_end_char",
            "aspect",
        ],
        "aspect_equivalence": [
            "segment_text_hash",
            "sentiment_label",
            "confidence",
            "model_id",
            "model_fingerprint",
            "config_fingerprint",
            "review_sentiment_id",
            "source_id",
            "source_release_id",
            "source_record_sk",
            "review_id",
        ],
        "selection": "prefer a frozen-18 processing generation, then processing_run_id",
    }
    return "sha256:" + hashlib.sha256(_canonical(policy)).hexdigest()


def _required(row: Mapping[str, Any], field: str) -> Any:
    value = row.get(field)
    if value is None or value == "":
        raise GoldDeduplicationError(f"missing required deduplication field: {field}")
    return value


def _identity(row: Mapping[str, Any], fields: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(str(_required(row, field)) for field in fields)


def _assert_equivalent(
    rows: Iterable[Mapping[str, Any]],
    fields: tuple[str, ...],
    identity: tuple[str, ...],
    kind: str,
) -> None:
    iterator = iter(rows)
    first = next(iterator)
    expected = tuple(first.get(field) for field in fields)
    for row in iterator:
        observed = tuple(row.get(field) for field in fields)
        if observed != expected:
            raise GoldDeduplicationError(
                f"conflicting {kind} duplicate generation for identity: "
                f"{'|'.join(identity)}"
            )


def _select(
    rows: list[Mapping[str, Any]], frozen_runs: frozenset[str]
) -> Mapping[str, Any]:
    return min(
        rows,
        key=lambda row: (
            0 if str(row["processing_run_id"]) in frozen_runs else 1,
            str(row["processing_run_id"]),
        ),
    )


def reconcile_generations(
    review_rows: Iterable[Mapping[str, Any]],
    aspect_rows: Iterable[Mapping[str, Any]],
    *,
    frozen_processing_run_ids: Iterable[str],
) -> dict[str, Any]:
    """Return selected rows plus canonical supersession provenance.

    The caller must provide rows from every run's pinned Iceberg snapshot, not
    merely the latest mutable Silver table state.
    """
    frozen = frozenset(str(run_id) for run_id in frozen_processing_run_ids)
    review_identity_fields = (
        "source_id",
        "source_release_id",
        "source_record_sk",
        "review_id",
    )
    review_equivalence_fields = (
        "review_text_hash",
        "sentiment_label",
        "confidence",
        "model_id",
        "model_fingerprint",
        "config_fingerprint",
        "source_id",
        "source_release_id",
        "source_record_sk",
        "review_id",
    )
    aspect_identity_fields = review_identity_fields + (
        "segment_ordinal",
        "segment_start_char",
        "segment_end_char",
        "aspect",
    )
    aspect_equivalence_fields = (
        "segment_text_hash",
        "sentiment_label",
        "confidence",
        "model_id",
        "model_fingerprint",
        "config_fingerprint",
        "review_sentiment_id",
        "source_id",
        "source_release_id",
        "source_record_sk",
        "review_id",
    )
    review_groups: dict[tuple[str, ...], list[Mapping[str, Any]]] = defaultdict(list)
    for row in review_rows:
        _required(row, "processing_run_id")
        review_groups[_identity(row, review_identity_fields)].append(row)

    selected_reviews: list[Mapping[str, Any]] = []
    review_supersessions: list[dict[str, Any]] = []
    for identity in sorted(review_groups):
        rows = review_groups[identity]
        _assert_equivalent(rows, review_equivalence_fields, identity, "review")
        selected = _select(rows, frozen)
        selected_reviews.append(selected)
        superseded = sorted(
            str(row["processing_run_id"]) for row in rows if row is not selected
        )
        if superseded:
            review_supersessions.append(
                {
                    "identity": dict(
                        zip(review_identity_fields, identity, strict=True)
                    ),
                    "selected_processing_run_id": str(selected["processing_run_id"]),
                    "selected_review_sentiment_id": str(
                        selected["review_sentiment_id"]
                    ),
                    "superseded_processing_run_ids": superseded,
                }
            )

    selected_review_ids = {str(row["review_sentiment_id"]) for row in selected_reviews}
    aspect_groups: dict[tuple[str, ...], list[Mapping[str, Any]]] = defaultdict(list)
    for row in aspect_rows:
        _required(row, "processing_run_id")
        if str(_required(row, "review_sentiment_id")) not in selected_review_ids:
            raise GoldDeduplicationError("aspect row has no selected review parent")
        aspect_groups[_identity(row, aspect_identity_fields)].append(row)

    selected_aspects: list[Mapping[str, Any]] = []
    aspect_supersessions: list[dict[str, Any]] = []
    for identity in sorted(aspect_groups):
        rows = aspect_groups[identity]
        _assert_equivalent(rows, aspect_equivalence_fields, identity, "aspect")
        selected = _select(rows, frozen)
        selected_aspects.append(selected)
        superseded = sorted(
            str(row["processing_run_id"]) for row in rows if row is not selected
        )
        if superseded:
            aspect_supersessions.append(
                {
                    "identity": dict(
                        zip(aspect_identity_fields, identity, strict=True)
                    ),
                    "selected_processing_run_id": str(selected["processing_run_id"]),
                    "selected_aspect_sentiment_id": str(
                        selected["aspect_sentiment_id"]
                    ),
                    "superseded_processing_run_ids": superseded,
                }
            )

    return {
        "deduplication_policy_version": DEDUPLICATION_POLICY_VERSION,
        "deduplication_policy_fingerprint": deduplication_policy_fingerprint(),
        "raw_candidate_review_count": sum(len(rows) for rows in review_groups.values()),
        "raw_candidate_aspect_count": sum(len(rows) for rows in aspect_groups.values()),
        "effective_review_count": len(selected_reviews),
        "effective_aspect_count": len(selected_aspects),
        "superseded_generation_counts": {
            "review": sum(
                len(item["superseded_processing_run_ids"])
                for item in review_supersessions
            ),
            "aspect": sum(
                len(item["superseded_processing_run_ids"])
                for item in aspect_supersessions
            ),
        },
        "review_supersessions": review_supersessions,
        "aspect_supersessions": aspect_supersessions,
        "selected_reviews": selected_reviews,
        "selected_aspects": selected_aspects,
    }


__all__ = [
    "DEDUPLICATION_POLICY_VERSION",
    "GoldDeduplicationError",
    "deduplication_policy_fingerprint",
    "reconcile_generations",
]
