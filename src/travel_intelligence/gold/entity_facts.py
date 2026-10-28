"""Pure, fail-closed T07-D entity sentiment and aspect fact derivation."""

from __future__ import annotations

import hashlib
from collections import defaultdict
from collections.abc import Iterable, Mapping
from decimal import ROUND_HALF_UP, Decimal
from typing import Any

from travel_intelligence.gold.dimensions import deterministic_key

VALID_SENTIMENT_LABELS = frozenset({"positive", "neutral", "negative"})
PERCENTAGE_QUANTUM = Decimal("0.0001")


class GoldFactError(ValueError):
    """Raised when effective enrichment rows cannot form contracted facts."""


def _evidence_set_id(prefix: str, identities: Iterable[str]) -> str:
    value = "|".join(sorted(set(identities))).encode("utf-8")
    return f"{prefix}-{hashlib.sha256(value).hexdigest()}"


def _confidence(
    rows: list[Mapping[str, Any]],
) -> tuple[float | None, float | None, float | None]:
    values = [
        float(row["confidence"]) for row in rows if row.get("confidence") is not None
    ]
    if not values:
        return None, None, None
    return sum(values) / len(values), min(values), max(values)


def _percentage(numerator: int, denominator: int) -> Decimal:
    return (Decimal(numerator) * Decimal("100") / Decimal(denominator)).quantize(
        PERCENTAGE_QUANTUM, rounding=ROUND_HALF_UP
    )


def _validate_review(row: Mapping[str, Any]) -> None:
    if row.get("sentiment_label") not in VALID_SENTIMENT_LABELS:
        raise GoldFactError(
            f"unsupported sentiment label: {row.get('sentiment_label')}"
        )
    confidence = row.get("confidence")
    if confidence is not None and not 0 <= float(confidence) <= 1:
        raise GoldFactError("review confidence is outside [0, 1]")


def _validate_aspect(row: Mapping[str, Any]) -> None:
    _validate_review(row)
    if not row.get("aspect"):
        raise GoldFactError("aspect row has no aspect")


def derive_entity_facts(
    reviews: Iterable[Mapping[str, Any]],
    aspects: Iterable[Mapping[str, Any]],
    *,
    entity_by_id: Mapping[str, Mapping[str, Any]],
    release_id: str,
    gold_run_id: str,
    contract_version: str,
    config_fingerprint: str,
    source_release_id: str,
    reporting_period: str,
    model_id: str,
    model_fingerprint: str,
    enrichment_config_fingerprint: str,
    built_at: Any,
) -> dict[str, Any]:
    """Aggregate only effective, parent-linked enrichment rows by entity."""
    effective_reviews = list(reviews)
    seen_occurrences: set[tuple[str, str, str, str]] = set()
    by_entity: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in effective_reviews:
        _validate_review(row)
        occurrence = tuple(
            str(row[field])
            for field in (
                "source_id",
                "source_release_id",
                "source_record_sk",
                "review_id",
            )
        )
        if occurrence in seen_occurrences:
            raise GoldFactError("duplicate effective review occurrence")
        seen_occurrences.add(occurrence)
        entity_id = row.get("place_id")
        if not entity_id or str(entity_id) not in entity_by_id:
            raise GoldFactError("effective review has no approved candidate entity")
        by_entity[str(entity_id)].append(row)

    lineage = {
        "release_id": release_id,
        "gold_run_id": gold_run_id,
        "contract_version": contract_version,
        "config_fingerprint": config_fingerprint,
        "source_release_id": source_release_id,
        "reporting_period": reporting_period,
        "time_basis": "source_release_processing_period",
        "model_id": model_id,
        "model_fingerprint": model_fingerprint,
        "enrichment_config_fingerprint": enrichment_config_fingerprint,
        "data_status": "insufficient_data",
        "built_at": built_at,
    }
    sentiment_rows: list[dict[str, Any]] = []
    selected_review_ids = {str(row["review_sentiment_id"]) for row in effective_reviews}
    for entity_id in sorted(by_entity):
        rows = by_entity[entity_id]
        entity = entity_by_id[entity_id]
        denominator = len(rows)
        for label in sorted({str(row["sentiment_label"]) for row in rows}):
            members = [row for row in rows if row["sentiment_label"] == label]
            average, minimum, maximum = _confidence(members)
            evidence_set_id = _evidence_set_id(
                "entity-sentiment-evidence",
                (str(row["review_sentiment_id"]) for row in members),
            )
            sentiment_rows.append(
                {
                    "entity_sentiment_fact_sk": deterministic_key(
                        "entity-sentiment",
                        release_id,
                        entity_id,
                        reporting_period,
                        label,
                    ),
                    "entity_sk": entity["entity_sk"],
                    "entity_id": entity_id,
                    "sentiment_label": label,
                    "sentiment_count": len(members),
                    "eligible_review_count": denominator,
                    "sentiment_percentage": _percentage(len(members), denominator),
                    "average_confidence": average,
                    "minimum_confidence": minimum,
                    "maximum_confidence": maximum,
                    "evidence_set_id": evidence_set_id,
                    **lineage,
                }
            )

    aspect_groups: dict[tuple[str, str, str], list[Mapping[str, Any]]] = defaultdict(
        list
    )
    for row in aspects:
        _validate_aspect(row)
        if str(row.get("review_sentiment_id")) not in selected_review_ids:
            raise GoldFactError("aspect row has no selected effective review parent")
        entity_id = row.get("place_id")
        if not entity_id or str(entity_id) not in entity_by_id:
            raise GoldFactError("effective aspect has no approved candidate entity")
        aspect_groups[
            (str(entity_id), str(row["aspect"]), str(row["sentiment_label"]))
        ].append(row)

    aspect_rows: list[dict[str, Any]] = []
    for (entity_id, aspect, label), members in sorted(aspect_groups.items()):
        entity = entity_by_id[entity_id]
        denominator = len(by_entity[entity_id])
        average, _, _ = _confidence(members)
        evidence_set_id = _evidence_set_id(
            "entity-aspect-evidence",
            (str(row["aspect_sentiment_id"]) for row in members),
        )
        distinct_reviews = len({str(row["review_id"]) for row in members})
        aspect_rows.append(
            {
                "entity_aspect_fact_sk": deterministic_key(
                    "entity-aspect",
                    release_id,
                    entity_id,
                    reporting_period,
                    aspect,
                    label,
                ),
                "entity_sk": entity["entity_sk"],
                "entity_id": entity_id,
                "aspect": aspect,
                "sentiment_label": label,
                "mention_count": len(members),
                "distinct_review_count": distinct_reviews,
                "eligible_review_count": denominator,
                "review_percentage": _percentage(distinct_reviews, denominator),
                "average_confidence": average,
                "evidence_set_id": evidence_set_id,
                **lineage,
            }
        )
    return {
        "fact_entity_sentiment_summary": sentiment_rows,
        "fact_entity_aspect_summary": aspect_rows,
        "effective_review_count": len(effective_reviews),
        "effective_aspect_count": sum(len(rows) for rows in aspect_groups.values()),
    }


__all__ = ["GoldFactError", "VALID_SENTIMENT_LABELS", "derive_entity_facts"]
