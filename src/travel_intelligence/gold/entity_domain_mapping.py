"""Exact-match application planning for approved Gold entity domain mappings."""

from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from travel_intelligence.gold.domain_mapping import (
    PERMITTED_DOMAIN_IDS,
    effective_approved_mappings,
    normalize_source_category,
    validate_mapping_contract,
)


class EntityDomainMappingError(ValueError):
    """Raised when a candidate mapping application would be unsafe."""


@dataclass(frozen=True)
class EntityDomainMappingPlan:
    """Candidate-only mapping plan; it has no Iceberg write behavior."""

    updated_rows: tuple[dict[str, Any], ...]
    report: dict[str, Any]


def plan_entity_domain_mapping(
    entities: Iterable[Mapping[str, Any]],
    source_bridges: Iterable[Mapping[str, Any]],
    mapping_contract: Mapping[str, Any],
    *,
    release_id: str,
) -> EntityDomainMappingPlan:
    """Plan exact approved mappings and fail closed for all other categories."""
    validate_mapping_contract(mapping_contract)
    mappings = effective_approved_mappings(mapping_contract)
    entity_rows = [dict(row) for row in entities]
    if len({str(row.get("entity_id")) for row in entity_rows}) != len(entity_rows):
        raise EntityDomainMappingError("candidate entity IDs are not unique")
    entity_by_id = {str(row["entity_id"]): row for row in entity_rows}
    sources: dict[str, set[str]] = defaultdict(set)
    for bridge in source_bridges:
        if bridge.get("release_id") != release_id:
            continue
        entity_id = str(bridge.get("entity_id") or "")
        if entity_id not in entity_by_id:
            raise EntityDomainMappingError("source bridge references an unknown entity")
        sources[entity_id].add(str(bridge.get("source_id") or ""))
    if set(sources) != set(entity_by_id) or any(
        values != {"tripadvisor"} for values in sources.values()
    ):
        raise EntityDomainMappingError(
            "candidate source lineage is not one TripAdvisor row per entity"
        )

    assignments: dict[str, str | None] = {}
    category_counts: Counter[str | None] = Counter()
    raw_category_counts: Counter[str] = Counter()
    excluded_counts: Counter[str] = Counter()
    approved_rule_by_category = {
        str(rule["normalized_category"]): rule
        for rule in mapping_contract["rules"]
        if rule.get("approval_status") == "approved"
    }
    for entity_id, row in entity_by_id.items():
        raw = row.get("entity_type_raw")
        category = (
            None
            if raw is None or not str(raw).strip()
            else normalize_source_category(raw)
        )
        category_counts[category] += 1
        raw_category_counts["" if raw is None else str(raw)] += 1
        desired = mappings.get(("tripadvisor", category)) if category else None
        if desired is not None and desired not in PERMITTED_DOMAIN_IDS:
            raise EntityDomainMappingError(
                "mapping contract returned an invalid domain"
            )
        assignments[entity_id] = desired
        rule = approved_rule_by_category.get(category or "")
        if rule is not None and rule.get("mapping_state") == "excluded":
            excluded_counts[str(rule["raw_category"])] += 1

    updates = []
    for entity_id, row in entity_by_id.items():
        desired = assignments[entity_id]
        current = row.get("product_domain_id")
        if current is not None and current not in PERMITTED_DOMAIN_IDS:
            raise EntityDomainMappingError(
                "candidate contains an invalid persisted domain"
            )
        if current == desired:
            continue
        if current is not None and desired is None:
            raise EntityDomainMappingError(
                "unapproved category would clear an existing domain"
            )
        if current is not None and current != desired:
            raise EntityDomainMappingError(
                "candidate contains a conflicting persisted domain"
            )
        updates.append({**row, "product_domain_id": desired})

    assigned = Counter(value for value in assignments.values() if value is not None)
    report = {
        "total_entity_count": len(entity_rows),
        "distinct_entity_count": len(entity_by_id),
        "mapped_entity_count": sum(assigned.values()),
        "null_domain_entity_count": sum(
            value is None for value in assignments.values()
        ),
        "domain_counts": {
            domain: assigned[domain] for domain in sorted(PERMITTED_DOMAIN_IDS)
        },
        "excluded_entity_count": sum(excluded_counts.values()),
        "excluded_category_counts": dict(sorted(excluded_counts.items())),
        "unreviewed_entity_count": sum(
            count
            for raw, count in raw_category_counts.items()
            if not raw.strip()
            or ("tripadvisor", normalize_source_category(raw)) not in mappings
        )
        - sum(excluded_counts.values()),
        "unreviewed_category_count": sum(
            1
            for raw in raw_category_counts
            if not raw.strip()
            or ("tripadvisor", normalize_source_category(raw)) not in mappings
        ) - len(excluded_counts),
        "changed_entity_count": len(updates),
        "sample_mappings": [
            {
                "entity_id": entity_id,
                "raw_category": entity_by_id[entity_id].get("entity_type_raw"),
                "product_domain_id": assignments[entity_id],
            }
            for entity_id in sorted(assignments)
            if assignments[entity_id] is not None
        ][:5],
    }
    return EntityDomainMappingPlan(updated_rows=tuple(updates), report=report)


def assert_expected_candidate_counts(report: Mapping[str, Any]) -> None:
    """Reject an application plan that differs from the approved B1 scope."""
    expected = {
        "total_entity_count": 4000,
        "distinct_entity_count": 4000,
        "mapped_entity_count": 1973,
        "null_domain_entity_count": 2027,
        "excluded_entity_count": 47,
        "unreviewed_entity_count": 1980,
        "unreviewed_category_count": 562,
    }
    actual = {key: report.get(key) for key in expected}
    if actual != expected:
        raise EntityDomainMappingError(f"candidate mapping counts differ: {actual}")
    expected_domains = {
        "accommodation": 0,
        "destination": 1722,
        "food_dining": 108,
        "transportation": 143,
    }
    if report.get("domain_counts") != expected_domains:
        raise EntityDomainMappingError("candidate domain counts differ")


__all__ = [
    "EntityDomainMappingError",
    "EntityDomainMappingPlan",
    "assert_expected_candidate_counts",
    "plan_entity_domain_mapping",
]
