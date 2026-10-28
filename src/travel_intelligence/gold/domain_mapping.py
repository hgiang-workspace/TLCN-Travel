"""Fail-closed contracts for reviewed source-category to product-domain mapping.

T07-E2A deliberately stops before assigning ``dim_entity.product_domain_id``.
Rules in this module are review artifacts: a category resolves only when an
exact normalized category has one approved ``mapped`` rule.  It contains no
entity-name, address, review, sentiment, or token-based inference.
"""

from __future__ import annotations

import hashlib
import json
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from unicodedata import normalize as unicode_normalize

from travel_intelligence.common.configuration import load_yaml

PERMITTED_DOMAIN_IDS = frozenset(
    {"destination", "accommodation", "food_dining", "transportation"}
)
MAPPING_STATES = frozenset({"mapped", "ambiguous", "excluded", "unmapped"})
APPROVAL_STATUSES = frozenset({"proposed", "approved", "rejected"})
DOMAIN_MAPPING_CONFIG_PATH = "configs/gold/tripadvisor_domain_mapping_v1.yaml"


class DomainMappingError(ValueError):
    """Raised when a source-category mapping contract is unsafe to use."""


@dataclass(frozen=True)
class CategoryInventoryRow:
    """One normalized raw category and its source-provenance-backed count."""

    source_id: str
    normalized_category: str | None
    entity_count: int
    raw_categories: tuple[str, ...]


def normalize_source_category(raw_category: object) -> str:
    """Normalize Unicode and whitespace without splitting or interpreting tokens.

    Composite labels (for example, ``A • B``) remain one exact category.  The
    original string is retained separately in inventory and rule artifacts.
    """
    if raw_category is None:
        raise DomainMappingError("source category is required")
    raw = unicode_normalize("NFC", str(raw_category))
    normalized = " ".join(raw.split())
    if not normalized:
        raise DomainMappingError("source category is blank")
    return normalized


def build_category_inventory(
    rows: Iterable[Mapping[str, object]], *, source_id: str
) -> tuple[CategoryInventoryRow, ...]:
    """Count source categories while preserving every observed raw representation."""
    counts: Counter[str | None] = Counter()
    raw_values: dict[str | None, set[str]] = defaultdict(set)
    for row in rows:
        observed_source = row.get("source_id")
        if observed_source is not None and str(observed_source) != source_id:
            raise DomainMappingError("inventory rows contain more than one source")
        raw = row.get("entity_type_raw", row.get("place_type"))
        category = (
            None
            if raw is None or not str(raw).strip()
            else normalize_source_category(raw)
        )
        counts[category] += 1
        raw_values[category].add("" if raw is None else str(raw))
    return tuple(
        CategoryInventoryRow(
            source_id=source_id,
            normalized_category=category,
            entity_count=counts[category],
            raw_categories=tuple(sorted(raw_values[category])),
        )
        for category in sorted(counts, key=lambda value: value or "")
    )


def validate_mapping_contract(document: Mapping[str, Any]) -> None:
    """Validate a versioned mapping artifact before it can be reviewed or used."""
    if document.get("version") != "tripadvisor-domain-mapping-v1":
        raise DomainMappingError("unsupported domain mapping version")
    _required(document, "mapping_version")
    _required(document, "revision")
    if document.get("artifact_fingerprint") != mapping_contract_fingerprint(document):
        raise DomainMappingError("mapping artifact fingerprint does not match content")
    source = document.get("source")
    if not isinstance(source, Mapping) or source.get("source_id") != "tripadvisor":
        raise DomainMappingError("mapping contract is restricted to TripAdvisor")
    rules = document.get("rules")
    if not isinstance(rules, list):
        raise DomainMappingError("mapping rules must be a list")

    rule_ids: set[str] = set()
    approved_mapped_categories: set[tuple[str, str]] = set()
    for rule in rules:
        if not isinstance(rule, Mapping):
            raise DomainMappingError("mapping rule must be an object")
        rule_id = _required(rule, "rule_id")
        if rule_id in rule_ids:
            raise DomainMappingError(f"duplicate mapping rule id: {rule_id}")
        rule_ids.add(rule_id)
        if _required(rule, "source_id") != "tripadvisor":
            raise DomainMappingError(f"rule {rule_id} has an unexpected source")
        normalized_category = normalize_source_category(
            _required(rule, "normalized_category")
        )
        if normalized_category != _required(rule, "normalized_category"):
            raise DomainMappingError(
                f"rule {rule_id} category must already use contract normalization"
            )
        raw_category = normalize_source_category(_required(rule, "raw_category"))
        if raw_category != normalized_category:
            raise DomainMappingError(
                f"rule {rule_id} raw category does not match normalized category"
            )
        state = _required(rule, "mapping_state")
        if state not in MAPPING_STATES:
            raise DomainMappingError(f"rule {rule_id} has invalid mapping state")
        approval = _required(rule, "approval_status")
        if approval not in APPROVAL_STATUSES:
            raise DomainMappingError(f"rule {rule_id} has invalid approval status")
        _required(rule, "rationale")
        _required(rule, "reviewer")
        _required(rule, "mapping_version")
        provenance = rule.get("provenance")
        if not isinstance(provenance, Mapping):
            raise DomainMappingError(f"rule {rule_id} is missing provenance")
        _required(provenance, "source_release_id")
        _required(provenance, "candidate_release_id")

        domain_id = rule.get("domain_id")
        if state == "mapped":
            if domain_id not in PERMITTED_DOMAIN_IDS:
                raise DomainMappingError(f"rule {rule_id} has invalid domain")
        elif domain_id is not None:
            raise DomainMappingError(
                f"rule {rule_id} may not set a domain for state {state}"
            )
        if state == "mapped" and approval == "approved":
            _validate_approved_reviewer_provenance(rule, rule_id)
            identity = (str(rule["source_id"]), normalized_category)
            if identity in approved_mapped_categories:
                raise DomainMappingError(
                    "multiple effective approved mappings for one source category"
                )
            approved_mapped_categories.add(identity)
        elif approval == "approved":
            _validate_reviewed_nonmapped_provenance(rule, rule_id)


def mapping_contract_fingerprint(document: Mapping[str, Any]) -> str:
    """Fingerprint a review artifact without its self-referential field."""
    canonical = {
        key: value for key, value in document.items() if key != "artifact_fingerprint"
    }
    body = json.dumps(
        canonical, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(body).hexdigest()


def validate_mapping_against_inventory(
    document: Mapping[str, Any], raw_categories: Iterable[Mapping[str, Any]]
) -> dict[str, int]:
    """Reconcile approved B1 scope without applying it to Gold entities."""
    validate_mapping_contract(document)
    inventory = {
        str(row["raw_category"]): int(row["entity_count"])
        for row in raw_categories
    }
    reviewed = [
        rule
        for rule in document["rules"]
        if rule.get("review_provenance", {}).get("review_batch") == "B1-high-impact"
        and rule.get("approval_status") == "approved"
    ]
    if len(reviewed) != 20 or len({rule["raw_category"] for rule in reviewed}) != 20:
        raise DomainMappingError("B1 must contain exactly 20 reviewed categories")
    if any(str(rule["raw_category"]) not in inventory for rule in reviewed):
        raise DomainMappingError("reviewed B1 category is absent from pinned inventory")
    mapped = [rule for rule in reviewed if rule["mapping_state"] == "mapped"]
    excluded = [rule for rule in reviewed if rule["mapping_state"] == "excluded"]
    unmapped = [rule for rule in reviewed if rule["mapping_state"] == "unmapped"]
    reviewed_entities = _rule_entity_count(inventory, reviewed)
    summary = {
        "reviewed_category_count": len(reviewed),
        "reviewed_entity_count": reviewed_entities,
        "mapped_category_count": len(mapped),
        "mapped_entity_count": _rule_entity_count(inventory, mapped),
        "excluded_category_count": len(excluded),
        "excluded_entity_count": _rule_entity_count(inventory, excluded),
        "unmapped_category_count": len(unmapped),
        "unmapped_entity_count": _rule_entity_count(inventory, unmapped),
        "destination_entity_count": _rule_entity_count(
            inventory, mapped, domain_id="destination"
        ),
        "food_dining_entity_count": _rule_entity_count(
            inventory, mapped, domain_id="food_dining"
        ),
        "transportation_entity_count": _rule_entity_count(
            inventory, mapped, domain_id="transportation"
        ),
        "accommodation_entity_count": _rule_entity_count(
            inventory, mapped, domain_id="accommodation"
        ),
        "remaining_category_count": len(inventory) - len(reviewed),
        "remaining_entity_count": sum(inventory.values()) - reviewed_entities,
        "total_entity_count": sum(inventory.values()),
    }
    expected = document.get("approved_b1_reconciliation")
    if not isinstance(expected, Mapping) or summary != dict(expected):
        raise DomainMappingError("approved B1 reconciliation does not match inventory")
    return summary


def effective_approved_mappings(
    document: Mapping[str, Any],
) -> dict[tuple[str, str], str]:
    """Return the only rules that may assign a product domain downstream."""
    validate_mapping_contract(document)
    return {
        (str(rule["source_id"]), str(rule["normalized_category"])): str(
            rule["domain_id"]
        )
        for rule in document["rules"]
        if rule["mapping_state"] == "mapped"
        and rule["approval_status"] == "approved"
    }


def load_domain_mapping_contract(
    root: str | Path = ".",
) -> dict[str, Any]:
    """Load and validate the static review artifact without applying it."""
    base = Path(root).resolve()
    document = load_yaml(base / DOMAIN_MAPPING_CONFIG_PATH, root=base)
    validate_mapping_contract(document)
    return document


def _required(row: Mapping[str, Any], field: str) -> str:
    value = row.get(field)
    if value is None or str(value).strip() == "":
        raise DomainMappingError(f"missing required field: {field}")
    return str(value)


def _rule_entity_count(
    inventory: Mapping[str, int],
    rules: Iterable[Mapping[str, Any]],
    *,
    domain_id: str | None = None,
) -> int:
    return sum(
        inventory[str(rule["raw_category"])]
        for rule in rules
        if domain_id is None or rule["domain_id"] == domain_id
    )


def _validate_approved_reviewer_provenance(
    rule: Mapping[str, Any], rule_id: str
) -> None:
    if _required(rule, "review_status") != "reviewed":
        raise DomainMappingError(f"rule {rule_id} is approved but not reviewed")
    _require_reviewer_provenance(rule, rule_id)
    if _required(rule, "reviewer_decision") != "approved_mapped":
        raise DomainMappingError(f"rule {rule_id} lacks an approved mapped decision")


def _validate_reviewed_nonmapped_provenance(
    rule: Mapping[str, Any], rule_id: str
) -> None:
    state = _required(rule, "mapping_state")
    if state not in {"excluded", "unmapped"}:
        raise DomainMappingError(f"rule {rule_id} has unsupported approved state")
    if _required(rule, "review_status") != "reviewed":
        raise DomainMappingError(f"rule {rule_id} is approved but not reviewed")
    _require_reviewer_provenance(rule, rule_id)
    if _required(rule, "reviewer_decision") != f"reviewed_{state}":
        raise DomainMappingError(f"rule {rule_id} lacks a reviewed {state} decision")


def _require_reviewer_provenance(rule: Mapping[str, Any], rule_id: str) -> None:
    if _required(rule, "reviewer") == "unassigned":
        raise DomainMappingError(
            f"rule {rule_id} is approved without reviewer provenance"
        )
    provenance = rule.get("review_provenance")
    if not isinstance(provenance, Mapping):
        raise DomainMappingError(
            f"rule {rule_id} is approved without reviewer provenance"
        )
    _required(provenance, "decision_source")


__all__ = [
    "APPROVAL_STATUSES",
    "CategoryInventoryRow",
    "DOMAIN_MAPPING_CONFIG_PATH",
    "DomainMappingError",
    "MAPPING_STATES",
    "PERMITTED_DOMAIN_IDS",
    "build_category_inventory",
    "effective_approved_mappings",
    "load_domain_mapping_contract",
    "mapping_contract_fingerprint",
    "normalize_source_category",
    "validate_mapping_contract",
    "validate_mapping_against_inventory",
]
