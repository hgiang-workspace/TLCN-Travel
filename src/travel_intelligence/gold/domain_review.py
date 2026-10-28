"""Proposal-only review workflow for exact source-category domain mappings."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass
from typing import Any

from travel_intelligence.gold.domain_mapping import normalize_source_category

_DOMAIN_SIGNALS = {
    "destination": (
        "điểm thu hút",
        "thắng cảnh",
        "địa điểm tôn giáo",
        "bãi biển",
        "bảo tàng",
        "di tích",
        "lịch sử",
        "triển lãm",
        "phòng trưng bày",
        "chuyến tham quan",
        "tour thăm quan",
        "chuyến đi trong ngày",
    ),
    "accommodation": (
        "khách sạn",
        "hotel",
        "resort",
        "homestay",
        "nhà nghỉ",
        "villa",
        "hostel",
    ),
    "food_dining": (
        "nhà hàng",
        "quán ăn",
        "quán bar",
        "cà phê",
        "cafe",
        "ẩm thực",
        "tiệm bánh",
    ),
    "transportation": (
        "taxi",
        "xe đưa đón",
        "cho thuê xe",
        "xe buýt",
        "ga tàu",
        "sân bay",
        "phà",
    ),
}
_OUTSIDE_SIGNALS = (
    "spa",
    "mua sắm",
    "cửa hàng",
    "lớp nấu ăn",
    "trung tâm trò chơi",
    "giải trí",
)


@dataclass(frozen=True)
class ReviewProposal:
    raw_category: str
    normalized_category: str | None
    entity_count: int
    review_group: str
    component_evidence: str
    proposed_disposition: str
    proposed_domain_id: str | None
    rationale: str
    existing_rule_id: str | None
    existing_rule_status: str | None
    requires_product_owner: bool
    original_proposal_disposition: str | None = None
    reviewer: str = ""
    reviewer_decision: str = ""
    review_status: str = "unreviewed"
    runtime_effect: str = "unmapped_no_assignment"


def build_review_proposals(
    raw_categories: Iterable[Mapping[str, Any]],
    mapping_contract: Mapping[str, Any],
) -> tuple[ReviewProposal, ...]:
    """Classify categories for human review without producing mappings."""
    existing = {
        str(rule["raw_category"]): rule
        for rule in mapping_contract.get("rules", [])
    }
    proposals = []
    for row in raw_categories:
        raw = str(row["raw_category"])
        normalized = row.get("normalized_category")
        count = int(row["entity_count"])
        rule = existing.get(raw)
        if rule is not None:
            proposals.append(_from_existing_rule(raw, normalized, count, rule))
        else:
            proposals.append(_proposal_from_components(raw, normalized, count))
    return tuple(
        sorted(proposals, key=lambda row: (-row.entity_count, row.raw_category))
    )


def _from_existing_rule(
    raw: str, normalized: str | None, count: int, rule: Mapping[str, Any]
) -> ReviewProposal:
    disposition = (
        (
            "approved_mapped"
            if rule["mapping_state"] == "mapped"
            else f"reviewed_{rule['mapping_state']}"
        )
        if rule["approval_status"] == "approved"
        else f"proposed_{rule['mapping_state']}"
    )
    domain = rule.get("domain_id")
    is_spa = normalize_source_category(raw).casefold() == "spa"
    category = normalize_source_category(raw)
    components = tuple(part.strip() for part in category.split(" • "))
    review_group = "simple_unambiguous"
    if rule["mapping_state"] == "ambiguous":
        domains = _domain_hits(components)
        outside = _outside_hits(components)
        review_group = (
            "composite_conflicting_domains"
            if len(components) > 1 and (len(domains) > 1 or (domains and outside))
            else "missing_or_unclear"
        )
    return ReviewProposal(
        raw_category=raw,
        normalized_category=normalized,
        entity_count=count,
        review_group=review_group,
        component_evidence=raw,
        proposed_disposition=disposition,
        proposed_domain_id=str(domain) if domain else None,
        rationale=str(rule["rationale"]),
        existing_rule_id=str(rule["rule_id"]),
        existing_rule_status=str(rule["approval_status"]),
        requires_product_owner=is_spa,
        original_proposal_disposition=rule.get("original_proposal_disposition"),
        reviewer=str(rule.get("reviewer", "")),
        reviewer_decision=str(rule.get("reviewer_decision") or ""),
        review_status=str(rule.get("review_status", "unreviewed")),
    )


def _proposal_from_components(
    raw: str, normalized: str | None, count: int
) -> ReviewProposal:
    if normalized is None:
        return ReviewProposal(
            raw_category=raw,
            normalized_category=None,
            entity_count=count,
            review_group="missing_or_unclear",
            component_evidence="empty raw category",
            proposed_disposition="unreviewed_unmapped",
            proposed_domain_id=None,
            rationale="Missing source category cannot support a domain proposal.",
            existing_rule_id=None,
            existing_rule_status=None,
            requires_product_owner=False,
        )
    components = tuple(part.strip() for part in normalized.split(" • "))
    domains = _domain_hits(components)
    outside = _outside_hits(components)
    component_evidence = "; ".join(components)
    if len(components) > 1 and len(domains) == 1 and not outside:
        domain = next(iter(domains))
        return _proposal(
            raw, normalized, count, "composite_same_domain", "proposed_mapped", domain,
            f"All composite components contain only {domain} review signals.",
            component_evidence,
        )
    if len(components) > 1 and (len(domains) > 1 or (domains and outside)):
        return _proposal(
            raw,
            normalized,
            count,
            "composite_conflicting_domains",
            "proposed_ambiguous",
            None,
            (
                "Composite components indicate more than one product-domain or "
                "outside-taxonomy meaning."
            ),
            component_evidence,
        )
    if len(domains) == 1 and not outside:
        domain = next(iter(domains))
        return _proposal(
            raw, normalized, count, "simple_unambiguous", "proposed_mapped", domain,
            f"Exact category has only {domain} review signals; reviewer must confirm.",
            component_evidence,
        )
    if outside and not domains:
        return _proposal(
            raw,
            normalized,
            count,
            "outside_four_domain_taxonomy",
            "proposed_excluded",
            None,
            (
                "Category signals a service/activity outside the approved "
                "four-domain taxonomy."
            ),
            component_evidence,
            requires_product_owner="spa" in outside,
        )
    return _proposal(
        raw,
        normalized,
        count,
        "missing_or_unclear",
        "unreviewed_unmapped",
        None,
        (
            "No single exact category-to-domain proposal is safe from "
            "source-category text alone."
        ),
        component_evidence,
    )


def _proposal(
    raw: str,
    normalized: str,
    count: int,
    group: str,
    disposition: str,
    domain: str | None,
    rationale: str,
    evidence: str,
    *,
    requires_product_owner: bool = False,
) -> ReviewProposal:
    return ReviewProposal(
        raw_category=raw,
        normalized_category=normalized,
        entity_count=count,
        review_group=group,
        component_evidence=evidence,
        proposed_disposition=disposition,
        proposed_domain_id=domain,
        rationale=rationale,
        existing_rule_id=None,
        existing_rule_status=None,
        requires_product_owner=requires_product_owner,
    )


def _domain_hits(components: Iterable[str]) -> set[str]:
    text = " ".join(components).casefold()
    return {
        domain
        for domain, signals in _DOMAIN_SIGNALS.items()
        if any(signal in text for signal in signals)
    }


def _outside_hits(components: Iterable[str]) -> set[str]:
    text = " ".join(components).casefold()
    return {signal for signal in _OUTSIDE_SIGNALS if signal in text}


def proposal_dict(proposal: ReviewProposal) -> dict[str, Any]:
    """Serialize a review-only row; never expose a domain assignment field."""
    return asdict(proposal)


__all__ = ["ReviewProposal", "build_review_proposals", "proposal_dict"]
