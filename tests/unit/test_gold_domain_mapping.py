from __future__ import annotations

from copy import deepcopy

import pytest

from travel_intelligence.gold.domain_mapping import (
    DomainMappingError,
    build_category_inventory,
    effective_approved_mappings,
    load_domain_mapping_contract,
    mapping_contract_fingerprint,
    normalize_source_category,
    validate_mapping_against_inventory,
    validate_mapping_contract,
)
from travel_intelligence.gold.domain_review import build_review_proposals


def _refingerprint(contract):
    contract["artifact_fingerprint"] = mapping_contract_fingerprint(contract)


def test_category_normalization_preserves_composite_label_and_raw_values():
    inventory = build_category_inventory(
        [
            {"source_id": "tripadvisor", "entity_type_raw": " A • B "},
            {"source_id": "tripadvisor", "entity_type_raw": "A • B"},
        ],
        source_id="tripadvisor",
    )
    assert inventory[0].normalized_category == "A • B"
    assert inventory[0].entity_count == 2
    assert inventory[0].raw_categories == (" A • B ", "A • B")
    assert normalize_source_category("A • B") == "A • B"


def test_inventory_rejects_cross_source_and_blank_categories():
    with pytest.raises(DomainMappingError, match="more than one source"):
        build_category_inventory(
            [{"source_id": "foody", "entity_type_raw": "Nhà hàng"}],
            source_id="tripadvisor",
        )
    with pytest.raises(DomainMappingError, match="blank"):
        normalize_source_category(" \t ")


def test_inventory_keeps_blank_raw_category_as_unmapped_candidate():
    inventory = build_category_inventory(
        [{"source_id": "tripadvisor", "entity_type_raw": ""}],
        source_id="tripadvisor",
    )
    assert inventory[0].normalized_category is None
    assert inventory[0].raw_categories == ("",)


def test_reviewed_b1_artifact_has_exact_effective_assignment_count():
    contract = load_domain_mapping_contract()
    assert contract["source"]["distinct_raw_category_count"] == 582
    assert contract["mapping_version"] == "tripadvisor-domain-mapping-v1.1"
    assert len(effective_approved_mappings(contract)) == 19


def test_unknown_category_fails_closed_to_no_assignment():
    contract = load_domain_mapping_contract()
    mappings = effective_approved_mappings(contract)
    assert ("tripadvisor", "unseen category") not in mappings


def test_fingerprint_is_deterministic_and_exact_match_normalizes_nfc_only():
    contract = load_domain_mapping_contract()
    assert mapping_contract_fingerprint(contract) == contract["artifact_fingerprint"]
    assert (
        mapping_contract_fingerprint(deepcopy(contract))
        == contract["artifact_fingerprint"]
    )
    mappings = effective_approved_mappings(contract)
    religious = normalize_source_category("Địa điểm tôn giáo")
    assert mappings[("tripadvisor", religious)] == "destination"
    mixed = "Chuyến tham quan nhiều ngày • Taxi & xe đưa đón"
    assert mappings[("tripadvisor", mixed)] == "transportation"
    assert ("tripadvisor", "Taxi") not in mappings


def test_only_b1_rules_are_reviewed_and_other_existing_rule_stays_unapproved():
    contract = load_domain_mapping_contract()
    b1 = [
        rule
        for rule in contract["rules"]
        if rule.get("review_provenance", {}).get("review_batch") == "B1-high-impact"
    ]
    outside_b1 = [rule for rule in contract["rules"] if rule not in b1]
    assert len(b1) == 20
    assert all(rule["review_status"] == "reviewed" for rule in b1)
    assert len(outside_b1) == 1
    assert outside_b1[0]["review_status"] == "unreviewed"
    assert outside_b1[0]["approval_status"] == "proposed"


def test_rejects_duplicate_effective_mapping_for_same_source_category():
    contract = deepcopy(load_domain_mapping_contract())
    first = contract["rules"][0]
    duplicate = deepcopy(first)
    duplicate["rule_id"] = "duplicate"
    duplicate["domain_id"] = "destination"
    contract["rules"].append(duplicate)
    _refingerprint(contract)
    with pytest.raises(DomainMappingError, match="multiple effective"):
        validate_mapping_contract(contract)


def test_rejects_invalid_state_domain_and_approved_mapping_without_reviewer():
    contract = deepcopy(load_domain_mapping_contract())
    contract["rules"][15]["domain_id"] = "accommodation"
    _refingerprint(contract)
    with pytest.raises(DomainMappingError, match="may not set a domain"):
        validate_mapping_contract(contract)

    contract = deepcopy(load_domain_mapping_contract())
    contract["rules"][0]["domain_id"] = "wellness"
    _refingerprint(contract)
    with pytest.raises(DomainMappingError, match="invalid domain"):
        validate_mapping_contract(contract)

    contract = deepcopy(load_domain_mapping_contract())
    contract["rules"][0]["reviewer"] = "unassigned"
    _refingerprint(contract)
    with pytest.raises(DomainMappingError, match="without reviewer provenance"):
        validate_mapping_contract(contract)


def test_all_twenty_explicit_b1_decisions_and_inventory_reconcile():
    contract = load_domain_mapping_contract()
    expected = {
        "Spa": "destination",
        "Cửa hàng đặc sản & cửa hàng quà tặng": "destination",
        "Quán bar & câu lạc bộ": "food_dining",
        "Tour thăm quan thành phố • Chuyến tham quan nhiều ngày": "destination",
        "Chuyến tham quan nhiều ngày • Chuyến tham quan tư nhân": "destination",
        "Điểm thu hút khách tham quan & thắng cảnh": "destination",
        "Địa điểm tôn giáo": "destination",
        "Taxi & xe đưa đón": "transportation",
        "Tour thăm quan thành phố • Chuyến tham quan văn hóa": "destination",
        "Chuyến tham quan nhiều ngày • Taxi & xe đưa đón": "transportation",
        "Spa • Spa nước nóng": "destination",
        "Trung tâm mua sắm": "destination",
        "Chuyến tham quan nhiều ngày • Chuyến tham quan bằng thuyền": "destination",
        "Chuyến tham quan nhiều ngày • Chuyến tham quan văn hóa": "destination",
        "Cho thuê tiện ích": None,
        "Chuyến tham quan bằng thuyền": "destination",
        "Chợ trời & chợ đường phố": "destination",
        "Lớp nấu ăn": "destination",
        "Phòng trưng bày nghệ thuật": "destination",
        "Chuyến tham quan ngắm cảnh": "destination",
    }
    reviewed = {
        rule["raw_category"]: rule
        for rule in contract["rules"]
        if rule.get("review_provenance", {}).get("review_batch") == "B1-high-impact"
    }
    assert {key: rule["domain_id"] for key, rule in reviewed.items()} == expected
    assert all(rule["reviewer"] == "hgiang" for rule in reviewed.values())
    assert all(rule["approval_status"] == "approved" for rule in reviewed.values())
    inventory = [
        {"raw_category": raw, "entity_count": count}
        for raw, count in (
            ("Spa", 691),
            ("Cửa hàng đặc sản & cửa hàng quà tặng", 193),
            ("Quán bar & câu lạc bộ", 108),
            ("Tour thăm quan thành phố • Chuyến tham quan nhiều ngày", 106),
            ("Chuyến tham quan nhiều ngày • Chuyến tham quan tư nhân", 84),
            ("Điểm thu hút khách tham quan & thắng cảnh", 81),
            ("Địa điểm tôn giáo", 79),
            ("Taxi & xe đưa đón", 77),
            ("Tour thăm quan thành phố • Chuyến tham quan văn hóa", 74),
            ("Chuyến tham quan nhiều ngày • Taxi & xe đưa đón", 66),
            ("Spa • Spa nước nóng", 65),
            ("Trung tâm mua sắm", 59),
            ("Chuyến tham quan nhiều ngày • Chuyến tham quan bằng thuyền", 51),
            ("Chuyến tham quan nhiều ngày • Chuyến tham quan văn hóa", 48),
            ("Cho thuê tiện ích", 47),
            ("Chuyến tham quan bằng thuyền", 45),
            ("Chợ trời & chợ đường phố", 38),
            ("Lớp nấu ăn", 37),
            ("Phòng trưng bày nghệ thuật", 36),
            ("Chuyến tham quan ngắm cảnh", 35),
        )
    ]
    inventory.extend(
        {"raw_category": f"unreviewed-{number}", "entity_count": 1}
        for number in range(561)
    )
    inventory.append({"raw_category": "unreviewed-large", "entity_count": 1419})
    summary = validate_mapping_against_inventory(contract, inventory)
    assert summary["mapped_entity_count"] == 1973
    assert summary["excluded_category_count"] == 1
    assert summary["excluded_entity_count"] == 47
    assert summary["unmapped_category_count"] == 0
    assert summary["total_entity_count"] == 4000


def test_reviewed_excluded_category_has_no_assignment_or_pending_review_row():
    contract = load_domain_mapping_contract()
    excluded = next(
        rule
        for rule in contract["rules"]
        if rule["raw_category"] == "Cho thuê tiện ích"
    )
    assert excluded["mapping_state"] == "excluded"
    assert excluded["reviewer_decision"] == "reviewed_excluded"
    assert excluded["domain_id"] is None
    assert ("tripadvisor", "Cho thuê tiện ích") not in effective_approved_mappings(
        contract
    )
    queue = build_review_proposals(
        [
            {
                "raw_category": "Cho thuê tiện ích",
                "normalized_category": "Cho thuê tiện ích",
                "entity_count": 47,
            }
        ],
        contract,
    )
    assert queue[0].review_status == "reviewed"
    assert queue[0].proposed_disposition == "reviewed_excluded"


