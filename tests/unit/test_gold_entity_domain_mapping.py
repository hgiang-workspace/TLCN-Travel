from __future__ import annotations

from travel_intelligence.gold.domain_mapping import load_domain_mapping_contract
from travel_intelligence.gold.entity_domain_mapping import plan_entity_domain_mapping

RELEASE_ID = "gold-release-test"


def _entity(entity_id: str, category: str, domain: str | None = None):
    return {
        "entity_id": entity_id,
        "entity_type_raw": category,
        "product_domain_id": domain,
    }


def _bridge(entity_id: str):
    return {
        "release_id": RELEASE_ID,
        "entity_id": entity_id,
        "source_id": "tripadvisor",
    }


def test_application_uses_exact_approved_rules_and_exclusion_is_unassigned():
    contract = load_domain_mapping_contract()
    entities = [
        _entity("spa", "Spa"),
        _entity("mixed", "Chuyến tham quan nhiều ngày • Taxi & xe đưa đón"),
        _entity("excluded", "Cho thuê tiện ích"),
        _entity("unreviewed", "Bãi biển"),
    ]
    plan = plan_entity_domain_mapping(
        entities, [_bridge(row["entity_id"]) for row in entities], contract,
        release_id=RELEASE_ID,
    )
    updates = {row["entity_id"]: row["product_domain_id"] for row in plan.updated_rows}
    assert updates == {"spa": "destination", "mixed": "transportation"}
    assert plan.report["excluded_entity_count"] == 1
    assert plan.report["unreviewed_entity_count"] == 1


def test_application_is_idempotent_after_the_same_rows_are_applied():
    contract = load_domain_mapping_contract()
    entities = [_entity("spa", "Spa")]
    first = plan_entity_domain_mapping(
        entities, [_bridge("spa")], contract, release_id=RELEASE_ID
    )
    second = plan_entity_domain_mapping(
        first.updated_rows, [_bridge("spa")], contract, release_id=RELEASE_ID
    )
    assert len(first.updated_rows) == 1
    assert second.updated_rows == ()
