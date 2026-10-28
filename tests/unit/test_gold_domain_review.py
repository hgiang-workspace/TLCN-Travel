from __future__ import annotations

from travel_intelligence.gold.domain_review import build_review_proposals


def test_review_proposals_are_unreviewed_and_never_assign_domains():
    proposals = build_review_proposals(
        [
            {
                "raw_category": "Taxi & xe đưa đón",
                "normalized_category": "Taxi & xe đưa đón",
                "entity_count": 3,
            },
            {
                "raw_category": "Taxi • Bãi biển",
                "normalized_category": "Taxi • Bãi biển",
                "entity_count": 2,
            },
            {"raw_category": "", "normalized_category": None, "entity_count": 1},
        ],
        {"rules": []},
    )
    by_raw = {proposal.raw_category: proposal for proposal in proposals}
    assert by_raw["Taxi & xe đưa đón"].proposed_domain_id == "transportation"
    assert by_raw["Taxi • Bãi biển"].review_group == "composite_conflicting_domains"
    assert by_raw[""].review_group == "missing_or_unclear"
    assert all(proposal.review_status == "unreviewed" for proposal in proposals)
    assert all(
        proposal.runtime_effect == "unmapped_no_assignment" for proposal in proposals
    )


def test_existing_ambiguous_composite_is_grouped_as_conflicting_when_signals_differ():
    proposals = build_review_proposals(
        [
            {
                "raw_category": "Chuyến tham quan nhiều ngày • Taxi & xe đưa đón",
                "normalized_category": (
                    "Chuyến tham quan nhiều ngày • Taxi & xe đưa đón"
                ),
                "entity_count": 4,
            }
        ],
        {
            "rules": [
                {
                    "raw_category": "Chuyến tham quan nhiều ngày • Taxi & xe đưa đón",
                    "mapping_state": "ambiguous",
                    "approval_status": "proposed",
                    "rationale": "Conflicting components.",
                    "rule_id": "rule-1",
                }
            ]
        },
    )
    assert proposals[0].review_group == "composite_conflicting_domains"
