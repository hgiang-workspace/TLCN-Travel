from __future__ import annotations

from datetime import date, datetime

import pytest

from travel_intelligence.gold.dimensions import (
    GoldDimensionError,
    deterministic_key,
    project_date_dimension,
    project_dimensions_and_bridges,
)


def context():
    return {
        "release_id": "release-1",
        "gold_run_id": "run-1",
        "contract_version": "gold-contract-v1",
        "config_fingerprint": "sha256:config",
        "source_release_id": "silver-release-1",
    }


def place(
    place_id: str = "place-1", *, status: str = "resolved", geo: str | None = "geo-1"
):
    return {
        "place_id": place_id,
        "place_name": "Đà Nẵng Unicode",
        "place_type": "Khách sạn",
        "province_geo_id": geo,
        "province_canonical": "Đà Nẵng",
        "province_resolution_status": "resolved" if geo else "unknown",
        "province_rule_version": "province-v1",
        "identity_status": status,
        "identity_method": "source_resolved",
        "updated_at": datetime(2026, 1, 1),
    }


def mapping(
    mapping_id: str = "mapping-1",
    *,
    place_id: str | None = "place-1",
    status: str = "resolved",
):
    return {
        "mapping_id": mapping_id,
        "source_record_sk": f"record-{mapping_id}",
        "source_record_id": "source-1",
        "place_id": place_id,
        "decision_status": status,
        "mapping_method": "source_record",
        "confidence": 1.0,
        "mapping_version": "mapping-v1",
    }


def test_projection_is_deterministic_and_preserves_unicode():
    first = project_dimensions_and_bridges(
        [place()],
        [mapping()],
        context=context(),
        valid_from=datetime(2026, 1, 2),
        built_at=datetime(2026, 1, 2),
        source_id="tripadvisor",
        source_name="TripAdvisor",
    )
    second = project_dimensions_and_bridges(
        [place()],
        [mapping()],
        context=context(),
        valid_from=datetime(2026, 1, 2),
        built_at=datetime(2026, 1, 2),
        source_id="tripadvisor",
        source_name="TripAdvisor",
    )
    assert first == second
    assert first["dim_entity"][0]["entity_name"] == "Đà Nẵng Unicode"
    assert len(first["dim_geography"]) == 1
    assert len(first["bridge_entity_source"]) == 1
    assert len(first["bridge_entity_geography"]) == 1
    assert first["dim_entity"][0]["product_domain_id"] is None


def test_projection_excludes_unresolved_mapping_and_geography_without_fabrication():
    result = project_dimensions_and_bridges(
        [place(), place("place-2", geo=None)],
        [
            mapping(),
            mapping("mapping-2", place_id="place-2"),
            mapping("bad", status="ambiguous"),
        ],
        context=context(),
        valid_from=datetime(2026, 1, 2),
        built_at=datetime(2026, 1, 2),
        source_id="tripadvisor",
        source_name="TripAdvisor",
    )
    assert len(result["dim_entity"]) == 2
    assert len(result["bridge_entity_geography"]) == 1
    assert result["exclusion_counts"] == {
        "unresolved_or_ambiguous_mapping": 1,
        "unresolved_geography": 1,
    }


def test_projection_rejects_multiple_resolved_parents_for_one_source_record():
    duplicate = mapping("mapping-2")
    duplicate["source_record_sk"] = "record-mapping-1"
    with pytest.raises(GoldDimensionError, match="multiple resolved mappings"):
        project_dimensions_and_bridges(
            [place()],
            [mapping(), duplicate],
            context=context(),
            valid_from=datetime(2026, 1, 2),
            built_at=datetime(2026, 1, 2),
            source_id="tripadvisor",
            source_name="TripAdvisor",
        )


def test_deterministic_key_and_bounded_date_dimension():
    assert deterministic_key("entity", "a") == deterministic_key("entity", "a")
    dates = project_date_dimension(
        date(2026, 1, 31),
        date(2026, 2, 1),
        contract_version="v1",
        built_at=datetime(2026, 2, 1),
    )
    assert [row["date_sk"] for row in dates] == [20260131, 20260201]
    assert dates[1]["calendar_quarter"] == 1
