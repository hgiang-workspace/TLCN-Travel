import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from travel_intelligence.ingestion.multisource import (
    adapt_foody,
    adapt_tiktok,
    adapt_vexere,
)
from travel_intelligence.ingestion.tripadvisor import adapt_tripadvisor
from travel_intelligence.silver.normalization import (
    _timestamp,
    bronze_parquet_timestamp,
    nested_input_count,
    normalize_foody_row,
    normalize_tiktok_row,
    normalize_tripadvisor_row,
    normalize_vexere_row,
    reconcile_manifest_nested_count,
    validate_and_normalize_manifest,
)
from travel_intelligence.silver.province import normalize_province


def test_nested_input_count_uses_explicit_source_mapping():
    assert nested_input_count(
        "tripadvisor",
        {"review": [{"review_id": "r1"}, {"review_id": "r2"}]}
    ) == 2
    assert nested_input_count(
        "tiktok", {"social_comment": [{"comment_id": "c1"}]}
    ) == 1
    assert nested_input_count("foody", {"review": [{"review_id": "ignored"}]}) == 0
    assert nested_input_count(
        "vexere", {"social_comment": [{"comment_id": "ignored"}]}
    ) == 0


@pytest.mark.parametrize("count", [0, 1, 3])
def test_tiktok_nested_input_count_handles_zero_one_and_multiple(count):
    records = {"social_comment": [{"comment_id": str(i)} for i in range(count)]}
    assert nested_input_count("tiktok", records) == count


def test_nested_input_count_rejects_unsupported_source():
    with pytest.raises(ValueError, match="Unsupported source"):
        nested_input_count("unknown", {})


def test_manifest_nested_count_accepts_match_or_absence():
    reconcile_manifest_nested_count({}, 2)
    reconcile_manifest_nested_count({"nested_review_count": 2}, 2)


@pytest.mark.parametrize("manifest_count", [3, -1, True, "2"])
def test_manifest_nested_count_rejects_mismatch_or_invalid_value(manifest_count):
    with pytest.raises(ValueError, match="nested"):
        reconcile_manifest_nested_count(
            {"nested_review_count": manifest_count},
            2,
        )


def test_timestamp_accepts_aware_datetime_and_normalizes_to_utc():
    value = _timestamp(datetime(2026, 1, 1, 8, 0, tzinfo=timezone.utc))
    assert value == datetime(2026, 1, 1, 8, 0, tzinfo=timezone.utc)


def test_bronze_parquet_naive_datetime_is_trusted_as_utc():
    value = bronze_parquet_timestamp(datetime(2026, 1, 1, 8, 0))
    assert value == datetime(2026, 1, 1, 8, 0, tzinfo=timezone.utc)


def test_bronze_parquet_aware_values_are_normalized_once():
    value = bronze_parquet_timestamp(
        datetime(2026, 1, 1, 10, 0, tzinfo=timezone.utc)
    )
    offset = bronze_parquet_timestamp("2026-01-01T10:00:00+02:00")
    assert value == datetime(2026, 1, 1, 10, 0, tzinfo=timezone.utc)
    assert offset == datetime(2026, 1, 1, 8, 0, tzinfo=timezone.utc)


def test_timestamp_accepts_iso_strings_and_timezone_offsets():
    assert _timestamp("2026-01-01T08:00:00Z").hour == 8
    assert _timestamp("2026-01-01T10:00:00+02:00").hour == 8


@pytest.mark.parametrize(
    "value, error",
    [
        (datetime(2026, 1, 1, 8, 0), "Timezone-naive"),
        ("2026-01-01T08:00:00", "Timezone-naive"),
        ("not-a-timestamp", "Invalid ISO-8601"),
        (123, "Timestamp must be"),
    ],
)
def test_timestamp_rejects_unsupported_or_timezone_unknown_values(value, error):
    with pytest.raises((TypeError, ValueError), match=error):
        _timestamp(value)


def test_tripadvisor_review_occurrences_and_lineage():
    result = adapt_tripadvisor(Path("tests/fixtures/tripadvisor_minimal.json"))
    normalized = normalize_tripadvisor_row(result.rows[0], "bronze/ta.parquet")
    assert len(normalized["source_record"]) == 1
    assert len(normalized["place"]) == 1
    assert len(normalized["place_rating"]) == 1
    assert len(normalized["review"]) == 2
    assert [row["review_ordinal"] for row in normalized["review"]] == [0, 1]
    assert len({row["review_id"] for row in normalized["review"]}) == 2
    assert all(
        row["source_record_sk"] == normalized["source_record"][0]["source_record_sk"]
        for row in normalized["review"]
    )


@pytest.mark.parametrize(
    ("raw", "canonical"),
    [
        ("TP. HCM", "Hồ Chí Minh"),
        ("Da Nang", "Đà Nẵng"),
        ("Khánh Hoà", "Khánh Hòa"),
    ],
)
def test_province_rules_normalize_approved_aliases(raw, canonical):
    result = normalize_province(raw)
    assert result["province_raw"] == raw
    assert result["province_canonical"] == canonical
    assert result["province_resolution_status"] == "resolved"
    assert result["province_match_method"] == "approved_alias"


def test_province_rules_do_not_infer_unknown_values():
    result = normalize_province("Một địa danh không có trong rule")
    assert result["province_canonical"] is None
    assert result["province_geo_id"] is None
    assert result["province_resolution_status"] == "unknown"


def test_foody_has_rating_and_no_fabricated_reviews():
    result = adapt_foody(Path("tests/fixtures/foody_minimal.json"))
    normalized = normalize_foody_row(result.rows[0], "bronze/foody.parquet")
    assert len(normalized["place"]) == 1
    assert normalized["place_rating"][0]["price_min"] == 12000
    assert normalized["place_rating"][0]["rating"] is None
    assert normalized["review"] == []


def test_tiktok_preserves_comment_occurrences_and_count_mismatch():
    result = adapt_tiktok(Path("tests/fixtures/tiktok_minimal.json"))
    normalized = normalize_tiktok_row(result.rows[0], "bronze/tiktok.parquet")
    content = normalized["social_content"][0]
    assert content["reported_comment_count"] == 2
    assert content["collected_comment_count"] == 2
    assert normalized["social_comment"][0]["comment_ordinal"] == 0
    assert len({item["comment_id"] for item in normalized["social_comment"]}) == 2
    assert nested_input_count("tiktok", normalized) == 2


def test_tiktok_nested_count_uses_collected_not_reported_comments(tmp_path):
    snapshot = tmp_path / "reported-collected-mismatch.json"
    snapshot.write_text(
        json.dumps(
            [
                {
                    "video_id": "reported-mismatch",
                    "comment_count": 99,
                    "comments": [{"comment_id": "one", "text": "synthetic"}],
                }
            ]
        ),
        encoding="utf-8",
    )
    adapted = adapt_tiktok(snapshot)
    normalized = normalize_tiktok_row(adapted.rows[0], "bronze/tiktok.parquet")

    assert normalized["social_content"][0]["reported_comment_count"] == 99
    assert normalized["social_content"][0]["collected_comment_count"] == 1
    assert nested_input_count("tiktok", normalized) == 1


def test_tiktok_duplicate_video_observations_remain_distinct(tmp_path):
    snapshot = tmp_path / "duplicate-tiktok.json"
    record = {
        "video_id": "same-video",
        "video_url": "https://example.test/video/same-video",
        "comment_count": 0,
        "comments": [],
    }
    snapshot.write_text(json.dumps([record, record]), encoding="utf-8")
    result = adapt_tiktok(snapshot)
    normalized = [
        normalize_tiktok_row(row, "bronze/tiktok.parquet")
        for row in result.rows
    ]
    assert len(normalized) == 2
    assert normalized[0]["source_record"][0]["source_record_sk"] != (
        normalized[1]["source_record"][0]["source_record_sk"]
    )


def test_tiktok_empty_comments_has_zero_collected_count():
    result = adapt_tiktok(Path("tests/fixtures/tiktok_minimal.json"))
    row = result.rows[0]
    payload = json.loads(row.payload_json)
    payload["comments"] = []
    payload["comment_count"] = 0
    row = row.__class__(**{**row.__dict__, "payload_json": json.dumps(payload)})
    normalized = normalize_tiktok_row(row, "bronze/tiktok.parquet")
    assert normalized["social_content"][0]["collected_comment_count"] == 0
    assert normalized["social_comment"] == []


def test_vexere_preserves_route_observation_and_raw_times():
    result = adapt_vexere(Path("tests/fixtures/vexere_minimal.json"))
    normalized = normalize_vexere_row(result.rows[0], "bronze/vexere.parquet")
    route = normalized["transport_route"][0]
    assert route["departure_time_raw"] == "11:00"
    assert route["arrival_time_raw"] == "13:25"
    assert route["crawl_date_raw"] == "27-09-2026"
    assert route["crawl_date"].isoformat() == "2026-09-27"


def test_vexere_repeated_url_and_anomalies_remain_observations(tmp_path):
    snapshot = tmp_path / "duplicate-vexere.json"
    records = [
        {
            "origin": "A",
            "destination": "B",
            "operator_name": "O",
                "departure_time": "unknown",
                "arrival_time": "unknown",
            "duration_min": -1,
            "price_vnd": -2,
            "crawl_date": "ambiguous",
            "source_url": "https://example.test/route",
        },
        {
            "origin": "A",
            "destination": "B",
            "operator_name": "O",
                "departure_time": "unknown",
                "arrival_time": "unknown",
                "crawl_date": "27-09-2026",
            "source_url": "https://example.test/route",
        },
    ]
    snapshot.write_text(json.dumps(records), encoding="utf-8")
    result = adapt_vexere(snapshot)
    normalized = [
        normalize_vexere_row(row, "bronze/vexere.parquet") for row in result.rows
    ]
    assert len(normalized) == 2
    assert normalized[0]["transport_route"][0]["source_url"] == (
        normalized[1]["transport_route"][0]["source_url"]
    )
    quality_codes = normalized[0]["transport_route"][0]["quality_codes"]
    assert "duration_anomaly" in quality_codes
    assert "crawl_date_unparseable" in quality_codes


def test_v1_manifest_is_rejected_before_payload_read():
    manifest = {
        "status": "success",
        "contract_version": "bronze-contract-v1",
        "schema_version": "bronze-v1",
        "source_id": "tripadvisor",
        "batch_id": "b1",
        "parquet_key": "bronze/x",
        "row_count": 0,
        "payload_hashes": [],
    }
    with pytest.raises(ValueError, match="not eligible"):
        validate_and_normalize_manifest(manifest)

