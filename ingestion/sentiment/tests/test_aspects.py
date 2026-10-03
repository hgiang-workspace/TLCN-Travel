"""Tests cho ABSA aspects package."""

from __future__ import annotations

import pytest

from ingestion.sentiment.aspects import (
    load_aspect_keywords,
    build_aspect_keyword_index,
    get_all_aspects,
    detect_aspects,
    deduplicate_aspects,
    run_s6_aspect_detection,
)
from ingestion.sentiment.aspects.detect import AspectMatch


def test_load_aspect_keywords():
    """Test nạp từ điển aspect từ YAML."""
    keywords = load_aspect_keywords()
    assert isinstance(keywords, dict)
    assert len(keywords) > 0
    assert "food" in keywords
    assert "service" in keywords
    assert "other" in keywords
    assert isinstance(keywords["food"], list)
    assert len(keywords["food"]) > 0


def test_build_aspect_keyword_index():
    """Test build index keyword -> aspect."""
    keywords = load_aspect_keywords()
    index = build_aspect_keyword_index(keywords)
    assert isinstance(index, dict)
    # Mỗi keyword chỉ map tới 1 aspect (đầu tiên)
    assert index.get("ngon") == "food"
    assert index.get("phục vụ") == "service"


def test_get_all_aspects():
    """Test lấy danh sách tất cả aspects."""
    aspects = get_all_aspects()
    assert isinstance(aspects, list)
    assert len(aspects) > 0
    assert "food" in aspects
    assert "service" in aspects
    assert aspects == sorted(aspects)  # Đã sắp xếp


def test_detect_aspects_basic():
    """Test phát hiện aspect cơ bản."""
    keywords = load_aspect_keywords()
    index = build_aspect_keyword_index(keywords)

    text = "món ăn rất ngon và phục vụ tốt"
    matches = detect_aspects(text, keyword_index=index)

    assert len(matches) >= 2
    aspect_names = {m.aspect for m in matches}
    assert "food" in aspect_names
    assert "service" in aspect_names


def test_detect_aspects_negation():
    """Test phát hiện phủ định."""
    keywords = load_aspect_keywords()
    index = build_aspect_keyword_index(keywords)

    # "không ngon" -> food bị phủ định
    text = "món ăn không ngon"
    matches = detect_aspects(text, keyword_index=index)

    # Có thể match nhiều keyword cho food (món ăn, món, ngon)
    # Lọc ra các match của food
    food_matches = [m for m in matches if m.aspect == "food"]
    # Ít nhất 1 match của food phải bị phủ định (chứa "ngon")
    negated_food = [m for m in food_matches if m.negated]
    assert len(negated_food) >= 1
    # Và ít nhất 1 match không bị phủ định (chứa "món ăn" hoặc "món")
    non_negated_food = [m for m in food_matches if not m.negated]
    assert len(non_negated_food) >= 1


def test_detect_aspects_no_match():
    """Test khi không khớp aspect nào."""
    keywords = load_aspect_keywords()
    index = build_aspect_keyword_index(keywords)

    text = "xyz abc def"  # Không có keyword nào
    matches = detect_aspects(text, keyword_index=index)
    assert len(matches) == 0


def test_deduplicate_aspects():
    """Test loại bỏ aspect trùng lặp."""
    matches = [
        AspectMatch(aspect="food", keyword="ngon", start=0, end=4, negated=False),
        AspectMatch(aspect="food", keyword="món ăn", start=10, end=16, negated=False),
        AspectMatch(aspect="service", keyword="phục vụ", start=20, end=27, negated=True),
    ]
    deduped = deduplicate_aspects(matches)
    assert len(deduped) == 2
    aspects = {m.aspect for m in deduped}
    assert aspects == {"food", "service"}


def test_deduplicate_prefer_non_negated():
    """Test ưu tiên match không bị phủ định khi dedupe."""
    matches = [
        AspectMatch(aspect="food", keyword="ngon", start=0, end=4, negated=True),
        AspectMatch(aspect="food", keyword="món ăn", start=10, end=16, negated=False),
    ]
    deduped = deduplicate_aspects(matches)
    assert len(deduped) == 1
    assert deduped[0].negated is False


def test_run_s6_aspect_detection():
    """Test chạy S6 aspect detection."""
    s2_rows = [
        {
            "source": "tripadvisor",
            "comment_id": "c1",
            "place_id": "p1",
            "place_name": "Test Place",
            "sentence_id": "c1_s0",
            "sentence_hash": "abc123",
            "sentence_text": "món ăn ngon, phục vụ tốt",
            "sentence_idx": 0,
            "rating": 4.5,
            "run_id": "test_run",
            "processed_at": "2024-01-01T00:00:00",
        },
        {
            "source": "tripadvisor",
            "comment_id": "c2",
            "place_id": "p1",
            "place_name": "Test Place",
            "sentence_id": "c2_s0",
            "sentence_hash": "def456",
            "sentence_text": "không gian đẹp",
            "sentence_idx": 0,
            "rating": 4.0,
            "run_id": "test_run",
            "processed_at": "2024-01-01T00:00:00",
        },
    ]

    s6_rows = run_s6_aspect_detection(s2_rows, run_id="test_run")

    assert len(s6_rows) >= 2  # Ít nhất 2 aspect (food, service, ambiance)
    for row in s6_rows:
        assert row["run_id"] == "test_run"
        assert "aspect" in row
        assert "aspect_keyword" in row
        assert "aspect_negated" in row


def test_run_s6_aspect_detection_other():
    """Test S6 gán 'other' khi không khớp aspect."""
    s2_rows = [
        {
            "source": "tripadvisor",
            "comment_id": "c1",
            "place_id": "p1",
            "place_name": "Test Place",
            "sentence_id": "c1_s0",
            "sentence_hash": "abc123",
            "sentence_text": "xyz abc def",  # Không có keyword
            "sentence_idx": 0,
            "rating": 4.5,
            "run_id": "test_run",
            "processed_at": "2024-01-01T00:00:00",
        },
    ]

    s6_rows = run_s6_aspect_detection(s2_rows, run_id="test_run")

    assert len(s6_rows) == 1
    assert s6_rows[0]["aspect"] == "other"
    assert s6_rows[0]["aspect_keyword"] == ""


if __name__ == "__main__":
    pytest.main([__file__, "-v"])