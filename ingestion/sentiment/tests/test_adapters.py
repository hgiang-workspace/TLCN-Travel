"""Tests cho adapters."""

from __future__ import annotations

import pytest

from ingestion.sentiment.adapters import extract_comments_tripadvisor, extract_comments_tiktok, get_adapter


class TestTripAdvisorAdapter:
    def test_extract_basic(self):
        raw = [{
            "record_id": "1",
            "entity_name": "Test Hotel",
            "province": "Hà Nội",
            "rating": 4.5,
            "reviews": [{
                "index": 1,
                "detail_review": "Phòng sạch, view đẹp",
                "key_review": "Tuyệt vời",
            }]
        }]
        comments = extract_comments_tripadvisor(raw)
        assert len(comments) == 1
        c = comments[0]
        assert c.source == "tripadvisor"
        assert c.comment_id == "1"
        assert c.place_id == "1"
        assert c.place_name == "Test Hotel"
        assert c.raw_text == "Phòng sạch, view đẹp"
        assert c.rating == 4.5
        assert c.meta["province"] == "Hà Nội"

    def test_skip_empty_content(self):
        raw = [{
            "record_id": "1",
            "entity_name": "Test",
            "rating": 4.0,
            "reviews": [
                {"index": 1, "detail_review": "", "key_review": ""},
                {"index": 2, "detail_review": "Valid", "key_review": "Good"},
            ]
        }]
        comments = extract_comments_tripadvisor(raw)
        assert len(comments) == 1
        assert comments[0].comment_id == "2"

    def test_fallback_content_field(self):
        raw = [{
            "record_id": "1",
            "entity_name": "Test",
            "rating": 4.0,
            "reviews": [{"index": 1, "key_review": "Dùng field key_review", "detail_review": ""}]
        }]
        comments = extract_comments_tripadvisor(raw)
        assert len(comments) == 1
        assert comments[0].raw_text == "Dùng field key_review"

    def test_blocked_not_applicable(self):
        # Dữ liệu thực tế không có crawl_status, test này không còn áp dụng
        raw = [{
            "record_id": "1",
            "entity_name": "Test",
            "rating": 4.0,
            "reviews": [{"index": 1, "detail_review": "Test"}]
        }]
        comments = extract_comments_tripadvisor(raw)
        assert len(comments) == 1


class TestTikTokAdapter:
    def test_extract_basic(self):
        raw = [{
            "aweme_id": "vid123",
            "desc": "Check-in resort",
            "comments": [{
                "cid": "c1",
                "text": "Resort đẹp quá!",
                "create_time": "2024-01-15T10:00:00",
                "user": {"uid": "u1", "nickname": "User1"},
                "digg_count": 10,
            }]
        }]
        comments = extract_comments_tiktok(raw)
        assert len(comments) == 1
        c = comments[0]
        assert c.source == "tiktok"
        assert c.comment_id == "c1"
        assert c.place_id == "vid123"
        assert c.raw_text == "Resort đẹp quá!"
        assert c.rating is None
        assert c.meta["author_name"] == "User1"

    def test_empty_comments(self):
        raw = [{"aweme_id": "vid123", "comments": []}]
        assert extract_comments_tiktok(raw) == []


class TestGetAdapter:
    def test_valid_sources(self):
        assert get_adapter("tripadvisor") is not None
        assert get_adapter("tiktok") is not None

    def test_invalid_source(self):
        with pytest.raises(ValueError):
            get_adapter("invalid_source")