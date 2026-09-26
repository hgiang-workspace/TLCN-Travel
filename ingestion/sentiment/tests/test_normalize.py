"""Tests cho text normalization."""

from __future__ import annotations

import pytest

from ingestion.sentiment.text.normalize import normalize_text, normalize_batch


class TestNormalizeText:
    def test_basic(self):
        text = "Phòng  sạch,  view đẹp!"
        assert normalize_text(text) == "phòng sạch, view đẹp!"

    def test_url_removal(self):
        text = "Xem thêm tại https://example.com nhé"
        assert "http" not in normalize_text(text)
        assert "example.com" not in normalize_text(text)

    def test_email_removal(self):
        text = "Liên hệ: test@example.com"
        assert "@" not in normalize_text(text)

    def test_emoji_handling(self):
        text = "Tuyệt vời 😍👍"
        result = normalize_text(text)
        assert "tuyệt vời" in result

    def test_lowercase_disabled(self):
        text = "Phòng Sạch"
        result = normalize_text(text, cfg={"lowercase": False})
        assert result == "Phòng Sạch"

    def test_empty_input(self):
        assert normalize_text("") == ""
        assert normalize_text(None) == ""  # type: ignore

    def test_batch(self):
        texts = ["Phòng sạch", "View đẹp 😍"]
        results = normalize_batch(texts)
        assert len(results) == 2
        assert results[0] == "phòng sạch"
        assert "view đẹp" in results[1]