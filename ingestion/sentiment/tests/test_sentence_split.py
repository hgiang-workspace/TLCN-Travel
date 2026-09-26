"""Tests cho sentence splitting."""

from __future__ import annotations

import pytest

from ingestion.sentiment.text.sentence_split import split_sentences, split_sentences_batch


class TestSentenceSplit:
    def test_basic_split(self):
        text = "Phòng sạch, view đẹp. Nhưng nhân viên chậm!"
        sentences = split_sentences(text)
        assert len(sentences) == 2
        assert "Phòng sạch, view đẹp" in sentences[0]
        assert "Nhưng nhân viên chậm" in sentences[1]

    def test_single_sentence(self):
        text = "Phòng sạch."
        sentences = split_sentences(text)
        assert len(sentences) == 1
        assert sentences[0] == "Phòng sạch"

    def test_short_filtered(self):
        text = "Ok. Phòng rất sạch và đẹp."
        sentences = split_sentences(text)
        # "Ok" bị lọc vì < 3 ký tự
        assert len(sentences) == 1
        assert "Phòng rất sạch và đẹp" in sentences[0]

    def test_empty_input(self):
        assert split_sentences("") == []
        assert split_sentences(None) == []  # type: ignore

    def test_batch(self):
        texts = ["Câu 1. Câu 2.", "Một câu."]
        results = split_sentences_batch(texts)
        assert len(results) == 2
        assert len(results[0]) == 2
        assert len(results[1]) == 1