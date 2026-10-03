"""Tests cho clause_split."""

from __future__ import annotations

import pytest

from ingestion.sentiment.text import split_clauses, split_clauses_batch


def test_split_clauses_basic():
    """Test tách clause cơ bản."""
    text = "món ăn ngon nhưng phục vụ kém"
    clauses = split_clauses(text)
    assert len(clauses) == 2
    assert "món ăn ngon" in clauses[0]
    assert "phục vụ kém" in clauses[1]


def test_split_clauses_multiple_connectors():
    """Test tách clause với nhiều từ nối."""
    text = "món ăn ngon vì nguyên liệu tươi, nên rất hấp dẫn"
    clauses = split_clauses(text)
    assert len(clauses) >= 2


def test_split_clauses_no_connector():
    """Test khi không có từ nối - trả về cả câu."""
    text = "món ăn rất ngon"
    clauses = split_clauses(text)
    assert len(clauses) == 1
    assert clauses[0] == "món ăn rất ngon"


def test_split_clauses_disabled():
    """Test tắt clause split."""
    text = "món ăn ngon nhưng phục vụ kém"
    clauses = split_clauses(text, cfg={"use_clause_split": False})
    assert len(clauses) == 1
    assert clauses[0] == "món ăn ngon nhưng phục vụ kém"


def test_split_clauses_min_len():
    """Test lọc clause quá ngắn."""
    text = "ngon nhưng kém"
    clauses = split_clauses(text, cfg={"min_clause_len": 5})
    # "ngon" và "kém" đều < 5 ký tự, nên bị lọc
    assert len(clauses) == 0


def test_split_clauses_negation():
    """Test clause với từ phủ định."""
    text = "món ăn không ngon nhưng giá rẻ"
    clauses = split_clauses(text)
    assert len(clauses) == 2
    # Clause đầu có "không ngon"
    assert "không ngon" in clauses[0]


def test_split_clauses_batch():
    """Test batch version."""
    texts = [
        "món ăn ngon nhưng phục vụ kém",
        "không gian đẹp",
    ]
    results = split_clauses_batch(texts, cfg=None)
    assert len(results) == 2
    assert len(results[0]) == 2
    assert len(results[1]) == 1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])