"""Unit test helper thuần của tripadvisor_comment_crawler."""

import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[4]  # collectors/tripadvisor/tests -> repo root
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from ingestion.collectors.tripadvisor.tripadvisor_comment_crawler import (
    _norm_url,
    make_slug,
    parse_index_tokens,
    parse_rating,
    resolve_entities,
    same_photo,
    upgrade_photo_url,
)


def test_make_slug():
    assert make_slug("Núi Ngũ Hành Sơn", "Da Nang") == "nui-ngu-hanh-son-da-nang"
    assert make_slug("", "") == "unknown"


def test_norm_url():
    assert _norm_url("https://x/A#REVIEWS") == "https://x/A"
    assert _norm_url("  https://x/A  ") == "https://x/A"
    assert _norm_url("") == ""


def test_parse_rating():
    assert parse_rating("4,5") == 4.5
    assert parse_rating("4.5 places") == 4.5
    assert parse_rating("") is None
    assert parse_rating("N/A") is None
    assert parse_rating("9,0") is None


def test_upgrade_photo_url():
    assert upgrade_photo_url(
        "https://media-cdn.tripadvisor.com/media/photo-s/1a/2b/3c/x.jpg"
    ) == "https://media-cdn.tripadvisor.com/media/photo-o/1a/2b/3c/x.jpg"


def test_same_photo():
    a = "https://media-cdn.tripadvisor.com/media/photo-s/1a/2b/3c/x.jpg"
    b = "https://media-cdn.tripadvisor.com/media/photo-o/1a/2b/3c/x.jpg"
    c = "https://media-cdn.tripadvisor.com/media/photo-o/9z/8y/7x/y.jpg"
    assert same_photo(a, b) is True
    assert same_photo(a, c) is False


def test_parse_index_tokens():
    assert parse_index_tokens(["0"], 4271) == [0]
    assert parse_index_tokens(["0", "5", "12"], 4271) == [0, 5, 12]
    assert parse_index_tokens(["0,5,12"], 4271) == [0, 5, 12]
    assert parse_index_tokens(["0-4", "9"], 4271) == [0, 1, 2, 3, 4, 9]
    assert parse_index_tokens(["3", "0-2", "3"], 4271) == [0, 1, 2, 3]
    assert parse_index_tokens(["9-5"], 4271) == [5, 6, 7, 8, 9]
    for bad in (["4271"], ["-1"], ["0-5000"], ["abc"], ["1-"]):
        try:
            parse_index_tokens(bad, 4271)
            raise AssertionError(f"phải raise cho {bad}")
        except IndexError:
            pass


def test_resolve_entities():
    rows = [{"name": f"e{i}"} for i in range(10)]
    assert resolve_entities(rows, [0, 5], None) == [(0, rows[0]), (5, rows[5])]
    assert resolve_entities(rows, [0, 5, 7], 2) == [(0, rows[0]), (5, rows[5])]
