from __future__ import annotations

import math

import pytest

from travel_intelligence.qdrant.indexing import (
    IndexingError,
    TextChunk,
    build_aspect_points,
    build_review_points,
    generation_id,
    point_id,
    upsert_with_retry,
    validate_vector,
)


class FakeEmbedder:
    model_id = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
    revision = "rev"
    model_fingerprint = "model-fp"
    config_fingerprint = "config-fp"

    def chunks(self, text: str):
        return (TextChunk(0, 1, text, f"hash:{text}"),)

    def encode(self, chunks):
        return tuple(([0.1, 0.2, 0.3] for _ in chunks))


def _review():
    return {
        "review_sentiment_id": "rs-1",
        "review_id": "review-1",
        "review_text": "Đồ ăn ngon",
        "review_text_hash": "text-hash",
        "place_id": "place-1",
        "source_id": "tripadvisor",
        "source_record_sk": "source-1",
        "source_release_id": "release-1",
        "processing_run_id": "enrichment-1",
        "sentiment_label": "positive",
        "confidence": 0.9,
        "model_id": "visobert",
        "model_fingerprint": "sentiment-model",
        "config_fingerprint": "sentiment-config",
    }


def _aspect():
    return {
        "aspect_sentiment_id": "as-1",
        "review_sentiment_id": "rs-1",
        "review_id": "review-1",
        "segment_ordinal": 0,
        "segment_text": "Đồ ăn ngon",
        "segment_text_hash": "segment-hash",
        "segment_start_char": 0,
        "segment_end_char": 10,
        "sentence_ordinal": 0,
        "clause_ordinal": 0,
        "aspect": "food",
        "matched_keyword": "đồ ăn",
        "negation_detected": False,
        "negation_metadata": None,
        "source_id": "tripadvisor",
        "source_record_sk": "source-1",
        "source_release_id": "release-1",
        "processing_run_id": "enrichment-1",
        "sentiment_label": "positive",
        "confidence": 0.8,
        "model_id": "visobert",
        "model_fingerprint": "sentiment-model",
        "config_fingerprint": "sentiment-config",
    }


def test_generation_and_point_ids_are_stable_and_kind_scoped():
    generation = generation_id("release", "run", "model", "config")
    assert generation == generation_id("release", "run", "model", "config")
    assert point_id(generation, "review", "identity", 0) == point_id(
        generation, "review", "identity", 0
    )
    assert point_id(generation, "review", "identity", 0) != point_id(
        generation, "aspect", "identity", 0
    )


def test_validate_vector_rejects_wrong_dimension_and_nonfinite_values():
    with pytest.raises(IndexingError):
        validate_vector([1.0], 3)
    with pytest.raises(IndexingError):
        validate_vector([1.0, math.inf, 0.0], 3)


def test_points_preserve_lineage_and_aspect_occurrence():
    embedder = FakeEmbedder()
    reviews = build_review_points([_review()], embedder, generation="generation")
    aspects = build_aspect_points(
        [_aspect(), {**_aspect(), "aspect_sentiment_id": "as-2", "aspect": "service"}],
        embedder,
        generation="generation",
        reviews_by_id={"rs-1": _review()},
    )
    assert len(reviews) == 1
    assert reviews[0][2]["source_release_id"] == "release-1"
    assert len(aspects) == 2
    assert {item[2]["aspect"] for item in aspects} == {"food", "service"}
    assert {item[0] for item in aspects}.__len__() == 2


class FakeQdrant:
    def __init__(self):
        self.calls = []
        self.points = {}

    def upsert_points(self, name, points):
        self.calls.append((name, points))
        self.points.update({(name, item["id"]): item for item in points})

    def point(self, name, point_id):
        return self.points.get((name, point_id))


def test_upsert_is_replay_safe_and_reconciles():
    api = FakeQdrant()
    points = [("point-1", [0.1, 0.2, 0.3], {"index_generation": "g"})]
    upsert_with_retry(api, "reviews", points, batch_size=1)
    upsert_with_retry(api, "reviews", points, batch_size=1)
    assert len(api.points) == 1
    assert api.point("reviews", "point-1") is not None
