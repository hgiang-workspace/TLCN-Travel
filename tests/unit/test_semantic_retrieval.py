from __future__ import annotations

import pytest

from travel_intelligence.qdrant.indexing import TextChunk
from travel_intelligence.qdrant.retrieval import (
    PublicationError,
    RetrievalError,
    SemanticRetriever,
    select_active_generation,
)


class FakeEmbedder:
    model_id = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
    revision = "rev"
    model_fingerprint = "model-fp"
    config_fingerprint = "config-fp"
    dimension = 3
    normalize = True

    def chunks(self, text):
        return (TextChunk(0, 1, text, "query-hash"),)

    def encode(self, chunks):
        return tuple(([1 / 3**0.5] * 3 for _ in chunks))


def ledger(generation="g-1", started_at="2026-01-01T00:00:00"):
    return {
        "index_generation": generation,
        "source_id": "tripadvisor",
        "source_release_id": "release-1",
        "enrichment_run_id": "enrichment-1",
        "status": "committed",
        "expected_review_point_count": 2,
        "actual_review_point_count": 2,
        "expected_aspect_point_count": 1,
        "actual_aspect_point_count": 1,
        "embedding_model_id": FakeEmbedder.model_id,
        "embedding_model_revision": FakeEmbedder.revision,
        "embedding_model_fingerprint": FakeEmbedder.model_fingerprint,
        "embedding_config_fingerprint": FakeEmbedder.config_fingerprint,
        "started_at": started_at,
        "lineage_consistent": True,
    }


class FakeApi:
    def __init__(self):
        self.calls = []

    def search_points(
        self, name, vector, *, limit, score_threshold=None, query_filter=None
    ):
        self.calls.append((name, vector, limit, score_threshold, query_filter))
        if name == "reviews":
            return [
                {
                    "id": "chunk-2",
                    "score": 0.8,
                    "payload": {
                        "review_sentiment_id": "review-1",
                        "chunk_index": 1,
                        "chunk_text_hash": "h2",
                    },
                },
                {
                    "id": "chunk-1",
                    "score": 0.9,
                    "payload": {
                        "review_sentiment_id": "review-1",
                        "chunk_index": 0,
                        "chunk_text_hash": "h1",
                    },
                },
            ]
        return [
            {
                "id": "aspect-1",
                "score": 0.7,
                "payload": {"aspect_sentiment_id": "aspect-1", "aspect": "food"},
            }
        ]


def test_historical_t04c_lineage_mismatch_is_excluded():
    old = {**ledger("old"), "lineage_consistent": False}
    result = select_active_generation(
        [old, ledger("valid", "2026-01-02T00:00:00")],
        model_id=FakeEmbedder.model_id,
        model_revision=FakeEmbedder.revision,
        model_fingerprint=FakeEmbedder.model_fingerprint,
        config_fingerprint=FakeEmbedder.config_fingerprint,
    )
    assert result.index_generation == "valid"


def test_select_active_generation_is_newest_and_fails_closed():
    result = select_active_generation(
        [ledger("old", "2026-01-01T00:00:00"), ledger("new", "2026-01-02T00:00:00")],
        model_id=FakeEmbedder.model_id,
        model_revision=FakeEmbedder.revision,
        model_fingerprint=FakeEmbedder.model_fingerprint,
        config_fingerprint=FakeEmbedder.config_fingerprint,
    )
    assert result.index_generation == "new"
    with pytest.raises(PublicationError):
        select_active_generation(
            [{**ledger(), "lineage_consistent": False}],
            model_id=FakeEmbedder.model_id,
            model_revision=FakeEmbedder.revision,
            model_fingerprint=FakeEmbedder.model_fingerprint,
            config_fingerprint=FakeEmbedder.config_fingerprint,
        )


def test_retrieval_groups_chunks_applies_publication_filter_and_ranks():
    api = FakeApi()
    result = SemanticRetriever(
        api,
        FakeEmbedder(),
        [ledger()],
        {"reviews": "reviews", "review_aspects": "aspects"},
    ).search("đồ ăn ngon", top_k=2, filters={"place_id": "place-1"})
    assert result["index_generation"] == "g-1"
    assert result["total"] == 2
    assert result["results"][0]["collection"] == "reviews"
    assert len(result["results"][0]["matched_chunks"]) == 2
    assert all("index_generation" in call[4]["must"][0]["key"] for call in api.calls)
    assert all(call[4]["must"][-1]["key"] == "place_id" for call in api.calls)


def test_invalid_filter_and_generation_are_rejected():
    service = SemanticRetriever(
        FakeApi(),
        FakeEmbedder(),
        [ledger()],
        {"reviews": "reviews", "review_aspects": "aspects"},
    )
    with pytest.raises(RetrievalError):
        service.search("query", filters={"not_indexed": "x"})
    with pytest.raises(RetrievalError):
        service.search("query", filters={"index_generation": "old"})
    with pytest.raises(RetrievalError):
        service.search("", top_k=1)
