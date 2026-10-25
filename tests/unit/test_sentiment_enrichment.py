# ruff: noqa: E501
from __future__ import annotations

from copy import deepcopy
from dataclasses import replace

import pytest

from travel_intelligence.common.configuration import (
    default_config_paths,
    load_validated,
)
from travel_intelligence.sentiment.contracts import review_sentiment_id
from travel_intelligence.sentiment.enrichment import (
    AspectSegmenter,
    EnrichmentError,
    SentimentPrediction,
    enrich_reviews,
    publish_enrichment,
    token_aware_chunks,
)
from travel_intelligence.silver.tripadvisor_source import (
    TRIPADVISOR_REQUIRED_RELEASE_TABLES,
    select_tripadvisor_reviews,
)


class FakeTokenizer:
    def encode(self, text, *, add_special_tokens, truncation):
        return list(range(len(text.split())))

    def num_special_tokens_to_add(self, pair=False):
        return 2

    def decode(self, token_ids, skip_special_tokens=True):
        return " ".join(f"token-{item}" for item in token_ids)


class FakeClassifier:
    model_id = "5CD-AI/Vietnamese-Sentiment-visobert"
    model_fingerprint = "sha256:model-test"
    max_length = 6
    tokenizer = FakeTokenizer()

    def predict(self, texts):
        return tuple(
            SentimentPrediction("positive", 0.8, (0.1, 0.1, 0.8))
            for _ in texts
        )


def committed_run(batch_id="batch-1", run_id="release-1"):
    return {
        "run_id": run_id,
        "source_id": "tripadvisor",
        "bronze_batch_id": batch_id,
        "status": "committed",
        "table_snapshot_refs": {
            table: f"snapshot-{table}" for table in TRIPADVISOR_REQUIRED_RELEASE_TABLES
        },
    }


def review(**overrides):
    value = {
        "review_id": "review-1",
        "review_ordinal": 0,
        "review_text": "Món ăn ngon và không gian đẹp.",
        "source_record_sk": "source-1",
        "source_id": "tripadvisor",
        "batch_id": "batch-1",
        "source_row_index": 1,
        "source_record_id": "ta-1",
        "place_id": "place-1",
        "source_uri": "https://example.test/1",
    }
    value.update(overrides)
    return value


def _pipeline_without_optional_sentence_backend():
    pipeline, _ = load_validated("pipeline", default_config_paths()["pipeline"])
    pipeline = deepcopy(pipeline)
    pipeline["text"]["use_underthesea"] = False
    return pipeline


def test_selector_keeps_distinct_occurrences_with_reused_review_id():
    selected = select_tripadvisor_reviews(
        [review(), review(source_record_sk="source-2", source_row_index=2)],
        {"batch-1": committed_run()},
    )
    assert len(selected) == 2
    assert {item.source_record_sk for item in selected} == {"source-1", "source-2"}
    assert {item.source_release_id for item in selected} == {"release-1"}


def test_token_aware_chunking_is_deterministic_and_does_not_truncate():
    first = token_aware_chunks("one two three four five", FakeTokenizer(), max_length=4)
    second = token_aware_chunks("one two three four five", FakeTokenizer(), max_length=4)
    assert first == second
    assert len(first) == 3
    assert [(item.token_start, item.token_end) for item in first] == [(0, 2), (2, 4), (4, 5)]
    with pytest.raises(EnrichmentError):
        token_aware_chunks(" ", FakeTokenizer(), max_length=4)


def test_segmenter_preserves_same_text_for_distinct_aspects_and_negation_metadata():
    pipeline = _pipeline_without_optional_sentence_backend()
    keywords, _ = load_validated("aspect_keywords", default_config_paths()["aspect_keywords"])
    segments = AspectSegmenter(pipeline, keywords).segment("Không đẹp và món ăn ngon.")
    assert {segment.aspect for segment in segments} >= {"ambiance", "food"}
    assert any(segment.negation_detected for segment in segments)
    assert all(segment.negation_metadata for segment in segments)


def test_enrichment_rows_are_lineage_preserving_and_replay_stable():
    pipeline = _pipeline_without_optional_sentence_backend()
    keywords, _ = load_validated("aspect_keywords", default_config_paths()["aspect_keywords"])
    segmenter = AspectSegmenter(pipeline, keywords)
    occurrence = select_tripadvisor_reviews([review()], {"batch-1": committed_run()})[0]
    first = enrich_reviews(
        [occurrence], FakeClassifier(), segmenter,
        config_fingerprint="sha256:config", processing_run_id="run-1",
    )
    second = enrich_reviews(
        [occurrence], FakeClassifier(), segmenter,
        config_fingerprint="sha256:config", processing_run_id="run-2",
    )
    assert first.review_rows[0]["review_sentiment_id"] == second.review_rows[0]["review_sentiment_id"]
    assert first.review_rows[0]["source_release_id"] == "release-1"
    assert all(row["review_sentiment_id"] == first.review_rows[0]["review_sentiment_id"] for row in first.aspect_rows)
    assert {row["aspect"] for row in first.aspect_rows} >= {"ambiance", "food"}
    changed = replace(occurrence, review_text="Món ăn dở.")
    changed_batch = enrich_reviews(
        [changed], FakeClassifier(), segmenter,
        config_fingerprint="sha256:config", processing_run_id="run-1",
    )
    assert changed_batch.review_rows[0]["review_sentiment_id"] != first.review_rows[0]["review_sentiment_id"]


def test_enrichment_reuses_precomputed_segments():
    pipeline = _pipeline_without_optional_sentence_backend()
    keywords, _ = load_validated("aspect_keywords", default_config_paths()["aspect_keywords"])
    delegate = AspectSegmenter(pipeline, keywords)

    class CountingSegmenter:
        def __init__(self):
            self.calls = 0
            self.segmentation = delegate.segmentation

        def segment(self, text):
            self.calls += 1
            return delegate.segment(text)

    segmenter = CountingSegmenter()
    occurrence = select_tripadvisor_reviews([review()], {"batch-1": committed_run()})[0]
    segments = segmenter.segment(occurrence.review_text)
    enrich_reviews(
        [occurrence],
        FakeClassifier(),
        segmenter,
        config_fingerprint="sha256:config",
        processing_run_id="run-1",
        precomputed_segments=(segments,),
    )

    assert segmenter.calls == 1


class FakeStore:
    def __init__(self, fail_writes=False):
        self.fail_writes = fail_writes
        self.statuses = []
        self.review_ids = set()
        self.aspect_ids = set()

    def begin(self, run):
        self.statuses.append("partial")

    def upsert_reviews(self, rows):
        self.review_ids.update(row["review_sentiment_id"] for row in rows)

    def upsert_aspects(self, rows):
        if self.fail_writes:
            raise RuntimeError("injected aspect write failure")
        self.aspect_ids.update(row["aspect_sentiment_id"] for row in rows)

    def reconcile(self, run):
        return {"review_count": len(self.review_ids), "aspect_count": len(self.aspect_ids)}

    def commit(self, run, reconciliation):
        self.statuses.append("committed")

    def fail(self, run, error):
        self.statuses.append("failed")


def test_publication_replay_and_partial_failure_are_explicit():
    pipeline = _pipeline_without_optional_sentence_backend()
    keywords, _ = load_validated("aspect_keywords", default_config_paths()["aspect_keywords"])
    occurrence = select_tripadvisor_reviews([review()], {"batch-1": committed_run()})[0]
    batch = enrich_reviews(
        [occurrence], FakeClassifier(), AspectSegmenter(pipeline, keywords),
        config_fingerprint="sha256:config", processing_run_id="run-1",
    )
    store = FakeStore()
    first = publish_enrichment(
        store, batch, source_release_id="release-1", model_id="model",
        model_fingerprint="model-fp", config_fingerprint="config-fp",
        input_fingerprint="input-fp", processing_run_id="run-1",
    )
    second = publish_enrichment(
        store, batch, source_release_id="release-1", model_id="model",
        model_fingerprint="model-fp", config_fingerprint="config-fp",
        input_fingerprint="input-fp", processing_run_id="run-1",
    )
    assert first["status"] == second["status"] == "committed"
    assert len(store.review_ids) == len(batch.review_rows)
    failing = FakeStore(fail_writes=True)
    with pytest.raises(RuntimeError, match="injected"):
        publish_enrichment(
            failing, batch, source_release_id="release-1", model_id="model",
            model_fingerprint="model-fp", config_fingerprint="config-fp",
            input_fingerprint="input-fp", processing_run_id="run-2",
        )
    assert failing.statuses == ["partial", "failed"]


def test_review_identity_backwards_compatible_but_text_versioned():
    assert review_sentiment_id("r", "m", "c") == review_sentiment_id("r", "m", "c")
    assert review_sentiment_id("r", "m", "c", "text-a") != review_sentiment_id("r", "m", "c", "text-b")
