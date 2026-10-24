# ruff: noqa: E501
"""Deterministic TripAdvisor sentiment enrichment primitives.

The module keeps source selection, segmentation, model inference, and durable
publication separate.  Transformers and Spark are imported lazily so the
contract and orchestration tests do not require heavyweight runtimes.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol

from travel_intelligence.common.configuration import configuration_fingerprint
from travel_intelligence.sentiment.contracts import (
    aspect_sentiment_id,
    review_sentiment_id,
    text_hash,
)
from travel_intelligence.silver.tripadvisor_source import TripAdvisorReviewOccurrence

LABELS = ("negative", "neutral", "positive")
_NEGATION_TERMS = (
    "không",
    "chẳng",
    "chưa",
    "chả",
    "đừng",
    "không phải",
)


class EnrichmentError(ValueError):
    """Raised when enrichment input, inference, or publication is invalid."""


class InferenceFailure(EnrichmentError):
    """Raised when the configured model cannot produce a valid prediction."""


def _finite_probability(value: Any) -> float:
    result = float(value)
    if not math.isfinite(result) or result < 0 or result > 1:
        raise InferenceFailure(f"model returned invalid probability: {value!r}")
    return result


def validate_text(value: Any, *, field: str = "text") -> str:
    if not isinstance(value, str):
        raise EnrichmentError(f"{field} must be a string")
    if not value.strip():
        raise EnrichmentError(f"{field} must not be empty")
    return value


@dataclass(frozen=True)
class TextChunk:
    ordinal: int
    text: str
    token_start: int
    token_end: int


def token_aware_chunks(
    text: str,
    tokenizer: Any,
    *,
    max_length: int,
) -> tuple[TextChunk, ...]:
    """Split without truncation, reserving the tokenizer's special tokens."""
    value = validate_text(text)
    if max_length < 4:
        raise EnrichmentError("max_length must leave room for content tokens")
    try:
        token_ids = tokenizer.encode(
            value, add_special_tokens=False, truncation=False
        )
        special_count = int(tokenizer.num_special_tokens_to_add(pair=False))
        decode = tokenizer.decode
    except Exception as error:  # pragma: no cover - backend-specific failure
        raise EnrichmentError("tokenizer could not encode text without truncation") from error
    budget = max_length - special_count
    if budget <= 0:
        raise EnrichmentError("tokenizer special-token budget exceeds max_length")
    if not token_ids:
        raise EnrichmentError("tokenizer produced no content tokens")
    chunks = []
    for ordinal, start in enumerate(range(0, len(token_ids), budget)):
        end = min(start + budget, len(token_ids))
        chunk_text = decode(token_ids[start:end], skip_special_tokens=True).strip()
        if not chunk_text:
            raise EnrichmentError("tokenizer produced an empty chunk")
        chunks.append(TextChunk(ordinal, chunk_text, start, end))
    return tuple(chunks)


@dataclass(frozen=True)
class SentimentPrediction:
    label: str
    confidence: float
    probabilities: tuple[float, float, float]


class SentimentClassifier(Protocol):
    model_id: str
    model_fingerprint: str
    max_length: int

    def predict(self, texts: Sequence[str]) -> tuple[SentimentPrediction, ...]: ...


class ViSoBERTClassifier:
    """Actual Transformers/PyTorch ViSoBERT inference backend."""

    def __init__(
        self,
        model: Any,
        tokenizer: Any,
        *,
        model_id: str,
        model_fingerprint: str,
        max_length: int,
        batch_size: int,
        device: str,
        label_map: Mapping[int, str],
        torch_module: Any,
    ) -> None:
        self.model = model
        self.tokenizer = tokenizer
        self.model_id = model_id
        self.model_fingerprint = model_fingerprint
        self.max_length = max_length
        self.batch_size = batch_size
        self.device = device
        self.label_map = dict(label_map)
        self.torch = torch_module

    @classmethod
    def from_config(
        cls,
        config: Mapping[str, Any],
        config_fingerprint: str,
        *,
        repository_root: Path,
    ) -> "ViSoBERTClassifier":
        try:
            import torch
            from transformers import AutoModelForSequenceClassification, AutoTokenizer
        except ImportError as error:  # pragma: no cover - environment-specific
            raise EnrichmentError(
                "ViSoBERT requires pinned transformers and torch dependencies"
            ) from error
        model_config = config["model"]
        inference = config["inference"]
        model_id = str(model_config["id"])
        artifact = repository_root / str(model_config["local_artifact_path"])
        if not artifact.is_dir():
            raise EnrichmentError(f"ViSoBERT local artifact is missing: {artifact}")
        revision = model_config.get("revision")
        load_kwargs = {"revision": revision} if revision else {}
        try:
            tokenizer = AutoTokenizer.from_pretrained(
                str(artifact), local_files_only=True, **load_kwargs
            )
            model = AutoModelForSequenceClassification.from_pretrained(
                str(artifact), local_files_only=True, **load_kwargs
            )
        except Exception as error:
            raise EnrichmentError(
                "ViSoBERT artifact could not be loaded; no model substitution is allowed"
            ) from error
        id2label = getattr(model.config, "id2label", None)
        label_map = _validate_label_map(id2label, config["classification"]["labels"])
        configured_device = str(inference["device"]).lower()
        if configured_device == "auto":
            device = "cuda" if torch.cuda.is_available() else "cpu"
        else:
            device = configured_device
        model.to(device)
        model.eval()
        revision_text = str(revision or "artifact-unpinned")
        model_fingerprint = configuration_fingerprint(
            {
                "model_id": model_id,
                "revision": revision_text,
                "config_fingerprint": config_fingerprint,
                "label_map": label_map,
            }
        )
        return cls(
            model,
            tokenizer,
            model_id=model_id,
            model_fingerprint=model_fingerprint,
            max_length=int(inference["max_length"]),
            batch_size=int(inference["batch_size"]),
            device=device,
            label_map=label_map,
            torch_module=torch,
        )

    def predict(self, texts: Sequence[str]) -> tuple[SentimentPrediction, ...]:
        if not texts:
            return ()
        results: list[SentimentPrediction] = []
        try:
            with self.torch.inference_mode():
                for start in range(0, len(texts), self.batch_size):
                    batch = [validate_text(item) for item in texts[start : start + self.batch_size]]
                    encoded = self.tokenizer(
                        batch,
                        padding=True,
                        truncation=False,
                        max_length=self.max_length,
                        return_tensors="pt",
                    )
                    encoded = {key: value.to(self.device) for key, value in encoded.items()}
                    logits = self.model(**encoded).logits
                    if logits.ndim != 2 or logits.shape[1] != 3:
                        raise InferenceFailure("ViSoBERT output must have three logits")
                    probabilities = self.torch.softmax(logits, dim=-1).detach().cpu().tolist()
                    for row in probabilities:
                        values = tuple(_finite_probability(value) for value in row)
                        index = max(range(3), key=values.__getitem__)
                        results.append(
                            SentimentPrediction(
                                self.label_map[index], values[index], values
                            )
                        )
        except InferenceFailure:
            raise
        except Exception as error:
            raise InferenceFailure("ViSoBERT inference failed") from error
        return tuple(results)


def _validate_label_map(
    id2label: Mapping[Any, Any] | None, expected: Sequence[str]
) -> dict[int, str]:
    if not isinstance(id2label, Mapping):
        raise EnrichmentError("model configuration does not provide id2label")
    expected_set = {label.lower() for label in expected}
    mapping: dict[int, str] = {}
    aliases = {"neg": "negative", "negative": "negative", "neu": "neutral", "neutral": "neutral", "pos": "positive", "positive": "positive"}
    for key, value in id2label.items():
        normalized = aliases.get(str(value).strip().lower())
        if normalized in expected_set:
            mapping[int(key)] = normalized
    if set(mapping.values()) != expected_set or set(mapping) != {0, 1, 2}:
        raise EnrichmentError(
            f"model labels {dict(id2label)!r} do not map exactly to {tuple(expected)!r}"
        )
    return mapping


@dataclass(frozen=True)
class AspectSegment:
    segment_ordinal: int
    sentence_ordinal: int
    clause_ordinal: int | None
    start_char: int
    end_char: int
    text: str
    aspect: str
    matched_keyword: str
    segment_type: str
    negation_detected: bool
    negation_metadata: str


class AspectSegmenter:
    def __init__(self, pipeline: Mapping[str, Any], keywords: Mapping[str, Any]) -> None:
        self.pipeline = pipeline
        self.matching = keywords["matching"]
        self.aspects = keywords["aspects"]
        self.segmentation = pipeline["segmentation"]
        self.negation = pipeline["negation"]
        self.use_underthesea = bool(pipeline["text"].get("use_underthesea"))
        if self.use_underthesea:
            try:
                from underthesea import sent_tokenize
            except ImportError as error:
                raise EnrichmentError(
                    "pipeline requires underthesea sentence splitting, but it is not installed"
                ) from error
            self._sent_tokenize = sent_tokenize
        else:
            self._sent_tokenize = None
        self._patterns = tuple(
            (aspect, keyword, self._pattern(keyword))
            for aspect, entries in self.aspects.items()
            for keyword in entries
        )

    def _pattern(self, keyword: str) -> re.Pattern[str]:
        value = re.escape(keyword.lower() if self.matching["lowercase"] else keyword)
        boundary = r"\b" if self.matching["word_boundary"] else ""
        return re.compile(boundary + value + boundary, re.IGNORECASE)

    def segment(self, text: str) -> tuple[AspectSegment, ...]:
        value = validate_text(text)
        sentences = self._sentence_spans(value)
        matches: list[tuple[int, int, int, str, str, str]] = []
        max_per_sentence = int(self.segmentation["max_aspects_per_sentence"])
        for sentence_idx, (sentence_start, sentence_end, sentence_text) in enumerate(sentences):
            local = []
            for aspect, keyword, pattern in self._patterns:
                for found in pattern.finditer(sentence_text):
                    local.append((found.start(), found.end(), aspect, keyword))
            local.sort(key=lambda item: (item[0], item[1], item[2], item[3]))
            for start, end, aspect, keyword in local[:max_per_sentence]:
                matches.append((sentence_idx, sentence_start, start, end, aspect, keyword))
        segments: list[AspectSegment] = []
        seen: set[tuple[int, str, str, int, int]] = set()
        for sentence_idx, sentence_start, local_start, local_end, aspect, keyword in matches:
            sentence = sentences[sentence_idx][2]
            tokens = list(re.finditer(r"\S+", sentence))
            token_hits = [
                i for i, token in enumerate(tokens)
                if token.start() < local_end and token.end() > local_start
            ]
            if not token_hits:
                continue
            before = int(self.segmentation["window_before"])
            after = int(self.segmentation["window_after"])
            first = max(0, token_hits[0] - before)
            last = min(len(tokens), token_hits[-1] + after + 1)
            start = sentence_start + tokens[first].start()
            end = sentence_start + tokens[last - 1].end()
            segment_text = value[start:end].strip()
            if len(segment_text) < int(self.segmentation["min_segment_len"]):
                continue
            segment_hash = text_hash(segment_text)
            key = (sentence_idx, aspect, segment_hash, start, end)
            if self.segmentation["dedupe_by_hash"] and key in seen:
                continue
            seen.add(key)
            keyword_start = sentence_start + local_start
            prefix = value[max(sentence_start, keyword_start - 80) : keyword_start].lower()
            negated = any(term in prefix.split()[-3:] for term in _NEGATION_TERMS)
            clause_idx = _clause_index(sentence, local_start)
            metadata = json.dumps(
                {"strategy": self.negation["strategy"], "window_tokens": self.negation["window_tokens"]},
                ensure_ascii=False,
                sort_keys=True,
            )
            segments.append(
                AspectSegment(
                    len(segments), sentence_idx, clause_idx, start, end, segment_text,
                    aspect, keyword, "aspect_span", negated, metadata,
                )
            )
        return tuple(segments)

    def _sentence_spans(self, value: str) -> tuple[tuple[int, int, str], ...]:
        if self._sent_tokenize is None:
            return tuple(
                (match.start(), match.end(), match.group(0))
                for match in re.finditer(r"[^.!?。！？\n]+", value)
            )
        result = []
        cursor = 0
        for sentence in self._sent_tokenize(value):
            start = value.find(sentence, cursor)
            if start < 0:
                raise EnrichmentError("underthesea sentence offsets could not be reconciled")
            end = start + len(sentence)
            result.append((start, end, sentence))
            cursor = end
        return tuple(result)


def _clause_index(sentence: str, offset: int) -> int:
    return len(re.findall(r"[,;:，；：]", sentence[:offset]))


@dataclass(frozen=True)
class EnrichmentBatch:
    review_rows: tuple[dict[str, Any], ...]
    aspect_rows: tuple[dict[str, Any], ...]
    exclusions: tuple[dict[str, Any], ...]


def enrich_reviews(
    occurrences: Iterable[TripAdvisorReviewOccurrence],
    classifier: SentimentClassifier,
    segmenter: AspectSegmenter,
    *,
    config_fingerprint: str,
    processing_run_id: str,
    processed_at: datetime | None = None,
) -> EnrichmentBatch:
    timestamp = processed_at or datetime.now(timezone.utc).replace(tzinfo=None)
    review_rows: list[dict[str, Any]] = []
    aspect_rows: list[dict[str, Any]] = []
    exclusions: list[dict[str, Any]] = []
    prepared: list[tuple[TripAdvisorReviewOccurrence, str, tuple[TextChunk, ...], tuple[AspectSegment, ...]]] = []
    for occurrence in occurrences:
        try:
            text = validate_text(occurrence.review_text, field="review_text")
            chunks = token_aware_chunks(
                text, classifier.tokenizer, max_length=classifier.max_length
            ) if hasattr(classifier, "tokenizer") else (_single_chunk(text),)
            segments = segmenter.segment(text)
            prepared.append((occurrence, text, chunks, segments))
        except EnrichmentError as error:
            exclusions.append({"review_id": occurrence.review_id, "reason": str(error)})

    chunk_inputs = [chunk.text for _, _, chunks, _ in prepared for chunk in chunks]
    segment_inputs = [segment.text for _, _, _, segments in prepared for segment in segments]
    chunk_predictions = classifier.predict(chunk_inputs)
    segment_predictions = classifier.predict(segment_inputs)
    if len(chunk_predictions) != len(chunk_inputs) or len(segment_predictions) != len(segment_inputs):
        raise InferenceFailure("classifier returned the wrong prediction count")

    chunk_cursor = 0
    segment_cursor = 0
    for occurrence, text, chunks, segments in prepared:
        try:
            occurrence_chunk_predictions = chunk_predictions[chunk_cursor : chunk_cursor + len(chunks)]
            chunk_cursor += len(chunks)
            prediction = _aggregate_predictions(occurrence_chunk_predictions, chunks)
            review_text_hash = text_hash(text)
            review_id = review_sentiment_id(
                occurrence.review_id,
                classifier.model_fingerprint,
                config_fingerprint,
                review_text_hash,
                occurrence.source_record_sk,
                occurrence.review_ordinal,
            )
            review_rows.append(
                {
                    "review_sentiment_id": review_id,
                    "review_id": occurrence.review_id,
                    "source_record_sk": occurrence.source_record_sk,
                    "source_id": occurrence.source_id,
                    "batch_id": occurrence.batch_id,
                    "source_row_index": occurrence.source_row_index,
                    "source_record_id": occurrence.source_record_id,
                    "place_id": occurrence.place_id,
                    "review_text_hash": review_text_hash,
                    "sentiment_label": prediction.label,
                    "confidence": prediction.confidence,
                    "model_id": classifier.model_id,
                    "model_fingerprint": classifier.model_fingerprint,
                    "config_fingerprint": config_fingerprint,
                    "processing_run_id": processing_run_id,
                    "source_release_id": occurrence.source_release_id,
                    "processed_at": timestamp,
                    "quality_codes": ["chunked"] if len(chunks) > 1 else [],
                }
            )
            if not segments:
                continue
            occurrence_segment_predictions = segment_predictions[segment_cursor : segment_cursor + len(segments)]
            segment_cursor += len(segments)
            for segment, segment_prediction in zip(segments, occurrence_segment_predictions, strict=True):
                aspect_rows.append(
                    {
                        "aspect_sentiment_id": aspect_sentiment_id(
                            review_id,
                            segment.segment_ordinal,
                            segment.aspect,
                            segment.matched_keyword,
                            text_hash(segment.text),
                            classifier.model_fingerprint,
                            config_fingerprint,
                        ),
                        "review_sentiment_id": review_id,
                        "review_id": occurrence.review_id,
                        "source_record_sk": occurrence.source_record_sk,
                        "source_id": occurrence.source_id,
                        "batch_id": occurrence.batch_id,
                        "source_row_index": occurrence.source_row_index,
                        "segment_ordinal": segment.segment_ordinal,
                        "sentence_ordinal": segment.sentence_ordinal,
                        "clause_ordinal": segment.clause_ordinal,
                        "segment_start_char": segment.start_char,
                        "segment_end_char": segment.end_char,
                        "segment_text": segment.text,
                        "segment_text_hash": text_hash(segment.text),
                        "aspect": segment.aspect,
                        "matched_keyword": segment.matched_keyword,
                        "segmentation_strategy": segmenter.segmentation["strategy"],
                        "negation_detected": segment.negation_detected,
                        "negation_metadata": segment.negation_metadata,
                        "sentiment_label": segment_prediction.label,
                        "confidence": segment_prediction.confidence,
                        "model_id": classifier.model_id,
                        "model_fingerprint": classifier.model_fingerprint,
                        "config_fingerprint": config_fingerprint,
                        "processing_run_id": processing_run_id,
                        "source_release_id": occurrence.source_release_id,
                        "processed_at": timestamp,
                        "quality_codes": [],
                    }
                )
        except EnrichmentError as error:
            exclusions.append({"review_id": occurrence.review_id, "reason": str(error)})
    return EnrichmentBatch(tuple(review_rows), tuple(aspect_rows), tuple(exclusions))


def _single_chunk(text: str) -> TextChunk:
    return TextChunk(0, text, 0, 0)


def _aggregate_predictions(
    predictions: Sequence[SentimentPrediction], chunks: Sequence[TextChunk]
) -> SentimentPrediction:
    if not predictions:
        raise InferenceFailure("no chunk predictions to aggregate")
    weights = [max(1, item.token_end - item.token_start) for item in chunks]
    total = sum(weights)
    values = tuple(
        sum(prediction.probabilities[index] * weight for prediction, weight in zip(predictions, weights, strict=True))
        / total
        for index in range(3)
    )
    chosen = max(range(3), key=values.__getitem__)
    return SentimentPrediction(LABELS[chosen], _finite_probability(values[chosen]), values)


class EnrichmentStore(Protocol):
    def begin(self, run: Mapping[str, Any]) -> None: ...
    def upsert_reviews(self, rows: Sequence[Mapping[str, Any]]) -> None: ...
    def upsert_aspects(self, rows: Sequence[Mapping[str, Any]]) -> None: ...
    def reconcile(self, run: Mapping[str, Any]) -> Mapping[str, Any]: ...
    def commit(self, run: Mapping[str, Any], reconciliation: Mapping[str, Any]) -> None: ...
    def fail(self, run: Mapping[str, Any], error: str) -> None: ...


def publish_enrichment(
    store: EnrichmentStore,
    batch: EnrichmentBatch,
    *,
    source_release_id: str,
    model_id: str,
    model_fingerprint: str,
    config_fingerprint: str,
    input_fingerprint: str,
    processing_run_id: str,
    fail_after: str | None = None,
) -> Mapping[str, Any]:
    run = {
        "processing_run_id": processing_run_id,
        "source_id": "tripadvisor",
        "source_release_id": source_release_id,
        "model_id": model_id,
        "model_fingerprint": model_fingerprint,
        "config_fingerprint": config_fingerprint,
        "input_fingerprint": input_fingerprint,
        "status": "partial",
        "expected_review_count": len(batch.review_rows),
        "expected_aspect_count": len(batch.aspect_rows),
    }
    store.begin(run)
    try:
        store.upsert_reviews(batch.review_rows)
        if fail_after == "reviews":
            raise EnrichmentError("controlled failure after review write")
        store.upsert_aspects(batch.aspect_rows)
        if fail_after == "aspects":
            raise EnrichmentError("controlled failure after aspect write")
        reconciliation = store.reconcile(run)
        if reconciliation.get("review_count") != len(batch.review_rows) or reconciliation.get("aspect_count") != len(batch.aspect_rows):
            raise EnrichmentError("enrichment result reconciliation failed")
        store.commit(run, reconciliation)
        return {**run, **reconciliation, "status": "committed"}
    except Exception as error:
        store.fail(run, str(error))
        raise


def input_fingerprint(occurrences: Iterable[TripAdvisorReviewOccurrence]) -> str:
    values = [
        {
            "review_id": item.review_id,
            "source_record_sk": item.source_record_sk,
            "review_ordinal": item.review_ordinal,
            "source_release_id": item.source_release_id,
            "text_hash": text_hash(item.review_text),
        }
        for item in occurrences
    ]
    canonical = json.dumps(sorted(values, key=lambda value: json.dumps(value, sort_keys=True)), sort_keys=True, separators=(",", ":"))
    return "sha256:" + hashlib.sha256(canonical.encode()).hexdigest()


__all__ = [
    "AspectSegment",
    "AspectSegmenter",
    "EnrichmentBatch",
    "EnrichmentError",
    "InferenceFailure",
    "SentimentPrediction",
    "ViSoBERTClassifier",
    "enrich_reviews",
    "input_fingerprint",
    "publish_enrichment",
    "token_aware_chunks",
    "validate_text",
]
