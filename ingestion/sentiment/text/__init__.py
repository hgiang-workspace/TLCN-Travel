"""Text processing: normalize + sentence split + clause split."""

from __future__ import annotations

from ingestion.sentiment.text.normalize import normalize_batch, normalize_text
from ingestion.sentiment.text.sentence_split import split_sentences, split_sentences_batch
from ingestion.sentiment.text.clause_split import (
    split_clauses,
    split_clauses_batch,
    window_segment,
    window_segment_batch,
)

__all__ = [
    "normalize_text",
    "normalize_batch",
    "split_sentences",
    "split_sentences_batch",
    "split_clauses",
    "split_clauses_batch",
    "window_segment",
    "window_segment_batch",
]