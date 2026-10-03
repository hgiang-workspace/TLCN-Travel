"""Aspect package: aspect detection + ABSA assembly.

Cấu trúc:
- dictionary.py: nạp từ điển aspect từ YAML
- detect.py: aspect detection (keyword-based) + negation handling
- absa.py: gắn sentiment vào aspect (sentence-level + clause-level)
"""

from __future__ import annotations

from ingestion.sentiment.aspects.dictionary import (
    load_aspect_keywords,
    build_aspect_keyword_index,
    get_all_aspects,
    get_aspect_keywords,
)
from ingestion.sentiment.aspects.detect import (
    AspectMatch,
    detect_negation,
    detect_aspects,
    deduplicate_aspects,
    get_aspect_sentiment_modifier,
)
from ingestion.sentiment.aspects.segment import (
    AspectSegment,
    segment_hash,
    build_aspect_segments,
    build_aspect_segments_batch,
)
from ingestion.sentiment.aspects.absa import (
    assemble_absa,
    assemble_absa_sentence_level,
    assemble_absa_clause_level,
    assemble_absa_segment_level,
    run_s6_aspect_detection,
)

__all__ = [
    "load_aspect_keywords",
    "build_aspect_keyword_index",
    "get_all_aspects",
    "get_aspect_keywords",
    "AspectMatch",
    "detect_negation",
    "detect_aspects",
    "deduplicate_aspects",
    "get_aspect_sentiment_modifier",
    "AspectSegment",
    "segment_hash",
    "build_aspect_segments",
    "build_aspect_segments_batch",
    "assemble_absa",
    "assemble_absa_sentence_level",
    "assemble_absa_clause_level",
    "assemble_absa_segment_level",
    "run_s6_aspect_detection",
]