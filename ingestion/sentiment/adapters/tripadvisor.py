"""Adapter TripAdvisor - wrapper dùng chung."""

from __future__ import annotations

from ingestion.sentiment.adapters import Comment, extract_comments_tripadvisor

__all__ = ["Comment", "extract_comments_tripadvisor"]