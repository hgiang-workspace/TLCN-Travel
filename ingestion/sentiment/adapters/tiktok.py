"""Adapter TikTok - wrapper dùng chung."""

from __future__ import annotations

from ingestion.sentiment.adapters import Comment, extract_comments_tiktok

__all__ = ["Comment", "extract_comments_tiktok"]