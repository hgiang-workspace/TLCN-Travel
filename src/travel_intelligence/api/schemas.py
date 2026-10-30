"""Typed frontend-facing contracts for Gold market and coverage analytics."""

from __future__ import annotations

from enum import Enum

from pydantic import BaseModel, Field


class SupportStatus(str, Enum):
    insufficient_data = "insufficient_data"
    no_data = "no_data"
    sufficient = "sufficient"
    stale_data = "stale_data"
    unsupported = "unsupported"


class MarketOverviewResponse(BaseModel):
    collected_entity_count: int | None = None
    mapped_entity_count: int | None = None
    excluded_entity_count: int | None = None
    unreviewed_entity_count: int | None = None
    entity_count_with_sentiment_evidence: int | None = None
    entity_count_with_aspect_evidence: int | None = None
    mapping_coverage_ratio: float | None = Field(default=None, ge=0, le=1)
    sentiment_entity_coverage_ratio: float | None = Field(default=None, ge=0, le=1)
    aspect_entity_coverage_ratio: float | None = Field(default=None, ge=0, le=1)
    review_evidence_count: int | None = None
    aspect_mention_count: int | None = None
    support_status: SupportStatus


class ProvinceComparisonItem(BaseModel):
    province_id: str
    domain_id: str | None
    metric_id: str
    unit: str
    count: int | None
    support_status: SupportStatus


class DomainAnalyticsItem(BaseModel):
    domain_id: str
    entity_count: int | None = None
    entity_count_with_sentiment_evidence: int | None = None
    entity_count_with_aspect_evidence: int | None = None
    review_evidence_count: int | None = None
    positive_review_count: int | None = None
    neutral_review_count: int | None = None
    negative_review_count: int | None = None
    aspect_mention_count: int | None = None
    support_status: SupportStatus


class CoverageItem(BaseModel):
    province_id: str
    collected_entity_count: int | None = None
    mapped_entity_count: int | None = None
    excluded_entity_count: int | None = None
    unreviewed_entity_count: int | None = None
    entity_count_with_sentiment_evidence: int | None = None
    entity_count_with_aspect_evidence: int | None = None
    mapping_coverage_ratio: float | None = Field(default=None, ge=0, le=1)
    sentiment_entity_coverage_ratio: float | None = Field(default=None, ge=0, le=1)
    aspect_entity_coverage_ratio: float | None = Field(default=None, ge=0, le=1)
    support_status: SupportStatus


__all__ = [
    "CoverageItem",
    "DomainAnalyticsItem",
    "MarketOverviewResponse",
    "ProvinceComparisonItem",
    "SupportStatus",
]
