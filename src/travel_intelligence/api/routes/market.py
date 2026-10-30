"""Read-only market overview, comparison, and domain analytics routes."""

from __future__ import annotations

import logging
import re
from typing import Annotated, Literal

from fastapi import APIRouter, HTTPException, Query, Request, status

from travel_intelligence.api.database import (
    GoldReadinessError,
    QueryScopeError,
    resolve_gold_candidate,
    resolve_source_period,
)
from travel_intelligence.api.schemas import (
    DomainAnalyticsItem,
    MarketOverviewResponse,
    ProvinceComparisonItem,
    SupportStatus,
)
from travel_intelligence.gold.market_read_queries import (
    DOMAIN_ANALYSIS_QUERY,
    MARKET_METRIC_DEFINITIONS,
    MARKET_OVERVIEW_QUERY,
    province_comparison_query,
)

LOGGER = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1/market", tags=["market"])

MetricId = Literal[
    "entity_count",
    "entity_count_with_sentiment_evidence",
    "entity_count_with_aspect_evidence",
    "review_evidence_count",
    "positive_review_count",
    "neutral_review_count",
    "negative_review_count",
    "aspect_mention_count",
]
DomainId = Literal[
    "destination",
    "accommodation",
    "food_dining",
    "transportation",
]
DOMAIN_IDS: tuple[DomainId, ...] = (
    "destination",
    "accommodation",
    "food_dining",
    "transportation",
)
_METRICS = {
    definition.metric_id: definition for definition in MARKET_METRIC_DEFINITIONS
}
_PROVINCE_ID = re.compile(r"^vn-province:[0-9a-f]{24}$")


def _context_and_scope(
    request: Request,
    source_id: str | None,
    reporting_period: str | None,
):
    try:
        context = resolve_gold_candidate(
            request.app.state.database, request.app.state.settings
        )
        scope = resolve_source_period(
            request.app.state.database, context, source_id, reporting_period
        )
        return context, scope
    except QueryScopeError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail=str(exc),
        ) from exc
    except GoldReadinessError as exc:
        _dependency_unavailable(exc)


def _fetch(request: Request, sql: str, parameters: tuple):
    try:
        return request.app.state.database.fetch_all(sql, parameters)
    except GoldReadinessError as exc:
        _dependency_unavailable(exc)


def _dependency_unavailable(exc: Exception):
    LOGGER.exception("Gold analytics query failed", exc_info=exc)
    raise HTTPException(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        detail="Gold analytics dependencies are unavailable",
    ) from exc


def _support_status(value: object) -> SupportStatus:
    try:
        return SupportStatus(str(value))
    except ValueError as exc:
        _dependency_unavailable(exc)


def _validated_provinces(province_ids: list[str]) -> list[str]:
    if len(set(province_ids)) != len(province_ids):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail="province_id values must be unique",
        )
    if any(not _PROVINCE_ID.fullmatch(value) for value in province_ids):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail="province_id has an invalid canonical format",
        )
    return province_ids


@router.get("/overview", response_model=MarketOverviewResponse)
def market_overview(
    request: Request,
    source_id: str | None = None,
    reporting_period: str | None = None,
) -> MarketOverviewResponse:
    context, (selected_source, selected_period) = _context_and_scope(
        request, source_id, reporting_period
    )
    rows = _fetch(
        request,
        MARKET_OVERVIEW_QUERY.sql,
        (
            context.release_id,
            selected_source,
            selected_period,
            context.snapshots["fact_data_coverage"],
            context.snapshots["fact_market_summary"],
        ),
    )
    if len(rows) != 1 or rows[0].get("collected_entity_count") is None:
        return MarketOverviewResponse(support_status=SupportStatus.no_data)
    row = rows[0]
    return MarketOverviewResponse(
        collected_entity_count=int(row["collected_entity_count"]),
        mapped_entity_count=int(row["mapped_entity_count"]),
        excluded_entity_count=int(row["excluded_entity_count"]),
        unreviewed_entity_count=int(row["unreviewed_entity_count"]),
        entity_count_with_sentiment_evidence=int(row["sentiment_entity_count"]),
        entity_count_with_aspect_evidence=int(row["aspect_entity_count"]),
        mapping_coverage_ratio=float(row["mapping_coverage_ratio"]),
        sentiment_entity_coverage_ratio=float(
            row["sentiment_entity_coverage_ratio"]
        ),
        aspect_entity_coverage_ratio=float(row["aspect_entity_coverage_ratio"]),
        review_evidence_count=int(row["review_evidence_count"]),
        aspect_mention_count=int(row["aspect_mention_count"]),
        support_status=_support_status(row["data_status"]),
    )


@router.get("/provinces/compare", response_model=list[ProvinceComparisonItem])
def compare_provinces(
    request: Request,
    province_id: Annotated[list[str], Query(min_length=1, max_length=20)],
    metric_id: MetricId,
    domain_id: DomainId | None = None,
    source_id: str | None = None,
    reporting_period: str | None = None,
) -> list[ProvinceComparisonItem]:
    province_ids = _validated_provinces(province_id)
    context, (selected_source, selected_period) = _context_and_scope(
        request, source_id, reporting_period
    )
    query = province_comparison_query(
        len(province_ids), filter_domain=domain_id is not None
    )
    parameters = [
        context.release_id,
        selected_source,
        selected_period,
        context.snapshots["fact_market_summary"],
    ]
    if domain_id is not None:
        parameters.append(domain_id)
    parameters.extend((metric_id, *province_ids))
    rows = _fetch(request, query.sql, tuple(parameters))
    by_province = {str(row["geo_id"]): row for row in rows}
    unit = _METRICS[metric_id].unit
    return [
        ProvinceComparisonItem(
            province_id=selected_province,
            domain_id=domain_id,
            metric_id=metric_id,
            unit=unit,
            count=(
                int(by_province[selected_province]["value"])
                if selected_province in by_province
                else None
            ),
            support_status=(
                _support_status(by_province[selected_province]["data_status"])
                if selected_province in by_province
                else SupportStatus.no_data
            ),
        )
        for selected_province in province_ids
    ]


@router.get("/domains", response_model=list[DomainAnalyticsItem])
def market_domains(
    request: Request,
    source_id: str | None = None,
    reporting_period: str | None = None,
) -> list[DomainAnalyticsItem]:
    context, (selected_source, selected_period) = _context_and_scope(
        request, source_id, reporting_period
    )
    rows = _fetch(
        request,
        DOMAIN_ANALYSIS_QUERY.sql,
        (
            context.release_id,
            selected_source,
            selected_period,
            context.snapshots["fact_market_summary"],
        ),
    )
    by_domain = {str(row["domain_id"]): row for row in rows}
    result = []
    for domain_id in DOMAIN_IDS:
        row = by_domain.get(domain_id)
        if row is None:
            result.append(
                DomainAnalyticsItem(
                    domain_id=domain_id,
                    support_status=SupportStatus.no_data,
                )
            )
            continue
        result.append(
            DomainAnalyticsItem(
                domain_id=domain_id,
                entity_count=int(row["entity_count"]),
                entity_count_with_sentiment_evidence=int(
                    row["entity_count_with_sentiment_evidence"]
                ),
                entity_count_with_aspect_evidence=int(
                    row["entity_count_with_aspect_evidence"]
                ),
                review_evidence_count=int(row["review_evidence_count"]),
                positive_review_count=int(row["positive_review_count"]),
                neutral_review_count=int(row["neutral_review_count"]),
                negative_review_count=int(row["negative_review_count"]),
                aspect_mention_count=int(row["aspect_mention_count"]),
                support_status=_support_status(row["data_status"]),
            )
        )
    return result


__all__ = ["router"]
