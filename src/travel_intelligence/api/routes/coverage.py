"""Read-only province coverage route."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Query, Request

from travel_intelligence.api.routes.market import (
    _context_and_scope,
    _fetch,
    _support_status,
    _validated_provinces,
)
from travel_intelligence.api.schemas import CoverageItem, SupportStatus
from travel_intelligence.gold.market_read_queries import DATA_COVERAGE_QUERY

router = APIRouter(prefix="/api/v1/data", tags=["coverage"])


@router.get("/coverage", response_model=list[CoverageItem])
def data_coverage(
    request: Request,
    province_id: Annotated[list[str] | None, Query(max_length=63)] = None,
    source_id: str | None = None,
    reporting_period: str | None = None,
) -> list[CoverageItem]:
    requested_provinces = (
        _validated_provinces(province_id) if province_id is not None else None
    )
    context, (selected_source, selected_period) = _context_and_scope(
        request, source_id, reporting_period
    )
    rows = _fetch(
        request,
        DATA_COVERAGE_QUERY.sql,
        (
            context.release_id,
            selected_source,
            selected_period,
            context.snapshots["fact_data_coverage"],
        ),
    )
    by_province = {str(row["geo_id"]): row for row in rows}
    province_ids = requested_provinces or list(by_province)
    result = []
    for selected_province in province_ids:
        row = by_province.get(selected_province)
        if row is None:
            result.append(
                CoverageItem(
                    province_id=selected_province,
                    support_status=SupportStatus.no_data,
                )
            )
            continue
        result.append(
            CoverageItem(
                province_id=selected_province,
                collected_entity_count=int(row["collected_entity_count"]),
                mapped_entity_count=int(row["mapped_entity_count"]),
                excluded_entity_count=int(row["excluded_entity_count"]),
                unreviewed_entity_count=int(row["unreviewed_entity_count"]),
                entity_count_with_sentiment_evidence=int(
                    row["sentiment_entity_count"]
                ),
                entity_count_with_aspect_evidence=int(row["aspect_entity_count"]),
                mapping_coverage_ratio=_optional_float(
                    row["mapping_coverage_ratio"]
                ),
                sentiment_entity_coverage_ratio=_optional_float(
                    row["sentiment_entity_coverage_ratio"]
                ),
                aspect_entity_coverage_ratio=_optional_float(
                    row["aspect_entity_coverage_ratio"]
                ),
                support_status=_support_status(row["data_status"]),
            )
        )
    return result


def _optional_float(value: object) -> float | None:
    return None if value is None else float(value)


__all__ = ["router"]
