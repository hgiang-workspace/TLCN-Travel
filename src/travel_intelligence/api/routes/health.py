"""Liveness and Gold dependency readiness routes."""

from __future__ import annotations

import logging
from typing import Literal

from fastapi import APIRouter, Request, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from travel_intelligence.api.database import (
    GoldReadinessError,
    resolve_gold_candidate,
)

LOGGER = logging.getLogger(__name__)
router = APIRouter(tags=["health"])


class HealthResponse(BaseModel):
    status: Literal["ok"]
    service: Literal["travel-intelligence-api"]


class ReadinessResponse(BaseModel):
    status: Literal["ready"]
    service: Literal["travel-intelligence-api"]


class ReadinessErrorResponse(BaseModel):
    status: Literal["unavailable"]
    service: Literal["travel-intelligence-api"]
    message: Literal["Gold analytics dependencies are unavailable"]


@router.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    return HealthResponse(status="ok", service="travel-intelligence-api")


@router.get(
    "/ready",
    response_model=ReadinessResponse,
    responses={status.HTTP_503_SERVICE_UNAVAILABLE: {"model": ReadinessErrorResponse}},
)
def ready(request: Request) -> ReadinessResponse | JSONResponse:
    try:
        resolve_gold_candidate(request.app.state.database, request.app.state.settings)
    except GoldReadinessError:
        LOGGER.exception("Gold readiness check failed")
        body = ReadinessErrorResponse(
            status="unavailable",
            service="travel-intelligence-api",
            message="Gold analytics dependencies are unavailable",
        )
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content=body.model_dump(),
        )
    return ReadinessResponse(status="ready", service="travel-intelligence-api")


__all__ = ["router"]
