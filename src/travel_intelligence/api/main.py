"""FastAPI application factory for the read-only Travel Intelligence API."""

from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from travel_intelligence.api.config import ApiSettings
from travel_intelligence.api.database import ReadOnlyDatabase, TrinoDatabase
from travel_intelligence.api.routes.coverage import router as coverage_router
from travel_intelligence.api.routes.health import router as health_router
from travel_intelligence.api.routes.market import router as market_router


def create_app(
    settings: ApiSettings | None = None,
    database: ReadOnlyDatabase | None = None,
) -> FastAPI:
    resolved_settings = settings or ApiSettings.from_env()
    app = FastAPI(
        title="Travel Intelligence API",
        version="0.1.0",
        description="Read-only API for the Travel Intelligence Gold layer.",
    )
    app.state.settings = resolved_settings
    app.state.database = database or TrinoDatabase(resolved_settings)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=list(resolved_settings.cors_origins),
        allow_credentials=False,
        allow_methods=["GET"],
        allow_headers=["Accept", "Content-Type"],
    )
    app.include_router(health_router)
    app.include_router(market_router)
    app.include_router(coverage_router)
    return app


app = create_app()

__all__ = ["app", "create_app"]
