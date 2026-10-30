"""Environment configuration for the minimal FastAPI application."""

from __future__ import annotations

import os
from dataclasses import dataclass


def _integer(name: str, default: int) -> int:
    raw = os.getenv(name, str(default))
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if value <= 0:
        raise ValueError(f"{name} must be positive")
    return value


@dataclass(frozen=True)
class ApiSettings:
    """Small, dependency-free settings object loaded from environment variables."""

    trino_host: str
    trino_port: int
    trino_user: str
    trino_catalog: str
    trino_schema: str
    trino_http_scheme: str
    trino_request_timeout_seconds: int
    gold_release_id: str | None
    cors_origins: tuple[str, ...]

    @classmethod
    def from_env(cls) -> ApiSettings:
        origins = tuple(
            value.strip()
            for value in os.getenv("CORS_ORIGINS", "http://localhost:5173").split(",")
            if value.strip()
        )
        scheme = os.getenv("TRINO_HTTP_SCHEME", "http").strip().lower()
        if scheme not in {"http", "https"}:
            raise ValueError("TRINO_HTTP_SCHEME must be http or https")
        return cls(
            trino_host=os.getenv("TRINO_HOST", "localhost").strip(),
            trino_port=_integer("TRINO_PORT", 8080),
            trino_user=os.getenv("TRINO_USER", "travel_api").strip(),
            trino_catalog=os.getenv("TRINO_CATALOG", "travel").strip(),
            trino_schema=os.getenv("TRINO_SCHEMA", "gold").strip(),
            trino_http_scheme=scheme,
            trino_request_timeout_seconds=_integer(
                "TRINO_REQUEST_TIMEOUT_SECONDS", 10
            ),
            gold_release_id=os.getenv("GOLD_RELEASE_ID") or None,
            cors_origins=origins,
        )


__all__ = ["ApiSettings"]
