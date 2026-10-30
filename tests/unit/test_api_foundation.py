from __future__ import annotations

from typing import Any

import pytest
from fastapi.testclient import TestClient

from travel_intelligence.api.config import ApiSettings
from travel_intelligence.api.database import (
    GoldReleaseError,
    TrinoDatabase,
    TrinoUnavailableError,
    resolve_gold_candidate,
)
from travel_intelligence.api.main import create_app

RELEASE_ID = "gold-release-test"
SNAPSHOTS = {
    "dim_entity": "101",
    "fact_data_coverage": "102",
    "fact_market_summary": "103",
}
COUNTS = {
    "dim_entity": 4000,
    "fact_data_coverage": 24,
    "fact_market_summary": 448,
}


def settings(*, release_id: str | None = RELEASE_ID) -> ApiSettings:
    return ApiSettings(
        trino_host="trino",
        trino_port=8080,
        trino_user="travel_api",
        trino_catalog="travel",
        trino_schema="gold",
        trino_http_scheme="http",
        trino_request_timeout_seconds=10,
        gold_release_id=release_id,
        cors_origins=("http://localhost:5173",),
    )


class FakeDatabase:
    def __init__(
        self,
        *,
        release_exists: bool = True,
        release_status: str = "partial",
        snapshots: dict[str, str] | None = None,
        missing_snapshot: str | None = None,
        failure: Exception | None = None,
    ) -> None:
        self.release_exists = release_exists
        self.release_status = release_status
        self.snapshots = SNAPSHOTS if snapshots is None else snapshots
        self.missing_snapshot = missing_snapshot
        self.failure = failure
        self.calls: list[tuple[str, tuple[Any, ...]]] = []

    def fetch_all(self, sql: str, parameters=()):
        bound = tuple(parameters)
        self.calls.append((sql, bound))
        if self.failure:
            raise self.failure
        if "gold_build_run" in sql:
            if not self.release_exists:
                return []
            return [
                {
                    "release_id": RELEASE_ID,
                    "status": self.release_status,
                    "output_snapshot_refs": self.snapshots,
                }
            ]
        if "$snapshots" in sql:
            table = next(name for name in SNAPSHOTS if name in sql)
            if table == self.missing_snapshot:
                return []
            return [{"snapshot_id": bound[0]}]
        if "FOR VERSION AS OF" in sql:
            table = next(name for name in SNAPSHOTS if name in sql)
            return [{"row_count": COUNTS[table]}]
        raise AssertionError(f"unexpected query: {sql}")


def test_application_health_and_openapi_start_without_trino():
    client = TestClient(create_app(settings(), FakeDatabase()))
    assert client.get("/health").status_code == 200
    assert client.get("/health").json() == {
        "status": "ok",
        "service": "travel-intelligence-api",
    }
    assert client.get("/docs").status_code == 200
    assert client.get("/openapi.json").status_code == 200


def test_ready_resolves_candidate_and_every_required_snapshot():
    database = FakeDatabase()
    response = TestClient(create_app(settings(), database)).get("/ready")
    assert response.status_code == 200
    assert response.json() == {
        "status": "ready",
        "service": "travel-intelligence-api",
    }
    assert len(database.calls) == 7
    assert database.calls[0][1] == (RELEASE_ID,)


def test_ready_returns_safe_503_when_trino_is_unavailable():
    database = FakeDatabase(failure=TrinoUnavailableError("secret internal error"))
    response = TestClient(create_app(settings(), database)).get("/ready")
    assert response.status_code == 503
    assert response.json() == {
        "status": "unavailable",
        "service": "travel-intelligence-api",
        "message": "Gold analytics dependencies are unavailable",
    }
    assert "secret" not in response.text


@pytest.mark.parametrize(
    "configured_settings,database",
    [
        (settings(release_id=None), FakeDatabase()),
        (settings(), FakeDatabase(release_exists=False)),
        (settings(), FakeDatabase(release_status="failed")),
        (
            settings(),
            FakeDatabase(
                snapshots={
                    "dim_entity": "101",
                    "fact_data_coverage": "102",
                }
            ),
        ),
        (settings(), FakeDatabase(missing_snapshot="fact_market_summary")),
    ],
)
def test_ready_rejects_invalid_release_or_snapshot(configured_settings, database):
    response = TestClient(create_app(configured_settings, database)).get("/ready")
    assert response.status_code == 503
    assert response.json()["message"] == (
        "Gold analytics dependencies are unavailable"
    )


def test_release_context_contains_only_server_resolved_pinned_snapshots():
    context = resolve_gold_candidate(FakeDatabase(), settings())
    assert context.release_id == RELEASE_ID
    assert context.status == "partial"
    assert context.snapshots == {
        "dim_entity": 101,
        "fact_data_coverage": 102,
        "fact_market_summary": 103,
    }
    assert context.row_counts == COUNTS


def test_cors_allows_only_configured_react_origin():
    client = TestClient(create_app(settings(), FakeDatabase()))
    allowed = client.options(
        "/health",
        headers={
            "Origin": "http://localhost:5173",
            "Access-Control-Request-Method": "GET",
        },
    )
    assert allowed.status_code == 200
    assert allowed.headers["access-control-allow-origin"] == "http://localhost:5173"
    denied = client.options(
        "/health",
        headers={
            "Origin": "http://evil.example",
            "Access-Control-Request-Method": "GET",
        },
    )
    assert "access-control-allow-origin" not in denied.headers


def test_trino_adapter_binds_parameters_and_closes_resources(monkeypatch):
    events: list[Any] = []

    class Cursor:
        description = (("value",),)

        def execute(self, sql, parameters):
            events.append(("execute", sql, parameters))

        def fetchall(self):
            return [[1]]

        def close(self):
            events.append("cursor_closed")

    class Connection:
        def cursor(self):
            return Cursor()

        def close(self):
            events.append("connection_closed")

    monkeypatch.setattr(
        "travel_intelligence.api.database.trino.dbapi.connect",
        lambda **kwargs: Connection(),
    )
    rows = TrinoDatabase(settings()).fetch_all("SELECT ? AS value", (1,))
    assert rows == [{"value": 1}]
    assert events == [
        ("execute", "SELECT ? AS value", (1,)),
        "cursor_closed",
        "connection_closed",
    ]


def test_trino_adapter_rejects_write_or_multiple_statements():
    database = TrinoDatabase(settings())
    with pytest.raises(ValueError, match="read-only"):
        database.fetch_all("DELETE FROM travel.gold.dim_entity")
    with pytest.raises(ValueError, match="multiple"):
        database.fetch_all("SELECT 1; SELECT 2")


def test_resolver_exposes_specific_internal_error_only_to_python_callers():
    with pytest.raises(GoldReleaseError, match="not found uniquely"):
        resolve_gold_candidate(FakeDatabase(release_exists=False), settings())
