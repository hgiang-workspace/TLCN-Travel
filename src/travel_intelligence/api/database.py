"""Minimal read-only Trino access and Gold candidate resolution."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

import trino.dbapi

from travel_intelligence.api.config import ApiSettings

REQUIRED_GOLD_TABLES = (
    "dim_entity",
    "fact_data_coverage",
    "fact_market_summary",
)


class GoldReadinessError(RuntimeError):
    """Base class for expected, safely reportable readiness failures."""


class TrinoUnavailableError(GoldReadinessError):
    """Raised when Trino cannot execute a query."""


class GoldReleaseError(GoldReadinessError):
    """Raised when the configured candidate cannot be resolved safely."""


class QueryScopeError(ValueError):
    """Raised when source or processing-period selection is invalid or ambiguous."""


class ReadOnlyDatabase(Protocol):
    def fetch_all(
        self, sql: str, parameters: Sequence[Any] = ()
    ) -> list[dict[str, Any]]: ...


@dataclass(frozen=True)
class GoldReleaseContext:
    release_id: str
    status: str
    snapshots: Mapping[str, int]
    row_counts: Mapping[str, int]


class TrinoDatabase:
    """Open one Trino connection per small read-only operation."""

    def __init__(self, settings: ApiSettings) -> None:
        self._settings = settings

    def fetch_all(
        self, sql: str, parameters: Sequence[Any] = ()
    ) -> list[dict[str, Any]]:
        _assert_read_only(sql)
        connection = None
        cursor = None
        try:
            connection = trino.dbapi.connect(
                host=self._settings.trino_host,
                port=self._settings.trino_port,
                user=self._settings.trino_user,
                catalog=self._settings.trino_catalog,
                schema=self._settings.trino_schema,
                http_scheme=self._settings.trino_http_scheme,
                request_timeout=self._settings.trino_request_timeout_seconds,
            )
            cursor = connection.cursor()
            cursor.execute(sql, tuple(parameters))
            names = tuple(column[0] for column in (cursor.description or ()))
            return [dict(zip(names, row, strict=True)) for row in cursor.fetchall()]
        except Exception as exc:
            raise TrinoUnavailableError("Trino query failed") from exc
        finally:
            if cursor is not None:
                cursor.close()
            if connection is not None:
                connection.close()


def resolve_gold_candidate(
    database: ReadOnlyDatabase, settings: ApiSettings
) -> GoldReleaseContext:
    """Resolve one explicit partial candidate and validate its pinned snapshots."""
    release_id = settings.gold_release_id
    if not release_id:
        raise GoldReleaseError("GOLD_RELEASE_ID is required")
    ledger = database.fetch_all(
        """
        SELECT release_id, status, output_snapshot_refs
        FROM travel.gold.gold_build_run
        WHERE release_id = ?
        """,
        (release_id,),
    )
    if len(ledger) != 1:
        raise GoldReleaseError("configured Gold release was not found uniquely")
    status = str(ledger[0]["status"])
    if status != "partial":
        raise GoldReleaseError("configured Gold release status is not permitted")
    raw_snapshots = ledger[0].get("output_snapshot_refs")
    if not isinstance(raw_snapshots, Mapping):
        raise GoldReleaseError("configured Gold release has no snapshot references")

    snapshots: dict[str, int] = {}
    row_counts: dict[str, int] = {}
    for table in REQUIRED_GOLD_TABLES:
        raw_snapshot = raw_snapshots.get(table)
        try:
            snapshot_id = int(raw_snapshot)
        except (TypeError, ValueError) as exc:
            raise GoldReleaseError(f"required snapshot is missing for {table}") from exc
        snapshots[table] = snapshot_id
        _validate_snapshot_exists(database, table, snapshot_id)
        row_counts[table] = _pinned_release_count(
            database, table, snapshot_id, release_id
        )
    return GoldReleaseContext(
        release_id=release_id,
        status=status,
        snapshots=snapshots,
        row_counts=row_counts,
    )


def resolve_source_period(
    database: ReadOnlyDatabase,
    context: GoldReleaseContext,
    source_id: str | None,
    reporting_period: str | None,
) -> tuple[str, str]:
    """Resolve optional query scope only when Gold has one unambiguous choice."""
    rows = database.fetch_all(
        """
        SELECT DISTINCT source_id, reporting_period
        FROM travel.gold.fact_data_coverage FOR VERSION AS OF ?
        WHERE release_id = ?
        ORDER BY source_id, reporting_period
        """,
        (context.snapshots["fact_data_coverage"], context.release_id),
    )
    sources = sorted({str(row["source_id"]) for row in rows})
    if source_id is None:
        if len(sources) != 1:
            raise QueryScopeError(
                "source_id is required when source scope is ambiguous"
            )
        selected_source = sources[0]
    else:
        selected_source = source_id
        if selected_source not in sources:
            raise QueryScopeError(
                "source_id is not available in the configured release"
            )
    periods = sorted(
        {
            str(row["reporting_period"])
            for row in rows
            if str(row["source_id"]) == selected_source
        }
    )
    if reporting_period is None:
        if len(periods) != 1:
            raise QueryScopeError(
                "reporting_period is required when period scope is ambiguous"
            )
        selected_period = periods[0]
    else:
        selected_period = reporting_period
        if selected_period not in periods:
            raise QueryScopeError(
                "reporting_period is not available for the selected source"
            )
    return selected_source, selected_period


def _validate_snapshot_exists(
    database: ReadOnlyDatabase, table: str, snapshot_id: int
) -> None:
    _assert_required_table(table)
    rows = database.fetch_all(
        f'SELECT snapshot_id FROM travel.gold."{table}$snapshots" '
        "WHERE snapshot_id = ?",
        (snapshot_id,),
    )
    if len(rows) != 1 or int(rows[0]["snapshot_id"]) != snapshot_id:
        raise GoldReleaseError(f"required snapshot is unavailable for {table}")


def _pinned_release_count(
    database: ReadOnlyDatabase,
    table: str,
    snapshot_id: int,
    release_id: str,
) -> int:
    _assert_required_table(table)
    rows = database.fetch_all(
        f"SELECT count(*) AS row_count FROM travel.gold.{table} "
        "FOR VERSION AS OF ? WHERE release_id = ?",
        (snapshot_id, release_id),
    )
    if len(rows) != 1:
        raise GoldReleaseError(f"pinned snapshot is not queryable for {table}")
    count = int(rows[0]["row_count"])
    if count <= 0:
        raise GoldReleaseError(f"pinned snapshot contains no release rows for {table}")
    return count


def _assert_required_table(table: str) -> None:
    if table not in REQUIRED_GOLD_TABLES:
        raise GoldReleaseError("table is not allowlisted for readiness")


def _assert_read_only(sql: str) -> None:
    statement = sql.strip().removesuffix(";").strip()
    if ";" in statement:
        raise ValueError("multiple SQL statements are not allowed")
    first = statement.split(maxsplit=1)[0].upper() if statement else ""
    if first not in {"SELECT", "WITH", "SHOW", "DESCRIBE", "EXPLAIN"}:
        raise ValueError("only read-only SQL statements are allowed")


__all__ = [
    "GoldReadinessError",
    "GoldReleaseContext",
    "GoldReleaseError",
    "QueryScopeError",
    "REQUIRED_GOLD_TABLES",
    "TrinoDatabase",
    "TrinoUnavailableError",
    "resolve_gold_candidate",
    "resolve_source_period",
]
