from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from travel_intelligence.gold.spark_store import SparkGoldStore


@dataclass(frozen=True)
class _Field:
    name: str


@dataclass(frozen=True)
class _Schema:
    fields: tuple[_Field, ...]


@dataclass(frozen=True)
class _Table:
    schema: _Schema


class _Frame:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows
        self.views: list[str] = []

    def createOrReplaceTempView(self, view: str) -> None:
        self.views.append(view)


class _Spark:
    def __init__(self) -> None:
        self.schema = _Schema((_Field("entity_sk"), _Field("release_id")))
        self.frames: list[_Frame] = []
        self.statements: list[str] = []

    def table(self, _name: str) -> _Table:
        return _Table(self.schema)

    def createDataFrame(
        self, rows: list[dict[str, Any]], *, schema: _Schema
    ) -> _Frame:
        assert schema == self.schema
        frame = _Frame(rows)
        self.frames.append(frame)
        return frame

    def sql(self, statement: str) -> None:
        self.statements.append(statement)


def test_repeated_dim_entity_upsert_uses_stable_merge_key_without_append() -> None:
    spark = _Spark()
    store = SparkGoldStore(spark)
    rows = [{"entity_sk": "entity-release-1-place-1", "release_id": "release-1"}]

    store.upsert("dim_entity", rows)
    store.upsert("dim_entity", rows)

    assert len(spark.statements) == 2
    assert spark.statements[0] == spark.statements[1]
    assert "MERGE INTO travel.gold.dim_entity" in spark.statements[0]
    assert "ON t.entity_sk=s.entity_sk" in spark.statements[0]
    assert "WHEN MATCHED THEN UPDATE" in spark.statements[0]
    assert "WHEN NOT MATCHED THEN INSERT" in spark.statements[0]
    assert all("APPEND" not in statement.upper() for statement in spark.statements)
    assert [frame.rows for frame in spark.frames] == [rows, rows]

