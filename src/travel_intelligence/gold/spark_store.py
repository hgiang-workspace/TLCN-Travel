# ruff: noqa: E501
"""Minimal Iceberg storage adapter for non-active T07-C candidate dimensions."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from travel_intelligence.gold.contracts import GOLD_TABLE_BY_NAME, GOLD_TABLES

DIMENSION_KEYS = {
    "dim_entity": "entity_sk",
    "dim_geography": "geography_sk",
    "dim_domain": "domain_sk",
    "dim_source": "source_sk",
    "dim_date": "date_sk",
    "bridge_entity_source": "entity_source_sk",
    "bridge_entity_geography": "entity_geography_sk",
    "gold_build_run": "gold_run_id",
}


class SparkGoldStore:
    """MERGE candidate rows deterministically; never activates a Gold release."""

    def __init__(self, spark: Any) -> None:
        self.spark = spark

    def create_contract_tables(self) -> None:
        self.spark.sql("CREATE NAMESPACE IF NOT EXISTS travel.gold")
        for table in GOLD_TABLES:
            self.spark.sql(table.create_sql())

    def upsert(self, table_name: str, rows: Sequence[Mapping[str, Any]]) -> None:
        if not rows:
            return
        if table_name not in DIMENSION_KEYS:
            raise ValueError(f"no T07-C merge key registered for: {table_name}")
        table = GOLD_TABLE_BY_NAME[table_name].qualified_name()
        schema = self.spark.table(table).schema
        fields = [field.name for field in schema.fields]
        missing = sorted(set(fields) - set(rows[0]))
        unexpected = sorted(set(rows[0]) - set(fields))
        if missing or unexpected:
            raise ValueError(
                f"{table_name} schema mismatch; missing={missing}, unexpected={unexpected}"
            )
        frame = self.spark.createDataFrame([dict(row) for row in rows], schema=schema)
        view = f"t07c_{table_name}"
        frame.createOrReplaceTempView(view)
        key = DIMENSION_KEYS[table_name]
        assignments = ", ".join(f"t.{field}=s.{field}" for field in fields)
        inserts = ", ".join(fields)
        values = ", ".join(f"s.{field}" for field in fields)
        self.spark.sql(
            f"MERGE INTO {table} t USING {view} s ON t.{key}=s.{key} "
            f"WHEN MATCHED THEN UPDATE SET {assignments} "
            f"WHEN NOT MATCHED THEN INSERT ({inserts}) VALUES ({values})"
        )

    def snapshot_id(self, table_name: str) -> str:
        table = GOLD_TABLE_BY_NAME[table_name].qualified_name()
        rows = self.spark.sql(
            f"SELECT snapshot_id FROM {table}.snapshots "
            "ORDER BY committed_at DESC LIMIT 1"
        ).collect()
        if not rows:
            raise ValueError(f"candidate table has no snapshot: {table_name}")
        return str(rows[0][0])

    def release_count(self, table_name: str, release_id: str) -> int:
        table = GOLD_TABLE_BY_NAME[table_name].qualified_name()
        escaped_release_id = release_id.replace("'", "''")
        row = self.spark.sql(
            f"SELECT count(*) FROM {table} WHERE release_id = '{escaped_release_id}'"
        ).collect()[0]
        return int(row[0])


__all__ = ["DIMENSION_KEYS", "SparkGoldStore"]
