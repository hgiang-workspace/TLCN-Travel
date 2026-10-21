# ruff: noqa: E501
"""Spark/Iceberg persistence for T04-C enrichment results.

PySpark is intentionally imported only when this adapter is instantiated.  A
partial run is never published: the run ledger remains ``partial`` or
``failed`` until both result tables reconcile successfully.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from travel_intelligence.sentiment.contracts import (
    REVIEW_ASPECT_SENTIMENT_TABLE,
    REVIEW_ENRICHMENT_RUN_TABLE,
    REVIEW_SENTIMENT_TABLE,
)


class SparkIcebergEnrichmentStore:
    def __init__(self, spark: Any) -> None:
        self.spark = spark
        self.review_table = "travel.silver.review_sentiment"
        self.aspect_table = "travel.silver.review_aspect_sentiment"
        self.run_table = "travel.silver.review_enrichment_run"

    def create_tables(self) -> None:
        self.spark.sql(REVIEW_SENTIMENT_TABLE)
        self.spark.sql(REVIEW_ASPECT_SENTIMENT_TABLE)
        self.spark.sql(REVIEW_ENRICHMENT_RUN_TABLE)

    def begin(self, run: Mapping[str, Any]) -> None:
        self._merge(
            self.run_table,
            [
                {
                    **run,
                    "actual_review_count": 0,
                    "actual_aspect_count": 0,
                    "review_snapshot_id": None,
                    "aspect_snapshot_id": None,
                    "started_at": self._now(),
                    "finished_at": None,
                    "error_summary": None,
                }
            ],
            self._run_schema(),
            "processing_run_id",
        )

    def upsert_reviews(self, rows: Sequence[Mapping[str, Any]]) -> None:
        self._merge(self.review_table, rows, self._review_schema(), "review_sentiment_id")

    def upsert_aspects(self, rows: Sequence[Mapping[str, Any]]) -> None:
        self._merge(self.aspect_table, rows, self._aspect_schema(), "aspect_sentiment_id")

    def reconcile(self, run: Mapping[str, Any]) -> Mapping[str, Any]:
        processing_run_id = self._sql_literal(str(run["processing_run_id"]))
        release_id = self._sql_literal(str(run["source_release_id"]))
        review_count = self._count(
            self.review_table,
            f"processing_run_id = {processing_run_id} AND source_release_id = {release_id}",
            "review_sentiment_id",
        )
        aspect_count = self._count(
            self.aspect_table,
            f"processing_run_id = {processing_run_id} AND source_release_id = {release_id}",
            "aspect_sentiment_id",
        )
        return {
            "review_count": review_count,
            "aspect_count": aspect_count,
            "review_snapshot_id": self._snapshot(self.review_table),
            "aspect_snapshot_id": self._snapshot(self.aspect_table),
        }

    def commit(self, run: Mapping[str, Any], reconciliation: Mapping[str, Any]) -> None:
        self._merge(
            self.run_table,
            [
                {
                    **run,
                    "status": "committed",
                    "actual_review_count": int(reconciliation["review_count"]),
                    "actual_aspect_count": int(reconciliation["aspect_count"]),
                    "review_snapshot_id": reconciliation["review_snapshot_id"],
                    "aspect_snapshot_id": reconciliation["aspect_snapshot_id"],
                    "started_at": self._existing_start(run["processing_run_id"]),
                    "finished_at": self._now(),
                    "error_summary": None,
                }
            ],
            self._run_schema(),
            "processing_run_id",
        )

    def fail(self, run: Mapping[str, Any], error: str) -> None:
        self._merge(
            self.run_table,
            [
                {
                    **run,
                    "status": "failed",
                    "actual_review_count": self._count(
                        self.review_table,
                        f"processing_run_id = {self._sql_literal(str(run['processing_run_id']))}",
                        "review_sentiment_id",
                    ),
                    "actual_aspect_count": self._count(
                        self.aspect_table,
                        f"processing_run_id = {self._sql_literal(str(run['processing_run_id']))}",
                        "aspect_sentiment_id",
                    ),
                    "review_snapshot_id": self._snapshot(self.review_table),
                    "aspect_snapshot_id": self._snapshot(self.aspect_table),
                    "started_at": self._existing_start(run["processing_run_id"]),
                    "finished_at": self._now(),
                    "error_summary": str(error)[:2000],
                }
            ],
            self._run_schema(),
            "processing_run_id",
        )

    def _merge(self, table: str, rows: Sequence[Mapping[str, Any]], schema: Any, key: str) -> None:
        if not rows:
            return
        view = "t04c_" + table.rsplit(".", 1)[-1]
        frame = self.spark.createDataFrame([dict(row) for row in rows], schema=schema)
        frame.createOrReplaceTempView(view)
        columns = [field.name for field in schema.fields]
        assignments = ", ".join(f"t.{column}=s.{column}" for column in columns)
        insert_columns = ", ".join(columns)
        insert_values = ", ".join(f"s.{column}" for column in columns)
        self.spark.sql(
            f"MERGE INTO {table} t USING {view} s ON t.{key}=s.{key} "
            f"WHEN MATCHED THEN UPDATE SET {assignments} "
            f"WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({insert_values})"
        )

    def _count(self, table: str, predicate: str, key: str) -> int:
        row = self.spark.sql(
            f"SELECT COUNT(DISTINCT {key}) AS count FROM {table} WHERE {predicate}"
        ).collect()[0]
        return int(row["count"] if isinstance(row, dict) else row[0])

    def _snapshot(self, table: str) -> str | None:
        rows = self.spark.sql(
            f"SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1"
        ).collect()
        return str(rows[0][0]) if rows else None

    def _existing_start(self, run_id: str) -> Any:
        rows = self.spark.sql(
            f"SELECT started_at FROM {self.run_table} "
            f"WHERE processing_run_id = {self._sql_literal(run_id)}"
        ).collect()
        return rows[0][0] if rows else self._now()

    @staticmethod
    def _now() -> Any:
        from datetime import datetime, timezone

        return datetime.now(timezone.utc).replace(tzinfo=None)

    @staticmethod
    def _sql_literal(value: str) -> str:
        return "'" + value.replace("'", "''") + "'"

    @staticmethod
    def _types() -> Any:
        from pyspark.sql.types import (
            ArrayType,
            BooleanType,
            DoubleType,
            LongType,
            StringType,
            StructField,
            StructType,
            TimestampType,
        )

        return {
            "ArrayType": ArrayType,
            "BooleanType": BooleanType,
            "DoubleType": DoubleType,
            "LongType": LongType,
            "StringType": StringType,
            "StructField": StructField,
            "StructType": StructType,
            "TimestampType": TimestampType,
        }

    @classmethod
    def _review_schema(cls) -> Any:
        t = cls._types()
        S = t["StructField"]
        return t["StructType"]([
            S("review_sentiment_id", t["StringType"](), False), S("review_id", t["StringType"](), False),
            S("source_record_sk", t["StringType"](), False), S("source_id", t["StringType"](), False), S("batch_id", t["StringType"](), False),
            S("source_row_index", t["LongType"](), False), S("source_record_id", t["StringType"](), True), S("place_id", t["StringType"](), True),
            S("review_text_hash", t["StringType"](), False), S("sentiment_label", t["StringType"](), False), S("confidence", t["DoubleType"](), False),
            S("model_id", t["StringType"](), False), S("model_fingerprint", t["StringType"](), False), S("config_fingerprint", t["StringType"](), False),
            S("processing_run_id", t["StringType"](), False), S("source_release_id", t["StringType"](), False), S("processed_at", t["TimestampType"](), False),
            S("quality_codes", t["ArrayType"](t["StringType"](), False), False),
        ])

    @classmethod
    def _aspect_schema(cls) -> Any:
        t = cls._types()
        S = t["StructField"]
        return t["StructType"]([
            S("aspect_sentiment_id", t["StringType"](), False), S("review_sentiment_id", t["StringType"](), False), S("review_id", t["StringType"](), False),
            S("source_record_sk", t["StringType"](), False), S("source_id", t["StringType"](), False), S("batch_id", t["StringType"](), False), S("source_row_index", t["LongType"](), False),
            S("segment_ordinal", t["LongType"](), False), S("sentence_ordinal", t["LongType"](), True), S("clause_ordinal", t["LongType"](), True),
            S("segment_start_char", t["LongType"](), True), S("segment_end_char", t["LongType"](), True), S("segment_text", t["StringType"](), False), S("segment_text_hash", t["StringType"](), False),
            S("aspect", t["StringType"](), False), S("matched_keyword", t["StringType"](), True), S("segmentation_strategy", t["StringType"](), False),
            S("negation_detected", t["BooleanType"](), False), S("negation_metadata", t["StringType"](), True), S("sentiment_label", t["StringType"](), False), S("confidence", t["DoubleType"](), False),
            S("model_id", t["StringType"](), False), S("model_fingerprint", t["StringType"](), False), S("config_fingerprint", t["StringType"](), False), S("processing_run_id", t["StringType"](), False),
            S("source_release_id", t["StringType"](), False), S("processed_at", t["TimestampType"](), False), S("quality_codes", t["ArrayType"](t["StringType"](), False), False),
        ])

    @classmethod
    def _run_schema(cls) -> Any:
        t = cls._types()
        S = t["StructField"]
        return t["StructType"]([
            S("processing_run_id", t["StringType"](), False), S("source_id", t["StringType"](), False), S("source_release_id", t["StringType"](), False),
            S("model_id", t["StringType"](), False), S("model_fingerprint", t["StringType"](), False), S("config_fingerprint", t["StringType"](), False), S("input_fingerprint", t["StringType"](), False),
            S("status", t["StringType"](), False), S("expected_review_count", t["LongType"](), False), S("expected_aspect_count", t["LongType"](), False),
            S("actual_review_count", t["LongType"](), False), S("actual_aspect_count", t["LongType"](), False), S("review_snapshot_id", t["StringType"](), True), S("aspect_snapshot_id", t["StringType"](), True),
            S("started_at", t["TimestampType"](), False), S("finished_at", t["TimestampType"](), True), S("error_summary", t["StringType"](), True),
        ])


__all__ = ["SparkIcebergEnrichmentStore"]
