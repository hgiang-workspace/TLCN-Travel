"TripAdvisor-only Silver review eligibility and lineage contract."

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Iterable, Mapping

from .foundation import release_is_visible
from .normalization import _id

TRIPADVISOR_SOURCE_ID = "tripadvisor"
TRIPADVISOR_REQUIRED_RELEASE_TABLES = (
    "source_record",
    "place",
    "place_rating",
    "review",
    "source_entity_mapping",
    "normalization_run",
)

TRIPADVISOR_REVIEW_SQL = """
WITH committed_tripadvisor_batches AS (
    SELECT DISTINCT source_id, bronze_batch_id
    FROM travel.silver.normalization_run
    WHERE source_id = 'tripadvisor'
      AND status = 'committed'
      AND table_snapshot_refs IS NOT NULL
      AND element_at(table_snapshot_refs, 'source_record') IS NOT NULL
      AND element_at(table_snapshot_refs, 'place') IS NOT NULL
      AND element_at(table_snapshot_refs, 'place_rating') IS NOT NULL
      AND element_at(table_snapshot_refs, 'review') IS NOT NULL
      AND element_at(table_snapshot_refs, 'source_entity_mapping') IS NOT NULL
      AND element_at(table_snapshot_refs, 'normalization_run') IS NOT NULL
)
SELECT
    r.review_id, r.review_ordinal, r.review_text, r.detail_review,
    r.key_review, r.rating, r.language_raw, r.quality_codes,
    r.source_record_sk, sr.source_id, sr.batch_id, sr.source_row_index,
    sr.source_record_id, sr.source_uri, sr.payload_hash, sem.place_id,
    p.place_name, p.place_type
FROM travel.silver.review r
JOIN travel.silver.source_record sr
  ON sr.source_record_sk = r.source_record_sk
JOIN committed_tripadvisor_batches cb
  ON cb.source_id = sr.source_id
 AND cb.bronze_batch_id = sr.batch_id
LEFT JOIN travel.silver.source_entity_mapping sem
  ON sem.source_record_sk = sr.source_record_sk
LEFT JOIN travel.silver.place p
  ON p.place_id = sem.place_id
WHERE sr.source_id = 'tripadvisor'
  AND nullif(trim(coalesce(r.review_text, '')), '') IS NOT NULL
"""


@dataclass(frozen=True)
class TripAdvisorReviewOccurrence:
    review_id: str
    review_ordinal: int
    review_text: str
    source_record_sk: str
    source_id: str
    batch_id: str
    source_row_index: int
    source_record_id: str | None
    place_id: str | None
    source_uri: str | None
    source_release_id: str


def validate_tripadvisor_release(
    normalization_run: Mapping[str, Any],
    *,
    snapshot_exists: Any = None,
) -> None:
    "Reject non-committed or incomplete release evidence."
    if normalization_run.get("source_id") != TRIPADVISOR_SOURCE_ID:
        raise ValueError("normalization run is not a TripAdvisor release")
    refs = normalization_run.get("table_snapshot_refs")
    if not release_is_visible(
        normalization_run.get("status", ""),
        TRIPADVISOR_REQUIRED_RELEASE_TABLES,
        refs,
    ):
        raise ValueError("TripAdvisor Silver release is not committed and complete")
    if snapshot_exists is not None:
        missing = [
            table
            for table in TRIPADVISOR_REQUIRED_RELEASE_TABLES
            if not snapshot_exists(table, refs[table])
        ]
        if missing:
            raise ValueError(
                "TripAdvisor release references missing snapshots: "
                + ", ".join(missing)
            )


def review_occurrence_id(source_record_sk: str, review_ordinal: int) -> str:
    return _id("silver-review-occurrence-v2", source_record_sk, review_ordinal)


def _review_text(row: Mapping[str, Any]) -> str | None:
    value = row.get("review_text")
    if value is None:
        value = row.get("detail_review")
    if not isinstance(value, str) or not value.strip():
        return None
    return value


def select_tripadvisor_reviews(
    rows: Iterable[Mapping[str, Any]],
    committed_runs: Mapping[str, Mapping[str, Any]],
    *,
    snapshot_exists: Any = None,
) -> tuple[TripAdvisorReviewOccurrence, ...]:
    "Select eligible review occurrences from a joined Silver result."
    for run in committed_runs.values():
        if (
            run.get("source_id") == TRIPADVISOR_SOURCE_ID
            and run.get("status") == "committed"
        ):
            validate_tripadvisor_release(run, snapshot_exists=snapshot_exists)
    selected: dict[tuple[str, str, int], TripAdvisorReviewOccurrence] = {}
    for row in rows:
        if row.get("source_id") != TRIPADVISOR_SOURCE_ID:
            continue
        batch_id = row.get("batch_id")
        if not isinstance(batch_id, str) or batch_id not in committed_runs:
            continue
        if committed_runs[batch_id].get("source_id") != TRIPADVISOR_SOURCE_ID:
            continue
        text = _review_text(row)
        if text is None:
            continue
        source_sk = row.get("source_record_sk")
        ordinal = row.get("review_ordinal")
        source_index = row.get("source_row_index")
        if not isinstance(source_sk, str) or not isinstance(ordinal, int):
            raise ValueError("TripAdvisor review lineage is incomplete")
        if not isinstance(source_index, int):
            raise ValueError("TripAdvisor source occurrence lineage is incomplete")
        occurrence = TripAdvisorReviewOccurrence(
            review_id=str(
                row.get("review_id") or review_occurrence_id(source_sk, ordinal)
            ),
            review_ordinal=ordinal,
            review_text=text,
            source_record_sk=source_sk,
            source_id=TRIPADVISOR_SOURCE_ID,
            batch_id=batch_id,
            source_row_index=source_index,
            source_record_id=row.get("source_record_id"),
            place_id=row.get("place_id"),
            source_uri=row.get("source_uri"),
            source_release_id=str(
                committed_runs[batch_id].get("run_id") or batch_id
            ),
        )
        # A repeated join row has the same occurrence identity.  A source may
        # legitimately reuse review_id across source records/releases, so it
        # must not be collapsed by review_id alone.
        occurrence_key = (
            occurrence.review_id,
            occurrence.source_record_sk,
            occurrence.review_ordinal,
        )
        prior = selected.get(occurrence_key)
        if prior is None or _stable_occurrence(occurrence) < _stable_occurrence(prior):
            selected[occurrence_key] = occurrence
    return tuple(
        sorted(
            selected.values(),
            key=lambda item: (
                item.source_id,
                item.batch_id,
                item.source_row_index,
                item.review_ordinal,
            ),
        )
    )


def _stable_occurrence(value: TripAdvisorReviewOccurrence) -> str:
    return json.dumps(value.__dict__, ensure_ascii=False, sort_keys=True)


__all__ = [
    "TRIPADVISOR_REQUIRED_RELEASE_TABLES",
    "TRIPADVISOR_REVIEW_SQL",
    "TripAdvisorReviewOccurrence",
    "review_occurrence_id",
    "select_tripadvisor_reviews",
    "validate_tripadvisor_release",
]
