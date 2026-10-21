"""Durable corpus-publication contracts for TASK-005B.

This module plans and validates a corpus manifest independently of per-index
generation publication. It never writes Qdrant. The
``SparkCorpusPublicationLedger`` writes only the additive publication tables;
the in-memory ledger is a deterministic reference implementation for tests and
dry-runs.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping, Sequence

TRIPADVISOR_SOURCE_ID = "tripadvisor"
CORPUS_CONTRACT_VERSION = "t05b-corpus-v1"
REQUIRED_SNAPSHOT_TABLES = (
    "source_record",
    "place",
    "place_rating",
    "review",
    "source_entity_mapping",
    "normalization_run",
)
PUBLICATION_STATUSES = frozenset({"planned", "publishing", "committed", "failed"})


class CorpusPublicationError(RuntimeError):
    """Raised when a corpus cannot safely be planned or published."""


class PublicationConflict(CorpusPublicationError):
    """Raised when optimistic publication state has changed."""


def _text(value: Any) -> str:
    return "" if value is None else str(value)


def _canonical(value: Any) -> str:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str
    )


def _digest(value: Any) -> str:
    return "sha256:" + hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def _timestamp(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    if value in (None, ""):
        return ""
    return str(value)


def _snapshot_complete(row: Mapping[str, Any]) -> bool:
    refs = row.get("table_snapshot_refs")
    return isinstance(refs, Mapping) and all(
        _text(refs.get(table)).strip() for table in REQUIRED_SNAPSHOT_TABLES
    )


def _model_key(row: Mapping[str, Any]) -> tuple[str, str, str, str]:
    return (
        _text(row.get("embedding_model_id")),
        _text(row.get("embedding_model_revision")),
        _text(row.get("embedding_model_fingerprint")),
        _text(row.get("embedding_config_fingerprint")),
    )


def _collection_key(row: Mapping[str, Any]) -> tuple[str, str]:
    return (
        _text(row.get("review_collection")),
        _text(row.get("aspect_collection")),
    )


def occurrence_key(row: Mapping[str, Any]) -> tuple[str, str, int, str, int]:
    """Return the source occurrence identity, never just logical review_id."""
    required = (
        "source_id",
        "source_record_sk",
        "source_row_index",
        "review_id",
        "review_ordinal",
    )
    missing = [key for key in required if row.get(key) in (None, "")]
    if missing:
        raise CorpusPublicationError(
            "occurrence identity missing: " + ", ".join(missing)
        )
    try:
        return (
            _text(row["source_id"]),
            _text(row["source_record_sk"]),
            int(row["source_row_index"]),
            _text(row["review_id"]),
            int(row["review_ordinal"]),
        )
    except (TypeError, ValueError) as error:
        raise CorpusPublicationError("occurrence identity has invalid types") from error


def generation_eligibility_reasons(
    generation: Mapping[str, Any],
    normalization_runs: Iterable[Mapping[str, Any]],
    enrichment_runs: Iterable[Mapping[str, Any]],
    *,
    qdrant_point_counts: Mapping[str, Mapping[str, int]] | None = None,
    result_counts: Mapping[str, Mapping[str, int]] | None = None,
    permitted_provenance: Sequence[str] = ("synthetic",),
    expected_model: tuple[str, str, str, str] | None = None,
    expected_collections: tuple[str, str] | None = None,
) -> tuple[str, ...]:
    """Return deterministic fail-closed reasons for one index generation."""
    reasons: list[str] = []
    generation_id = _text(generation.get("index_generation"))
    if generation.get("source_id") != TRIPADVISOR_SOURCE_ID:
        reasons.append("source_not_tripadvisor")
    if generation.get("status") != "committed":
        reasons.append("index_generation_not_committed")
    required = (
        "index_generation",
        "source_release_id",
        "enrichment_run_id",
        "embedding_model_id",
        "embedding_model_revision",
        "embedding_model_fingerprint",
        "embedding_config_fingerprint",
        "review_collection",
        "aspect_collection",
        "started_at",
    )
    reasons.extend(
        "missing_generation_field:" + key
        for key in required
        if generation.get(key) in (None, "")
    )
    if generation.get("expected_review_point_count") != generation.get(
        "actual_review_point_count"
    ):
        reasons.append("index_review_point_count_mismatch")
    if generation.get("expected_aspect_point_count") != generation.get(
        "actual_aspect_point_count"
    ):
        reasons.append("index_aspect_point_count_mismatch")
    if generation.get("lineage_consistent") is not True:
        reasons.append("index_lineage_inconsistent")
    if _text(generation.get("provenance_class")) not in permitted_provenance:
        reasons.append("provenance_not_permitted")
    if expected_model is not None and _model_key(generation) != expected_model:
        reasons.append("incompatible_embedding_model_or_config")
    if (
        expected_collections is not None
        and _collection_key(generation) != expected_collections
    ):
        reasons.append("incompatible_qdrant_collections")

    releases = [
        row
        for row in normalization_runs
        if _text(row.get("run_id")) == _text(generation.get("source_release_id"))
    ]
    if len(releases) != 1:
        reasons.append("normalization_release_missing_or_ambiguous")
    elif (
        releases[0].get("source_id") != TRIPADVISOR_SOURCE_ID
        or releases[0].get("status") != "committed"
    ):
        reasons.append("normalization_release_not_committed")
    elif not _snapshot_complete(releases[0]):
        reasons.append("normalization_snapshot_incomplete")

    enrichments = [
        row
        for row in enrichment_runs
        if _text(row.get("processing_run_id"))
        == _text(generation.get("enrichment_run_id"))
    ]
    if len(enrichments) != 1:
        reasons.append("enrichment_run_missing_or_ambiguous")
    else:
        run = enrichments[0]
        if run.get("source_id") != TRIPADVISOR_SOURCE_ID or _text(
            run.get("source_release_id")
        ) != _text(generation.get("source_release_id")):
            reasons.append("enrichment_lineage_mismatch")
        if run.get("status") != "committed":
            reasons.append("enrichment_run_not_committed")
        if run.get("expected_review_count") != run.get("actual_review_count"):
            reasons.append("enrichment_review_count_mismatch")
        if run.get("expected_aspect_count") != run.get("actual_aspect_count"):
            reasons.append("enrichment_aspect_count_mismatch")
        observed = (result_counts or {}).get(_text(generation.get("enrichment_run_id")))
        if observed is None:
            reasons.append("enrichment_result_evidence_missing")
        else:
            if observed.get("review") != run.get("actual_review_count"):
                reasons.append("enrichment_review_lineage_count_mismatch")
            if observed.get("aspect") != run.get("actual_aspect_count"):
                reasons.append("enrichment_aspect_lineage_count_mismatch")

    points = (qdrant_point_counts or {}).get(generation_id)
    if points is None:
        reasons.append("qdrant_point_evidence_missing")
    else:
        if points.get("review") != generation.get("actual_review_point_count"):
            reasons.append("qdrant_review_point_count_mismatch")
        if points.get("aspect") != generation.get("actual_aspect_point_count"):
            reasons.append("qdrant_aspect_point_count_mismatch")
    return tuple(dict.fromkeys(reasons))


@dataclass(frozen=True)
class OccurrenceMember:
    source_id: str
    source_record_sk: str
    source_row_index: int
    review_id: str
    review_ordinal: int
    index_generation: str
    source_release_id: str
    enrichment_run_id: str
    review_point_count: int
    aspect_point_count: int

    @property
    def key(self) -> tuple[str, str, int, str, int]:
        return (
            self.source_id,
            self.source_record_sk,
            self.source_row_index,
            self.review_id,
            self.review_ordinal,
        )

    def as_dict(self, corpus_id: str) -> dict[str, Any]:
        return {
            "corpus_id": corpus_id,
            "member_id": _digest([corpus_id, self.index_generation, self.key]),
            "index_generation": self.index_generation,
            "source_id": self.source_id,
            "source_release_id": self.source_release_id,
            "enrichment_run_id": self.enrichment_run_id,
            "source_record_sk": self.source_record_sk,
            "source_row_index": self.source_row_index,
            "review_id": self.review_id,
            "review_ordinal": self.review_ordinal,
            "review_point_count": self.review_point_count,
            "aspect_point_count": self.aspect_point_count,
        }


@dataclass(frozen=True)
class PublicationPlan:
    corpus_id: str
    source_id: str
    base_corpus_id: str | None
    model: tuple[str, str, str, str]
    collections: tuple[str, str]
    generations: tuple[str, ...]
    members: tuple[OccurrenceMember, ...]
    membership_fingerprint: str
    expected_review_point_count: int
    expected_aspect_point_count: int
    excluded_generations: tuple[dict[str, Any], ...] = ()

    @property
    def member_rows(self) -> tuple[dict[str, Any], ...]:
        return tuple(member.as_dict(self.corpus_id) for member in self.members)

    def manifest(
        self, *, publication_token: str, status: str = "planned"
    ) -> dict[str, Any]:
        if status not in PUBLICATION_STATUSES:
            raise CorpusPublicationError(f"invalid publication status: {status}")
        return {
            "corpus_id": self.corpus_id,
            "source_id": self.source_id,
            "contract_version": CORPUS_CONTRACT_VERSION,
            "status": status,
            "base_corpus_id": self.base_corpus_id,
            "publication_token": publication_token,
            "membership_fingerprint": self.membership_fingerprint,
            "member_count": len(self.members),
            "generation_count": len(self.generations),
            "expected_review_point_count": self.expected_review_point_count,
            "expected_aspect_point_count": self.expected_aspect_point_count,
            "actual_review_point_count": 0
            if status != "committed"
            else self.expected_review_point_count,
            "actual_aspect_point_count": 0
            if status != "committed"
            else self.expected_aspect_point_count,
            "embedding_model_id": self.model[0],
            "embedding_model_revision": self.model[1],
            "embedding_model_fingerprint": self.model[2],
            "embedding_config_fingerprint": self.model[3],
            "review_collection": self.collections[0],
            "aspect_collection": self.collections[1],
            "provenance_class": "synthetic",
        }


def _member_fingerprint(rows: Iterable[Mapping[str, Any]]) -> str:
    fields = (
        "member_id",
        "index_generation",
        "source_id",
        "source_release_id",
        "enrichment_run_id",
        "source_record_sk",
        "source_row_index",
        "review_id",
        "review_ordinal",
        "review_point_count",
        "aspect_point_count",
    )
    normalized = [{field: row.get(field) for field in fields} for row in rows]
    return _digest(sorted(normalized, key=_canonical))


def build_corpus_plan(
    generations: Iterable[Mapping[str, Any]],
    normalization_runs: Iterable[Mapping[str, Any]],
    enrichment_runs: Iterable[Mapping[str, Any]],
    occurrence_members: Iterable[Mapping[str, Any]],
    *,
    base_corpus_id: str | None = None,
    qdrant_point_counts: Mapping[str, Mapping[str, int]] | None = None,
    result_counts: Mapping[str, Mapping[str, int]] | None = None,
    permitted_provenance: Sequence[str] = ("synthetic",),
    expected_model: tuple[str, str, str, str] | None = None,
    expected_collections: tuple[str, str] | None = None,
) -> PublicationPlan:
    """Build an immutable corpus union and explicit generation exclusions."""
    generation_rows = sorted(
        (dict(row) for row in generations),
        key=lambda row: (
            _timestamp(row.get("started_at")),
            _text(row.get("index_generation")),
        ),
    )
    if len({_text(row.get("index_generation")) for row in generation_rows}) != len(
        generation_rows
    ):
        raise CorpusPublicationError("duplicate index generation identity")
    normalization = tuple(dict(row) for row in normalization_runs)
    enrichment = tuple(dict(row) for row in enrichment_runs)
    memberships_by_generation: dict[str, list[OccurrenceMember]] = {}
    for raw in occurrence_members:
        row = dict(raw)
        try:
            member = OccurrenceMember(
                source_id=_text(row["source_id"]),
                source_record_sk=_text(row["source_record_sk"]),
                source_row_index=int(row["source_row_index"]),
                review_id=_text(row["review_id"]),
                review_ordinal=int(row["review_ordinal"]),
                index_generation=_text(row["index_generation"]),
                source_release_id=_text(row["source_release_id"]),
                enrichment_run_id=_text(row["enrichment_run_id"]),
                review_point_count=int(row["review_point_count"]),
                aspect_point_count=int(row["aspect_point_count"]),
            )
        except (KeyError, TypeError, ValueError) as error:
            raise CorpusPublicationError("invalid occurrence membership") from error
        if member.review_point_count < 0 or member.aspect_point_count < 0:
            raise CorpusPublicationError("occurrence point counts cannot be negative")
        memberships_by_generation.setdefault(member.index_generation, []).append(member)

    eligible: list[dict[str, Any]] = []
    exclusions: list[dict[str, Any]] = []
    started_by_generation = {
        _text(row.get("index_generation")): _timestamp(row.get("started_at"))
        for row in generation_rows
    }
    for row in generation_rows:
        generation_id = _text(row.get("index_generation"))
        reasons = generation_eligibility_reasons(
            row,
            normalization,
            enrichment,
            qdrant_point_counts=qdrant_point_counts,
            result_counts=result_counts,
            permitted_provenance=permitted_provenance,
            expected_model=expected_model,
            expected_collections=expected_collections,
        )
        members = memberships_by_generation.get(generation_id, [])
        if len({member.key for member in members}) != len(members):
            reasons = (*reasons, "duplicate_occurrence_membership")
        if not members:
            reasons = (*reasons, "occurrence_membership_missing")
        else:
            if any(
                member.source_id != TRIPADVISOR_SOURCE_ID
                or member.source_release_id != _text(row.get("source_release_id"))
                or member.enrichment_run_id != _text(row.get("enrichment_run_id"))
                or member.source_row_index < 0
                or member.review_ordinal < 0
                or not member.source_record_sk
                or not member.review_id
                for member in members
            ):
                reasons = (*reasons, "occurrence_generation_lineage_mismatch")
            review_count = sum(member.review_point_count for member in members)
            aspect_count = sum(member.aspect_point_count for member in members)
            if review_count != row.get("actual_review_point_count"):
                reasons = (*reasons, "occurrence_review_coverage_mismatch")
            if aspect_count != row.get("actual_aspect_point_count"):
                reasons = (*reasons, "occurrence_aspect_coverage_mismatch")
        unique_reasons = tuple(dict.fromkeys(reasons))
        if unique_reasons:
            exclusions.append(
                {"index_generation": generation_id, "reasons": list(unique_reasons)}
            )
        else:
            eligible.append(row)

    if not eligible:
        model = expected_model or ("", "", "", "")
        collections = expected_collections or ("", "")
    else:
        anchor = max(
            eligible,
            key=lambda row: (
                _timestamp(row.get("started_at")),
                _text(row.get("index_generation")),
            ),
        )
        model = expected_model or _model_key(anchor)
        collections = expected_collections or _collection_key(anchor)
        compatible: list[dict[str, Any]] = []
        for row in eligible:
            reasons: list[str] = []
            if _model_key(row) != model:
                reasons.append("incompatible_embedding_model_or_config")
            if _collection_key(row) != collections:
                reasons.append("incompatible_qdrant_collections")
            if reasons:
                exclusions.append(
                    {
                        "index_generation": _text(row.get("index_generation")),
                        "reasons": reasons,
                    }
                )
            else:
                compatible.append(row)
        eligible = compatible

    winners: dict[tuple[str, str, int, str, int], OccurrenceMember] = {}
    for row in eligible:
        generation_id = _text(row.get("index_generation"))
        for member in memberships_by_generation[generation_id]:
            prior = winners.get(member.key)
            if prior is None or (
                started_by_generation[generation_id],
                generation_id,
            ) > (
                started_by_generation[prior.index_generation],
                prior.index_generation,
            ):
                winners[member.key] = member

    selected_generation_ids = tuple(
        sorted({member.index_generation for member in winners.values()})
    )
    for row in eligible:
        generation_id = _text(row.get("index_generation"))
        if generation_id not in selected_generation_ids:
            exclusions.append(
                {
                    "index_generation": generation_id,
                    "reasons": ["all_occurrences_superseded"],
                }
            )
    members = tuple(
        sorted(
            winners.values(), key=lambda member: (member.key, member.index_generation)
        )
    )
    identity = {
        "contract_version": CORPUS_CONTRACT_VERSION,
        "source_id": TRIPADVISOR_SOURCE_ID,
        "base_corpus_id": base_corpus_id,
        "generations": selected_generation_ids,
        "members": [member.as_dict("pending") for member in members],
        "model": model,
        "collections": collections,
    }
    corpus_id = _digest(identity)
    return PublicationPlan(
        corpus_id=corpus_id,
        source_id=TRIPADVISOR_SOURCE_ID,
        base_corpus_id=base_corpus_id,
        model=model,
        collections=collections,
        generations=selected_generation_ids,
        members=members,
        membership_fingerprint=_member_fingerprint(
            [member.as_dict(corpus_id) for member in members]
        ),
        expected_review_point_count=sum(
            member.review_point_count for member in members
        ),
        expected_aspect_point_count=sum(
            member.aspect_point_count for member in members
        ),
        excluded_generations=tuple(sorted(exclusions, key=_canonical)),
    )


def validate_committed_manifest(
    manifest: Mapping[str, Any], membership_rows: Iterable[Mapping[str, Any]]
) -> None:
    """Validate the reader-visible committed pointer and immutable membership."""
    if manifest.get("status") != "committed":
        raise CorpusPublicationError("corpus manifest is not committed")
    if manifest.get("source_id") != TRIPADVISOR_SOURCE_ID:
        raise CorpusPublicationError("corpus manifest is not TripAdvisor")
    rows = tuple(membership_rows)
    if any(
        _text(row.get("corpus_id")) != _text(manifest.get("corpus_id")) for row in rows
    ):
        raise CorpusPublicationError("corpus membership has an unexpected corpus_id")
    if _member_fingerprint(rows) != manifest.get("membership_fingerprint"):
        raise CorpusPublicationError("corpus membership fingerprint mismatch")
    if len(rows) != int(manifest.get("member_count", -1)):
        raise CorpusPublicationError("corpus membership count mismatch")
    if sum(int(row.get("review_point_count", 0)) for row in rows) != int(
        manifest.get("actual_review_point_count", -1)
    ):
        raise CorpusPublicationError("corpus review point count mismatch")
    if sum(int(row.get("aspect_point_count", 0)) for row in rows) != int(
        manifest.get("actual_aspect_point_count", -1)
    ):
        raise CorpusPublicationError("corpus aspect point count mismatch")


def select_active_corpus(
    manifests: Iterable[Mapping[str, Any]],
    memberships: Mapping[str, Iterable[Mapping[str, Any]]],
) -> dict[str, Any] | None:
    """Return the newest valid committed corpus; invalid rows are invisible."""
    valid: list[dict[str, Any]] = []
    seen: set[str] = set()
    for raw in manifests:
        row = dict(raw)
        corpus_id = _text(row.get("corpus_id"))
        if corpus_id in seen:
            raise CorpusPublicationError("duplicate corpus manifest identity")
        seen.add(corpus_id)
        if row.get("status") != "committed":
            continue
        try:
            validate_committed_manifest(row, memberships.get(corpus_id, ()))
        except (TypeError, ValueError, CorpusPublicationError):
            continue
        valid.append(row)
    if not valid:
        return None
    return max(
        valid,
        key=lambda row: (
            _timestamp(row.get("finished_at")),
            _text(row.get("corpus_id")),
        ),
    )


def publication_token(corpus_id: str, base_corpus_id: str | None) -> str:
    """Derive an idempotent caller token for one planned publication."""
    return _digest({"corpus_id": corpus_id, "base_corpus_id": base_corpus_id})


def transition_status(current: str, target: str) -> None:
    allowed = {
        "planned": {"publishing", "failed"},
        "publishing": {"publishing", "committed", "failed"},
        "failed": {"publishing"},
        "committed": {"committed"},
    }
    if current not in PUBLICATION_STATUSES or target not in allowed.get(current, set()):
        raise CorpusPublicationError(
            f"invalid corpus publication transition: {current} -> {target}"
        )


@dataclass
class InMemoryCorpusPublicationLedger:
    """Atomic reference ledger used by tests and orchestration dry-runs."""

    manifests: dict[str, dict[str, Any]] = field(default_factory=dict)
    memberships: dict[str, tuple[dict[str, Any], ...]] = field(default_factory=dict)

    def active(self) -> dict[str, Any] | None:
        return select_active_corpus(self.manifests.values(), self.memberships)

    def publish(
        self, plan: PublicationPlan, *, token: str | None = None
    ) -> dict[str, Any]:
        token = token or publication_token(plan.corpus_id, plan.base_corpus_id)
        existing = self.manifests.get(plan.corpus_id)
        if existing and existing.get("status") == "committed":
            validate_committed_manifest(
                existing, self.memberships.get(plan.corpus_id, ())
            )
            return existing
        active = self.active()
        active_id = active.get("corpus_id") if active else None
        if active_id != plan.base_corpus_id:
            raise PublicationConflict(
                "active corpus changed; replan from the new active corpus"
            )
        if existing and existing.get("publication_token") != token:
            raise PublicationConflict("corpus publication token conflicts")
        manifest = plan.manifest(publication_token=token, status="publishing")
        transition_status(
            existing.get("status", "planned") if existing else "planned", "publishing"
        )
        rows = tuple(plan.member_rows)
        self.manifests[plan.corpus_id] = manifest
        self.memberships[plan.corpus_id] = rows
        readback = tuple(self.memberships[plan.corpus_id])
        validate_committed_manifest(
            {
                **manifest,
                "status": "committed",
                "actual_review_point_count": plan.expected_review_point_count,
                "actual_aspect_point_count": plan.expected_aspect_point_count,
            },
            readback,
        )
        committed = plan.manifest(publication_token=token, status="committed")
        committed["finished_at"] = "committed"
        self.manifests[plan.corpus_id] = committed
        return committed

    def fail(self, corpus_id: str, error: str) -> None:
        manifest = self.manifests.get(corpus_id)
        if manifest is None:
            raise CorpusPublicationError("unknown corpus publication")
        transition_status(str(manifest.get("status")), "failed")
        self.manifests[corpus_id] = {
            **manifest,
            "status": "failed",
            "error_summary": error[:2000],
        }


class SparkCorpusPublicationLedger:
    """Iceberg-backed adapter for the two-phase publication protocol.

    Membership rows are inserted before the corpus manifest is changed to
    ``committed``. Readers only inspect committed manifests and validate their
    immutable membership fingerprint, so an interrupted write remains hidden.
    """

    def __init__(self, spark: Any):
        self.spark = spark
        self.run_table = "travel.silver.review_corpus_run"
        self.membership_table = "travel.silver.review_corpus_membership"

    def create_tables(self) -> None:
        self.spark.sql(REVIEW_CORPUS_RUN_TABLE)
        self.spark.sql(REVIEW_CORPUS_MEMBERSHIP_TABLE)

    def manifests(self) -> list[dict[str, Any]]:
        return [
            row.asDict(recursive=True)
            for row in self.spark.table(self.run_table).collect()
        ]

    def memberships(self, corpus_id: str) -> list[dict[str, Any]]:
        escaped = corpus_id.replace("'", "''")
        return [
            row.asDict(recursive=True)
            for row in self.spark.sql(
                f"SELECT * FROM {self.membership_table} WHERE corpus_id='{escaped}'"
            ).collect()
        ]

    def active(self) -> dict[str, Any] | None:
        rows = self.manifests()
        grouped = {
            str(row.get("corpus_id")): self.memberships(str(row.get("corpus_id")))
            for row in rows
        }
        return select_active_corpus(rows, grouped)

    @staticmethod
    def _run_schema() -> Any:
        from pyspark.sql.types import (
            LongType,
            StringType,
            StructField,
            StructType,
            TimestampType,
        )

        return StructType(
            [
                StructField("corpus_id", StringType(), False),
                StructField("source_id", StringType(), False),
                StructField("contract_version", StringType(), False),
                StructField("status", StringType(), False),
                StructField("base_corpus_id", StringType(), True),
                StructField("publication_token", StringType(), False),
                StructField("membership_fingerprint", StringType(), False),
                StructField("member_count", LongType(), False),
                StructField("generation_count", LongType(), False),
                StructField("expected_review_point_count", LongType(), False),
                StructField("expected_aspect_point_count", LongType(), False),
                StructField("actual_review_point_count", LongType(), False),
                StructField("actual_aspect_point_count", LongType(), False),
                StructField("embedding_model_id", StringType(), False),
                StructField("embedding_model_revision", StringType(), False),
                StructField("embedding_model_fingerprint", StringType(), False),
                StructField("embedding_config_fingerprint", StringType(), False),
                StructField("review_collection", StringType(), False),
                StructField("aspect_collection", StringType(), False),
                StructField("provenance_class", StringType(), False),
                StructField("started_at", TimestampType(), False),
                StructField("finished_at", TimestampType(), True),
                StructField("error_summary", StringType(), True),
            ]
        )

    @staticmethod
    def _membership_schema() -> Any:
        from pyspark.sql.types import LongType, StringType, StructField, StructType

        return StructType(
            [
                StructField("corpus_id", StringType(), False),
                StructField("member_id", StringType(), False),
                StructField("index_generation", StringType(), False),
                StructField("source_id", StringType(), False),
                StructField("source_release_id", StringType(), False),
                StructField("enrichment_run_id", StringType(), False),
                StructField("source_record_sk", StringType(), False),
                StructField("source_row_index", LongType(), False),
                StructField("review_id", StringType(), False),
                StructField("review_ordinal", LongType(), False),
                StructField("review_point_count", LongType(), False),
                StructField("aspect_point_count", LongType(), False),
            ]
        )

    def _write_manifest(self, row: Mapping[str, Any]) -> None:
        existing = next(
            (
                item
                for item in self.manifests()
                if item.get("corpus_id") == row.get("corpus_id")
            ),
            None,
        )
        if existing:
            if existing.get("publication_token") != row.get("publication_token"):
                raise PublicationConflict("corpus publication token conflicts")
            transition_status(str(existing.get("status")), str(row.get("status")))
        values = dict(row)
        values.setdefault("started_at", datetime.now(timezone.utc).replace(tzinfo=None))
        values.setdefault("finished_at", None)
        values.setdefault("error_summary", None)
        frame = self.spark.createDataFrame([values], schema=self._run_schema())
        frame.createOrReplaceTempView("t05b_corpus_run")
        columns = [field.name for field in self._run_schema().fields]
        assignments = ", ".join(
            f"t.{column}=s.{column}" for column in columns if column != "corpus_id"
        )
        self.spark.sql(
            f"MERGE INTO {self.run_table} t USING t05b_corpus_run s "
            f"ON t.corpus_id=s.corpus_id WHEN MATCHED THEN UPDATE SET {assignments} "
            f"WHEN NOT MATCHED THEN INSERT ({', '.join(columns)}) "
            f"VALUES ({', '.join('s.' + column for column in columns)})"
        )

    def _write_memberships(self, rows: Sequence[Mapping[str, Any]]) -> None:
        if not rows:
            return
        corpus_id = _text(rows[0].get("corpus_id"))
        existing = {row["member_id"]: row for row in self.memberships(corpus_id)}
        for row in rows:
            prior = existing.get(row["member_id"])
            if prior and _canonical(prior) != _canonical(row):
                raise CorpusPublicationError("immutable corpus membership was changed")
        frame = self.spark.createDataFrame(
            [dict(row) for row in rows], schema=self._membership_schema()
        )
        frame.createOrReplaceTempView("t05b_corpus_membership")
        columns = [field.name for field in self._membership_schema().fields]
        self.spark.sql(
            f"MERGE INTO {self.membership_table} t USING t05b_corpus_membership s "
            "ON t.corpus_id=s.corpus_id AND t.member_id=s.member_id "
            f"WHEN NOT MATCHED THEN INSERT ({', '.join(columns)}) "
            f"VALUES ({', '.join('s.' + column for column in columns)})"
        )

    def publish(
        self, plan: PublicationPlan, *, token: str | None = None
    ) -> dict[str, Any]:
        """Publish after membership write/readback; retry is idempotent by token."""
        self.create_tables()
        token = token or publication_token(plan.corpus_id, plan.base_corpus_id)
        existing = next(
            (row for row in self.manifests() if row.get("corpus_id") == plan.corpus_id),
            None,
        )
        if existing and existing.get("status") == "committed":
            validate_committed_manifest(existing, self.memberships(plan.corpus_id))
            return existing
        active = self.active()
        if (active.get("corpus_id") if active else None) != plan.base_corpus_id:
            raise PublicationConflict(
                "active corpus changed; replan from the new active corpus"
            )
        publishing = plan.manifest(publication_token=token, status="publishing")
        self._write_manifest(publishing)
        self._write_memberships(plan.member_rows)
        rows = self.memberships(plan.corpus_id)
        committed = plan.manifest(publication_token=token, status="committed")
        committed["started_at"] = next(
            row["started_at"]
            for row in self.manifests()
            if row.get("corpus_id") == plan.corpus_id
        )
        committed["finished_at"] = datetime.now(timezone.utc).replace(tzinfo=None)
        validate_committed_manifest(committed, rows)
        if (self.active() or {}).get("corpus_id") not in (None, plan.base_corpus_id):
            raise PublicationConflict("active corpus changed during publication")
        self._write_manifest(committed)
        return committed

    def fail(self, corpus_id: str, error: str) -> None:
        row = next(
            (item for item in self.manifests() if item.get("corpus_id") == corpus_id),
            None,
        )
        if row is None:
            raise CorpusPublicationError("unknown corpus publication")
        self._write_manifest({**row, "status": "failed", "error_summary": error[:2000]})


REVIEW_CORPUS_RUN_TABLE = """
CREATE TABLE IF NOT EXISTS travel.silver.review_corpus_run (
  corpus_id STRING NOT NULL,
  source_id STRING NOT NULL,
  contract_version STRING NOT NULL,
  status STRING NOT NULL,
  base_corpus_id STRING,
  publication_token STRING NOT NULL,
  membership_fingerprint STRING NOT NULL,
  member_count BIGINT NOT NULL,
  generation_count BIGINT NOT NULL,
  expected_review_point_count BIGINT NOT NULL,
  expected_aspect_point_count BIGINT NOT NULL,
  actual_review_point_count BIGINT NOT NULL,
  actual_aspect_point_count BIGINT NOT NULL,
  embedding_model_id STRING NOT NULL,
  embedding_model_revision STRING NOT NULL,
  embedding_model_fingerprint STRING NOT NULL,
  embedding_config_fingerprint STRING NOT NULL,
  review_collection STRING NOT NULL,
  aspect_collection STRING NOT NULL,
  provenance_class STRING NOT NULL,
  started_at TIMESTAMP NOT NULL,
  finished_at TIMESTAMP,
  error_summary STRING
) USING iceberg
TBLPROPERTIES ('format-version'='2', 'write.parquet.compression-codec'='zstd')
"""

REVIEW_CORPUS_MEMBERSHIP_TABLE = """
CREATE TABLE IF NOT EXISTS travel.silver.review_corpus_membership (
  corpus_id STRING NOT NULL,
  member_id STRING NOT NULL,
  index_generation STRING NOT NULL,
  source_id STRING NOT NULL,
  source_release_id STRING NOT NULL,
  enrichment_run_id STRING NOT NULL,
  source_record_sk STRING NOT NULL,
  source_row_index BIGINT NOT NULL,
  review_id STRING NOT NULL,
  review_ordinal BIGINT NOT NULL,
  review_point_count BIGINT NOT NULL,
  aspect_point_count BIGINT NOT NULL
) USING iceberg
TBLPROPERTIES ('format-version'='2', 'write.parquet.compression-codec'='zstd')
"""


__all__ = [
    "CORPUS_CONTRACT_VERSION",
    "CorpusPublicationError",
    "InMemoryCorpusPublicationLedger",
    "OccurrenceMember",
    "PublicationConflict",
    "PublicationPlan",
    "REQUIRED_SNAPSHOT_TABLES",
    "REVIEW_CORPUS_MEMBERSHIP_TABLE",
    "REVIEW_CORPUS_RUN_TABLE",
    "SparkCorpusPublicationLedger",
    "build_corpus_plan",
    "generation_eligibility_reasons",
    "occurrence_key",
    "publication_token",
    "select_active_corpus",
    "transition_status",
    "validate_committed_manifest",
]
