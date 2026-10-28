# ruff: noqa: E501
"""Minimal, non-duplicating T07-E2C market and source-coverage facts."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Mapping
from decimal import Decimal
from typing import Any

from travel_intelligence.gold.dimensions import deterministic_key
from travel_intelligence.gold.domain_mapping import (
    effective_approved_mappings,
    normalize_source_category,
)


class MarketFactError(ValueError):
    """Raised when candidate facts cannot be safely aggregated."""


METRICS = (
    "entity_count",
    "entity_count_with_sentiment_evidence",
    "entity_count_with_aspect_evidence",
    "review_evidence_count",
    "positive_review_count",
    "neutral_review_count",
    "negative_review_count",
    "aspect_mention_count",
)


def derive_market_and_coverage_facts(
    entities: Iterable[Mapping[str, Any]],
    entity_sources: Iterable[Mapping[str, Any]],
    entity_geographies: Iterable[Mapping[str, Any]],
    sentiment_facts: Iterable[Mapping[str, Any]],
    aspect_facts: Iterable[Mapping[str, Any]],
    mapping_contract: Mapping[str, Any],
    *,
    context: Mapping[str, Any],
) -> dict[str, Any]:
    """Aggregate entity-level facts independently before combining at market grain."""
    release_id = str(context["release_id"])
    source_id = str(context["source_id"])
    source_sk = str(context["source_sk"])
    reporting_period = str(context["reporting_period"])
    lineage = {
        key: context[key]
        for key in (
            "release_id",
            "gold_run_id",
            "contract_version",
            "config_fingerprint",
            "source_release_id",
            "built_at",
        )
    }
    entity_rows = [dict(row) for row in entities]
    entity_by_id = {str(row["entity_id"]): row for row in entity_rows}
    if len(entity_by_id) != len(entity_rows):
        raise MarketFactError("entity grain is not unique")
    source_rows = [dict(row) for row in entity_sources]
    geography_rows = [dict(row) for row in entity_geographies]
    sources = {str(row["entity_id"]): row for row in source_rows}
    geographies = {str(row["entity_id"]): row for row in geography_rows}
    if set(sources) != set(entity_by_id) or any(
        row.get("source_id") != source_id for row in sources.values()
    ) or len(sources) != len(source_rows):
        raise MarketFactError("source bridge is incomplete or unexpected")
    if set(geographies) != set(entity_by_id) or len(geographies) != len(geography_rows):
        raise MarketFactError("geography bridge is incomplete or non-unique")

    sentiment = _sentiment_by_entity(sentiment_facts, entity_by_id)
    aspects = _aspects_by_entity(aspect_facts, entity_by_id)
    approved = effective_approved_mappings(mapping_contract)
    reviewed_excluded = {
        str(rule["normalized_category"])
        for rule in mapping_contract["rules"]
        if rule.get("approval_status") == "approved"
        and rule.get("mapping_state") == "excluded"
    }
    coverage: dict[tuple[str | None, str | None], dict[str, Any]] = {}
    market: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    for entity_id, entity in entity_by_id.items():
        geo = geographies[entity_id]
        geo_id = geo.get("geo_id")
        geography_sk = geo.get("geography_sk")
        coverage_key = (str(geo_id) if geo_id else None, geography_sk)
        bucket = coverage.setdefault(
            coverage_key,
            _coverage_bucket(geo_id, geography_sk),
        )
        bucket["observed_entity_count"] += 1
        raw = entity.get("entity_type_raw")
        category = None if raw is None or not str(raw).strip() else normalize_source_category(raw)
        domain_id = approved.get((source_id, category)) if category else None
        if domain_id:
            bucket["mapped_entity_count"] += 1
        elif category in reviewed_excluded:
            bucket["excluded_entity_count"] += 1
        else:
            bucket["unreviewed_or_unmapped_entity_count"] += 1
        if entity_id in sentiment:
            bucket["entity_count_with_sentiment_evidence"] += 1
            bucket["eligible_review_count"] += sentiment[entity_id]["reviews"]
        if entity_id in aspects:
            bucket["entity_count_with_aspect_evidence"] += 1
            bucket["aspect_mention_count"] += aspects[entity_id]["mentions"]
        if not domain_id or not geo_id or not geography_sk:
            continue
        key = (str(geo_id), str(geography_sk), domain_id, entity_id)
        market[key] = {
            "geo_id": str(geo_id),
            "geography_sk": str(geography_sk),
            "domain_id": domain_id,
            "sentiment": sentiment.get(entity_id, _empty_sentiment()),
            "aspects": aspects.get(entity_id, {"mentions": 0}),
        }

    coverage_rows = []
    for bucket in coverage.values():
        observed = bucket["observed_entity_count"]
        bucket["mapped_entity_ratio"] = (
            (Decimal(bucket["mapped_entity_count"]) / Decimal(observed)).quantize(
                Decimal("0.0001")
            )
            if observed
            else None
        )
        coverage_rows.append(
            {
                "data_coverage_fact_sk": deterministic_key(
                    "data-coverage",
                    release_id,
                    source_id,
                    bucket["geo_id"] or "unknown",
                    reporting_period,
                ),
                "source_sk": source_sk,
                "source_id": source_id,
                "reporting_period": reporting_period,
                "time_basis": "source_release_processing_period",
                "collected_at": None,
                "processed_at": context.get("processed_at"),
                "freshness_at": None,
                "coverage_tier": None,
                "data_status": "insufficient_data",
                "status_reason": "numeric_sufficiency_and_freshness_thresholds_not_approved",
                "threshold_version": "gold-sufficiency-v1",
                "domain_sk": None,
                "domain_id": None,
                **bucket,
                **lineage,
            }
        )
    market_rows = _market_rows(
        market.values(), context=context, source_id=source_id, source_sk=source_sk,
        reporting_period=reporting_period, lineage=lineage,
    )
    return {
        "fact_data_coverage": sorted(coverage_rows, key=lambda row: str(row["geo_id"])),
        "fact_market_summary": market_rows,
    }


def _sentiment_by_entity(rows, entities):
    result = defaultdict(_empty_sentiment)
    for row in rows:
        entity_id = str(row["entity_id"])
        if entity_id not in entities:
            raise MarketFactError("sentiment fact has unknown entity")
        label = str(row["sentiment_label"])
        if label not in {"positive", "neutral", "negative"}:
            raise MarketFactError("invalid sentiment label")
        result[entity_id][label] += int(row["sentiment_count"])
        result[entity_id]["reviews"] = max(
            result[entity_id]["reviews"], int(row["eligible_review_count"])
        )
    for value in result.values():
        if value["positive"] + value["neutral"] + value["negative"] != value["reviews"]:
            raise MarketFactError("sentiment labels do not reconcile to entity denominator")
    return result


def _aspects_by_entity(rows, entities):
    result = defaultdict(lambda: {"mentions": 0})
    for row in rows:
        entity_id = str(row["entity_id"])
        if entity_id not in entities:
            raise MarketFactError("aspect fact has unknown entity")
        result[entity_id]["mentions"] += int(row["mention_count"])
    return result


def _empty_sentiment():
    return {"reviews": 0, "positive": 0, "neutral": 0, "negative": 0}


def _coverage_bucket(geo_id, geography_sk):
    return {
        "geography_sk": geography_sk,
        "geo_id": geo_id,
        "observed_entity_count": 0,
        "mapped_entity_count": 0,
        "excluded_entity_count": 0,
        "unreviewed_or_unmapped_entity_count": 0,
        "entity_count_with_sentiment_evidence": 0,
        "entity_count_with_aspect_evidence": 0,
        "eligible_review_count": 0,
        "aspect_mention_count": 0,
    }


def _market_rows(values, *, context, source_id, source_sk, reporting_period, lineage):
    groups = defaultdict(lambda: defaultdict(int))
    for item in values:
        key = (item["geo_id"], item["domain_id"])
        group = groups[key]
        group["entity_count"] += 1
        sentiment = item["sentiment"]
        aspects = item["aspects"]
        group["entity_count_with_sentiment_evidence"] += int(sentiment["reviews"] > 0)
        group["entity_count_with_aspect_evidence"] += int(aspects["mentions"] > 0)
        group["review_evidence_count"] += sentiment["reviews"]
        group["positive_review_count"] += sentiment["positive"]
        group["neutral_review_count"] += sentiment["neutral"]
        group["negative_review_count"] += sentiment["negative"]
        group["aspect_mention_count"] += aspects["mentions"]
    rows = []
    domain_sks = context["domain_sks"]
    geography_sks = context["geography_sks"]
    for (geo_id, domain_id), metrics in sorted(groups.items()):
        for metric_id in METRICS:
            numerator = metrics[metric_id]
            denominator = metrics["review_evidence_count"] if metric_id.endswith("review_count") else None
            rows.append({
                "market_summary_fact_sk": deterministic_key("market-summary", context["release_id"], source_id, geo_id, domain_id, reporting_period, metric_id),
                "geography_sk": geography_sks[geo_id], "geo_id": geo_id,
                "domain_sk": domain_sks[domain_id], "domain_id": domain_id,
                "source_sk": source_sk, "source_id": source_id,
                "reporting_period": reporting_period, "time_basis": "source_release_processing_period",
                "metric_id": metric_id, "numerator": numerator, "denominator": denominator,
                "metric_value": Decimal(numerator), "sample_size": metrics["entity_count"],
                "data_status": "insufficient_data", "evidence_set_id": None,
                **lineage,
            })
    return rows


__all__ = ["METRICS", "MarketFactError", "derive_market_and_coverage_facts"]
