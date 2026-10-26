"""Deterministic TripAdvisor and Foody Bronze v2 normalization primitives."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from .foundation import NORMALIZATION_VERSION, validate_bronze_manifest
from .province import normalize_province


def _id(*parts: object) -> str:
    encoded = b"".join(
        f"{len(str(part).encode('utf-8'))}:".encode("ascii")
        + str(part).encode("utf-8")
        for part in parts
    )
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def source_record_id(source_id: str, batch_id: str, source_row_index: int) -> str:
    return _id("silver-source-record-v2", source_id, batch_id, source_row_index)


def _timestamp(raw: object) -> datetime:
    """Return an aware UTC timestamp from Bronze's typed or textual value.

    Bronze persists ``ingested_at`` as a UTC-aware timestamp, but Spark may
    materialize it as ``datetime`` while adapters and JSON paths provide an
    ISO-8601 string. A naive value is rejected because the contract does not
    define a timezone that could be safely assigned to it.
    """
    if isinstance(raw, datetime):
        value = raw
    elif isinstance(raw, str):
        try:
            value = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError as error:
            raise ValueError(f"Invalid ISO-8601 timestamp: {raw!r}") from error
    else:
        raise TypeError(
            "Timestamp must be an aware datetime or ISO-8601 string; "
            f"received {type(raw).__name__}"
        )
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Timezone-naive timestamps are not valid Bronze values")
    return value.astimezone(timezone.utc)


def bronze_parquet_timestamp(raw: object) -> datetime:
    """Interpret Spark's naive readback of Bronze's UTC timestamp column.

    Bronze Parquet declares ``ingested_at`` as ``timestamp[us, tz=UTC]``.
    Spark can materialize that trusted UTC column as a naive Python datetime;
    only this persisted-Bronze boundary may attach UTC to such a value.
    """
    if isinstance(raw, datetime) and raw.tzinfo is None:
        return raw.replace(tzinfo=timezone.utc)
    return _timestamp(raw)


def nested_input_count(
    source_id: str, records: dict[str, list[dict[str, Any]]]
) -> int:
    """Count approved nested input occurrences for one source batch."""
    nested_target = {
        "tripadvisor": "review",
        "tiktok": "social_comment",
        "foody": None,
        "vexere": None,
    }
    if source_id not in nested_target:
        raise ValueError(f"Unsupported source for nested input count: {source_id}")
    table = nested_target[source_id]
    return len(records.get(table, [])) if table is not None else 0


def reconcile_manifest_nested_count(
    manifest: dict[str, Any], computed_count: int
) -> None:
    """Reject a Bronze/Silver nested occurrence mismatch when evidence exists."""
    if "nested_review_count" not in manifest:
        return
    manifest_count = manifest["nested_review_count"]
    if (
        isinstance(manifest_count, bool)
        or not isinstance(manifest_count, int)
        or manifest_count < 0
    ):
        raise ValueError(
            "Bronze manifest nested_review_count must be a non-negative int"
        )
    if manifest_count != computed_count:
        raise ValueError(
            "Bronze/Silver nested input count mismatch: "
            f"manifest={manifest_count}, normalized={computed_count}"
        )


def _place_id(source_id: str, source_uri: str | None) -> str | None:
    return _id("silver-place-v2", source_id, source_uri) if source_uri else None


def _base_source_record(row: Any, bronze_object_path: str) -> dict[str, Any]:
    ingested_at = _timestamp(row.ingested_at)
    return {
        "source_record_sk": source_record_id(
            row.source_id, row.batch_id, row.source_row_index
        ),
        "source_id": row.source_id,
        "source_row_index": row.source_row_index,
        "record_type": row.record_type,
        "source_record_id": row.source_record_id,
        "source_uri": row.source_uri,
        "batch_id": row.batch_id,
        "payload_hash": row.payload_hash,
        "bronze_object_path": bronze_object_path,
        "duplicate_status": "not_duplicate",
        "duplicate_of_source_record_sk": None,
        "source_collected_at_raw": row.source_collected_at_raw,
        "ingested_at": ingested_at,
        "normalization_version": NORMALIZATION_VERSION,
        "quality_codes": [],
        "processed_at": ingested_at,
        "ingest_date": ingested_at.date(),
    }


def _place_row(
    payload: dict[str, Any], row: Any
) -> tuple[str | None, dict[str, Any] | None]:
    place_id = _place_id(row.source_id, row.source_uri)
    if place_id is None:
        return None, None
    name = payload.get("entity_name", payload.get("name"))
    if not isinstance(name, str) or not name:
        return None, None
    now = _timestamp(row.ingested_at)
    province = normalize_province(payload.get("province"))
    return place_id, {
        "place_id": place_id,
        "place_name": name,
        "place_type": payload.get("entity_type"),
        "province": payload.get("province"),
        **province,
        "district": payload.get("district"),
        "address": payload.get("address"),
        "latitude": payload.get("lat"),
        "longitude": payload.get("lng"),
        "identity_status": "resolved",
        "identity_method": "validated_source_url",
        "created_at": now,
        "updated_at": now,
    }


def _mapping(row: Any, place_id: str | None) -> dict[str, Any]:
    now = _timestamp(row.ingested_at)
    source_sk = source_record_id(row.source_id, row.batch_id, row.source_row_index)
    return {
        "mapping_id": _id("silver-mapping-v2", source_sk),
        "source_record_sk": source_sk,
        "place_id": place_id,
        "mapping_method": "validated_source_url" if place_id else "unresolved",
        "confidence": None,
        "decision_status": "resolved" if place_id else "unresolved",
        "evidence_ref": row.source_uri,
        "mapping_version": "place-mapping-v1",
        "created_at": now,
        "updated_at": now,
    }


def _rating(row: Any, payload: dict[str, Any], place_id: str | None) -> dict[str, Any]:
    source_sk = source_record_id(row.source_id, row.batch_id, row.source_row_index)
    return {
        "place_rating_id": _id("silver-place-rating-v2", source_sk),
        "source_record_sk": source_sk,
        "place_id": place_id,
        "source_id": row.source_id,
        "observed_at": None,
        "observed_at_raw": payload.get("scraped_at"),
        "rating": payload.get("rating", payload.get("rating_avg")),
        "rating_scale": None,
        "review_count": payload.get("review_count"),
        "rating_quality": payload.get("rating_quality"),
        "rating_price": payload.get("rating_price"),
        "rating_service": payload.get("rating_service"),
        "rating_location": payload.get("rating_location"),
        "rating_space": payload.get("rating_space"),
        "price_min": payload.get("price_min"),
        "price_max": payload.get("price_max"),
        "currency": payload.get("currency"),
        "quality_codes": [],
    }


def normalize_tripadvisor_row(
    row: Any, bronze_object_path: str
) -> dict[str, list[dict[str, Any]]]:
    payload = json.loads(row.payload_json)
    source = _base_source_record(row, bronze_object_path)
    place_id, place = _place_row(payload, row)
    reviews = []
    source_sk = source["source_record_sk"]
    for ordinal, review in enumerate(payload.get("reviews", [])):
        reviews.append(
            {
                "review_id": _id("silver-review-occurrence-v2", source_sk, ordinal),
                "source_record_sk": source_sk,
                "place_id": place_id,
                "review_ordinal": ordinal,
                "source_review_index_raw": (
                    str(review["index"]) if "index" in review else None
                ),
                "key_review": review.get("key_review"),
                "review_text": review.get("detail_review"),
                "detail_review": review.get("detail_review"),
                "rating": review.get("rating"),
                "language_raw": review.get("language"),
                "quality_codes": [],
            }
        )
    return {
        "source_record": [source],
        "place": [place] if place else [],
        "place_rating": [_rating(row, payload, place_id)],
        "review": reviews,
        "source_entity_mapping": [_mapping(row, place_id)],
    }


def normalize_foody_row(
    row: Any, bronze_object_path: str
) -> dict[str, list[dict[str, Any]]]:
    payload = json.loads(row.payload_json)
    source = _base_source_record(row, bronze_object_path)
    place_id, place = _place_row(payload, row)
    return {
        "source_record": [source],
        "place": [place] if place else [],
        "place_rating": [_rating(row, payload, place_id)],
        "review": [],
        "source_entity_mapping": [_mapping(row, place_id)],
    }


def _quality_codes(payload: dict[str, Any]) -> list[str]:
    codes = []
    if payload.get("comment_count") is not None and payload.get("comments") is not None:
        if payload["comment_count"] != len(payload["comments"]):
            codes.append("comment_count_mismatch")
    return codes


def normalize_tiktok_row(
    row: Any, bronze_object_path: str
) -> dict[str, list[dict[str, Any]]]:
    payload = json.loads(row.payload_json)
    source = _base_source_record(row, bronze_object_path)
    source_sk = source["source_record_sk"]
    content_id = _id("silver-social-content-v2", source_sk)
    comments = payload.get("comments") or []
    quality = _quality_codes(payload)
    content = {
        "content_id": content_id,
        "source_record_sk": source_sk,
        "platform": "tiktok",
        "source_video_id": payload.get("video_id"),
        "content_type": "video",
        "description": payload.get("description"),
        "keyword_raw": payload.get("keyword"),
        "province_raw": payload.get("province"),
        "hashtags": payload.get("hashtags"),
        "posted_date_raw": payload.get("posted_date"),
        "crawl_time_raw": payload.get("crawl_time"),
        "source_url": payload.get("video_url"),
        "reported_comment_count": payload.get("comment_count"),
        "collected_comment_count": len(comments),
        "quality_codes": quality,
    }
    engagement = {
        "engagement_id": _id("silver-social-engagement-v2", source_sk),
        "content_id": content_id,
        "source_record_sk": source_sk,
        "observed_at_raw": payload.get("crawl_time"),
        "view_count": payload.get("view_count"),
        "like_count": payload.get("like_count"),
        "comment_count_reported": payload.get("comment_count"),
        "share_count": payload.get("share_count"),
        "total_engagement": payload.get("total_engagement"),
        "quality_codes": quality,
    }
    social_comments = []
    for ordinal, comment in enumerate(comments):
        social_comments.append(
            {
                "comment_id": _id("silver-social-comment-v2", source_sk, ordinal),
                "content_id": content_id,
                "source_record_sk": source_sk,
                "comment_ordinal": ordinal,
                "source_comment_id_raw": comment.get("comment_id"),
                "comment_text": comment.get("text"),
                "like_count": comment.get("like_count"),
                "posted_time_raw": comment.get("posted_time"),
                "quality_codes": [],
            }
        )
    return {
        "source_record": [source],
        "social_content": [content],
        "social_engagement": [engagement],
        "social_comment": social_comments,
    }


def normalize_vexere_row(
    row: Any, bronze_object_path: str
) -> dict[str, list[dict[str, Any]]]:
    payload = json.loads(row.payload_json)
    source = _base_source_record(row, bronze_object_path)
    source_sk = source["source_record_sk"]
    required_fields = ("origin", "destination", "operator_name")
    missing = [field for field in required_fields if not payload.get(field)]
    if missing:
        raise ValueError("Vexere route missing required fields: " + ", ".join(missing))
    quality = []
    duration = payload.get("duration_min")
    price = payload.get("price_vnd")
    if isinstance(duration, (int, float)) and duration < 0:
        quality.append("duration_anomaly")
    if isinstance(price, (int, float)) and price < 0:
        quality.append("price_anomaly")
    crawl_date_raw = payload.get("crawl_date")
    crawl_date = None
    if isinstance(crawl_date_raw, str):
        try:
            crawl_date = datetime.strptime(crawl_date_raw, "%d-%m-%Y").date()
        except ValueError:
            quality.append("crawl_date_unparseable")
    route = {
        "transport_id": _id("silver-transport-route-v2", source_sk),
        "source_record_sk": source_sk,
        "origin": payload["origin"],
        "destination": payload["destination"],
        "operator_name": payload["operator_name"],
        "transport_type": payload.get("transport_type"),
        "vehicle_type": payload.get("vehicle_type"),
        "departure_time_raw": payload.get("departure_time"),
        "arrival_time_raw": payload.get("arrival_time"),
        "duration_min": duration,
        "price_vnd": Decimal(str(price)) if price is not None else None,
        "crawl_date_raw": crawl_date_raw,
        "crawl_date": crawl_date,
        "source": "vexere",
        "source_url": payload.get("source_url"),
        "quality_codes": quality,
    }
    return {"source_record": [source], "transport_route": [route]}


def validate_and_normalize_manifest(manifest: dict[str, Any]) -> None:
    """Common gate used before any Bronze payload is read."""
    validate_bronze_manifest(manifest)
    if manifest["source_id"] not in {"tripadvisor", "foody", "tiktok", "vexere"}:
        raise ValueError("Silver normalization source is not authorized")

