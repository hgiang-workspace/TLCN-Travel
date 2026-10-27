"""Pure T07-C Gold dimension and bridge projections.

These functions never infer an entity, province or product-domain membership.
They only project already-resolved Silver evidence selected by an input manifest.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Mapping
from datetime import date, timedelta
from typing import Any


class GoldDimensionError(ValueError):
    """Raised when a candidate dimension would violate a Gold invariant."""


def deterministic_key(namespace: str, *parts: object) -> str:
    """Create a stable surrogate key without execution-time input."""
    encoded = json.dumps(
        [namespace, *[str(part) for part in parts]],
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    return f"{namespace}-{hashlib.sha256(encoded).hexdigest()}"


def _required(row: Mapping[str, Any], field: str) -> str:
    value = row.get(field)
    if value is None or str(value).strip() == "":
        raise GoldDimensionError(f"missing required field: {field}")
    return str(value)


def _lineage(context: Mapping[str, str]) -> dict[str, str]:
    required = (
        "release_id",
        "gold_run_id",
        "contract_version",
        "config_fingerprint",
        "source_release_id",
    )
    return {field: _required(context, field) for field in required}


def project_dimensions_and_bridges(
    places: Iterable[Mapping[str, Any]],
    mappings: Iterable[Mapping[str, Any]],
    *,
    context: Mapping[str, str],
    valid_from: Any,
    built_at: Any,
    source_id: str,
    source_name: str,
) -> dict[str, Any]:
    """Project candidate dimensions from resolved Silver place/mapping evidence."""
    lineage = _lineage(context)
    places_by_id = {_required(row, "place_id"): row for row in places}
    resolved_mappings: list[Mapping[str, Any]] = []
    exclusions = {"unresolved_or_ambiguous_mapping": 0, "unresolved_geography": 0}
    seen_source_records: set[str] = set()
    for mapping in mappings:
        if mapping.get("decision_status") != "resolved":
            exclusions["unresolved_or_ambiguous_mapping"] += 1
            continue
        place_id = mapping.get("place_id")
        if not place_id or str(place_id) not in places_by_id:
            exclusions["unresolved_or_ambiguous_mapping"] += 1
            continue
        source_record_sk = _required(mapping, "source_record_sk")
        if source_record_sk in seen_source_records:
            raise GoldDimensionError(
                f"multiple resolved mappings for source record: {source_record_sk}"
            )
        seen_source_records.add(source_record_sk)
        place = places_by_id[str(place_id)]
        if place.get("identity_status") != "resolved":
            exclusions["unresolved_or_ambiguous_mapping"] += 1
            continue
        resolved_mappings.append(mapping)

    used_place_ids = sorted({_required(row, "place_id") for row in resolved_mappings})
    entity_rows: list[dict[str, Any]] = []
    geography_rows: dict[str, dict[str, Any]] = {}
    entity_by_id: dict[str, dict[str, Any]] = {}
    for place_id in used_place_ids:
        place = places_by_id[place_id]
        entity_sk = deterministic_key("entity", lineage["release_id"], place_id)
        row = {
            "entity_sk": entity_sk,
            "entity_id": place_id,
            "entity_name": _required(place, "place_name"),
            "entity_type_raw": place.get("place_type"),
            "product_domain_id": None,
            "address": place.get("address"),
            "district": place.get("district"),
            "province_geo_id": place.get("province_geo_id"),
            "latitude": place.get("latitude"),
            "longitude": place.get("longitude"),
            "identity_status": "resolved",
            "identity_method": _required(place, "identity_method"),
            "source_updated_at": place.get("updated_at"),
            "valid_from": valid_from,
            "valid_to": None,
            "is_current": True,
            **lineage,
            "built_at": built_at,
        }
        entity_rows.append(row)
        entity_by_id[place_id] = row
        geo_id = place.get("province_geo_id")
        if place.get("province_resolution_status") == "resolved" and geo_id:
            geography_rows.setdefault(
                str(geo_id),
                {
                    "geography_sk": deterministic_key(
                        "geography", lineage["release_id"], geo_id
                    ),
                    "geo_id": str(geo_id),
                    "geo_name": _required(place, "province_canonical"),
                    "geo_level": "province",
                    "parent_geo_id": None,
                    "coverage_tier": None,
                    "resolution_status": "resolved",
                    "valid_from": valid_from,
                    "valid_to": None,
                    "is_current": True,
                    **lineage,
                    "built_at": built_at,
                },
            )
        else:
            exclusions["unresolved_geography"] += 1

    source_sk = deterministic_key("source", lineage["release_id"], source_id)
    source_row = {
        "source_sk": source_sk,
        "source_id": source_id,
        "source_name": source_name,
        "source_type": None,
        "is_eligible": True,
        "exclusion_reason": None,
        **lineage,
        "built_at": built_at,
    }
    domain_rows = [
        {
            "domain_sk": deterministic_key("domain", domain_id),
            "domain_id": domain_id,
            "domain_name": domain_name,
            "mapping_status": "deferred",
            "is_active": False,
            **lineage,
            "built_at": built_at,
        }
        for domain_id, domain_name in (
            ("destination", "Điểm đến"),
            ("accommodation", "Lưu trú"),
            ("food_dining", "Ẩm thực"),
            ("transportation", "Di chuyển"),
        )
    ]
    entity_source_rows: list[dict[str, Any]] = []
    entity_geography_rows: list[dict[str, Any]] = []
    for mapping in sorted(
        resolved_mappings, key=lambda row: _required(row, "mapping_id")
    ):
        place_id = _required(mapping, "place_id")
        entity = entity_by_id[place_id]
        source_record_sk = _required(mapping, "source_record_sk")
        entity_source_rows.append(
            {
                "entity_source_sk": deterministic_key(
                    "entity-source",
                    lineage["release_id"],
                    _required(mapping, "mapping_id"),
                ),
                "entity_sk": entity["entity_sk"],
                "entity_id": place_id,
                "source_sk": source_sk,
                "source_id": source_id,
                "source_record_sk": source_record_sk,
                "source_record_id": mapping.get("source_record_id"),
                "mapping_id": _required(mapping, "mapping_id"),
                "mapping_status": "resolved",
                "mapping_method": _required(mapping, "mapping_method"),
                "mapping_confidence": mapping.get("confidence"),
                "mapping_version": _required(mapping, "mapping_version"),
                **lineage,
                "built_at": built_at,
            }
        )
        geo_id = entity.get("province_geo_id")
        if geo_id and str(geo_id) in geography_rows:
            geography = geography_rows[str(geo_id)]
            entity_geography_rows.append(
                {
                    "entity_geography_sk": deterministic_key(
                        "entity-geography", lineage["release_id"], place_id, geo_id
                    ),
                    "entity_sk": entity["entity_sk"],
                    "entity_id": place_id,
                    "geography_sk": geography["geography_sk"],
                    "geo_id": str(geo_id),
                    "relationship_type": "primary_province",
                    "resolution_status": "resolved",
                    "resolution_method": "silver_province_geo_id",
                    "rule_version": places_by_id[place_id].get("province_rule_version"),
                    **lineage,
                    "built_at": built_at,
                }
            )

    return {
        "dim_entity": entity_rows,
        "dim_geography": [geography_rows[key] for key in sorted(geography_rows)],
        "dim_domain": domain_rows,
        "dim_source": [source_row],
        "bridge_entity_source": entity_source_rows,
        "bridge_entity_geography": entity_geography_rows,
        "exclusion_counts": exclusions,
    }


def project_date_dimension(
    start: date, end: date, *, contract_version: str, built_at: Any
) -> list[dict[str, Any]]:
    """Build the bounded calendar range evidenced by the selected processing period."""
    if end < start:
        raise GoldDimensionError("date dimension end precedes start")
    rows = []
    current = start
    while current <= end:
        rows.append(
            {
                "date_sk": int(current.strftime("%Y%m%d")),
                "calendar_date": current,
                "calendar_year": current.year,
                "calendar_quarter": (current.month - 1) // 3 + 1,
                "calendar_month": current.month,
                "month_start": current.replace(day=1),
                "contract_version": contract_version,
                "built_at": built_at,
            }
        )
        current += timedelta(days=1)
    return rows


__all__ = [
    "GoldDimensionError",
    "deterministic_key",
    "project_date_dimension",
    "project_dimensions_and_bridges",
]
