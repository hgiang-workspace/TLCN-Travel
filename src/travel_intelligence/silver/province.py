"""Rule-based province normalization for Silver place records."""

from __future__ import annotations

import hashlib
import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from travel_intelligence.common.configuration import (
    configuration_fingerprint,
    load_yaml,
)

PROVINCE_RULE_PATH = Path("configs/geography/province_aliases.yaml")


def _lookup_key(value: str) -> str:
    decomposed = unicodedata.normalize("NFKD", value)
    ascii_like = "".join(
        char for char in decomposed if not unicodedata.combining(char)
    )
    ascii_like = ascii_like.replace("đ", "d").replace("Đ", "D")
    return re.sub(r"[^a-z0-9]+", " ", ascii_like.lower()).strip()


def _geo_id(canonical_name: str) -> str:
    digest = hashlib.sha256(
        f"province-rules-v1|{_lookup_key(canonical_name)}".encode("utf-8")
    ).hexdigest()[:24]
    return f"vn-province:{digest}"


@dataclass(frozen=True)
class ProvinceRules:
    version: str
    fingerprint: str
    lookup: dict[str, str]
    geo_ids: dict[str, str]


def load_province_rules(path: str | Path = PROVINCE_RULE_PATH) -> ProvinceRules:
    value = load_yaml(path)
    version = value.get("version")
    canonical_names = value.get("canonical_names")
    aliases = value.get("aliases")
    if not isinstance(version, str) or not version:
        raise ValueError("province rules require a non-empty version")
    if (
        not isinstance(canonical_names, list)
        or not canonical_names
        or not all(isinstance(item, str) and item.strip() for item in canonical_names)
    ):
        raise ValueError("province rules require non-empty canonical_names")
    if not isinstance(aliases, dict):
        raise ValueError("province rules aliases must be a mapping")
    canonical = {item.strip() for item in canonical_names}
    if len(canonical) != len(canonical_names):
        raise ValueError("province rules contain duplicate canonical names")
    geo_ids = {name: _geo_id(name) for name in canonical}
    lookup: dict[str, str] = {}
    for name in canonical:
        lookup_key = _lookup_key(name)
        if lookup_key in lookup and lookup[lookup_key] != name:
            raise ValueError(f"province canonical collision: {name}")
        lookup[lookup_key] = name
    for raw_alias, target in aliases.items():
        if not isinstance(raw_alias, str) or not isinstance(target, str):
            raise ValueError("province aliases must map strings to strings")
        if target not in canonical:
            raise ValueError(f"province alias target is not canonical: {target}")
        alias_key = _lookup_key(raw_alias)
        previous = lookup.get(alias_key)
        if previous is not None and previous != target:
            raise ValueError(f"province alias collision: {raw_alias}")
        lookup[alias_key] = target
    return ProvinceRules(
        version=version,
        fingerprint=configuration_fingerprint(value),
        lookup=lookup,
        geo_ids=geo_ids,
    )


PROVINCE_RULES = load_province_rules()


def normalize_province(
    value: Any, rules: ProvinceRules = PROVINCE_RULES
) -> dict[str, Any]:
    raw = value if isinstance(value, str) else None
    normalized = _lookup_key(raw) if raw and raw.strip() else None
    canonical = rules.lookup.get(normalized) if normalized else None
    if canonical is None:
        return {
            "province_raw": raw,
            "province_normalized": normalized,
            "province_geo_id": None,
            "province_canonical": None,
            "province_resolution_status": "unknown",
            "province_match_method": "unresolved",
            "province_rule_version": rules.version,
        }
    method = "exact" if normalized == _lookup_key(canonical) else "approved_alias"
    return {
        "province_raw": raw,
        "province_normalized": normalized,
        "province_geo_id": rules.geo_ids[canonical],
        "province_canonical": canonical,
        "province_resolution_status": "resolved",
        "province_match_method": method,
        "province_rule_version": rules.version,
    }


__all__ = [
    "PROVINCE_RULE_PATH",
    "PROVINCE_RULES",
    "ProvinceRules",
    "load_province_rules",
    "normalize_province",
]
