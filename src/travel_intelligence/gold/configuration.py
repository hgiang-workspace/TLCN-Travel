"""Validated loading and combined fingerprinting for Gold configuration."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from travel_intelligence.common.configuration import (
    configuration_fingerprint,
    load_yaml,
)

GOLD_CONFIG_PATHS = (
    "configs/gold/gold_contract.yaml",
    "configs/gold/metric_definitions.yaml",
    "configs/gold/sufficiency_thresholds.yaml",
    "configs/gold/source_domain_policy.yaml",
    "configs/gold/tripadvisor_domain_mapping_v1.yaml",
)


def load_gold_configuration(
    root: str | Path = ".",
) -> tuple[dict[str, dict[str, Any]], str]:
    base = Path(root).resolve()
    values = {path: load_yaml(base / path, root=base) for path in GOLD_CONFIG_PATHS}
    contract = values[GOLD_CONFIG_PATHS[0]]
    if contract.get("version") != "gold-contract-v1":
        raise ValueError("unsupported Gold contract version")
    if contract.get("product_namespace") != "gold":
        raise ValueError("Gold product namespace must remain 'gold'")
    scope = contract.get("scope")
    if not isinstance(scope, dict) or scope.get("source_id") != "tripadvisor":
        raise ValueError("Gold v1 is restricted to the approved TripAdvisor slice")
    thresholds = values[GOLD_CONFIG_PATHS[2]].get("policy")
    if not isinstance(thresholds, dict):
        raise ValueError("Gold sufficiency policy is missing")
    if thresholds.get("sample_threshold") is not None:
        raise ValueError("sample threshold was not approved in T07-A")
    if thresholds.get("freshness_threshold_days") is not None:
        raise ValueError("freshness threshold was not approved in T07-A")
    policy = values[GOLD_CONFIG_PATHS[3]].get("domain_mapping")
    if not isinstance(policy, dict) or policy.get("status") != "deferred":
        raise ValueError("product-domain mapping must remain deferred in Gold v1")
    if policy.get("application_status") != "not_started":
        raise ValueError("domain mapping application is outside T07-E2A scope")
    mapping_contract = values[GOLD_CONFIG_PATHS[4]]
    if mapping_contract.get("status") not in {
        "review_required_no_assignment",
        "reviewed_b1_no_assignment",
    }:
        raise ValueError("domain mapping review artifact has an unsafe status")
    return values, configuration_fingerprint(values)


__all__ = ["GOLD_CONFIG_PATHS", "load_gold_configuration"]
