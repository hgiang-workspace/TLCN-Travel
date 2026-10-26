from __future__ import annotations

import pytest

from travel_intelligence.qdrant.manifest import ManifestError, build_manifest


def _run(run_id: str, *, status: str = "committed") -> dict[str, object]:
    return {
        "processing_run_id": run_id,
        "source_release_id": "release-1",
        "model_fingerprint": "model-1",
        "config_fingerprint": "config-1",
        "input_fingerprint": f"input-{run_id}",
        "status": status,
        "expected_review_count": 2,
        "actual_review_count": 2,
        "expected_aspect_count": 3,
        "actual_aspect_count": 3,
    }


def test_manifest_is_deterministic_and_reconciles_multiple_runs():
    first = build_manifest(
        source_id="tripadvisor",
        source_release_id="release-1",
        runs=[_run("run-2"), _run("run-1")],
    )
    second = build_manifest(
        source_id="tripadvisor",
        source_release_id="release-1",
        runs=[_run("run-1"), _run("run-2")],
    )
    assert first == second
    assert first["input_run_count"] == 2
    assert first["expected_input_review_count"] == 4
    assert first["expected_input_aspect_count"] == 6
    assert [item["processing_run_id"] for item in first["processing_runs"]] == [
        "run-1",
        "run-2",
    ]


def test_manifest_rejects_partial_or_mixed_fingerprint_runs():
    with pytest.raises(ManifestError, match="non-committed"):
        build_manifest(
            source_id="tripadvisor",
            source_release_id="release-1",
            runs=[_run("run-1", status="partial")],
        )
    mixed = _run("run-2")
    mixed["config_fingerprint"] = "config-2"
    with pytest.raises(ManifestError, match="mixed model/config"):
        build_manifest(
            source_id="tripadvisor",
            source_release_id="release-1",
            runs=[_run("run-1"), mixed],
        )
