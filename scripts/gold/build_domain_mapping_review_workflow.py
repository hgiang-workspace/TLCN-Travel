"""Create proposal-only, reviewer-friendly category mapping batches."""

from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from pathlib import Path

from travel_intelligence.common.configuration import load_yaml
from travel_intelligence.gold.domain_mapping import DOMAIN_MAPPING_CONFIG_PATH
from travel_intelligence.gold.domain_review import build_review_proposals, proposal_dict


def assign_batches(proposals):
    """Batch descending impact at 50%, 75%, 90%, then the remaining coverage."""
    total = sum(proposal.entity_count for proposal in proposals)
    thresholds = (("B1-high-impact", 0.50), ("B2", 0.75), ("B3", 0.90))
    cumulative = 0
    rows = []
    for proposal in proposals:
        cumulative += proposal.entity_count
        batch = "B4"
        for name, threshold in thresholds:
            previous = cumulative - proposal.entity_count
            if cumulative <= total * threshold or previous < total * threshold:
                batch = name
                break
        row = proposal_dict(proposal)
        row["review_batch"] = batch
        row["cumulative_entity_count"] = cumulative
        row["cumulative_coverage_pct"] = round(cumulative / total * 100, 4)
        rows.append(row)
    return rows


def build_workflow(inventory: dict, contract: dict) -> dict:
    proposals = build_review_proposals(inventory["raw_categories"], contract)
    rows = assign_batches(proposals)
    if len(rows) != 582 or sum(row["entity_count"] for row in rows) != 4000:
        raise ValueError("review queue does not reconcile to pinned inventory")
    return {
        "artifact_version": "t07e2a-domain-review-workflow-v1",
        "review_only": True,
        "candidate_release_id": inventory["candidate_release_id"],
        "source_release_id": inventory["source_release_id"],
        "source_id": inventory["source_id"],
        "runtime_mapping_effect": "none; all rows remain unmapped until approved",
        "batch_summary": _summaries(rows, "review_batch"),
        "group_summary": _summaries(rows, "review_group"),
        "proposal_summary": _summaries(rows, "proposed_disposition"),
        "product_owner_decisions": [
            row for row in rows if row["requires_product_owner"]
        ],
        "rows": rows,
    }


def _summaries(rows, field):
    counter = Counter(row[field] for row in rows)
    coverage = Counter()
    for row in rows:
        coverage[row[field]] += row["entity_count"]
    return [
        {field: key, "category_count": counter[key], "entity_count": coverage[key]}
        for key in sorted(counter)
    ]


def write_workflow(workflow: dict, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "TASK-007E2A-domain-review-workflow.json"
    csv_path = output_dir / "TASK-007E2A-domain-review-queue.csv"
    markdown_path = output_dir / "TASK-007E2A-review-batches.md"
    json_path.write_text(json.dumps(workflow, ensure_ascii=False, indent=2) + "\n")
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(workflow["rows"][0]))
        writer.writeheader()
        writer.writerows(workflow["rows"])
    markdown_path.write_text(_markdown_report(workflow), encoding="utf-8")


def _markdown_report(workflow: dict) -> str:
    lines = [
        "# T07-E2A — Reviewer approval workflow",
        "",
        "This queue preserves reviewer states and has runtime effect "
        "`unmapped_no_assignment`; component analysis never changes Gold "
        "entities. Approved rules remain review-contract evidence until the "
        "separate T07-E2B application task is authorized.",
        "",
        "## Review batches",
        "",
        "| Batch | Categories | Entities | Coverage |",
        "| --- | ---: | ---: | ---: |",
    ]
    total = sum(row["entity_count"] for row in workflow["rows"])
    for summary in workflow["batch_summary"]:
        coverage = summary["entity_count"] / total * 100
        lines.append(
            f"| {summary['review_batch']} | {summary['category_count']} | "
            f"{summary['entity_count']} | {coverage:.2f}% |"
        )
    lines.extend(
        [
            "",
            "## High-impact first batch",
            "",
            "B1 records the explicit product-owner decisions. B2–B4 remain "
            "unreviewed and require explicit decisions in the CSV.",
            "",
            "| Raw category | Entities | Proposal | Domain | Review group |",
            "| --- | ---: | --- | --- | --- |",
        ]
    )
    for row in workflow["rows"]:
        if row["review_batch"] == "B1-high-impact":
            domain = row["proposed_domain_id"] or "—"
            lines.append(
                f"| {row['raw_category'] or '(empty)'} | {row['entity_count']} | "
                f"{row['proposed_disposition']} | {domain} | "
                f"{row['review_group']} |"
            )
    lines.extend(
        [
            "",
            "## Product-owner decisions",
            "",
            "Spa and every Spa-derived category need a product-owner decision; "
            "they cannot be automatically treated as accommodation or excluded.",
            "",
            "## Approval fields",
            "",
            "The CSV contains `reviewer`, `reviewer_decision`, and `review_status`. "
            "`approved_mapped` and `reviewed_unmapped` are reviewed decisions; "
            "`proposed_*` and `unreviewed_unmapped` are not. No row applies a "
            "domain to Gold before T07-E2B.",
            "",
            "## Repository-path discrepancy",
            "",
            "AGENTS.md names `docs/prd/` and `docs/architecture/`, but approved "
            "reference documents currently live under `docs/architechture/`. A "
            "separate documentation-only cleanup should correct or alias those paths; "
            "it is outside this mapping workflow.",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--inventory",
        type=Path,
        default=Path("docs/tasks/TASK-007E2A-tripadvisor-category-inventory.json"),
    )
    parser.add_argument("--root", type=Path, default=Path("."))
    parser.add_argument("--output-dir", type=Path, default=Path("docs/tasks"))
    args = parser.parse_args()
    inventory = json.loads(args.inventory.read_text(encoding="utf-8"))
    contract = load_yaml(args.root / DOMAIN_MAPPING_CONFIG_PATH, root=args.root)
    workflow = build_workflow(inventory, contract)
    write_workflow(workflow, args.output_dir)
    print(
        f"wrote {len(workflow['rows'])} review rows for "
        f"{sum(row['entity_count'] for row in workflow['rows'])} entities"
    )


if __name__ == "__main__":
    main()
