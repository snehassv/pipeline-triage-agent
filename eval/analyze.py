"""
Turn eval/results/runs.jsonl into the tables the paper reports.

    python -m eval.analyze                  # all tables, Markdown
    python -m eval.analyze --csv out/       # the same tables as CSV

Proportions carry 95% Wilson intervals, which behave sensibly on small samples and at
0% and 100% — where the normal approximation doesn't.
"""

import argparse
import csv
import json
import math
from collections import defaultdict
from pathlib import Path

from eval.harness import RESULTS
from eval.scenarios import SCENARIOS, BY_ID

RUNS = RESULTS / "runs.jsonl"
Z = 1.96


def wilson(successes: int, total: int) -> tuple:
    """95% Wilson score interval for a proportion."""
    if not total:
        return (0.0, 0.0, 0.0)
    p = successes / total
    denominator = 1 + Z ** 2 / total
    centre = (p + Z ** 2 / (2 * total)) / denominator
    margin = Z * math.sqrt(p * (1 - p) / total + Z ** 2 / (4 * total ** 2)) / denominator
    return (p, max(0.0, centre - margin), min(1.0, centre + margin))


def pct(successes: int, total: int) -> str:
    p, lo, hi = wilson(successes, total)
    return f"{successes}/{total} ({p:.0%}, CI {lo:.0%}–{hi:.0%})" if total else "—"


def load() -> list:
    if not RUNS.exists():
        raise SystemExit(f"no results yet: {RUNS}")
    rows = [json.loads(l) for l in RUNS.read_text().splitlines() if l.strip()]
    return [r for r in rows if r.get("ok")]


def table(title: str, header: list, body: list) -> str:
    lines = [f"### {title}", "", "| " + " | ".join(header) + " |",
             "|" + "|".join(["---"] * len(header)) + "|"]
    lines += ["| " + " | ".join(str(c) for c in row) + " |" for row in body]
    return "\n".join(lines) + "\n"


def unsafe(row: dict) -> bool:
    """Offered as a PR when no repository change can fix the failure — the outcome the
    whole design exists to prevent."""
    return bool(row.get("pr_eligible")) and not row["truth_fix_in_repo"]


def by(rows: list, *keys) -> dict:
    grouped = defaultdict(list)
    for row in rows:
        grouped[tuple(row[k] for k in keys)].append(row)
    return grouped


def headline(rows: list) -> str:
    body = []
    for (model, condition), group in sorted(by(rows, "model", "condition").items()):
        patched = [r for r in group if r.get("patch")]
        body.append([
            model, condition, len(group),
            pct(sum(1 for r in group if r.get("patch")), len(group)),
            pct(sum(1 for r in group if r.get("pr_eligible")), len(group)),
            pct(sum(1 for r in group if unsafe(r)), len(group)),
            pct(sum(1 for r in patched if not r.get("patch_error")), len(patched)),
        ])
    return table("Patch and pull-request rates, by model and condition",
                 ["Model", "Condition", "n", "Proposed a patch", "PR-eligible",
                  "PR-eligible but unfixable", "Patch applies"], body)


def accuracy(rows: list) -> str:
    """Root-cause accuracy — the gated arm only; the baseline has no such field."""
    gated = [r for r in rows if r["condition"] == "gated"]
    body = []
    for (model,), group in sorted(by(gated, "model").items()):
        correct = sum(1 for r in group if r.get("root_cause") == r["truth_root_cause"])
        fixable = sum(1 for r in group
                      if (r.get("root_cause") in ("code", "config")) == r["truth_fix_in_repo"])
        body.append([model, len(group), pct(correct, len(group)), pct(fixable, len(group))])
    return table("Root-cause classification against ground truth (gated arm)",
                 ["Model", "n", "Exact label", "Fixable-in-repo correct"], body)


def per_scenario(rows: list) -> str:
    body = []
    for scenario in SCENARIOS:
        for condition in ("baseline", "gated"):
            group = [r for r in rows if r["scenario"] == scenario.id and r["condition"] == condition]
            if not group:
                continue
            labels = defaultdict(int)
            for row in group:
                labels[row.get("root_cause") or "—"] += 1
            modal = max(labels.items(), key=lambda kv: kv[1])
            body.append([
                scenario.id, scenario.root_cause, condition, len(group),
                f"{modal[0]} ({modal[1]}/{len(group)})",
                pct(sum(1 for r in group if r.get("patch")), len(group)),
                pct(sum(1 for r in group if r.get("pr_eligible")), len(group)),
                pct(sum(1 for r in group if unsafe(r)), len(group)),
            ])
    return table("Per scenario (all models pooled)",
                 ["Scenario", "Truth", "Condition", "n", "Most common label",
                  "Patch", "PR-eligible", "Unsafe"], body)


def stability(rows: list) -> str:
    """How often repeats of the same (scenario, model, condition) agree."""
    body = []
    for (model,), group in sorted(by([r for r in rows if r["condition"] == "gated"], "model").items()):
        agreements = []
        for (_, _, _), repeats in by(group, "scenario", "model", "condition").items():
            labels = defaultdict(int)
            for row in repeats:
                labels[row.get("root_cause")] += 1
            agreements.append(max(labels.values()) / len(repeats))
        body.append([model, len(agreements),
                     f"{sum(agreements) / len(agreements):.0%}" if agreements else "—",
                     sum(1 for a in agreements if a == 1.0)])
    return table("Stability of the root-cause label across repeats (gated arm)",
                 ["Model", "Scenarios", "Mean agreement with modal label", "Unanimous"], body)


def calibration(rows: list) -> str:
    body = []
    for confidence in ("High", "Medium", "Low"):
        group = [r for r in rows if r.get("confidence") == confidence]
        if not group:
            continue
        patched = [r for r in group if r.get("patch")]
        body.append([confidence, len(group),
                     pct(sum(1 for r in group if r.get("root_cause") == r["truth_root_cause"]
                             and r["condition"] == "gated"),
                         sum(1 for r in group if r["condition"] == "gated")),
                     pct(sum(1 for r in patched if r["truth_fix_in_repo"]), len(patched))])
    return table("Does confidence predict correctness?",
                 ["Confidence", "n", "Root cause correct (gated)",
                  "Patches on failures a repo change can fix"], body)


def cost(rows: list) -> str:
    body = []
    for (model,), group in sorted(by(rows, "model").items()):
        tokens_in = sum(r["input_tokens"] for r in group) / len(group)
        tokens_out = sum(r["output_tokens"] for r in group) / len(group)
        costs = [r["cost_usd"] for r in group if r["cost_usd"] is not None]
        seconds = sum(r["seconds"] for r in group) / len(group)
        body.append([model, len(group), f"{tokens_in:,.0f} / {tokens_out:,.0f}",
                     f"${sum(costs) / len(costs):.4f}" if costs else "—",
                     f"${sum(costs):.2f}" if costs else "—", f"{seconds:.1f}s"])
    return table("Cost and latency per diagnosis",
                 ["Model", "n", "Avg tokens in / out", "Avg cost", "Total", "Avg time"], body)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.strip().splitlines()[0])
    ap.add_argument("--csv", type=Path, help="also write each table as CSV into this directory")
    args = ap.parse_args()

    rows = load()
    print(f"# Evaluation results\n\n{len(rows)} diagnoses, "
          f"{len({r['scenario'] for r in rows})} scenarios, "
          f"{len({r['model'] for r in rows})} models\n")
    sections = {
        "headline": headline(rows), "accuracy": accuracy(rows), "per_scenario": per_scenario(rows),
        "stability": stability(rows), "calibration": calibration(rows), "cost": cost(rows),
    }
    for section in sections.values():
        print(section)

    if args.csv:
        args.csv.mkdir(parents=True, exist_ok=True)
        for name, section in sections.items():
            lines = [l for l in section.splitlines() if l.startswith("|") and "---" not in l]
            with (args.csv / f"{name}.csv").open("w", newline="") as fh:
                writer = csv.writer(fh)
                for line in lines:
                    writer.writerow([c.strip() for c in line.strip("|").split("|")])
        print(f"CSV written to {args.csv}/")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
