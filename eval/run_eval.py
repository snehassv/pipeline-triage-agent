"""
Run the evaluation: every scenario × model × prompt condition × repeat.

    python -m eval.run_eval capture                      # break each pipeline, save the evidence
    python -m eval.run_eval diagnose --dry-run           # what it would cost
    python -m eval.run_eval diagnose --repeats 10 --budget 20
    python -m eval.run_eval diagnose --models claude-haiku-4-5 --repeats 2

Results append to eval/results/runs.jsonl, one row per diagnosis. Re-running skips work
already in that file, so an interrupted run resumes where it stopped.

Spending guards: --dry-run prints the estimate and exits; --budget stops before the call
that would exceed it; the estimate uses measured averages per model, so it tracks reality
rather than a guess once a few rows exist.
"""

import argparse
import datetime as dt
import json
import sys
import time
from pathlib import Path

from agent import diagnosis
from agent.main import REPO_ROOT
from eval import baseline, gates, harness
from eval.scenarios import SCENARIOS, BY_ID

RUNS = harness.RESULTS / "runs.jsonl"
MODELS = ["claude-haiku-4-5", "claude-sonnet-5", "claude-opus-5"]
CONDITIONS = {
    "baseline": (baseline.SYSTEM, baseline.FIX_SCHEMA),
    "gated": (diagnosis.SYSTEM, diagnosis.FIX_SCHEMA),
}
# Rough per-diagnosis cost before we have measurements for a model (USD).
PRIOR_COST = {"claude-haiku-4-5": 0.011, "claude-sonnet-5": 0.023, "claude-opus-5": 0.080}


def load_rows() -> list:
    if not RUNS.exists():
        return []
    return [json.loads(line) for line in RUNS.read_text().splitlines() if line.strip()]


def done_keys(rows: list) -> set:
    return {(r["scenario"], r["model"], r["condition"], r["repeat"]) for r in rows if r.get("ok")}


def average_cost(rows: list, model: str) -> float:
    costs = [r["cost_usd"] for r in rows
             if r.get("model") == model and r.get("cost_usd") is not None]
    return sum(costs) / len(costs) if costs else PRIOR_COST.get(model, 0.03)


def spent(rows: list) -> float:
    return sum(r.get("cost_usd") or 0 for r in rows)


def diagnose_one(scenario_id: str, model: str, condition: str, repeat: int) -> dict:
    """One diagnosis of saved evidence. Never touches the cache or Airflow."""
    captured = harness.load(scenario_id)
    system, schema = CONDITIONS[condition]
    truth = BY_ID[scenario_id]
    row = {
        "scenario": scenario_id, "model": model, "condition": condition, "repeat": repeat,
        "at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
        "truth_root_cause": truth.root_cause, "truth_fix_in_repo": truth.fix_in_repo,
    }
    try:
        result = diagnosis.diagnose(captured["failure"], captured["files"],
                                    model=model, system=system, schema=schema)
    except diagnosis.DiagnosisError as e:
        return {**row, "ok": False, "error": str(e)}

    patch = result.get("patch") or ""
    row.update({
        "ok": True,
        "type": result.get("type"), "cat": result.get("cat"),
        "confidence": result.get("confidence"),
        "root_cause": result.get("rootCause"),            # absent in the baseline arm
        "fix_summary": result.get("fixSummary"),
        "patch": patch,
        "patch_files": result.get("fixFile"),
        "policy_error": result.get("policyError"),
        "patch_error": diagnosis.check_patch(patch, REPO_ROOT) if patch else None,
        "cause_engineer": (result.get("cause") or {}).get("engineer"),
        "cause_analyst": (result.get("cause") or {}).get("analyst"),
        **{k: result["usage"][k] for k in ("input_tokens", "output_tokens", "cost_usd", "seconds")},
    })
    row["pr_eligible"] = gates.pr_eligible(row)
    return row


def cmd_capture(args) -> int:
    scenarios = [BY_ID[s] for s in args.scenarios] if args.scenarios else SCENARIOS
    for i, scenario in enumerate(scenarios, 1):
        print(f"[{i}/{len(scenarios)}] {scenario.id}: resetting, breaking, running…", flush=True)
        try:
            record = harness.capture(scenario)
        except harness.HarnessError as e:
            print(f"    FAILED: {e}", file=sys.stderr)
            continue
        print(f"    captured: {record['failure']['error'][:90]}")
    return 0


def cmd_diagnose(args) -> int:
    rows = load_rows()
    done = done_keys(rows)
    scenarios = args.scenarios or [s.id for s in SCENARIOS]
    todo = [(s, m, c, r)
            for s in scenarios for m in args.models for c in args.conditions
            for r in range(1, args.repeats + 1)
            if (s, m, c, r) not in done]

    estimate = sum(average_cost(rows, m) for _, m, _, _ in todo)
    print(f"{len(todo)} diagnoses to run (skipping {len(done)} already done)")
    print(f"estimated cost: ${estimate:.2f}" + (f", budget ${args.budget:.2f}" if args.budget else ""))
    for model in args.models:
        n = sum(1 for _, m, _, _ in todo if m == model)
        print(f"  {model}: {n} × ~${average_cost(rows, model):.4f}")
    if args.dry_run:
        return 0

    session_cost = 0.0
    with RUNS.open("a") as fh:
        for i, (scenario_id, model, condition, repeat) in enumerate(todo, 1):
            if args.budget and session_cost + average_cost(rows, model) > args.budget:
                print(f"\nstopping: ${session_cost:.2f} spent, next call would exceed "
                      f"the ${args.budget:.2f} budget")
                break
            row = diagnose_one(scenario_id, model, condition, repeat)
            fh.write(json.dumps(row) + "\n")
            fh.flush()
            rows.append(row)
            session_cost += row.get("cost_usd") or 0
            status = (f"{row.get('root_cause') or '-':>11} | conf {row.get('confidence'):<6} | "
                      f"patch {'yes' if row.get('patch') else 'no ':<3} | "
                      f"PR {'YES' if row.get('pr_eligible') else 'no '}"
                      if row["ok"] else f"ERROR {row['error'][:60]}")
            print(f"[{i}/{len(todo)}] {scenario_id:<16} {model:<18} {condition:<8} #{repeat:<2} "
                  f"{status}  ${session_cost:.2f}", flush=True)
            time.sleep(args.pause)
    print(f"\nsession cost ${session_cost:.2f}; {RUNS} now has {len(load_rows())} rows")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.strip().splitlines()[0])
    sub = ap.add_subparsers(dest="command", required=True)

    cap = sub.add_parser("capture", help="break each pipeline and save the failure evidence")
    cap.add_argument("--scenarios", nargs="*", help="scenario ids (default: all)")
    cap.set_defaults(func=cmd_capture)

    dia = sub.add_parser("diagnose", help="diagnose captured failures across the matrix")
    dia.add_argument("--models", nargs="*", default=MODELS)
    dia.add_argument("--conditions", nargs="*", default=list(CONDITIONS))
    dia.add_argument("--scenarios", nargs="*")
    dia.add_argument("--repeats", type=int, default=10)
    dia.add_argument("--budget", type=float, help="stop before exceeding this many dollars")
    dia.add_argument("--pause", type=float, default=0.5, help="seconds between calls")
    dia.add_argument("--dry-run", action="store_true", help="print the estimate and exit")
    dia.set_defaults(func=cmd_diagnose)

    args = ap.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
