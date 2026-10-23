"""
Does the proposed patch actually fix the failure?

For each distinct patch the models produced: put the harness back into the state that
caused the failure, apply the patch, apply any migration it adds, re-run the DAG, and
record whether the task passed. Then revert, always.

    python -m eval.verify_patches --dry-run     # what would be verified
    python -m eval.verify_patches

Identical patches are verified once and the verdict shared, keyed by a hash of the patch
text. Results go to eval/results/patch_checks.jsonl. No API cost — this is all Airflow.

The working tree must be clean: the patches are applied to the real files that Docker
mounts, so anything uncommitted would be rolled back with them.
"""

import argparse
import datetime as dt
import hashlib
import json
import subprocess
import sys
import time

from agent.main import REPO_ROOT
from eval import harness
from eval.scenarios import BY_ID
from scripts.ddl_policy import _file_blocks

CHECKS = harness.RESULTS / "patch_checks.jsonl"
RUNS = harness.RESULTS / "runs.jsonl"
PARSE_PAUSE = 12          # let the DAG processor pick the edited file up


def patch_id(patch: str) -> str:
    return hashlib.sha256(patch.encode()).hexdigest()[:12]


def git(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(["git", "-C", str(REPO_ROOT), *args], capture_output=True, text=True)


def working_tree_clean() -> bool:
    return not git("status", "--porcelain").stdout.strip()


def load_rows(path) -> list:
    if not path.exists():
        return []
    return [json.loads(l) for l in path.read_text().splitlines() if l.strip()]


def candidates() -> dict:
    """{patch_id: row} for every distinct patch that applies, newest wins."""
    out = {}
    for row in load_rows(RUNS):
        if row.get("ok") and row.get("patch") and not row.get("patch_error"):
            out.setdefault(patch_id(row["patch"]), row)
    return out


def verify(row: dict) -> dict:
    """Apply one patch and re-run its scenario. Always leaves the tree as it found it."""
    scenario = BY_ID[row["scenario"]]
    record = {
        "patch_id": patch_id(row["patch"]), "scenario": scenario.id, "model": row["model"],
        "condition": row["condition"], "truth_fix_in_repo": scenario.fix_in_repo,
        "at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
    }
    try:
        harness.reset()
        if scenario.setup:
            harness.run_module(scenario.setup)

        applied = subprocess.run(["git", "-C", str(REPO_ROOT), "apply", "--recount", "-"],
                                 input=row["patch"], capture_output=True, text=True)
        if applied.returncode != 0:
            return {**record, "applied": False, "fixed": False,
                    "detail": (applied.stderr or "git apply failed").strip()[:300]}

        # A migration only changes the warehouse once someone runs it — the same manual
        # step the PR description asks for.
        if "sql/migrations/" in row["patch"]:
            try:
                harness.run_module("scripts.migrate")
            except harness.HarnessError as e:
                return {**record, "applied": True, "fixed": False,
                        "detail": f"migration failed: {e}"[:300]}

        time.sleep(PARSE_PAUSE)
        started = dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=5)
        harness._airflow_cli("dags", "unpause", scenario.dag_id)
        harness._airflow_cli("dags", "trigger", scenario.dag_id)

        deadline = time.monotonic() + (harness.SENSOR_TIMEOUT if scenario.sensor else harness.TASK_TIMEOUT)
        while time.monotonic() < deadline:
            failed = harness.latest_failure(scenario.dag_id, scenario.task_id, started)
            if failed:
                _, error = harness.agent_main.read_task_log({
                    "dag": failed["dag_id"], "task": failed["task_id"],
                    "run_id": failed["dag_run_id"], "try_number": failed.get("try_number") or 1,
                })
                return {**record, "applied": True, "fixed": False, "detail": error[:300]}
            if succeeded(scenario, started):
                return {**record, "applied": True, "fixed": True, "detail": "task succeeded"}
            time.sleep(5)
        return {**record, "applied": True, "fixed": False, "detail": "no terminal state in time"}
    finally:
        revert(row["patch"])


def revert(patch: str) -> None:
    """Undo exactly the files this patch touched.

    Not `git checkout -- .`: that reverts every tracked file, which quietly threw away
    this script's own results once they were committed."""
    for block in _file_blocks(patch):
        path = block["path"]
        tracked = git("cat-file", "-e", f"HEAD:{path}").returncode == 0
        if tracked:
            git("checkout", "--", path)
        else:
            (REPO_ROOT / path).unlink(missing_ok=True)


def succeeded(scenario, after: dt.datetime) -> bool:
    page = harness.agent_main.airflow.get(
        f"/api/v2/dags/{scenario.dag_id}/dagRuns/~/taskInstances",
        params={"state": "success", "end_date_gte": harness.agent_main._iso(after),
                "order_by": "-end_date", "limit": 5},
    )
    return any(ti["task_id"] == scenario.task_id for ti in page.get("task_instances", []))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.strip().splitlines()[0])
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--limit", type=int, help="verify at most this many patches")
    ap.add_argument("--per-group", type=int,
                    help="verify at most this many distinct patches per scenario+condition; "
                         "each takes ~2.5 minutes, so the full set is hours")
    args = ap.parse_args()

    todo = candidates()
    done = {r["patch_id"] for r in load_rows(CHECKS)}
    todo = {pid: row for pid, row in todo.items() if pid not in done}
    if args.per_group:
        kept, seen = {}, {}
        for pid in sorted(todo):                      # deterministic sample
            row = todo[pid]
            key = (row["scenario"], row["condition"])
            if seen.get(key, 0) < args.per_group:
                seen[key] = seen.get(key, 0) + 1
                kept[pid] = row
        todo = kept
    if args.limit:
        todo = dict(list(todo.items())[:args.limit])

    print(f"{len(todo)} distinct patches to verify ({len(done)} already done)")
    for pid, row in todo.items():
        print(f"  {pid}  {row['scenario']:<16} {row['model']:<18} {row['condition']}")
    if args.dry_run:
        return 0
    if not working_tree_clean():
        print("error: working tree has uncommitted changes — commit or stash first",
              file=sys.stderr)
        return 1

    with CHECKS.open("a") as fh:
        for i, (pid, row) in enumerate(todo.items(), 1):
            result = verify(row)
            fh.write(json.dumps(result) + "\n")
            fh.flush()
            verdict = "FIXED" if result["fixed"] else "still fails"
            print(f"[{i}/{len(todo)}] {pid} {result['scenario']:<16} {verdict}: "
                  f"{result['detail'][:80]}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
