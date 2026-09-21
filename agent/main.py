#!/usr/bin/env python3
"""
Pipeline Triage Agent — backend
================================
Reads failed task instances from a local Airflow 3 deployment over its REST API,
pulls each failure's real task log and DAG source, and (when an Anthropic API key is
set) asks Claude for a diagnosis and a suggested fix.

    GET  /                                          -> the dashboard (ui/index.html)
    GET  /failures?start=YYYY-MM-DD&end=YYYY-MM-DD  -> failed tasks in the window, diagnosed
    POST /pull-requests  {"id": "<dag>::<task>"}    -> opens a PR with that failure's fix

Pipeline, per failure (cached — a failed attempt never changes):
    1. list failures  — agent/airflow.py, GET /api/v2/dags/~/dagRuns/~/taskInstances
    2. read the log   — reduced to the exception, DAG-code frames and preceding context
    3. read the code  — the DAG's file from Airflow + dags/config/pipelines.yaml
    4. diagnose       — agent/diagnosis.py; skipped without ANTHROPIC_API_KEY
    5. check the fix  — `git apply --check` against this checkout

Run it (from the repo root, with `docker compose up -d` already done):
    uvicorn agent.main:app --port 8787   # dashboard at http://localhost:8787
    python -m agent.main                 # print today's failures to the terminal
    python -m agent.main --usage         # token usage and cost of diagnoses so far

Configuration — environment variables, or agent/.env (see agent/.env.example):
    ANTHROPIC_API_KEY  turns on diagnosis
    AGENT_MODEL        Claude model, default claude-sonnet-5
    AGENT_DIAGNOSIS_WORKERS  diagnoses run in parallel in the background, default 4
    GITHUB_REPO        owner/name the PR endpoint targets; PRs are disabled if unset
    AGENT_TOKEN        shared secret for the X-Agent-Token header; generated if unset
    AIRFLOW_URL / AIRFLOW_USERNAME / AIRFLOW_PASSWORD   default http://localhost:8080, airflow/airflow
    AGENT_ENV          environment label shown in the UI (dev|qa|prd), default dev
    AGENT_TZ           timezone the UI's calendar days are in, default America/Chicago

Nothing here merges anything. PRs still require human review.
"""

import argparse
import datetime as dt
import json
import os
import re
import secrets
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor, wait
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo

from fastapi import Depends, FastAPI, Header, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse

AGENT_DIR = Path(__file__).resolve().parent
REPO_ROOT = AGENT_DIR.parent


def _load_dotenv(path: Path) -> None:
    """Minimal .env reader. Real environment variables win over the file.

    This is deliberately agent/.env and not the repo-root .env: docker compose passes the
    root .env into every Airflow container, so a key kept there would leak into all of them."""
    if not path.exists():
        return
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        value = value.strip().strip('"').strip("'")
        if value:
            os.environ.setdefault(key.strip(), value)


_load_dotenv(AGENT_DIR / ".env")

# Imported after the .env load so AGENT_MODEL etc. are visible to them.
from agent import diagnosis  # noqa: E402
from agent.airflow import AirflowClient, AirflowError, reduce_log  # noqa: E402

AIRFLOW_URL      = os.environ.get("AIRFLOW_URL", "http://localhost:8080").rstrip("/")
AIRFLOW_USERNAME = os.environ.get("AIRFLOW_USERNAME", "airflow")
AIRFLOW_PASSWORD = os.environ.get("AIRFLOW_PASSWORD", "airflow")
ENV_NAME         = os.environ.get("AGENT_ENV", "dev")
LOCAL_TZ         = ZoneInfo(os.environ.get("AGENT_TZ", "America/Chicago"))
GITHUB_REPO      = os.environ.get("GITHUB_REPO", "")  # owner/name
PIPELINES_CONFIG = "dags/config/pipelines.yaml"
USAGE_LOG        = AGENT_DIR / "usage.jsonl"
MAX_RANGE_DAYS   = 7      # a wider window multiplies the per-poll diagnosis cost
RETRY_DIAGNOSIS_AFTER = 300  # seconds before re-trying a diagnosis that errored
DIAGNOSIS_WORKERS = int(os.environ.get("AGENT_DIAGNOSIS_WORKERS", "4"))  # parallel Claude calls

# Without this token, any local process — or a stray browser tab — could call these
# endpoints and trigger real git/gh actions under your identity.
AGENT_TOKEN = os.environ.get("AGENT_TOKEN") or secrets.token_urlsafe(24)
_TOKEN_WAS_GENERATED = not os.environ.get("AGENT_TOKEN")

_BRANCH_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._/-]{0,99}$")


@asynccontextmanager
async def _lifespan(_app):
    if _TOKEN_WAS_GENERATED:
        print(f"\n>>> No AGENT_TOKEN set — generated one for this run:\n    {AGENT_TOKEN}\n")
    yield


app = FastAPI(title="Pipeline Triage Agent", lifespan=_lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[f"http://{host}:8787" for host in ("localhost", "127.0.0.1")],
    allow_methods=["GET", "POST"],
    allow_headers=["Content-Type", "X-Agent-Token"],
)

airflow = AirflowClient(AIRFLOW_URL, AIRFLOW_USERNAME, AIRFLOW_PASSWORD)


def require_token(x_agent_token: str = Header(default="")) -> None:
    if not secrets.compare_digest(x_agent_token, AGENT_TOKEN):
        raise HTTPException(status_code=401, detail="missing/invalid X-Agent-Token header")


# ----------------------------------------------------------------------------------
# STEP 1 — list failed task instances in the window
# ----------------------------------------------------------------------------------
def read_failed_tasks(lo: dt.datetime, hi: dt.datetime) -> list:
    """Failed task instances in [lo, hi], grouped by (dag, task) so a task that failed on
    three runs shows up once — with its latest attempt."""
    grouped: dict = {}
    for ti in airflow.failed_task_instances(_iso(lo), _iso(hi)):
        key = (ti["dag_id"], ti["task_id"])
        if key in grouped:
            grouped[key]["occurrences"] += 1
            continue
        when = _parse_ts(ti.get("end_date"))
        grouped[key] = {
            "dag": ti["dag_id"],
            "task": ti["task_id"],
            "run_id": ti["dag_run_id"],
            "try_number": ti.get("try_number") or 1,
            "operator": ti.get("operator") or "",
            "env": ENV_NAME,
            "repo": GITHUB_REPO.split("/")[-1] if GITHUB_REPO else "local",
            "time": when.strftime("%H:%M:%S") if when else "",
            "day": when.strftime("%Y-%m-%d") if when else "",
            "duration": _fmt_duration(ti.get("duration")),
            "occurrences": 1,
        }
    return list(grouped.values())


# ----------------------------------------------------------------------------------
# STEPS 2 + 3 — the log, and the code Claude needs to reason about a fix
# ----------------------------------------------------------------------------------
def read_task_log(f: dict) -> tuple:
    try:
        content = airflow.task_log(f["dag"], f["run_id"], f["task"], f["try_number"])
    except KeyError:
        return [{"sev": "warn", "t": "task log not found (it may have been cleaned up)"}], ""
    return reduce_log(content)


def read_sources(dag_id: str) -> dict:
    """{repo-relative path: text} for the files a fix could touch. DAGs here come from
    dag_factory.py, so the factory alone doesn't say what a given DAG does — its
    pipelines.yaml entry does. Both are sent whole, so a patch can quote them exactly."""
    files = {}
    try:
        rel, source = airflow.dag_file(dag_id)
        files[f"dags/{rel}"] = source
    except (KeyError, AirflowError):
        pass
    config = REPO_ROOT / PIPELINES_CONFIG
    if config.exists():
        text = config.read_text()
        if re.search(rf"dag_id:\s*{re.escape(dag_id)}\s*$", text, re.MULTILINE):
            files[PIPELINES_CONFIG] = text
    return files


# ----------------------------------------------------------------------------------
# Orchestration
# ----------------------------------------------------------------------------------
# A failed task attempt is finished: its log won't change, so neither will its diagnosis.
# Caching per (dag, task, run, try) means the dashboard's 30s poll only does real work —
# including the Claude call — when a *new* failure appears.
#
# A diagnosis takes 10-25s, so it never runs inside a request: /failures returns at once
# with `diagnosing: true`, and a small thread pool fills the cache in the background (the
# dashboard polls quickly until nothing is pending). A diagnosis that errors is retried
# after RETRY_DIAGNOSIS_AFTER rather than on every poll, so a bad reply isn't re-billed
# twice a minute. Each entry is submitted at most once, so overlapping polls can't
# diagnose the same failure twice.
_cache: dict = {}
_cache_lock = threading.Lock()
_usage_lock = threading.Lock()
_CACHE_MAX = 500
_latest: dict = {}  # failure id -> record from the most recent /failures; used by PRs
_executor = ThreadPoolExecutor(max_workers=DIAGNOSIS_WORKERS, thread_name_prefix="diagnose")


def collect_failures(lo: dt.datetime, hi: dt.datetime, wait_for_diagnosis: bool = False) -> list:
    """Failure records for the window. Diagnoses still in flight come back as
    `diagnosing: true` unless wait_for_diagnosis is set (the CLI uses that)."""
    failed = read_failed_tasks(lo, hi)
    with _cache_lock:
        records = [_enrich(f) for f in failed]
    if wait_for_diagnosis:
        pending = [e["pending"] for e in _cache.values() if e["pending"]]
        wait(pending)
        with _cache_lock:
            records = [_enrich(f) for f in failed]
    with _cache_lock:
        _latest.update({r["id"]: r for r in records})
    return records


def _enrich(f: dict) -> dict:
    key = (f["dag"], f["task"], f["run_id"], f["try_number"])
    entry = _cache.get(key)
    if entry is None:
        if len(_cache) >= _CACHE_MAX:
            _cache.clear()
        log, error = read_task_log(f)
        entry = _cache[key] = {"log": log, "error": error, "files": read_sources(f["dag"]),
                               "diag": None, "failed": None, "pending": None}
    f = {**f, "log": entry["log"], "error": entry["error"]}
    # Identify a failure by what it *is*, not its position in the list — the UI
    # polls, and a positional id would make an open panel jump to another DAG.
    return {"id": f"{f['dag']}::{f['task']}", **f, **_diagnosis_state(f, entry)}


def _diagnosis_state(f: dict, entry: dict) -> dict:
    if entry["diag"]:
        return entry["diag"]
    if not os.environ.get("ANTHROPIC_API_KEY"):
        return _undiagnosed(f, "Automatic diagnosis is off — set ANTHROPIC_API_KEY in agent/.env to enable it.")
    failed = entry["failed"]
    if failed and time.monotonic() - failed[1] < RETRY_DIAGNOSIS_AFTER:
        return _undiagnosed(f, failed[0])
    if entry["pending"] is None or entry["pending"].done():
        entry["failed"] = None
        entry["pending"] = _executor.submit(_diagnose_in_background, f, entry)
    if entry["diag"]:  # an executor that runs inline (tests) has already finished
        return entry["diag"]
    if entry["failed"]:
        return _undiagnosed(f, entry["failed"][0])
    return {**_undiagnosed(f, f"Diagnosing with {diagnosis.MODEL}…"), "diagnosing": True}


def _diagnose_in_background(f: dict, entry: dict) -> None:
    try:
        diag = diagnosis.diagnose(f, entry["files"])
        diag["patchError"] = diagnosis.check_patch(diag["patch"], REPO_ROOT) if diag["patch"] else None
        diag["diagnosed"] = True
        diag["diagnosing"] = False
        with _usage_lock:
            diagnosis.record_usage(USAGE_LOG, f, diag)
        entry["diag"] = diag
    except diagnosis.DiagnosisError as e:
        entry["failed"] = (f"Could not auto-diagnose: {e}", time.monotonic())
    except Exception as e:  # never leave an entry stuck as "diagnosing"
        entry["failed"] = (f"Could not auto-diagnose: unexpected {type(e).__name__}: {e}", time.monotonic())


def _undiagnosed(f: dict, reason: str) -> dict:
    """What a failure looks like without a Claude diagnosis: the real exception, verbatim."""
    return {
        "type": "Unclassified", "cat": "", "confidence": "",
        "cause": {"engineer": f.get("error") or "see log", "analyst": reason},
        "impact": "", "fixSummary": "", "fixFile": "", "patch": "", "diff": [],
        "patchError": None, "prTitle": "", "prBranch": "", "usage": None,
        "diagnosed": False, "diagnosing": False,
    }


def _resolve_window(start: Optional[str], end: Optional[str]):
    """Turn two YYYY-MM-DD strings (local calendar days, inclusive) into a UTC window."""
    today = dt.datetime.now(LOCAL_TZ).date()
    try:
        first = dt.date.fromisoformat(start) if start else today
        last = dt.date.fromisoformat(end) if end else today
    except ValueError:
        raise HTTPException(status_code=400, detail="dates must be in YYYY-MM-DD form")
    if last < first:
        raise HTTPException(status_code=400, detail="the end date is before the start date")
    span = (last - first).days + 1
    if span > MAX_RANGE_DAYS:
        raise HTTPException(status_code=400,
                            detail=f"{span}-day range requested; the maximum is {MAX_RANGE_DAYS} days")
    lo = dt.datetime.combine(first, dt.time.min, tzinfo=LOCAL_TZ)
    hi = dt.datetime.combine(last, dt.time.max, tzinfo=LOCAL_TZ)
    return lo.astimezone(dt.timezone.utc), hi.astimezone(dt.timezone.utc)


def _iso(t: dt.datetime) -> str:
    return t.strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_ts(ts: Optional[str]) -> Optional[dt.datetime]:
    """Airflow timestamps are ISO-8601 UTC ('2026-09-21T18:13:28.738917Z'). Return local."""
    if not ts:
        return None
    return dt.datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(LOCAL_TZ)


def _fmt_duration(seconds: Optional[float]) -> str:
    if seconds is None:
        return ""
    m, s = divmod(int(round(seconds)), 60)
    return f"ran {m}m {s}s" if m else f"ran {s}s"


# ----------------------------------------------------------------------------------
# Dashboard — served from this app so its API calls are same-origin
# ----------------------------------------------------------------------------------
UI_FILE = REPO_ROOT / "ui" / "index.html"
_CONFIG_MARKER = "<!-- agent-config -->"


@app.get("/", response_class=HTMLResponse)
def dashboard():
    """Serve ui/index.html with the token injected. Keeping the token out of the file means
    a copy on disk carries no secret. This route is unauthenticated by necessity (a
    browser can't attach a header to an address-bar visit); the CORS allow-list is what
    stops a page on another origin from reading the token out of the response."""
    html = UI_FILE.read_text(encoding="utf-8")
    if _CONFIG_MARKER not in html:
        raise HTTPException(status_code=500, detail=f"{UI_FILE.name} is missing {_CONFIG_MARKER}")
    config = {
        "token": AGENT_TOKEN,
        "airflowUrl": AIRFLOW_URL,
        "env": ENV_NAME,
        "timezone": str(LOCAL_TZ),
        "prRepo": GITHUB_REPO,
        "maxRangeDays": MAX_RANGE_DAYS,
    }
    # json.dumps doesn't escape "</", which would let a value close the <script> early.
    payload = json.dumps(config).replace("</", "<\\/")
    return HTMLResponse(
        html.replace(_CONFIG_MARKER, f"<script>window.__AGENT_CONFIG = {payload};</script>", 1),
        headers={"Cache-Control": "no-store"},
    )


# ----------------------------------------------------------------------------------
# HTTP endpoints
# ----------------------------------------------------------------------------------
@app.get("/failures")
def failures(start: Optional[str] = None, end: Optional[str] = None,
             _auth: None = Depends(require_token)):
    """start/end are inclusive local calendar days (YYYY-MM-DD); both default to today."""
    lo, hi = _resolve_window(start, end)
    try:
        return collect_failures(lo, hi)
    except AirflowError as e:
        raise HTTPException(status_code=502, detail=str(e))


def pr_blocker(f: dict) -> Optional[str]:
    """Why a failure can't have a PR, or None. Enforced here, not just in the UI: PRs stay
    gated to High-confidence fixes whose patch applies; everything else is a suggestion."""
    if not GITHUB_REPO:
        return "set GITHUB_REPO=owner/name to enable PRs"
    if not f.get("diagnosed"):
        return "this failure hasn't been diagnosed"
    if not f.get("patch"):
        return "no code change was suggested"
    if f.get("confidence") != "High":
        return "only High-confidence fixes can open a PR"
    if f.get("patchError"):
        return "the suggested patch doesn't apply to the current code"
    if not _BRANCH_RE.match(f.get("prBranch") or ""):
        return f"invalid branch name: {f.get('prBranch')!r}"
    return None


@app.post("/pull-requests")
async def pull_request(req: Request, _auth: None = Depends(require_token)):
    """Open a PR for a failure the agent has already diagnosed. The client names the
    failure; the patch, title and branch come from the agent's own diagnosis, so a caller
    can't push arbitrary content through this endpoint."""
    body = await req.json()
    f = _latest.get(body.get("id") or "")
    if f is None:
        raise HTTPException(status_code=404, detail="unknown failure id — refresh the dashboard")
    why = pr_blocker(f)
    if why:
        raise HTTPException(status_code=409, detail=why)
    try:
        return _open_pull_request(f)
    except (subprocess.CalledProcessError, RuntimeError) as e:
        detail = getattr(e, "stderr", None) or str(e)
        raise HTTPException(status_code=502, detail=detail.strip()[-300:])


def _sh(cmd: list) -> str:
    return subprocess.run(cmd, capture_output=True, text=True, check=True).stdout


def _open_pull_request(f: dict) -> dict:
    workdir = tempfile.mkdtemp(prefix="pipeline-triage-pr-")
    try:
        _sh(["gh", "repo", "clone", GITHUB_REPO, workdir])
        _sh(["git", "-C", workdir, "checkout", "-b", f["prBranch"]])
        # The same `git apply` that vetted the patch at diagnosis time, now for real.
        # --index stages exactly the files the patch touches.
        err = diagnosis.check_patch(f["patch"], Path(workdir), index=True)
        if err:
            raise RuntimeError(f"patch no longer applies to {GITHUB_REPO}: {err}")
        _sh(["git", "-C", workdir, "commit", "-m", f["prTitle"]])
        _sh(["git", "-C", workdir, "push", "-u", "origin", f["prBranch"]])
        url = _sh(["gh", "pr", "create", "--repo", GITHUB_REPO, "--title", f["prTitle"],
                   "--body", _pr_body(f), "--head", f["prBranch"]]).strip()
    finally:
        shutil.rmtree(workdir, ignore_errors=True)
    number = int(url.rstrip("/").split("/")[-1]) if url else 0
    return {"number": number, "url": url}


def _pr_body(f: dict) -> str:
    return f"""Opened by Pipeline Triage Agent for a failure in `{f['dag']}` / `{f['task']}` \
(run `{f['run_id']}`). **Requires human review — nothing merges automatically.**

**Error:** `{f.get('error') or 'see task log'}`

**Root cause ({f['confidence']} confidence):** {f['cause']['engineer']}

**In plain terms:** {f['cause']['analyst']}

**Impact:** {f.get('impact') or 'n/a'}

**Fix:** {f.get('fixSummary') or 'n/a'}
"""


# ----------------------------------------------------------------------------------
# CLI — `python -m agent.main` prints failures without starting the server
# ----------------------------------------------------------------------------------
def _cli() -> int:
    ap = argparse.ArgumentParser(description="List failed Airflow tasks and their real errors.")
    ap.add_argument("--start", help="first local day, YYYY-MM-DD (default today)")
    ap.add_argument("--end", help="last local day, YYYY-MM-DD (default today)")
    ap.add_argument("--json", action="store_true", help="print the raw /failures JSON")
    ap.add_argument("--usage", action="store_true",
                    help=f"summarize token usage and cost from {USAGE_LOG.name}, then exit")
    args = ap.parse_args()

    if args.usage:
        print(diagnosis.summarize_usage(USAGE_LOG))
        return 0
    try:
        lo, hi = _resolve_window(args.start, args.end)
        found = collect_failures(lo, hi, wait_for_diagnosis=True)
    except HTTPException as e:
        print(f"error: {e.detail}", file=sys.stderr)
        return 2
    except AirflowError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1

    if args.json:
        print(json.dumps(found, indent=2))
        return 0
    print(f"{len(found)} failing task(s) in Airflow at {AIRFLOW_URL}\n")
    for f in found:
        times = f" ×{f['occurrences']}" if f["occurrences"] > 1 else ""
        print(f"● {f['dag']} :: {f['task']}  [{f['day']} {f['time']}, {f['duration']}{times}]")
        for l in f["log"]:
            print(f"    {l['sev']:>4}  {l['t']}")
        if f["diagnosed"]:
            print(f"    diagnosis: {f['type']} ({f['confidence']}) — {f['cause']['engineer']}")
            print(f"    fix: {f['fixSummary']}" + (f"  [{f['fixFile']}]" if f["fixFile"] else ""))
            if f["patchError"]:
                print(f"    patch does not apply: {f['patchError']}")
        else:
            print(f"    diagnosis: {f['cause']['analyst']}")
        print()
    return 0


if __name__ == "__main__":
    sys.exit(_cli())
