#!/usr/bin/env python3
"""
Pipeline Triage Agent — backend
================================
Reads failed task instances from a local Airflow 3 deployment over its REST API,
pulls each failure's real task log and DAG source, and (when an Anthropic API key is
set) asks Claude for a diagnosis and a suggested fix.

    GET  /failures?start=YYYY-MM-DD&end=YYYY-MM-DD  -> failed tasks in the window, diagnosed
    POST /pull-requests                             -> opens a PR with the suggested fix

Pipeline, per request to GET /failures:
    1. list failures  — GET /api/v2/dags/~/dagRuns/~/taskInstances?state=failed
    2. read the log   — GET .../taskInstances/{task}/logs/{try}  (structured JSON events)
    3. read the code  — GET /api/v2/dagSources/{dag_id} + the DAG's entry in pipelines.yaml
    4. diagnose       — Claude API; skipped (raw error shown instead) without ANTHROPIC_API_KEY

Run it (from the repo root, with `docker compose up -d` already done):
    uvicorn agent.main:app --port 8787
    python -m agent.main                 # or: print today's failures to the terminal, no server

Configuration (environment variables, or the repo's .env file):
    AIRFLOW_URL        default http://localhost:8080
    AIRFLOW_USERNAME   default airflow
    AIRFLOW_PASSWORD   default airflow
    AGENT_ENV          environment label shown in the UI (dev|qa|prd), default dev
    AGENT_TZ           timezone the UI's calendar days are in, default America/Chicago
    AGENT_TOKEN        shared secret for the X-Agent-Token header; generated if unset
    GITHUB_REPO        owner/name the PR endpoint targets; PRs are disabled if unset
    ANTHROPIC_API_KEY  turns on diagnosis

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
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo

import requests
import yaml
from fastapi import Depends, FastAPI, Header, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware

REPO_ROOT = Path(__file__).resolve().parent.parent


def _load_dotenv(path: Path) -> None:
    """Minimal .env reader so the agent picks up the same file docker compose uses.
    Real environment variables win over the file."""
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


_load_dotenv(REPO_ROOT / ".env")

AIRFLOW_URL      = os.environ.get("AIRFLOW_URL", "http://localhost:8080").rstrip("/")
AIRFLOW_USERNAME = os.environ.get("AIRFLOW_USERNAME", "airflow")
AIRFLOW_PASSWORD = os.environ.get("AIRFLOW_PASSWORD", "airflow")
ENV_NAME         = os.environ.get("AGENT_ENV", "dev")
LOCAL_TZ         = ZoneInfo(os.environ.get("AGENT_TZ", "America/Chicago"))
GITHUB_REPO      = os.environ.get("GITHUB_REPO", "")  # owner/name
PIPELINES_CONFIG = REPO_ROOT / "dags" / "config" / "pipelines.yaml"
MAX_RANGE_DAYS   = 7      # a wider window multiplies the per-poll diagnosis cost
MAX_LOG_LINES    = 12

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


def require_token(x_agent_token: str = Header(default="")) -> None:
    if not secrets.compare_digest(x_agent_token, AGENT_TOKEN):
        raise HTTPException(status_code=401, detail="missing/invalid X-Agent-Token header")


# ----------------------------------------------------------------------------------
# Airflow REST API client
# ----------------------------------------------------------------------------------
class AirflowError(RuntimeError):
    """Airflow was unreachable or rejected a request. The message is shown to the user."""


class AirflowClient:
    """Thin wrapper over the Airflow 3 REST API (/api/v2). Airflow 3 authenticates with a
    short-lived JWT from POST /auth/token; we fetch one lazily and refresh it on a 401."""

    def __init__(self, base_url: str, username: str, password: str):
        self.base_url = base_url
        self.username = username
        self.password = password
        self._token: Optional[str] = None
        self._session = requests.Session()

    def _login(self) -> str:
        try:
            r = self._session.post(
                f"{self.base_url}/auth/token",
                json={"username": self.username, "password": self.password},
                timeout=10,
            )
        except requests.ConnectionError:
            raise AirflowError(
                f"can't reach Airflow at {self.base_url} — is `docker compose up -d` running?"
            )
        if r.status_code in (401, 403):
            raise AirflowError("Airflow rejected the username/password (AIRFLOW_USERNAME / AIRFLOW_PASSWORD)")
        r.raise_for_status()
        return r.json()["access_token"]

    def get(self, path: str, params: Optional[dict] = None, retry: bool = True) -> dict:
        if self._token is None:
            self._token = self._login()
        try:
            r = self._session.get(
                f"{self.base_url}{path}",
                params=params,
                headers={"Authorization": f"Bearer {self._token}", "Accept": "application/json"},
                timeout=30,
            )
        except requests.ConnectionError:
            raise AirflowError(f"lost connection to Airflow at {self.base_url}")
        if r.status_code == 401 and retry:
            self._token = None  # expired — log in again once
            return self.get(path, params, retry=False)
        if r.status_code == 404:
            raise KeyError(path)
        if not r.ok:
            raise AirflowError(f"Airflow {r.status_code} on {path}: {r.text[:300]}")
        return r.json()


airflow = AirflowClient(AIRFLOW_URL, AIRFLOW_USERNAME, AIRFLOW_PASSWORD)


# ----------------------------------------------------------------------------------
# STEP 1 — list failed task instances in the window
# ----------------------------------------------------------------------------------
def read_failed_tasks(lo: dt.datetime, hi: dt.datetime) -> list:
    """Failed task instances whose end_date falls in [lo, hi], newest first, grouped by
    (dag, task) so a task that failed on three runs shows up once — with its latest log."""
    instances, offset = [], 0
    while True:
        page = airflow.get("/api/v2/dags/~/dagRuns/~/taskInstances", params={
            "state": "failed",
            "end_date_gte": _iso(lo),
            "end_date_lte": _iso(hi),
            "order_by": "-end_date",
            "limit": 100,
            "offset": offset,
        })
        batch = page.get("task_instances", [])
        instances.extend(batch)
        offset += len(batch)
        if not batch or offset >= page.get("total_entries", 0):
            break

    grouped: dict = {}
    for ti in instances:
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
# STEP 2 — the task's real log, reduced to the lines that explain the failure
# ----------------------------------------------------------------------------------
def read_task_log(f: dict) -> list:
    path = (f"/api/v2/dags/{f['dag']}/dagRuns/{requests.utils.quote(f['run_id'], safe='')}"
            f"/taskInstances/{f['task']}/logs/{f['try_number']}")
    try:
        content = airflow.get(path, params={"full_content": "true"}).get("content", [])
    except KeyError:
        return [{"sev": "warn", "t": "task log not found (it may have been cleaned up)"}]
    if isinstance(content, str):
        return _extract_from_text_log(content)
    return _extract_from_structured_log(content)


def _extract_from_structured_log(events: list) -> list:
    """Airflow 3 task logs are a list of structlog events. The failure event carries an
    `error_detail` with the exception type, message and stack frames — that's the part
    worth showing. We also keep the couple of info lines right before the failure (e.g.
    the SQL statement that was running), since they're usually the missing context."""
    lines: list = []
    recent_info: list = []
    for e in events:
        text = str(e.get("event", ""))
        level = e.get("level", "")
        if not text or text.startswith("::"):
            continue
        if level not in ("error", "critical", "warning"):
            if level == "info":
                recent_info = (recent_info + [text])[-2:]
            continue
        if level != "warning":
            lines.extend({"sev": "info", "t": t} for t in recent_info)
            recent_info = []
        lines.append({"sev": "err" if level != "warning" else "warn", "t": text})
        for exc in e.get("error_detail") or []:
            lines.append({"sev": "err", "t": f"{exc.get('exc_type')}: {exc.get('exc_value')}"})
            frames = exc.get("frames") or []
            # Frames from the DAG code are the ones a fix would touch; fall back to the
            # innermost frames when the error is raised entirely inside libraries.
            own = [fr for fr in frames if "/dags/" in fr.get("filename", "")]
            for fr in (own or frames)[-3:]:
                lines.append({"sev": "warn",
                              "t": f"  at {fr.get('filename')}:{fr.get('lineno')} in {fr.get('name')}"})
    if not lines:
        lines = [{"sev": "warn", "t": "no error-level lines in the task log"}]
    return [{"sev": l["sev"], "t": l["t"][:400]} for l in lines[-MAX_LOG_LINES:]]


def _extract_from_text_log(text: str) -> list:
    """Fallback for plain-text logs (older Airflow / other log handlers)."""
    raw = text.splitlines()
    start = next((i for i, l in enumerate(raw) if "Traceback" in l or " ERROR " in l), None)
    picked = raw[start:] if start is not None else raw[-MAX_LOG_LINES:]
    return [{"sev": "err" if ("ERROR" in l or "Error" in l) else "warn", "t": l[:400]}
            for l in picked[-MAX_LOG_LINES:] if l.strip()]


# ----------------------------------------------------------------------------------
# STEP 3 — the code Claude needs to reason about a fix
# ----------------------------------------------------------------------------------
def read_dag_source(dag_id: str) -> tuple:
    """Return (repo-relative path, source text). DAGs here come from dag_factory.py, so the
    factory alone doesn't say what a given DAG does — its pipelines.yaml entry does. We
    send both."""
    try:
        details = airflow.get(f"/api/v2/dags/{dag_id}")
        path = f"dags/{details.get('relative_fileloc') or dag_id + '.py'}"
        source = airflow.get(f"/api/v2/dagSources/{dag_id}").get("content", "")
    except (KeyError, AirflowError):
        path, source = f"dags/{dag_id}.py", ""

    entry = _pipeline_config_entry(dag_id)
    if entry:
        source += (f"\n\n# --- dags/config/pipelines.yaml entry for {dag_id} ---\n"
                   + yaml.safe_dump(entry, sort_keys=False))
    return path, source


def _pipeline_config_entry(dag_id: str) -> Optional[dict]:
    if not PIPELINES_CONFIG.exists():
        return None
    with PIPELINES_CONFIG.open() as fh:
        pipelines = (yaml.safe_load(fh) or {}).get("pipelines", [])
    return next((p for p in pipelines if p.get("dag_id") == dag_id), None)


# ----------------------------------------------------------------------------------
# STEP 4 — diagnose with Claude: root cause (two audiences) + a suggested diff
# ----------------------------------------------------------------------------------
def diagnose(failure: dict, fix_file: str, dag_source: str) -> dict:
    import anthropic
    client = anthropic.Anthropic()  # reads ANTHROPIC_API_KEY

    log_text = "\n".join(l["t"] for l in failure["log"])
    prompt = f"""You are the on-call data engineer for an Airflow data platform backed by a Postgres warehouse.

An Airflow task failed. Diagnose it and propose a minimal fix.

DAG: {failure['dag']}
Failed task: {failure['task']} ({failure['operator']})
Environment: {failure['env']}

--- Task log excerpt ---
{log_text}

--- DAG source ({fix_file}) ---
{dag_source[:8000]}

Respond ONLY with JSON matching this schema (no prose):
{{
  "type": "short failure class, e.g. 'Schema drift'",
  "cat": "one of: schema|gcs|sensor|table|dq|merge|auth",
  "confidence": "High|Medium|Low",
  "cause": {{
     "engineer": "technical root cause, 1-2 sentences",
     "analyst": "plain-language explanation for a data analyst, 1-2 sentences"
  }},
  "impact": "one sentence on business/data impact",
  "fixSummary": "one sentence describing the fix",
  "fixFile": "repo-relative path to change",
  "diff": [ {{"t":"ctx|add|del","s":"line of code"}} ],
  "prTitle": "conventional-commit style PR title",
  "prBranch": "agent/fix-<slug>"
}}"""

    msg = client.messages.create(
        model="claude-sonnet-4-5",
        max_tokens=1500,
        messages=[{"role": "user", "content": prompt}],
    )
    text = msg.content[0].text.strip()
    if text.startswith("```"):
        text = text.split("```")[1].removeprefix("json").strip()
    return json.loads(text)


def _undiagnosed(failure: dict, fix_file: str, reason: str) -> dict:
    """What a failure looks like without a Claude diagnosis: the real exception, verbatim."""
    errors = [l["t"] for l in failure["log"] if l["sev"] == "err"]
    return {
        "type": "Unclassified", "cat": "", "confidence": "Low",
        "cause": {"engineer": errors[-1] if errors else "see log", "analyst": reason},
        "impact": "", "fixSummary": "", "fixFile": fix_file, "diff": [],
        "prTitle": "", "prBranch": "",
    }


# ----------------------------------------------------------------------------------
# Orchestration
# ----------------------------------------------------------------------------------
def collect_failures(lo: dt.datetime, hi: dt.datetime) -> list:
    out = []
    for f in read_failed_tasks(lo, hi):
        f["log"] = read_task_log(f)
        path, src = read_dag_source(f["dag"])
        if not os.environ.get("ANTHROPIC_API_KEY"):
            diag = _undiagnosed(f, path, "Automatic diagnosis is off — set ANTHROPIC_API_KEY to enable it.")
        else:
            try:
                diag = diagnose(f, path, src)
            except Exception as e:
                diag = _undiagnosed(f, path, f"Could not auto-diagnose: {e}")
        # Identify a failure by what it *is*, not its position in the list — the UI
        # polls, and a positional id would make an open panel jump to another DAG.
        out.append({"id": f"{f['dag']}::{f['task']}", **f, **diag})
    return out


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


@app.post("/pull-requests")
async def pull_request(req: Request, _auth: None = Depends(require_token)):
    if not GITHUB_REPO:
        raise HTTPException(status_code=503, detail="set GITHUB_REPO=owner/name to enable PRs")
    body = await req.json()
    branch, path, title = body["branch"], body["file"], body["title"]
    diff = body.get("diff", [])

    if not _BRANCH_RE.match(branch or ""):
        raise HTTPException(status_code=400, detail=f"invalid branch name: {branch!r}")
    normalized = os.path.normpath(path or "")
    if os.path.isabs(path or "") or normalized in ("", ".") or normalized.split(os.sep)[0] == "..":
        raise HTTPException(status_code=400, detail=f"invalid file path: {path!r}")

    try:
        return _open_pull_request(branch, path, title, diff)
    except subprocess.CalledProcessError as e:
        raise HTTPException(status_code=502, detail=(e.stderr or str(e))[-300:])


def _sh(cmd: list) -> str:
    return subprocess.run(cmd, capture_output=True, text=True, check=True).stdout


def _open_pull_request(branch: str, path: str, title: str, diff: list) -> dict:
    workdir = os.path.join(tempfile.gettempdir(), "pipeline-triage-agent-pr")
    shutil.rmtree(workdir, ignore_errors=True)
    _sh(["gh", "repo", "clone", GITHUB_REPO, workdir])
    _sh(["git", "-C", workdir, "checkout", "-b", branch])

    # POC shortcut: replace each `del` line with its paired `add` line. A production
    # version should have Claude emit a real patch and pipe it through `git apply`.
    fp = os.path.join(workdir, path)
    with open(fp) as fh:
        content = fh.read()
    dels = [d["s"] for d in diff if d["t"] == "del"]
    adds = [d["s"] for d in diff if d["t"] == "add"]
    for old, new in zip(dels, adds):
        content = content.replace(old.strip(), new.strip(), 1)
    with open(fp, "w") as fh:
        fh.write(content)

    _sh(["git", "-C", workdir, "add", path])
    _sh(["git", "-C", workdir, "commit", "-m", title])
    _sh(["git", "-C", workdir, "push", "-u", "origin", branch])
    url = _sh(["gh", "pr", "create", "--repo", GITHUB_REPO, "--title", title,
               "--body", "Opened by Pipeline Triage Agent. Requires human review.",
               "--head", branch]).strip()
    number = int(url.rstrip("/").split("/")[-1]) if url else 0
    return {"number": number, "url": url}


# ----------------------------------------------------------------------------------
# CLI — `python -m agent.main` prints failures without starting the server
# ----------------------------------------------------------------------------------
def _cli() -> int:
    ap = argparse.ArgumentParser(description="List failed Airflow tasks and their real errors.")
    ap.add_argument("--start", help="first local day, YYYY-MM-DD (default today)")
    ap.add_argument("--end", help="last local day, YYYY-MM-DD (default today)")
    ap.add_argument("--json", action="store_true", help="print the raw /failures JSON")
    args = ap.parse_args()

    try:
        lo, hi = _resolve_window(args.start, args.end)
        found = collect_failures(lo, hi)
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
        print(f"    diagnosis: {f['type']} — {f['cause']['analyst']}\n")
    return 0


if __name__ == "__main__":
    sys.exit(_cli())
