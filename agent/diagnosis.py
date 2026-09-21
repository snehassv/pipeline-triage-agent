"""Diagnose a failed Airflow task with Claude, and handle the patch it proposes.

The response shape is enforced by structured outputs (a JSON schema on the request), so
a reply is either valid JSON matching FIX_SCHEMA or an explicit error — never free text
that has to be scraped. The fix comes back as a git-format unified diff, which we check
with `git apply --check` before anyone is offered a PR for it.
"""

import datetime as dt
import json
import os
import subprocess
import time
from pathlib import Path
from typing import Optional

MODEL = os.environ.get("AGENT_MODEL", "claude-sonnet-5")
MAX_SOURCE_CHARS = 60_000

# USD per million tokens (input, output). Used only to report cost; unknown models
# report tokens without a dollar figure rather than a guessed one.
PRICES = {
    "claude-sonnet-5": (2.00, 10.00),
    "claude-opus-5": (5.00, 25.00),
    "claude-haiku-4-5": (1.00, 5.00),
}

CATEGORIES = ["schema", "gcs", "sensor", "table", "dq", "merge", "auth"]

FIX_SCHEMA = {
    "type": "object",
    "properties": {
        "type": {"type": "string", "description": "short failure class, e.g. 'Schema drift'"},
        "cat": {"type": "string", "enum": CATEGORIES},
        "confidence": {"type": "string", "enum": ["High", "Medium", "Low"]},
        "cause": {
            "type": "object",
            "properties": {
                "engineer": {"type": "string"},
                "analyst": {"type": "string"},
            },
            "required": ["engineer", "analyst"],
            "additionalProperties": False,
        },
        "impact": {"type": "string"},
        "fixSummary": {"type": "string"},
        "patch": {"type": "string"},
        "prTitle": {"type": "string"},
        "prBranch": {"type": "string"},
    },
    "required": ["type", "cat", "confidence", "cause", "impact", "fixSummary",
                 "patch", "prTitle", "prBranch"],
    "additionalProperties": False,
}

SYSTEM = """You are the on-call data engineer for an Airflow data platform backed by a Postgres warehouse. \
You diagnose failed tasks from their logs and source code, explain the root cause to two audiences, \
and propose the smallest correct fix.

Guidelines:
- cause.engineer: the technical root cause in 1-2 sentences, naming the table, column, file or setting involved.
- cause.analyst: the same thing in plain language for a data analyst who doesn't read tracebacks, 1-2 sentences.
- impact: one sentence on what data or reports are stale or wrong because of this failure.
- patch: a git-format unified diff (paths prefixed a/ and b/, relative to the repository root) against \
the files exactly as shown, with three lines of context. Only propose a patch when changing code or \
config is the right remedy. When the real problem is upstream data or the environment — duplicate keys \
in a source table, a file that never arrived, a data-quality threshold doing its job — leave patch empty \
and say in fixSummary what a person should check or do instead. Never loosen a check just to make it pass.
- confidence: High only when the evidence pins down the root cause and the patch (or the recommended \
action) fully resolves it; Medium when the cause is likely but not certain; Low otherwise.
- prTitle: a conventional-commit style title. prBranch: agent/fix-<short-slug>, lowercase, no spaces."""


class DiagnosisError(RuntimeError):
    """Claude couldn't produce a usable diagnosis. The message is shown to the user."""


def build_prompt(failure: dict, files: dict) -> str:
    log_text = "\n".join(l["t"] for l in failure["log"])
    sources = []
    for path, text in files.items():
        if len(text) > MAX_SOURCE_CHARS:
            text = text[:MAX_SOURCE_CHARS] + f"\n[... truncated: file is {len(text)} characters ...]"
        sources.append(f'<file path="{path}">\n{text}\n</file>')
    return f"""A task failed. Diagnose it and propose a fix.

DAG: {failure['dag']}
Failed task: {failure['task']} ({failure['operator']})
Environment: {failure['env']}
Failed runs in this window: {failure.get('occurrences', 1)}

<task_log>
{log_text}
</task_log>

{chr(10).join(sources)}"""


def diagnose(failure: dict, files: dict, client=None) -> dict:
    """Return the diagnosis fields for a failure record, plus `usage`. Raises DiagnosisError."""
    import anthropic
    client = client or anthropic.Anthropic()  # credentials from ANTHROPIC_API_KEY

    started = time.monotonic()
    try:
        response = client.messages.create(
            model=MODEL,
            max_tokens=16000,
            system=SYSTEM,
            messages=[{"role": "user", "content": build_prompt(failure, files)}],
            output_config={"format": {"type": "json_schema", "schema": FIX_SCHEMA}},
        )
    except anthropic.AuthenticationError:
        raise DiagnosisError("Anthropic rejected the API key — check ANTHROPIC_API_KEY in agent/.env.")
    except anthropic.RateLimitError:
        raise DiagnosisError("Rate limited by the Anthropic API — will retry shortly.")
    except anthropic.APIStatusError as e:
        raise DiagnosisError(f"Anthropic API error {e.status_code}: {e.message}")
    except anthropic.APIConnectionError:
        raise DiagnosisError("Couldn't reach the Anthropic API.")
    seconds = time.monotonic() - started

    if response.stop_reason == "refusal":
        raise DiagnosisError("The model declined to diagnose this failure.")
    if response.stop_reason == "max_tokens":
        raise DiagnosisError("The diagnosis was cut off before it finished.")
    text = next((b.text for b in response.content if b.type == "text"), "")
    try:
        result = json.loads(text)
    except ValueError:
        raise DiagnosisError("The model's reply wasn't valid JSON.")

    patch = result.pop("patch", "").strip()
    patch = patch + "\n" if patch else ""
    result.update({
        "patch": patch,
        "diff": patch_to_diff(patch),
        "fixFile": ", ".join(files_in_patch(patch)),
        "usage": _usage(response, seconds),
    })
    return result


def _usage(response, seconds: float) -> dict:
    u = response.usage
    tokens_in, tokens_out = u.input_tokens, u.output_tokens
    price = PRICES.get(response.model) or PRICES.get(MODEL)
    cost = (tokens_in * price[0] + tokens_out * price[1]) / 1e6 if price else None
    return {
        "model": response.model,
        "input_tokens": tokens_in,
        "output_tokens": tokens_out,
        "cost_usd": round(cost, 5) if cost is not None else None,
        "seconds": round(seconds, 1),
    }


# ----------------------------------------------------------------------------------
# Patches
# ----------------------------------------------------------------------------------
def files_in_patch(patch: str) -> list:
    """Repo-relative paths a patch changes, from its `+++ b/...` lines."""
    return [line[6:].strip() for line in patch.splitlines()
            if line.startswith("+++ b/")]


def patch_to_diff(patch: str) -> list:
    """Turn a unified diff into the UI's line list: {t: file|hunk|add|del|ctx, s: text}."""
    out = []
    for line in patch.splitlines():
        if line.startswith("+++ b/"):
            out.append({"t": "file", "s": line[6:]})
        elif line.startswith(("diff --git", "index ", "--- ", "+++ ", "new file", "deleted file")):
            continue
        elif line.startswith("@@"):
            out.append({"t": "hunk", "s": line})
        elif line.startswith("+"):
            out.append({"t": "add", "s": line[1:]})
        elif line.startswith("-"):
            out.append({"t": "del", "s": line[1:]})
        else:
            out.append({"t": "ctx", "s": line[1:] if line.startswith(" ") else line})
    return out


def check_patch(patch: str, repo_dir: Path, index: bool = False) -> Optional[str]:
    """Apply `patch` in `repo_dir`. With index=False it's a dry run (`--check`). Returns
    None on success, or git's error text. `--recount` tolerates slightly-off hunk line
    counts, which models get wrong more often than the context lines themselves."""
    args = ["git", "-C", str(repo_dir), "apply", "--recount"]
    args += ["--index"] if index else ["--check"]
    proc = subprocess.run(args, input=patch, capture_output=True, text=True)
    if proc.returncode == 0:
        return None
    return (proc.stderr or proc.stdout or "git apply failed").strip()[:500]


# ----------------------------------------------------------------------------------
# Usage log — one JSON line per diagnosis, so cost claims come from real numbers
# ----------------------------------------------------------------------------------
def record_usage(path: Path, failure: dict, diag: dict) -> None:
    row = {
        "at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
        "dag": failure["dag"], "task": failure["task"], "run_id": failure["run_id"],
        "confidence": diag.get("confidence"), "has_patch": bool(diag.get("patch")),
        **diag["usage"],
    }
    with path.open("a") as fh:
        fh.write(json.dumps(row) + "\n")


def summarize_usage(path: Path) -> str:
    if not path.exists():
        return "No diagnoses recorded yet."
    rows = [json.loads(l) for l in path.read_text().splitlines() if l.strip()]
    if not rows:
        return "No diagnoses recorded yet."
    lines = [f"{len(rows)} diagnoses recorded in {path.name}\n"]
    for model in sorted({r["model"] for r in rows}):
        rs = [r for r in rows if r["model"] == model]
        n = len(rs)
        tin = sum(r["input_tokens"] for r in rs)
        tout = sum(r["output_tokens"] for r in rs)
        costs = [r["cost_usd"] for r in rs if r.get("cost_usd") is not None]
        secs = sum(r["seconds"] for r in rs)
        cost_txt = (f"${sum(costs):.4f} total, ${sum(costs) / len(costs):.4f} per diagnosis"
                    if costs else "cost unknown for this model")
        lines.append(
            f"{model}: {n} diagnoses · {tin:,} in / {tout:,} out tokens "
            f"(avg {tin // n:,} / {tout // n:,}) · {cost_txt} · avg {secs / n:.1f}s"
        )
    return "\n".join(lines)
