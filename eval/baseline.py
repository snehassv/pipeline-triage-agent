"""The agent as it was before the root-cause rule — the baseline arm of the study.

Copied verbatim from agent/diagnosis.py at commit 28b69ef^ (the commit before
"Refuse PRs unless the root cause is in code or config"), so the ablation compares
the two versions that actually existed rather than a reconstruction of the first.

Under this prompt the model may return a patch for any failure, and a patch was
offered as a PR whenever it was High confidence and applied cleanly.
"""

CATEGORIES = ["schema", "gcs", "sensor", "table", "dq", "merge", "auth"]

FIX_SCHEMA = {
    "type": "object",
    "properties": {
        "type": {"type": "string", "description": "2-4 word failure class, e.g. 'Schema drift'"},
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
- type: a 2-4 word failure class shown as a badge, e.g. "Schema drift", "Duplicate source keys", "Missing table".
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
