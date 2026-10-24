"""Would this diagnosis have been offered as a pull request?

Each arm is scored by its own rules, because that's the comparison: what each version of
the agent would actually have let through, given the same failure.

    baseline  (before the root-cause rule): High confidence + a patch that applies.
    gated     (current): the same, plus rootCause in {code, config} and the DDL-only
              migration policy.

`main.pr_blocker` is the live implementation of the gated rules; this module mirrors it
so the study can score both arms offline, and a test asserts the two agree.
"""

from typing import Optional

PATCHABLE_ROOT_CAUSES = {"code", "config"}


def baseline_blocker(row: dict) -> Optional[str]:
    if not row.get("patch"):
        return "no code change was suggested"
    if row.get("confidence") != "High":
        return "only High-confidence fixes can open a PR"
    if row.get("patch_error"):
        return "the suggested patch doesn't apply to the current code"
    return None


def gated_blocker(row: dict) -> Optional[str]:
    if row.get("root_cause") not in PATCHABLE_ROOT_CAUSES:
        where = row.get("root_cause") or "unknown"
        return f"the root cause is in the {where}, not the code"
    if row.get("policy_error"):
        return f"the patch breaks the migration rules: {row['policy_error']}"
    return baseline_blocker(row)


BLOCKERS = {"baseline": baseline_blocker, "gated": gated_blocker}


def pr_eligible(row: dict) -> bool:
    """True if this arm would have offered the diagnosis as a PR."""
    return BLOCKERS[row["condition"]](row) is None
