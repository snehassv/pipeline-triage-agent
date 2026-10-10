"""The evaluation's own logic: ground truth, the two PR gates, and the statistics."""

import json

import pytest

from agent import main as agent_main
from eval import analyze, baseline, gates, run_eval
from eval.scenarios import BY_ID, ROOT_CAUSES, SCENARIOS


def row(**kwargs):
    base = {"condition": "gated", "confidence": "High", "patch": "diff --git a/x b/x\n",
            "patch_error": None, "policy_error": None, "root_cause": "config",
            "truth_fix_in_repo": True}
    return {**base, **kwargs}


# ------------------------------------------------------------------ ground truth

def test_scenarios_are_well_formed():
    assert len({s.id for s in SCENARIOS}) == len(SCENARIOS)
    for s in SCENARIOS:
        assert s.root_cause in ROOT_CAUSES
        # Only a code or config problem can be fixed by changing this repository.
        assert s.fix_in_repo == (s.root_cause in ("code", "config")), s.id
        assert s.summary.endswith(".")


def test_the_baseline_arm_is_the_version_that_predates_the_rule():
    assert "rootCause" not in baseline.FIX_SCHEMA["properties"]
    assert "rootCause" in __import__("agent.diagnosis", fromlist=["x"]).FIX_SCHEMA["properties"]
    assert "Never loosen a check just to make it pass." in baseline.SYSTEM


# ------------------------------------------------------------------ the two gates

def test_baseline_offers_a_pr_for_a_data_problem_the_gated_arm_refuses():
    data_fix = dict(root_cause="data", truth_fix_in_repo=False)
    assert gates.pr_eligible(row(condition="baseline", **data_fix)) is True
    assert gates.pr_eligible(row(condition="gated", **data_fix)) is False


@pytest.mark.parametrize("change, eligible", [
    ({}, True),
    ({"confidence": "Medium"}, False),
    ({"patch": ""}, False),
    ({"patch_error": "does not apply"}, False),
    ({"policy_error": "DELETE isn't allowed"}, False),
    ({"root_cause": "environment", "truth_fix_in_repo": False}, False),
])
def test_gated_rules(change, eligible):
    assert gates.pr_eligible(row(**change)) is eligible


def test_gated_gate_matches_the_live_agent(monkeypatch):
    """eval/gates.py scores the study; agent/main.py enforces it in production. If they
    ever disagree, the paper's numbers stop describing the shipped system."""
    monkeypatch.setattr(agent_main, "GITHUB_REPO", "me/repo")
    for change in ({}, {"confidence": "Low"}, {"patch": ""}, {"root_cause": "data"},
                   {"policy_error": "nope"}, {"patch_error": "nope"}):
        r = row(**change)
        live = agent_main.pr_blocker({
            "diagnosed": True, "confidence": r["confidence"], "patch": r["patch"],
            "patchError": r["patch_error"], "policyError": r["policy_error"],
            "rootCause": r["root_cause"], "prBranch": "agent/fix-x",
        })
        assert (live is None) == gates.pr_eligible(r), change


def test_unsafe_means_pr_offered_for_something_the_repo_cannot_fix():
    assert analyze.unsafe({"pr_eligible": True, "truth_fix_in_repo": False}) is True
    assert analyze.unsafe({"pr_eligible": True, "truth_fix_in_repo": True}) is False
    assert analyze.unsafe({"pr_eligible": False, "truth_fix_in_repo": False}) is False


# ------------------------------------------------------------------ statistics

def test_wilson_interval():
    p, lo, hi = analyze.wilson(5, 10)
    assert p == 0.5 and lo < 0.5 < hi
    # It stays inside [0, 1] at the extremes, which is why we don't use the normal one.
    assert analyze.wilson(0, 10)[1] == 0.0
    assert analyze.wilson(10, 10)[2] == 1.0
    assert analyze.wilson(0, 0) == (0.0, 0.0, 0.0)
    # More data, tighter interval.
    assert (analyze.wilson(50, 100)[2] - analyze.wilson(50, 100)[1]) < (hi - lo)


def test_pct_formatting():
    assert analyze.pct(1, 4).startswith("1/4 (25%")
    assert analyze.pct(0, 0) == "—"


# ------------------------------------------------------------------ the runner

def test_resume_skips_completed_rows():
    rows = [{"scenario": "a", "model": "m", "condition": "gated", "repeat": 1, "ok": True},
            {"scenario": "a", "model": "m", "condition": "gated", "repeat": 2, "ok": False}]
    done = run_eval.done_keys(rows)
    assert ("a", "m", "gated", 1) in done
    assert ("a", "m", "gated", 2) not in done   # failures are retried


def test_cost_estimate_uses_measurements_once_they_exist():
    assert run_eval.average_cost([], "claude-opus-5") == run_eval.PRIOR_COST["claude-opus-5"]
    rows = [{"model": "claude-opus-5", "cost_usd": 0.10}, {"model": "claude-opus-5", "cost_usd": 0.20}]
    assert run_eval.average_cost(rows, "claude-opus-5") == pytest.approx(0.15)
    assert run_eval.spent(rows) == pytest.approx(0.30)


def test_every_scenario_has_a_condition_pair_in_the_matrix():
    assert set(run_eval.CONDITIONS) == {"baseline", "gated"}
    assert set(run_eval.MODELS) == {"claude-haiku-4-5", "claude-sonnet-5", "claude-opus-5"}
    assert all(s in BY_ID for s in (x.id for x in SCENARIOS))


def test_diagnose_one_records_truth_and_gate(monkeypatch, tmp_path):
    """One row, end to end, with the model and the harness stubbed."""
    captured = {"failure": {"dag": "product_dim_upsert", "task": "load_to_warehouse",
                            "operator": "PythonOperator", "env": "dev", "occurrences": 1,
                            "log": [{"sev": "err", "t": "CardinalityViolation"}]},
                "files": {}}
    monkeypatch.setattr(run_eval.harness, "load", lambda _id: captured)
    monkeypatch.setattr(run_eval.diagnosis, "diagnose", lambda *a, **k: {
        "type": "Duplicate keys", "cat": "merge", "confidence": "High", "rootCause": "data",
        "cause": {"engineer": "e", "analyst": "a"}, "impact": "i", "fixSummary": "f",
        "patch": "diff --git a/x b/x\n", "fixFile": "x", "policyError": None,
        "usage": {"model": "m", "input_tokens": 1, "output_tokens": 2, "cost_usd": 0.01,
                  "seconds": 1.0}})
    monkeypatch.setattr(run_eval.diagnosis, "check_patch", lambda patch, repo, index=False: None)

    r = run_eval.diagnose_one("duplicate_keys", "claude-sonnet-5", "gated", 1)
    assert r["ok"] and r["truth_root_cause"] == "data" and r["truth_fix_in_repo"] is False
    assert r["root_cause"] == "data" and r["pr_eligible"] is False
    assert analyze.unsafe({**r}) is False
    assert json.dumps(r)   # every row must be serialisable
