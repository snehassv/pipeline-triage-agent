"""HTTP behaviour: auth, caching, retry backoff and PR gating. Airflow and Claude are
stubbed, so nothing leaves the process."""

import threading
from concurrent.futures import Future, ThreadPoolExecutor

import pytest
from fastapi.testclient import TestClient

from agent import diagnosis, main

TOKEN = "test-token"
FAILED_TASK = {
    "dag": "orders_refresh", "task": "load_to_warehouse", "run_id": "manual__1",
    "try_number": 1, "operator": "PythonOperator", "env": "dev", "repo": "local",
    "time": "13:13:28", "day": "2026-09-21", "duration": "ran 5s", "occurrences": 1,
}
DIAGNOSIS = {
    "type": "Schema drift", "cat": "schema", "confidence": "High",
    "cause": {"engineer": "e", "analyst": "a"}, "impact": "i", "rootCause": "code", "fixSummary": "f",
    "patch": "diff --git a/x b/x\n", "diff": [], "fixFile": "x",
    "prTitle": "fix: x", "prBranch": "agent/fix-x",
    "usage": {"model": "claude-sonnet-5", "input_tokens": 1, "output_tokens": 1,
              "cost_usd": 0.0, "seconds": 0.1},
}


class InlineExecutor:
    """Runs background diagnoses immediately, so most tests can ignore the thread pool."""

    def submit(self, fn, *args):
        future = Future()
        future.set_result(fn(*args))
        return future


@pytest.fixture
def api(monkeypatch, tmp_path):
    monkeypatch.setattr(main, "_executor", InlineExecutor())
    monkeypatch.setattr(main, "AGENT_TOKEN", TOKEN)
    monkeypatch.setattr(main, "GITHUB_REPO", "me/repo")
    monkeypatch.setattr(main, "USAGE_LOG", tmp_path / "usage.jsonl")
    monkeypatch.setattr(main, "_cache", {})
    monkeypatch.setattr(main, "_latest", {})
    monkeypatch.setattr(main, "read_failed_tasks", lambda lo, hi: [dict(FAILED_TASK)])
    monkeypatch.setattr(main, "read_task_log", lambda f: ([{"sev": "err", "t": "boom"}], "Boom: x"))
    monkeypatch.setattr(main, "read_sources", lambda dag: {})
    monkeypatch.setattr(diagnosis, "check_patch", lambda patch, repo, index=False: None)
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
    return TestClient(main.app)


def get_failures(api):
    r = api.get("/failures", headers={"X-Agent-Token": TOKEN})
    assert r.status_code == 200, r.text
    return r.json()


def stub_diagnose(monkeypatch, result=None, error=None):
    calls = []

    def fake(failure, files):
        calls.append(failure["dag"])
        if error:
            raise diagnosis.DiagnosisError(error)
        return {**DIAGNOSIS, **(result or {})}
    monkeypatch.setattr(diagnosis, "diagnose", fake)
    return calls


def test_endpoints_require_the_token(api):
    assert api.get("/failures").status_code == 401
    assert api.get("/failures", headers={"X-Agent-Token": "wrong"}).status_code == 401
    assert api.post("/pull-requests", json={"id": "x"}).status_code == 401


def test_dashboard_injects_config_safely(api, monkeypatch):
    monkeypatch.setattr(main, "AGENT_TOKEN", "a</script><script>alert(1)")
    r = api.get("/")
    assert r.status_code == 200
    assert "window.__AGENT_CONFIG" in r.text
    assert "</script><script>alert(1)" not in r.text  # "</" is escaped inside the JSON
    assert r.headers["cache-control"] == "no-store"


def test_without_a_key_the_raw_error_is_shown(api, monkeypatch):
    monkeypatch.delenv("ANTHROPIC_API_KEY")
    calls = stub_diagnose(monkeypatch)
    [f] = get_failures(api)
    assert calls == []
    assert f["diagnosed"] is False
    assert f["cause"]["engineer"] == "Boom: x"
    assert "ANTHROPIC_API_KEY" in f["cause"]["analyst"]


def test_diagnosis_is_cached_across_polls_and_logged(api, monkeypatch):
    calls = stub_diagnose(monkeypatch)
    get_failures(api)
    [f] = get_failures(api)
    assert calls == ["orders_refresh"]  # one Claude call for two polls
    assert f["diagnosed"] is True and f["id"] == "orders_refresh::load_to_warehouse"
    assert len(main.USAGE_LOG.read_text().splitlines()) == 1


def test_failed_diagnosis_is_not_retried_on_every_poll(api, monkeypatch):
    calls = stub_diagnose(monkeypatch, error="The model's reply wasn't valid JSON.")
    get_failures(api)
    [f] = get_failures(api)
    assert calls == ["orders_refresh"]
    assert "wasn't valid JSON" in f["cause"]["analyst"]

    monkeypatch.setattr(main, "RETRY_DIAGNOSIS_AFTER", 0)
    get_failures(api)
    assert len(calls) == 2


def test_pr_for_unknown_failure_is_404(api):
    r = api.post("/pull-requests", json={"id": "nope"}, headers={"X-Agent-Token": TOKEN})
    assert r.status_code == 404


@pytest.mark.parametrize("override, reason", [
    ({"confidence": "Medium"}, "High-confidence"),
    # The product_dim_upsert case from a real run: High confidence, a patch that
    # applies, and a data problem. It must never become a PR.
    ({"rootCause": "data", "patchWithheld": True}, "root cause is in the data"),
    ({"rootCause": "environment"}, "root cause is in the environment"),
    ({"rootCause": None}, "doesn't say where the root cause is"),
    ({"patch": ""}, "no code change"),
    ({"prBranch": "bad branch; rm -rf"}, "invalid branch"),
])
def test_pr_is_refused_unless_every_gate_passes(api, monkeypatch, override, reason):
    stub_diagnose(monkeypatch, result=override)
    [f] = get_failures(api)
    r = api.post("/pull-requests", json={"id": f["id"]}, headers={"X-Agent-Token": TOKEN})
    assert r.status_code == 409 and reason in r.json()["detail"]


def test_pr_is_refused_when_patch_does_not_apply(api, monkeypatch):
    stub_diagnose(monkeypatch)
    monkeypatch.setattr(diagnosis, "check_patch", lambda patch, repo, index=False: "does not apply")
    [f] = get_failures(api)
    assert f["patchError"] == "does not apply"
    r = api.post("/pull-requests", json={"id": f["id"]}, headers={"X-Agent-Token": TOKEN})
    assert r.status_code == 409


def test_pr_uses_the_agents_own_patch_not_the_callers(api, monkeypatch):
    stub_diagnose(monkeypatch)
    opened = []
    monkeypatch.setattr(main, "_open_pull_request",
                        lambda f: opened.append(f) or {"number": 7, "url": "https://github.com/me/repo/pull/7"})
    [f] = get_failures(api)
    r = api.post("/pull-requests", headers={"X-Agent-Token": TOKEN},
                 json={"id": f["id"], "patch": "malicious", "branch": "evil"})
    assert r.json() == {"number": 7, "url": "https://github.com/me/repo/pull/7"}
    assert opened[0]["patch"] == DIAGNOSIS["patch"] and opened[0]["prBranch"] == "agent/fix-x"


def test_failures_return_immediately_while_diagnosis_runs(api, monkeypatch):
    """The request doesn't wait on Claude: the first poll says `diagnosing`, a later
    poll has the result."""
    monkeypatch.setattr(main, "_executor", ThreadPoolExecutor(max_workers=2))
    release = threading.Event()
    stub_diagnose(monkeypatch)
    real = diagnosis.diagnose
    monkeypatch.setattr(diagnosis, "diagnose", lambda f, files: release.wait(5) and real(f, files))

    [first] = get_failures(api)
    assert first["diagnosing"] is True and first["diagnosed"] is False
    assert "Diagnosing with" in first["cause"]["analyst"]

    release.set()
    main._executor.shutdown(wait=True)
    [second] = get_failures(api)
    assert second["diagnosing"] is False and second["diagnosed"] is True


def test_unexpected_worker_error_does_not_leave_it_stuck(api, monkeypatch):
    def boom(f, files):
        raise OSError("git not found")
    monkeypatch.setattr(diagnosis, "diagnose", boom)
    [f] = get_failures(api)
    assert f["diagnosing"] is False
    assert "unexpected OSError: git not found" in f["cause"]["analyst"]


def test_cli_mode_waits_for_diagnosis(api, monkeypatch):
    monkeypatch.setattr(main, "_executor", ThreadPoolExecutor(max_workers=2))
    stub_diagnose(monkeypatch)
    [f] = main.collect_failures(None, None, wait_for_diagnosis=True)
    assert f["diagnosed"] is True
