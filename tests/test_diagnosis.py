"""Diagnosis parsing, patch handling and usage accounting — with a fake Claude client,
so these run offline and cost nothing."""

import json
import subprocess
from types import SimpleNamespace

import pytest

from agent import diagnosis

PATCH = """diff --git a/dags/config/pipelines.yaml b/dags/config/pipelines.yaml
--- a/dags/config/pipelines.yaml
+++ b/dags/config/pipelines.yaml
@@ -1,3 +1,3 @@
 pipelines:
   - dag_id: orders_refresh
-    target_table: stg_orders
+    target_table: stg_orders_v2
"""

FAILURE = {
    "dag": "orders_refresh", "task": "load_to_warehouse", "run_id": "manual__1",
    "operator": "PythonOperator", "env": "dev", "occurrences": 2,
    "log": [{"sev": "err", "t": 'UndefinedColumn: column "effective_ts" does not exist'}],
}


def reply(payload, stop_reason="end_turn", model="claude-sonnet-5"):
    return SimpleNamespace(
        model=model, stop_reason=stop_reason,
        content=[SimpleNamespace(type="text", text=json.dumps(payload))],
        usage=SimpleNamespace(input_tokens=3000, output_tokens=500),
    )


class FakeClient:
    def __init__(self, response):
        self.response = response
        self.calls = []
        self.messages = self

    def create(self, **kwargs):
        self.calls.append(kwargs)
        return self.response


GOOD = {
    "type": "Schema drift", "cat": "schema", "confidence": "High",
    "cause": {"engineer": "effective_ts added upstream", "analyst": "the file has a new column"},
    "impact": "stg_orders is stale", "rootCause": "config", "fixSummary": "point the load at stg_orders_v2",
    "patch": PATCH, "prTitle": "fix: orders schema", "prBranch": "agent/fix-orders-schema",
}


def test_diagnose_parses_structured_reply():
    client = FakeClient(reply(GOOD))
    result = diagnosis.diagnose(FAILURE, {"dags/config/pipelines.yaml": "pipelines: ..."}, client)

    assert result["confidence"] == "High"
    assert result["fixFile"] == "dags/config/pipelines.yaml"
    assert {"t": "add", "s": "    target_table: stg_orders_v2"} in result["diff"]
    assert result["usage"] == {"model": "claude-sonnet-5", "input_tokens": 3000,
                               "output_tokens": 500, "cost_usd": 0.011,
                               "seconds": result["usage"]["seconds"]}


def test_request_uses_the_json_schema_and_sends_source_files():
    client = FakeClient(reply(GOOD))
    diagnosis.diagnose(FAILURE, {"dags/dag_factory.py": "def build_dag(): ..."}, client)
    call = client.calls[0]

    assert call["output_config"]["format"] == {"type": "json_schema", "schema": diagnosis.FIX_SCHEMA}
    prompt = call["messages"][0]["content"]
    assert '<file path="dags/dag_factory.py">' in prompt
    assert "effective_ts" in prompt


def test_root_cause_comes_before_the_fix_in_the_schema():
    """Structured output is generated in schema order: the model has to say where the
    problem is before it writes a patch."""
    order = list(diagnosis.FIX_SCHEMA["properties"])
    assert order.index("rootCause") < order.index("fixSummary") < order.index("patch")
    assert diagnosis.FIX_SCHEMA["properties"]["rootCause"]["enum"] == ["code", "config", "data", "environment"]


@pytest.mark.parametrize("root_cause, withheld", [
    ("code", False), ("config", False), ("data", True), ("environment", True),
])
def test_patch_for_data_or_environment_is_withheld(root_cause, withheld):
    result = diagnosis.diagnose(FAILURE, {}, FakeClient(reply({**GOOD, "rootCause": root_cause})))
    assert result["patchWithheld"] is withheld
    assert result["patch"]  # still returned, so the reader can see what was proposed


def test_schema_requires_every_field():
    schema = diagnosis.FIX_SCHEMA
    assert set(schema["required"]) == set(schema["properties"])
    assert schema["additionalProperties"] is False


@pytest.mark.parametrize("stop_reason, message", [
    ("refusal", "declined"),
    ("max_tokens", "cut off"),
])
def test_incomplete_replies_raise(stop_reason, message):
    with pytest.raises(diagnosis.DiagnosisError, match=message):
        diagnosis.diagnose(FAILURE, {}, FakeClient(reply(GOOD, stop_reason=stop_reason)))


def test_empty_patch_means_no_code_change():
    no_fix = {**GOOD, "patch": "", "confidence": "Medium"}
    result = diagnosis.diagnose(FAILURE, {}, FakeClient(reply(no_fix)))
    assert result["patch"] == "" and result["diff"] == [] and result["fixFile"] == ""


def test_unknown_model_reports_tokens_without_guessing_cost(monkeypatch):
    monkeypatch.setattr(diagnosis, "MODEL", "some-future-model")
    result = diagnosis.diagnose(FAILURE, {}, FakeClient(reply(GOOD, model="some-future-model")))
    assert result["usage"]["cost_usd"] is None
    assert result["usage"]["input_tokens"] == 3000


def test_patch_to_diff_marks_files_and_hunks():
    diff = diagnosis.patch_to_diff(PATCH)
    assert [d["t"] for d in diff] == ["file", "hunk", "ctx", "ctx", "del", "add"]
    assert diff[0]["s"] == "dags/config/pipelines.yaml"


@pytest.fixture
def repo(tmp_path):
    subprocess.run(["git", "init", "-q", str(tmp_path)], check=True)
    (tmp_path / "dags" / "config").mkdir(parents=True)
    (tmp_path / "dags" / "config" / "pipelines.yaml").write_text(
        "pipelines:\n  - dag_id: orders_refresh\n    target_table: stg_orders\n")
    return tmp_path


def test_check_patch_accepts_a_patch_that_applies(repo):
    assert diagnosis.check_patch(PATCH, repo) is None
    # --check must not modify anything
    assert "stg_orders\n" in (repo / "dags/config/pipelines.yaml").read_text()


def test_check_patch_tolerates_wrong_hunk_counts(repo):
    """Models often miscount hunk lengths; --recount makes git ignore them."""
    assert diagnosis.check_patch(PATCH.replace("@@ -1,3 +1,3 @@", "@@ -1,9 +1,7 @@"), repo) is None


def test_check_patch_rejects_a_patch_that_does_not_match(repo):
    stale = PATCH.replace("target_table: stg_orders\n+", "target_table: something_else\n+")
    assert diagnosis.check_patch(stale, repo) is not None


def test_check_patch_refuses_paths_outside_the_repo(repo):
    escape = PATCH.replace("dags/config/pipelines.yaml", "../outside.yaml")
    assert diagnosis.check_patch(escape, repo) is not None


def test_usage_log_round_trip(tmp_path):
    log = tmp_path / "usage.jsonl"
    result = diagnosis.diagnose(FAILURE, {}, FakeClient(reply(GOOD)))
    diagnosis.record_usage(log, FAILURE, result)
    diagnosis.record_usage(log, FAILURE, result)
    row = json.loads(log.read_text().splitlines()[0])
    assert row["root_cause"] == "config" and row["patch_withheld"] is False

    summary = diagnosis.summarize_usage(log)
    assert "2 diagnoses" in summary
    assert "claude-sonnet-5: 2 diagnoses · 6,000 in / 1,000 out tokens" in summary
    assert "$0.0220 total, $0.0110 per diagnosis" in summary
