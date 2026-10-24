"""Drive the harness: break a pipeline, run it, capture the failure the agent would see.

Capturing is separate from diagnosing on purpose. Each scenario is broken and run once,
and the resulting evidence (log + source files) is saved to eval/results/failures/. Every
repeat then diagnoses that same saved evidence, so repeated runs measure the model's
variability rather than the harness's.
"""

import datetime as dt
import json
import os
import subprocess
import sys
import time
from pathlib import Path

from agent import main as agent_main
from eval.scenarios import Scenario

RESULTS = Path(__file__).resolve().parent / "results"
FAILURES = RESULTS / "failures"
SCHEDULER = os.environ.get("AIRFLOW_CONTAINER", "pipeline-triage-agent-airflow-scheduler-1")
SENSOR_TIMEOUT = 240
TASK_TIMEOUT = 180


class HarnessError(RuntimeError):
    pass


def _airflow_cli(*args: str) -> str:
    proc = subprocess.run(["docker", "exec", SCHEDULER, "airflow", *args],
                          capture_output=True, text=True)
    if proc.returncode != 0:
        raise HarnessError(f"airflow {' '.join(args)}: {(proc.stderr or proc.stdout)[-300:]}")
    return proc.stdout


def run_module(module: str) -> None:
    """Run a scenario/reset script in this repo (python -m scripts.scenarios.x)."""
    # sys.executable, not "python": the scenario scripts need the venv's psycopg2.
    proc = subprocess.run([sys.executable, "-m", module], capture_output=True, text=True,
                          cwd=agent_main.REPO_ROOT)
    if proc.returncode != 0:
        raise HarnessError(f"{module}: {(proc.stderr or proc.stdout)[-300:]}")


def reset() -> None:
    run_module("scripts.scenarios.reset")


def latest_failure(dag_id: str, task_id: str, after: dt.datetime):
    """The failed task instance for this DAG that ended after `after`, or None."""
    page = agent_main.airflow.get(
        f"/api/v2/dags/{dag_id}/dagRuns/~/taskInstances",
        params={"state": "failed", "end_date_gte": agent_main._iso(after),
                "order_by": "-end_date", "limit": 5},
    )
    for ti in page.get("task_instances", []):
        if ti["task_id"] == task_id:
            return ti
    return None


def trigger_and_wait(scenario: Scenario) -> dict:
    """Run the DAG and return its failed task instance. Raises if it doesn't fail."""
    started = dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=5)
    _airflow_cli("dags", "unpause", scenario.dag_id)
    _airflow_cli("dags", "trigger", scenario.dag_id)

    deadline = time.monotonic() + (SENSOR_TIMEOUT if scenario.sensor else TASK_TIMEOUT)
    while time.monotonic() < deadline:
        ti = latest_failure(scenario.dag_id, scenario.task_id, started)
        if ti:
            return ti
        time.sleep(5)
    raise HarnessError(f"{scenario.id}: {scenario.dag_id} didn't fail within the timeout")


def capture(scenario: Scenario, reset_first: bool = True) -> dict:
    """Break the pipeline, run it, and save the evidence the agent would be given."""
    if reset_first:
        reset()
    if scenario.setup:
        run_module(scenario.setup)

    ti = trigger_and_wait(scenario)
    failure = {
        "dag": ti["dag_id"], "task": ti["task_id"], "run_id": ti["dag_run_id"],
        "try_number": ti.get("try_number") or 1, "operator": ti.get("operator") or "",
        "env": agent_main.ENV_NAME, "occurrences": 1,
    }
    failure["log"], failure["error"] = agent_main.read_task_log(failure)
    record = {
        "scenario": scenario.id,
        "captured_at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
        "failure": failure,
        "files": agent_main.read_sources(scenario.dag_id),
    }
    FAILURES.mkdir(parents=True, exist_ok=True)
    (FAILURES / f"{scenario.id}.json").write_text(json.dumps(record, indent=1))
    return record


def load(scenario_id: str) -> dict:
    path = FAILURES / f"{scenario_id}.json"
    if not path.exists():
        raise HarnessError(f"no captured failure for {scenario_id} — run `capture` first")
    return json.loads(path.read_text())
