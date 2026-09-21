"""Log reduction against real Airflow 3 task logs captured from the local harness."""

import json
from pathlib import Path

import pytest

from agent.airflow import MAX_LOG_LINES, extract_from_text_log, reduce_log

FIXTURES = Path(__file__).parent / "fixtures"


def load(dag_id):
    return json.loads((FIXTURES / f"log_{dag_id}.json").read_text())


@pytest.mark.parametrize("dag_id, expected_error", [
    ("orders_refresh", 'UndefinedColumn: column "effective_ts" of relation "stg_orders" does not exist'),
    ("product_dim_upsert", "CardinalityViolation: ON CONFLICT DO UPDATE command cannot affect row a second time"),
    ("product_deactivation_check", "ValueError: DQ check failed on dim_product: 650 rows match"),
    ("legacy_hierarchy_load", 'UndefinedTable: relation "dim_hierarchy_v1" does not exist'),
    ("daily_sales_export", "FileNotFoundError: [Errno 2] No such file or directory"),
    ("vendor_feed_ingest", "AirflowSensorTimeout: Sensor has timed out"),
])
def test_exception_is_extracted(dag_id, expected_error):
    lines, error = reduce_log(load(dag_id))
    assert error.startswith(expected_error)
    assert "\n" not in error  # one-line summary, even for multi-line Postgres errors
    assert any(expected_error in l["t"] for l in lines if l["sev"] == "err")


@pytest.mark.parametrize("dag_id", [
    "orders_refresh", "product_dim_upsert", "product_deactivation_check",
    "legacy_hierarchy_load", "daily_sales_export",
])
def test_frames_point_at_dag_code(dag_id):
    """For errors raised from the DAG, only frames in the DAG file are kept — those are
    the lines a fix would touch, not Airflow's own task runner."""
    lines, _ = reduce_log(load(dag_id))
    frames = [l["t"] for l in lines if l["t"].lstrip().startswith("at ")]
    assert frames and all("/opt/airflow/dags/" in f for f in frames)


def test_library_frames_are_kept_when_no_dag_frame_exists():
    """The sensor times out inside Airflow itself, so the innermost library frames stay."""
    lines, _ = reduce_log(load("vendor_feed_ingest"))
    frames = [l["t"] for l in lines if l["t"].lstrip().startswith("at ")]
    assert frames and any("sensor" in f for f in frames)


def test_context_before_the_failure_is_kept():
    """The SQL that was running is the most useful context, and it's logged at info."""
    lines, _ = reduce_log(load("legacy_hierarchy_load"))
    info = [l["t"] for l in lines if l["sev"] == "info"]
    assert any("SELECT count(*) FROM dim_hierarchy_v1" in t for t in info)


@pytest.mark.parametrize("dag_id", [p.stem[4:] for p in FIXTURES.glob("log_*.json")])
def test_output_is_bounded(dag_id):
    lines, _ = reduce_log(load(dag_id))
    assert 0 < len(lines) <= MAX_LOG_LINES
    assert all(len(l["t"]) <= 400 and l["sev"] in ("err", "warn", "info") for l in lines)


def test_log_with_no_errors():
    lines, error = reduce_log([{"event": "all good", "level": "info"}])
    assert error == ""
    assert lines == [{"sev": "warn", "t": "no error-level lines in the task log"}]


def test_plain_text_log_fallback():
    text = """[2026-09-21 02:00:01] INFO - Starting
[2026-09-21 02:00:02] ERROR - Task failed
Traceback (most recent call last):
  File "/opt/airflow/dags/x.py", line 3, in run
psycopg2.errors.UndefinedTable: relation "t" does not exist
"""
    lines, error = extract_from_text_log(text)
    assert error == 'psycopg2.errors.UndefinedTable: relation "t" does not exist'
    assert lines[0]["t"].endswith("ERROR - Task failed")
