"""
DAG factory: generates N DAGs from a YAML config.

Each generated DAG fails through a REAL mechanism (a genuine driver error, a real
sensor timeout, a real missing file) rather than `raise Exception("schema error")`.
That matters: your agent reads tracebacks, so synthetic ones would test nothing.

Drop this in dags/ alongside dags/config/pipelines.yaml.
"""

from __future__ import annotations

import csv
import os
from datetime import datetime, timedelta
from pathlib import Path

import yaml
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.filesystem import FileSensor

CONFIG_PATH = Path(__file__).parent / "config" / "pipelines.yaml"
DATA_DIR = Path(os.environ.get("HARNESS_DATA_DIR", "/opt/airflow/data"))

DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}


# --------------------------------------------------------------------------
# Failure task builders — one per category in your taxonomy.
# Each one does real work against real objects so the traceback is genuine.
# --------------------------------------------------------------------------

def _task_schema(cfg):
    """Schema drift: load a CSV whose columns no longer match the target table."""
    def run(**_):
        hook = PostgresHook(postgres_conn_id=cfg.get("conn_id", "warehouse"))
        src = DATA_DIR / cfg["source_file"]
        with src.open() as fh:
            reader = csv.reader(fh)
            header = next(reader)
            rows = list(reader)
        cols = ", ".join(header)
        placeholders = ", ".join(["%s"] * len(header))
        # Fails with a genuine UndefinedColumn if the source gained a column.
        hook.insert_rows(
            table=cfg["target_table"],
            rows=rows,
            target_fields=header,
            commit_every=500,
        )
    return run


def _task_merge(cfg):
    """Merge failure: upsert where the source has duplicate business keys."""
    def run(**_):
        hook = PostgresHook(postgres_conn_id=cfg.get("conn_id", "warehouse"))
        # ON CONFLICT ... DO UPDATE raises CardinalityViolation when the source
        # presents the same key twice in one statement.
        hook.run(f"""
            INSERT INTO {cfg['target_table']} ({cfg['columns']})
            SELECT {cfg['columns']} FROM {cfg['source_table']}
            ON CONFLICT ({cfg['key']}) DO UPDATE
              SET updated_at = EXCLUDED.updated_at;
        """)
    return run


def _task_dq(cfg):
    """DQ threshold: count flagged rows, fail when the configured limit is crossed."""
    def run(**_):
        hook = PostgresHook(postgres_conn_id=cfg.get("conn_id", "warehouse"))
        count = hook.get_first(
            f"SELECT count(*) FROM {cfg['target_table']} WHERE {cfg['predicate']}"
        )[0]
        limit = cfg["threshold"]
        if count > limit:
            raise ValueError(
                f"DQ check failed on {cfg['target_table']}: {count} rows match "
                f"'{cfg['predicate']}', threshold is {limit}. "
                f"Validate the volume before raising the threshold."
            )
    return run


def _task_missing_table(cfg):
    """Missing table / permissions: query an object that isn't there."""
    def run(**_):
        hook = PostgresHook(postgres_conn_id=cfg.get("conn_id", "warehouse"))
        hook.get_first(f"SELECT count(*) FROM {cfg['target_table']}")
    return run


def _task_file_export(cfg):
    """File export failure: write a table out to a path that doesn't exist."""
    def run(**_):
        hook = PostgresHook(postgres_conn_id=cfg.get("conn_id", "warehouse"))
        rows = hook.get_records(f"SELECT * FROM {cfg['source_table']}")
        out = DATA_DIR / cfg["export_path"]
        with out.open("w", newline="") as fh:
            csv.writer(fh).writerows(rows)
    return run


def _task_api_extract(cfg):
    """
    Extract records from a third-party API, one request per record, with a fresh
    token each time. On a good day it only burns tokens and time. When the API is
    throttled or has moved, the task fails on whatever the API returns — and this
    code gives no clue which happened, which is the point: the cause lives in a
    system the agent can't read (services/mock_api is never sent to it).
    """
    def run(**_):
        import requests
        base = cfg.get("api_base", "http://mock-api:9000")
        listing = requests.get(f"{base}/records", timeout=30)
        listing.raise_for_status()
        for rid in listing.json()["ids"]:
            token_response = requests.post(f"{base}/token", timeout=30)
            token_response.raise_for_status()
            token = token_response.json()["token"]
            record = requests.get(
                f"{base}/records/{rid}",
                headers={"Authorization": f"Bearer {token}"},
                timeout=30,
            )
            record.raise_for_status()
    return run


BUILDERS = {
    "schema": _task_schema,
    "merge": _task_merge,
    "dq": _task_dq,
    "table": _task_missing_table,
    "gcs": _task_file_export,     # keeping your category name; it's local FS here
    "auth": _task_api_extract,
    "api": _task_api_extract,
}


def build_dag(cfg: dict) -> DAG:
    dag = DAG(
        dag_id=cfg["dag_id"],
        default_args=DEFAULT_ARGS,
        description=cfg.get("description", ""),
        schedule=cfg.get("schedule", "@daily"),
        start_date=datetime(2026, 1, 1),
        catchup=False,
        tags=[cfg["category"], cfg.get("domain", "core")],
    )

    with dag:
        upstream = None

        # Sensor category gets a real FileSensor on a file that never lands.
        if cfg["category"] == "sensor":
            upstream = FileSensor(
                task_id="wait_for_upstream_file",
                fs_conn_id="harness_data",       # base path /opt/airflow/data, see docker-compose.yaml
                filepath=cfg["source_file"],
                poke_interval=10,
                timeout=cfg.get("timeout_seconds", 60),
                mode="reschedule",
            )

        builder = BUILDERS.get(cfg["category"])
        if builder:
            task = PythonOperator(
                task_id=cfg.get("task_id", "load_to_warehouse"),
                python_callable=builder(cfg),
            )
            if upstream:
                upstream >> task

    return dag


# --------------------------------------------------------------------------
# Generate every DAG in the config. 8 templates -> as many DAGs as you list.
# --------------------------------------------------------------------------

with CONFIG_PATH.open() as fh:
    _configs = yaml.safe_load(fh)["pipelines"]

for _cfg in _configs:
    globals()[_cfg["dag_id"]] = build_dag(_cfg)