"""Airflow 3 REST API client, and the reduction of a task log to the lines that matter."""

import re
from typing import Optional

import requests

MAX_LOG_LINES = 12


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

    def failed_task_instances(self, start_iso: str, end_iso: str) -> list:
        """Every failed task instance whose end_date is in [start, end], newest first."""
        instances, offset = [], 0
        while True:
            page = self.get("/api/v2/dags/~/dagRuns/~/taskInstances", params={
                "state": "failed",
                "end_date_gte": start_iso,
                "end_date_lte": end_iso,
                "order_by": "-end_date",
                "limit": 100,
                "offset": offset,
            })
            batch = page.get("task_instances", [])
            instances.extend(batch)
            offset += len(batch)
            if not batch or offset >= page.get("total_entries", 0):
                return instances

    def task_log(self, dag_id: str, run_id: str, task_id: str, try_number: int):
        """The raw log content: a list of structured events on Airflow 3, a string on
        other log handlers. Raises KeyError if the log is gone."""
        path = (f"/api/v2/dags/{dag_id}/dagRuns/{requests.utils.quote(run_id, safe='')}"
                f"/taskInstances/{task_id}/logs/{try_number}")
        return self.get(path, params={"full_content": "true"}).get("content", [])

    def dag_file(self, dag_id: str) -> tuple:
        """Return (path relative to the dags folder, source text) of the file defining a DAG."""
        details = self.get(f"/api/v2/dags/{dag_id}")
        source = self.get(f"/api/v2/dagSources/{dag_id}").get("content", "")
        return details.get("relative_fileloc") or f"{dag_id}.py", source


def reduce_log(content) -> tuple:
    """Return (log lines for the UI, one-line exception summary) for any log format."""
    if isinstance(content, str):
        return extract_from_text_log(content)
    return extract_from_structured_log(content)


def extract_from_structured_log(events: list) -> tuple:
    """Airflow 3 task logs are a list of structlog events. The failure event carries an
    `error_detail` with the exception type, message and stack frames — that's the part
    worth showing. We also keep the couple of info lines right before the failure (e.g.
    the SQL statement that was running), since they're usually the missing context."""
    lines: list = []
    recent_info: list = []
    error = ""
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
            summary = f"{exc.get('exc_type')}: {exc.get('exc_value')}"
            error = summary.splitlines()[0]
            lines.append({"sev": "err", "t": summary})
            frames = exc.get("frames") or []
            # Frames from the DAG code are the ones a fix would touch; fall back to the
            # innermost frames when the error is raised entirely inside libraries.
            own = [fr for fr in frames if "/dags/" in fr.get("filename", "")]
            for fr in (own or frames)[-3:]:
                lines.append({"sev": "warn",
                              "t": f"  at {fr.get('filename')}:{fr.get('lineno')} in {fr.get('name')}"})
    if not lines:
        lines = [{"sev": "warn", "t": "no error-level lines in the task log"}]
    return [{"sev": l["sev"], "t": l["t"][:400]} for l in lines[-MAX_LOG_LINES:]], error[:200]


def extract_from_text_log(text: str) -> tuple:
    """Fallback for plain-text logs (older Airflow / other log handlers)."""
    raw = text.splitlines()
    start = next((i for i, l in enumerate(raw) if "Traceback" in l or " ERROR " in l), None)
    picked = raw[start:] if start is not None else raw[-MAX_LOG_LINES:]
    lines = [{"sev": "err" if ("ERROR" in l or "Error" in l) else "warn", "t": l[:400]}
             for l in picked[-MAX_LOG_LINES:] if l.strip()]
    # The exception line of a Python traceback looks like "SomeError: message".
    exc = [l for l in raw if re.match(r"^[A-Za-z_][\w.]*: ", l)]
    return lines, (exc[-1] if exc else "")[:200]
