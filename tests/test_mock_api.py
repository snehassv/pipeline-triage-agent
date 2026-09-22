"""The stand-in third-party API, and the guard that keeps its source away from the model."""

import json
import sys
import threading
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "services" / "mock_api"))
import server  # noqa: E402


@pytest.fixture
def api():
    """The real handler on an ephemeral port, reset between tests."""
    with server._lock:
        server._state.update({"mode": "normal", "limit": 20})
        server._hits.clear()
    httpd = ThreadingHTTPServer(("127.0.0.1", 0), server.Handler)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{httpd.server_port}"
    httpd.shutdown()


def get(url, method="GET", body=None):
    data = json.dumps(body).encode() if body is not None else None
    request = urllib.request.Request(url, data=data, method=method,
                                     headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            return response.status, json.load(response), dict(response.headers)
    except urllib.error.HTTPError as e:
        return e.code, json.load(e), dict(e.headers)


def test_normal_mode_answers(api):
    status, body, _ = get(f"{api}/records")
    assert status == 200 and body["ids"][0] == 1
    assert get(f"{api}/token", "POST")[1]["token"].startswith("tok-")
    assert get(f"{api}/records/3")[1] == {"id": 3, "name": "record-3", "status": "ACTIVE"}


def test_throttled_mode_returns_429_after_the_limit(api):
    get(f"{api}/admin/mode", "POST", {"mode": "throttled", "limit": 3})
    codes = [get(f"{api}/records/1")[0] for _ in range(5)]
    assert codes == [200, 200, 200, 429, 429]
    status, body, headers = get(f"{api}/records/1")
    assert status == 429 and "rate limit" in body["detail"] and headers["Retry-After"] == "30"


def test_moved_mode_404s_old_paths_and_serves_v2(api):
    get(f"{api}/admin/mode", "POST", {"mode": "moved"})
    status, body, _ = get(f"{api}/records")
    assert status == 404 and "moved" in body["detail"]
    assert get(f"{api}/v2/records")[0] == 200
    # Admin stays reachable, so a scenario can always put it back.
    assert get(f"{api}/admin/mode")[1]["mode"] == "moved"


def test_unknown_paths_404(api):
    assert get(f"{api}/nope")[0] == 404


def test_the_agent_never_sees_the_api_source(monkeypatch):
    """The whole point of the API scenarios: the agent gets the DAG and the log, not
    the code of the system on the other side."""
    from agent import main

    monkeypatch.setattr(main.airflow, "dag_file",
                        lambda dag_id: ("dag_factory.py", "# the factory"))
    files = main.read_sources("vendor_contact_sync")

    assert "dags/dag_factory.py" in files
    assert not any(path.startswith("services/") for path in files)
    assert not any("mock_api" in path for path in files)
