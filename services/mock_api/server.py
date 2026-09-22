#!/usr/bin/env python3
"""
A stand-in for the third-party APIs data pipelines depend on.

Deliberately opaque: the triage agent is never given this file. It sees the DAG that
calls the API and the task log, exactly as you would for a vendor system you can't
read the source of. That's the point of the scenarios it powers — whether the agent
can tell "the pipeline is wrong" from "the thing it's talking to is wrong".

Standard library only, so docker compose can run it straight from a mount with no
image build and no dependencies.

Endpoints:
    GET  /records            -> {"ids": [...]}
    POST /token              -> {"token": "..."}
    GET  /records/{id}       -> one record
    GET  /admin/mode         -> current mode
    POST /admin/mode         -> {"mode": "normal|throttled|moved", "limit": 20}

Modes (set by the scenario scripts in scripts/scenarios/):
    normal     everything answers
    throttled  more than `limit` requests in 60s gets 429 + Retry-After, and every
               response slows down — the shape of a rate limit you don't control
    moved      the API now lives under /v2; old paths 404
"""

import json
import os
import re
import threading
import time
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PORT = int(os.environ.get("MOCK_API_PORT", "9000"))
RECORD_COUNT = int(os.environ.get("MOCK_API_RECORDS", "40"))
WINDOW_SECONDS = 60

_lock = threading.Lock()
_state = {"mode": "normal", "limit": 20}
_hits: deque = deque()          # request timestamps, for the rolling window
RECORD_PATH = re.compile(r"^/records/(\d+)$")


def _over_limit() -> bool:
    """True when this request exceeds the rolling per-minute limit."""
    now = time.monotonic()
    with _lock:
        if _state["mode"] != "throttled":
            return False
        while _hits and now - _hits[0] > WINDOW_SECONDS:
            _hits.popleft()
        _hits.append(now)
        return len(_hits) > _state["limit"]


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def _send(self, status: int, payload: dict, headers: dict = None) -> None:
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        for key, value in (headers or {}).items():
            self.send_header(key, value)
        self.end_headers()
        self.wfile.write(body)

    def _route(self, path: str, method: str):
        if path == "/records":
            return self._send(200, {"ids": list(range(1, RECORD_COUNT + 1))})
        if path == "/token" and method == "POST":
            return self._send(200, {"token": f"tok-{int(time.time() * 1000)}", "expires_in": 300})
        match = RECORD_PATH.match(path)
        if match:
            rid = int(match.group(1))
            return self._send(200, {"id": rid, "name": f"record-{rid}", "status": "ACTIVE"})
        return self._send(404, {"detail": f"no such path: {path}"})

    def _handle(self, method: str) -> None:
        path = self.path.split("?")[0].rstrip("/") or "/"

        if path.startswith("/admin"):
            return self._admin(method)

        with _lock:
            mode, limit = _state["mode"], _state["limit"]

        if mode == "moved":
            # The vendor moved everything under /v2 and left nothing behind.
            if not path.startswith("/v2/"):
                return self._send(404, {"detail": "endpoint moved; use /v2"})
            path = path[len("/v2"):]

        if _over_limit():
            time.sleep(0.4)
            return self._send(429, {"detail": f"rate limit exceeded: {limit} requests/minute"},
                              {"Retry-After": "30"})
        if mode == "throttled":
            time.sleep(0.2)   # everything is slower once you're near the limit
        self._route(path, method)

    def _admin(self, method: str) -> None:
        if method == "GET":
            with _lock:
                return self._send(200, dict(_state))
        length = int(self.headers.get("Content-Length") or 0)
        body = json.loads(self.rfile.read(length) or b"{}")
        with _lock:
            _state["mode"] = body.get("mode", "normal")
            _state["limit"] = int(body.get("limit", 20))
            _hits.clear()
            current = dict(_state)
        print(f"mode -> {current}", flush=True)
        self._send(200, current)

    def do_GET(self):      # noqa: N802 - BaseHTTPRequestHandler's naming
        self._handle("GET")

    def do_POST(self):     # noqa: N802
        self._handle("POST")

    def log_message(self, fmt, *args):
        # One line per request, so the pattern (a token for every record) is visible.
        print(f"{self.address_string()} {fmt % args}", flush=True)


if __name__ == "__main__":
    print(f"mock-api listening on {PORT} ({RECORD_COUNT} records)", flush=True)
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
