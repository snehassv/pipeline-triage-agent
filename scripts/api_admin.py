"""Shared helper: put the mock API into a mode (see services/mock_api/server.py)."""

import json
import os
import urllib.error
import urllib.request

API_ADMIN = os.environ.get("MOCK_API_ADMIN", "http://localhost:9000/admin/mode")


def set_mode(mode: str, limit: int = 20) -> dict:
    body = json.dumps({"mode": mode, "limit": limit}).encode()
    request = urllib.request.Request(API_ADMIN, data=body,
                                     headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            return json.load(response)
    except urllib.error.URLError as e:
        raise SystemExit(
            f"can't reach the mock API at {API_ADMIN} ({e.reason}).\n"
            f"Is it running?  docker compose up -d mock-api"
        )
