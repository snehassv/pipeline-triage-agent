"""
Scenario: the third-party API starts rate-limiting us.

The API begins returning 429 after a handful of requests a minute, and slows down
besides. Nothing in the repository changed, and nothing in the repository can fix
it — the limit lives in someone else's system. The DAG extracts one record at a
time with a fresh token for each, so it trips the limit quickly.

This is the scenario for "can the agent tell a pipeline bug from someone else's
system misbehaving?" — it never sees the API's code, only the DAG and the traceback.

Run:
    python -m scripts.scenarios.throttle_vendor_api
"""

from scripts.api_admin import set_mode

LIMIT = 15


def main():
    state = set_mode("throttled", LIMIT)
    print(f"mock API is now throttled: {state['limit']} requests/minute, and slower.")
    print("Trigger `ticket_sync`, `vendor_contact_sync`, `pricing_api_pull` or")
    print("`inventory_api_pull` in Airflow — they should fail with a 429.")
    print("Run `python -m scripts.scenarios.reset` to restore.")


if __name__ == "__main__":
    main()
