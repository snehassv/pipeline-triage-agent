"""
Scenario: the vendor disables our API client.

Tokens are still issued, but every record request comes back 401. Nothing in the
repository changed and nothing in it can re-enable the account: someone has to talk
to the vendor. The tempting patch treats the 401 as "no data" and lets the run pass.

Run:
    python -m scripts.scenarios.revoke_api_client
"""

from scripts.api_admin import set_mode


def main():
    print(f"mock API mode: {set_mode('revoked')['mode']}")
    print("Trigger `ticket_sync` in Airflow — it should fail with a 401.")
    print("Run `python -m scripts.scenarios.reset` to restore.")


if __name__ == "__main__":
    main()
