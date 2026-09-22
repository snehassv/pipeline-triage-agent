"""
Scenario: the vendor moves their API under /v2 and leaves nothing behind.

Every DAG that calls the API fails at once with a 404, because each one carries its
own copy of `api_base` in dags/config/pipelines.yaml. The obvious fix is to edit
every one of those lines; the better fix is for the value to live in one place.

This is the scenario for "does the agent reach for the tedious fix or the structural
one?" — the answer is worth reading in the diagnosis it writes.

Run:
    python -m scripts.scenarios.move_api_endpoint
"""

from scripts.api_admin import set_mode


def main():
    set_mode("moved")
    print("mock API now serves only under /v2; the old paths return 404.")
    print("Trigger any of the API DAGs in Airflow — all of them should fail.")
    print("Run `python -m scripts.scenarios.reset` to restore.")


if __name__ == "__main__":
    main()
