"""
Reset the harness to a clean state.

Re-runs the seed, which drops and recreates every table and rewrites the CSV
extracts, and puts the mock API back to normal. Undoes all scenarios in one go.

Does NOT clear Airflow's task history — failed runs stay visible in the UI,
which is usually what you want while testing the agent. Clear individual task
instances in the UI if you need a fresh run.

Run:
    python -m scripts.scenarios.reset
"""

from scripts.api_admin import set_mode
from scripts.seed_warehouse import main as seed


def main():
    print("resetting harness...")
    seed()
    print()
    print("restoring the mock API...")
    print(f"  mode: {set_mode('normal')['mode']}")
    print()
    print("Clean state restored. Failed DAG runs remain in Airflow history —")
    print("clear the task instance in the UI to re-run one.")


if __name__ == "__main__":
    main()