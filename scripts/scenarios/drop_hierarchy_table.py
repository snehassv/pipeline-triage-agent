"""
Scenario: a table disappears.

Drops the legacy hierarchy table, the way a rename or an over-eager cleanup
would in practice. Trivial to fix once you know what happened; annoying to
work out from the error alone, because the traceback tells you the relation
doesn't exist but not why it stopped existing.

Run:
    python -m scripts.scenarios.drop_hierarchy_table
"""

import psycopg2

from scripts.db import CONN

TABLE = "dim_hierarchy_v1"


def drop_table(cur) -> bool:
    cur.execute("SELECT to_regclass(%s)", (TABLE,))
    if cur.fetchone()[0] is None:
        return False
    cur.execute(f"DROP TABLE {TABLE}")
    return True


def main():
    conn = psycopg2.connect(**CONN)
    try:
        with conn.cursor() as cur:
            dropped = drop_table(cur)
        conn.commit()
    finally:
        conn.close()

    if dropped:
        print(f"Dropped {TABLE}.")
    else:
        print(f"{TABLE} was already missing.")
    print("Trigger `legacy_hierarchy_load` in Airflow — it should fail on an undefined table.")
    print("Run `python -m scripts.scenarios.reset` to restore.")


if __name__ == "__main__":
    main()