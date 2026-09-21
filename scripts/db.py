import os

CONN = dict(
    host="localhost",
    port=5433,
    user="warehouse",
    password="warehouse",
    # Overridable so the migration runner can be tried on a scratch database.
    dbname=os.environ.get("WAREHOUSE_DB", "warehouse"),
)
