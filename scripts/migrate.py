"""
Apply pending schema migrations from sql/migrations to the warehouse.

Each NNNN_slug.sql file runs once, in order, inside its own transaction, and is
recorded in schema_migrations. Every file is checked against the DDL-only policy
(scripts/ddl_policy.py) before anything runs, so a migration that touches data or
drops something is refused even if it was written by hand.

Run:
    python -m scripts.migrate            # apply what's pending
    python -m scripts.migrate --status   # list applied and pending migrations
"""

import argparse
import sys
from pathlib import Path

import psycopg2
import psycopg2.errors

from scripts import ddl_policy
from scripts.db import CONN

MIGRATIONS = Path(__file__).resolve().parent.parent / ddl_policy.MIGRATIONS_DIR


class MigrationError(RuntimeError):
    pass


def migration_files() -> list:
    files = sorted(p for p in MIGRATIONS.glob("*.sql"))
    bad = [p.name for p in files if not ddl_policy.MIGRATION_NAME.match(p.name)]
    if bad:
        raise MigrationError(f"badly named migration file(s): {', '.join(bad)}")
    return files


def applied_versions(cur) -> set:
    cur.execute("""
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version    TEXT PRIMARY KEY,
            applied_at TIMESTAMP NOT NULL DEFAULT now()
        )
    """)
    cur.execute("SELECT version FROM schema_migrations")
    return {row[0] for row in cur.fetchall()}


def apply_pending(conn, log=print) -> list:
    """Apply every migration not yet recorded. Returns the names applied."""
    files = migration_files()
    # Validate everything up front so a bad file later in the list can't leave
    # the schema half-migrated.
    for path in files:
        why = ddl_policy.check_sql(path.read_text())
        if why:
            raise MigrationError(f"{path.name} violates the DDL-only policy: {why}")

    with conn.cursor() as cur:
        done = applied_versions(cur)
    conn.commit()

    applied = []
    for path in files:
        if path.stem in done:
            continue
        try:
            with conn.cursor() as cur:
                cur.execute(path.read_text())
                cur.execute("INSERT INTO schema_migrations (version) VALUES (%s)", (path.stem,))
            conn.commit()
        except psycopg2.Error as e:
            conn.rollback()
            detail = (getattr(e, "pgerror", None) or str(e)).strip()
            hint = ""
            if not done and isinstance(e, psycopg2.errors.DuplicateTable):
                hint = ("\nThe warehouse was probably created before migrations existed. Rebuild it "
                        "once with `python -m scripts.scenarios.reset`, which applies every migration.")
            raise MigrationError(f"{path.name} failed and was rolled back: {detail}{hint}")
        applied.append(path.name)
        log(f"  applied {path.name}")
    return applied


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.strip().splitlines()[0])
    ap.add_argument("--status", action="store_true", help="list migrations and exit")
    args = ap.parse_args()

    conn = psycopg2.connect(**CONN)
    try:
        if args.status:  # read-only: doesn't create schema_migrations
            with conn.cursor() as cur:
                cur.execute("SELECT to_regclass('schema_migrations')")
                done = set()
                if cur.fetchone()[0]:
                    cur.execute("SELECT version FROM schema_migrations")
                    done = {row[0] for row in cur.fetchall()}
            conn.rollback()
            for path in migration_files():
                print(f"  {'applied' if path.stem in done else 'pending'}  {path.name}")
            return 0
        applied = apply_pending(conn)
        print(f"{len(applied)} migration(s) applied." if applied else "Schema is up to date.")
        return 0
    except MigrationError as e:
        conn.rollback()
        print(f"error: {e}", file=sys.stderr)
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
