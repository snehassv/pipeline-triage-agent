"""The DDL-only rules for schema migrations, and the runner that enforces them."""

from pathlib import Path

import pytest

from scripts import ddl_policy, migrate

REPO = Path(__file__).resolve().parent.parent


@pytest.mark.parametrize("sql", [
    "ALTER TABLE stg_orders ADD COLUMN effective_ts TIMESTAMP;",
    "ALTER TABLE stg_orders ADD COLUMN IF NOT EXISTS effective_ts TIMESTAMP",
    "CREATE TABLE stg_vendor (vendor_id INT PRIMARY KEY, name TEXT);",
    "CREATE INDEX stg_orders_order_date ON stg_orders (order_date);",
    "CREATE VIEW v_recent_orders AS SELECT * FROM stg_orders WHERE order_date > now() - interval '7 days';",
    "COMMENT ON COLUMN stg_orders.effective_ts IS 'when the source row became effective';",
    "-- we used to DROP this, never again\nALTER TABLE dim_store ADD COLUMN opened_on DATE;",
    "/* DELETE nothing */ ALTER TABLE dim_store ADD COLUMN closed_on DATE;",
])
def test_additive_ddl_is_allowed(sql):
    assert ddl_policy.check_sql(sql) is None


@pytest.mark.parametrize("sql, reason", [
    ("DELETE FROM stg_products WHERE product_id IN (1, 2);", "found DELETE"),
    ("UPDATE dim_product SET status = 'ACTIVE';", "found UPDATE"),
    ("INSERT INTO dim_hierarchy_v1 VALUES (1, NULL, 'All', 0);", "found INSERT"),
    ("TRUNCATE stg_orders;", "found TRUNCATE"),
    ("DROP TABLE dim_hierarchy_v1;", "found DROP"),
    ("ALTER TABLE stg_orders DROP COLUMN effective_ts;", "DROP isn't allowed"),
    ("ALTER TABLE dim_hierarchy_v1 RENAME TO dim_hierarchy;", "RENAME isn't allowed"),
    ("CREATE TABLE stg_products_dedup AS SELECT DISTINCT * FROM stg_products;", "copies data"),
    ("DO $$ BEGIN DELETE FROM stg_orders; END $$;", "found DO"),
    ("ALTER TABLE t ADD COLUMN c INT; DELETE FROM t;", "found DELETE"),
    ("-- nothing here", "no statements"),
])
def test_anything_else_is_refused(sql, reason):
    why = ddl_policy.check_sql(sql)
    assert why and reason in why


def test_the_initial_migration_follows_its_own_rules():
    for path in (REPO / ddl_policy.MIGRATIONS_DIR).glob("*.sql"):
        assert ddl_policy.check_sql(path.read_text()) is None, path.name


NEW_MIGRATION = """diff --git a/sql/migrations/0002_add_effective_ts_to_stg_orders.sql b/sql/migrations/0002_add_effective_ts_to_stg_orders.sql
new file mode 100644
--- /dev/null
+++ b/sql/migrations/0002_add_effective_ts_to_stg_orders.sql
@@ -0,0 +1,2 @@
+-- The orders extract now carries effective_ts.
+ALTER TABLE stg_orders ADD COLUMN effective_ts TIMESTAMP;
"""


def test_a_new_additive_migration_passes():
    assert ddl_policy.check_patch(NEW_MIGRATION) is None


def test_data_changes_in_a_new_migration_are_refused():
    patch = NEW_MIGRATION.replace("@@ -0,0 +1,2 @@", "@@ -0,0 +1,3 @@") + \
        "+DELETE FROM stg_orders WHERE effective_ts IS NULL;\n"
    assert "found DELETE" in ddl_policy.check_patch(patch)


def test_existing_migrations_cannot_be_edited():
    patch = """diff --git a/sql/migrations/0001_initial_schema.sql b/sql/migrations/0001_initial_schema.sql
--- a/sql/migrations/0001_initial_schema.sql
+++ b/sql/migrations/0001_initial_schema.sql
@@ -60,3 +60,4 @@
     order_date  DATE
+    , effective_ts TIMESTAMP
 );
"""
    assert "existing migrations can't be changed" in ddl_policy.check_patch(patch)


def test_migration_names_are_checked():
    patch = NEW_MIGRATION.replace("0002_add_effective_ts_to_stg_orders", "fix-orders")
    assert "NNNN_lowercase_slug.sql" in ddl_policy.check_patch(patch)


def test_schema_changes_hidden_in_code_are_refused():
    patch = """diff --git a/dags/dag_factory.py b/dags/dag_factory.py
--- a/dags/dag_factory.py
+++ b/dags/dag_factory.py
@@ -45,2 +45,3 @@ def _task_schema(cfg):
         cols = ", ".join(header)
+        hook.run("ALTER TABLE stg_orders ADD COLUMN IF NOT EXISTS effective_ts TIMESTAMP")
         hook.insert_rows(
"""
    assert "belong in sql/migrations" in ddl_policy.check_patch(patch)


def test_ordinary_code_changes_are_unaffected():
    patch = """diff --git a/dags/config/pipelines.yaml b/dags/config/pipelines.yaml
--- a/dags/config/pipelines.yaml
+++ b/dags/config/pipelines.yaml
@@ -1,2 +1,2 @@
-    export_path: exports/missing_dir/sales.csv
+    export_path: exports/sales.csv
"""
    assert ddl_policy.check_patch(patch) is None


def test_next_migration_number():
    assert ddl_policy.next_migration_number([]) == "0001"
    assert ddl_policy.next_migration_number(["0001_initial_schema.sql", "0007_x.sql", "notes.md"]) == "0008"


# ------------------------------------------------------------------ the runner

class FakeCursor:
    def __init__(self, db):
        self.db = db

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        self.db.executed.append((" ".join(sql.split())[:60], params))
        if params and sql.lstrip().startswith("INSERT INTO schema_migrations"):
            self.db.applied.add(params[0])

    def fetchall(self):
        return [(v,) for v in sorted(self.db.applied)]


class FakeConn:
    def __init__(self, applied=()):
        self.applied = set(applied)
        self.executed = []

    def cursor(self):
        return FakeCursor(self)

    def commit(self):
        pass


@pytest.fixture
def migrations_dir(tmp_path, monkeypatch):
    monkeypatch.setattr(migrate, "MIGRATIONS", tmp_path)
    (tmp_path / "0001_initial.sql").write_text("CREATE TABLE a (id INT);")
    (tmp_path / "0002_add_b.sql").write_text("ALTER TABLE a ADD COLUMN b INT;")
    return tmp_path


def test_runner_applies_only_pending_migrations_in_order(migrations_dir):
    conn = FakeConn(applied={"0001_initial"})
    assert migrate.apply_pending(conn, log=lambda _: None) == ["0002_add_b.sql"]
    assert conn.applied == {"0001_initial", "0002_add_b"}


def test_runner_refuses_a_bad_migration_before_running_anything(migrations_dir):
    (migrations_dir / "0003_cleanup.sql").write_text("DELETE FROM a;")
    conn = FakeConn()
    with pytest.raises(migrate.MigrationError, match="0003_cleanup.sql violates"):
        migrate.apply_pending(conn, log=lambda _: None)
    assert conn.applied == set()  # 0001 and 0002 weren't applied either


def test_runner_refuses_badly_named_files(migrations_dir):
    (migrations_dir / "fix.sql").write_text("ALTER TABLE a ADD COLUMN c INT;")
    with pytest.raises(migrate.MigrationError, match="badly named"):
        migrate.apply_pending(FakeConn(), log=lambda _: None)


# A real claude-sonnet-5 patch: no `diff --git` / `new file mode` header, which git
# applies fine. The first version of the checker missed files written this way.
HEADERLESS = """--- /dev/null
+++ b/sql/migrations/0002_add_effective_ts_to_stg_orders.sql
@@ -0,0 +1,4 @@
+-- Upstream orders extract now includes an effective_ts column.
+-- Add it additively to stg_orders so the existing loader (which inserts
+-- all CSV header columns) can write it without a schema error.
+ALTER TABLE stg_orders ADD COLUMN effective_ts TIMESTAMP;
"""


def test_headerless_patches_are_parsed():
    [block] = ddl_policy._file_blocks(HEADERLESS)
    assert block["path"] == "sql/migrations/0002_add_effective_ts_to_stg_orders.sql"
    assert block["old_dev_null"] and len(block["added"]) == 4
    assert ddl_policy.check_patch(HEADERLESS) is None


def test_headerless_patches_are_still_checked():
    bad = HEADERLESS.replace("@@ -0,0 +1,4 @@", "@@ -0,0 +1,5 @@") + "+TRUNCATE stg_orders;\n"
    assert "found TRUNCATE" in ddl_policy.check_patch(bad)
    code = """--- a/dags/dag_factory.py
+++ b/dags/dag_factory.py
@@ -45,2 +45,3 @@
         cols = ", ".join(header)
+        hook.run("DROP TABLE dim_hierarchy_v1")
"""
    assert "belong in sql/migrations" in ddl_policy.check_patch(code)


def test_removed_sql_comment_lines_are_not_mistaken_for_headers():
    patch = """diff --git a/dags/dag_factory.py b/dags/dag_factory.py
--- a/dags/dag_factory.py
+++ b/dags/dag_factory.py
@@ -1,3 +1,2 @@
 x = 1
--- an old SQL comment inside a string
 y = 2
"""
    [block] = ddl_policy._file_blocks(patch)
    assert block["path"] == "dags/dag_factory.py" and block["removed"] == 1


def test_multi_file_patches_check_every_file():
    patch = NEW_MIGRATION + """--- a/dags/config/pipelines.yaml
+++ b/dags/config/pipelines.yaml
@@ -1 +1 @@
-    target_table: stg_orders
+    target_table: stg_orders_v2
"""
    assert [b["path"] for b in ddl_policy._file_blocks(patch)] == [
        "sql/migrations/0002_add_effective_ts_to_stg_orders.sql", "dags/config/pipelines.yaml"]


def test_unreadable_patches_are_refused():
    assert "couldn't tell which files" in ddl_policy.check_patch("@@ -0,0 +1 @@\n+DELETE FROM t;\n")
    assert ddl_policy.check_patch("") is None
