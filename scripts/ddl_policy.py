"""
What a schema migration is allowed to contain.

The triage agent may propose warehouse schema changes, but only additive DDL: create
or alter structure, never touch data and never destroy anything. Data problems are
fixed at their source by a person; a DELETE or UPDATE in a migration would just be a
quieter way of hiding them. Removing things (DROP, TRUNCATE, RENAME) breaks whatever
still depends on them, so that stays a human decision too.

The same rules are enforced twice: by the agent before it offers a PR
(`check_patch`), and by the migration runner before it applies a file
(`check_sql`), so a bad migration can't slip in by being written by hand.
"""

import re
from typing import Optional

MIGRATIONS_DIR = "sql/migrations"
MIGRATION_NAME = re.compile(r"^(\d{4})_[a-z0-9_]+\.sql$")

ALLOWED_STARTS = ("CREATE", "ALTER", "COMMENT")
FORBIDDEN_WORDS = ("DROP", "TRUNCATE", "RENAME")
# CREATE TABLE ... AS SELECT copies data; a view defined AS SELECT does not.
CREATE_TABLE_AS = re.compile(r"^CREATE\b[^;]*?\bTABLE\b[^;(]*\bAS\b", re.IGNORECASE | re.DOTALL)
# Schema changes smuggled into Python instead of a migration.
DDL_IN_CODE = re.compile(r"\b(CREATE|ALTER|DROP)\s+TABLE\b", re.IGNORECASE)


def strip_comments(sql: str) -> str:
    sql = re.sub(r"/\*.*?\*/", " ", sql, flags=re.DOTALL)
    return re.sub(r"--[^\n]*", " ", sql)


def split_statements(sql: str) -> list:
    """Split on semicolons that actually end a statement.

    A plain `sql.split(";")` tears `COMMENT ON TABLE t IS 'recreated; reload it'` in two
    and then reads the tail as an illegal statement — a real migration the models wrote
    was rejected that way. Single-quoted strings ('' escapes) and $$ blocks are skipped.
    """
    statements, current, quote, dollar = [], [], False, False
    i = 0
    while i < len(sql):
        char = sql[i]
        if dollar:
            if sql.startswith("$$", i):
                dollar, i = False, i + 2
                current.append("$$")
                continue
        elif quote:
            if char == "'":
                if sql.startswith("''", i):     # an escaped quote, still inside the string
                    current.append("''")
                    i += 2
                    continue
                quote = False
        elif sql.startswith("$$", i):
            dollar, i = True, i + 2
            current.append("$$")
            continue
        elif char == "'":
            quote = True
        elif char == ";":
            statements.append("".join(current))
            current = []
            i += 1
            continue
        current.append(char)
        i += 1
    statements.append("".join(current))
    return [s.strip() for s in statements if s.strip()]


def strip_strings(sql: str) -> str:
    """Blank out string literals, so a DROP *described* in a COMMENT isn't read as a
    DROP being performed. Keyword checks run on this; previews use the original."""
    out, quote, dollar = [], False, False
    i = 0
    while i < len(sql):
        if dollar:
            if sql.startswith("$$", i):
                dollar, i = False, i + 2
                continue
        elif quote:
            if sql.startswith("''", i):
                i += 2
                continue
            if sql[i] == "'":
                quote = False
                out.append("'")
                i += 1
                continue
        elif sql.startswith("$$", i):
            dollar, i = True, i + 2
            continue
        elif sql[i] == "'":
            quote = True
            out.append("'")
            i += 1
            continue
        if not (quote or dollar):
            out.append(sql[i])
        i += 1
    return "".join(out)


def check_sql(sql: str) -> Optional[str]:
    """None if every statement is additive DDL, else why not."""
    statements = split_statements(strip_comments(sql))
    if not statements:
        return "the migration has no statements"
    for stmt in statements:
        bare = strip_strings(stmt)          # what the statement *does*, minus its prose
        if not bare.strip():
            return f"statement has no SQL: {' '.join(stmt.split())[:80]}"
        first = bare.split(None, 1)[0].upper()
        preview = " ".join(stmt.split())[:80]
        if first not in ALLOWED_STARTS:
            return f"only CREATE, ALTER and COMMENT are allowed, found {first}: {preview}"
        for word in FORBIDDEN_WORDS:
            if re.search(rf"\b{word}\b", bare, re.IGNORECASE):
                return f"{word} isn't allowed in a migration: {preview}"
        if CREATE_TABLE_AS.search(bare):
            return f"CREATE TABLE ... AS copies data, which isn't allowed: {preview}"
    return None


def _file_blocks(patch: str) -> list:
    """Split a unified diff into per-file blocks: path, whether it's a new file, the
    added lines and the number of removed lines.

    A file starts at `diff --git` *or* at a `--- ` line followed by `+++ ` — models
    often omit the `diff --git` header, and git applies those patches happily, so a
    parser that waited for it would wave them through unchecked. A `--- ` line not
    followed by `+++ ` is a removed line whose text starts with "--" (an SQL comment)."""
    lines = patch.splitlines()
    blocks, cur = [], None

    def start():
        block = {"path": None, "new": False, "old_dev_null": False, "added": [], "removed": 0}
        blocks.append(block)
        return block

    for i, line in enumerate(lines):
        nxt = lines[i + 1] if i + 1 < len(lines) else ""
        if line.startswith("diff --git "):
            cur = start()
        elif line.startswith("--- ") and nxt.startswith("+++ "):
            if cur is None or cur["path"] is not None:
                cur = start()
            cur["old_dev_null"] = line.startswith("--- /dev/null")
        elif cur is None:
            continue
        elif line.startswith("+++ "):
            cur["path"] = line[6:].strip() if line.startswith("+++ b/") else line[4:].strip()
        elif line.startswith("new file mode"):
            cur["new"] = True
        elif line.startswith("+"):
            cur["added"].append(line[1:])
        elif line.startswith("-"):
            cur["removed"] += 1
    return [b for b in blocks if b["path"]]


def check_patch(patch: str) -> Optional[str]:
    """None if the patch respects the migration rules, else why not."""
    blocks = _file_blocks(patch)
    if patch.strip() and not blocks:
        # Never pass something we couldn't read: git may still be able to apply it.
        return "couldn't tell which files this patch changes"
    for block in blocks:
        path = block["path"]
        if path.startswith(MIGRATIONS_DIR + "/"):
            name = path[len(MIGRATIONS_DIR) + 1:]
            if not MIGRATION_NAME.match(name):
                return f"{path}: migrations must be named NNNN_lowercase_slug.sql"
            if not (block["new"] or block["old_dev_null"]) or block["removed"]:
                return f"{path}: existing migrations can't be changed — add a new one instead"
            why = check_sql("\n".join(block["added"]))
            if why:
                return f"{path}: {why}"
        else:
            for line in block["added"]:
                if DDL_IN_CODE.search(strip_comments(line)):
                    return f"{path}: schema changes belong in {MIGRATIONS_DIR}/, not in code"
    return None


def next_migration_number(existing_names) -> str:
    numbers = [int(m.group(1)) for n in existing_names if (m := MIGRATION_NAME.match(n))]
    return f"{max(numbers, default=0) + 1:04d}"
