from __future__ import annotations

import time
from contextlib import contextmanager
from typing import Any, Generator

import psycopg
from psycopg.abc import Buffer
from psycopg.adapt import Loader
from psycopg.pq import Format

from sql_autoresearch.models import (
    MAX_BYTES,
    MAX_ROWS,
    SUPPORTED_TYPE_OIDS,
    ColumnDesc,
    ColumnInfo,
    IndexInfo,
    TableInfo,
    TableStats,
    UnsupportedQueryError,
)

# Session GUCs pinned for deterministic output.
# Values are SQL-ready (quoted where needed). These are hardcoded constants,
# never user input, so they're safe to interpolate directly.
_SESSION_GUCS = {
    "timezone": "'UTC'",
    "datestyle": "'ISO'",
    "extra_float_digits": "3",
    "intervalstyle": "'iso_8601'",
    "bytea_output": "'hex'",
    "search_path": "pg_catalog, public",
    "default_transaction_read_only": "on",
    "statement_timeout": "'120s'",
    "lock_timeout": "'5s'",
}


class RawTextLoader(Loader):
    """Return every value as its PG text representation (str)."""
    format = Format.TEXT

    def load(self, data: Buffer) -> str:
        if isinstance(data, memoryview):
            data = bytes(data)
        if isinstance(data, bytes):
            return data.decode("utf-8")
        return str(data)


def connect(dsn: str) -> psycopg.Connection:
    """Open a read-only connection with pinned GUCs and text loaders."""
    conn = psycopg.connect(
        dsn,
        autocommit=True,
        prepare_threshold=None,
    )
    # Pin session GUCs (all values are hardcoded constants, not user input)
    with conn.cursor() as cur:
        for guc, val in _SESSION_GUCS.items():
            cur.execute(f"SET {guc} = {val}")

    # Register RawTextLoader for all supported OIDs so values come back as strings.
    # Skip bool (OID 16) — it's deterministic and catalog queries rely on native
    # Python bool (the string 'f' is truthy, which breaks EXISTS checks).
    for oid in SUPPORTED_TYPE_OIDS:
        if oid == 16:
            continue
        conn.adapters.register_loader(oid, RawTextLoader)

    return conn


@contextmanager
def repeatable_read_txn(conn: psycopg.Connection) -> Generator[psycopg.Cursor, None, None]:
    """Context manager: REPEATABLE READ READ ONLY transaction."""
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            cur.execute(
                "START TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"
            )
            yield cur
            conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.autocommit = True


def execute_query(
    cur: psycopg.Cursor,
    sql: str,
    max_rows: int = MAX_ROWS,
    max_bytes: int = MAX_BYTES,
) -> tuple[list[ColumnDesc], list[tuple[str | None, ...]]]:
    """Execute a query, fetch results as PG text, enforce size limits.

    Returns (column_descriptions, rows).
    Raises UnsupportedQueryError if result exceeds limits.
    """
    cur.execute(sql)

    # Schema from cursor.description
    if cur.description is None:
        return [], []

    col_descs = [
        ColumnDesc(name=col.name, type_oid=col.type_code)
        for col in cur.description
    ]

    # Fetch with streaming byte count
    rows: list[tuple[str | None, ...]] = []
    byte_count = 0
    row_count = 0

    for row in cur:
        row_count += 1
        if row_count > max_rows:
            raise UnsupportedQueryError(
                f"Result exceeds {max_rows} row limit"
            )
        for val in row:
            if val is not None:
                byte_count += len(val.encode("utf-8")) if isinstance(val, str) else len(str(val))
        if byte_count > max_bytes:
            raise UnsupportedQueryError(
                f"Result exceeds {max_bytes // (1024*1024)}MB byte limit"
            )
        rows.append(tuple(row))

    return col_descs, rows


def check_column_types(col_descs: list[ColumnDesc]) -> None:
    """Raise UnsupportedQueryError if any column type is not supported."""
    for col in col_descs:
        if col.type_oid not in SUPPORTED_TYPE_OIDS:
            raise UnsupportedQueryError(
                f"Column '{col.name}' has unsupported type OID {col.type_oid}"
            )


def check_schema_equality(
    original_descs: list[ColumnDesc], candidate_descs: list[ColumnDesc]
) -> str | None:
    """Check schema equality between original and candidate results.

    Returns None if equal, or a description of the mismatch.
    """
    if len(original_descs) != len(candidate_descs):
        return (
            f"Column count mismatch: original={len(original_descs)}, "
            f"candidate={len(candidate_descs)}"
        )
    for i, (orig, cand) in enumerate(zip(original_descs, candidate_descs)):
        if orig.name != cand.name:
            return (
                f"Column {i} name mismatch: original='{orig.name}', "
                f"candidate='{cand.name}'"
            )
        if orig.type_oid != cand.type_oid:
            return (
                f"Column '{orig.name}' type OID mismatch: "
                f"original={orig.type_oid}, candidate={cand.type_oid}"
            )
    return None


def time_query(cur: psycopg.Cursor, sql: str) -> float:
    """Execute a query, drain all rows, return wall-clock time in ms."""
    start = time.monotonic()
    cur.execute(sql)
    for _ in cur:
        pass
    elapsed = (time.monotonic() - start) * 1000
    return elapsed


def get_explain_json(cur: psycopg.Cursor, sql: str) -> list[dict[str, Any]]:
    """Run EXPLAIN (FORMAT JSON) and return the plan."""
    cur.execute(f"EXPLAIN (FORMAT JSON) {sql}")
    row = cur.fetchone()
    return row[0] if row else []


def get_table_info(conn: psycopg.Connection, oid: int) -> TableInfo:
    """Fetch table metadata: columns, indexes, stats (no data values)."""
    with conn.cursor() as cur:
        # Basic info
        cur.execute(
            """
            SELECT n.nspname, c.relname, c.reltuples
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.oid = %s
            """,
            (oid,),
        )
        row = cur.fetchone()
        if row is None:
            raise ValueError(f"Table OID {oid} not found")
        schema, name, reltuples = row

        # Columns
        cur.execute(
            """
            SELECT a.attname, t.typname, a.atttypid, a.attnotnull
            FROM pg_attribute a
            JOIN pg_type t ON t.oid = a.atttypid
            WHERE a.attrelid = %s AND a.attnum > 0 AND NOT a.attisdropped
            ORDER BY a.attnum
            """,
            (oid,),
        )
        columns = [
            ColumnInfo(name=r[0], type_name=r[1], type_oid=r[2], not_null=r[3])
            for r in cur.fetchall()
        ]

        # Indexes
        cur.execute(
            """
            SELECT ic.relname, pg_get_indexdef(i.indexrelid),
                   i.indisunique, i.indisprimary
            FROM pg_index i
            JOIN pg_class ic ON ic.oid = i.indexrelid
            WHERE i.indrelid = %s
            ORDER BY ic.relname
            """,
            (oid,),
        )
        indexes = [
            IndexInfo(name=r[0], definition=r[1], is_unique=r[2], is_primary=r[3])
            for r in cur.fetchall()
        ]

        # Stats (numeric distributions only — no most_common_vals or histogram_bounds)
        cur.execute(
            """
            SELECT s.attname, s.n_distinct, s.null_frac, s.correlation
            FROM pg_stats s
            WHERE s.schemaname = %s AND s.tablename = %s
            """,
            (schema, name),
        )
        stats = TableStats()
        for attname, n_distinct, null_frac, correlation in cur.fetchall():
            if n_distinct is not None:
                stats.n_distinct[attname] = float(n_distinct)
            if null_frac is not None:
                stats.null_frac[attname] = float(null_frac)
            if correlation is not None:
                stats.correlation[attname] = float(correlation)

    return TableInfo(
        schema=schema,
        name=name,
        oid=oid,
        columns=columns,
        indexes=indexes,
        stats=stats,
        row_estimate=float(reltuples),
    )
