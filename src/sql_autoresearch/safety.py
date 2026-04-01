from __future__ import annotations

import pglast
from pglast import ast, prettify
from pglast.enums.parsenodes import (
    FRAMEOPTION_BETWEEN,
    FRAMEOPTION_END_UNBOUNDED_FOLLOWING,
    FRAMEOPTION_START_UNBOUNDED_PRECEDING,
    SetOperation,
)
from pglast.visitors import Visitor

from sql_autoresearch.models import (
    ALLOWED_AGGREGATES,
    ALLOWED_WINDOW_FUNCTIONS,
    BUILTIN_CAST_TYPE_NAMES,
    ORDER_DEPENDENT_AGGREGATES,
    REJECTED_WINDOW_FUNCTIONS,
    AstCheckResult,
    RelationRef,
    ResolvedRelation,
    UnsafeQueryError,
)

_FULL_PARTITION_MASK = (
    FRAMEOPTION_BETWEEN
    | FRAMEOPTION_START_UNBOUNDED_PRECEDING
    | FRAMEOPTION_END_UNBOUNDED_FOLLOWING
)

# STABLE functions allowed because we pin all GUCs (timezone=UTC, DateStyle=ISO, etc.).
# These are transaction-stable under REPEATABLE READ on a quiescent DB.
STABLE_FUNCTION_ALLOWLIST: frozenset[str] = frozenset({
    # Cross-type timestamp/timestamptz comparisons
    "timestamp_cmp_timestamptz", "timestamp_eq_timestamptz",
    "timestamp_ne_timestamptz", "timestamp_lt_timestamptz",
    "timestamp_le_timestamptz", "timestamp_gt_timestamptz",
    "timestamp_ge_timestamptz",
    "timestamptz_cmp_timestamp", "timestamptz_eq_timestamp",
    "timestamptz_ne_timestamp", "timestamptz_lt_timestamp",
    "timestamptz_le_timestamp", "timestamptz_gt_timestamp",
    "timestamptz_ge_timestamp",
    # Cross-type date/timestamp comparisons
    "date_cmp_timestamp", "date_eq_timestamp",
    "date_ne_timestamp", "date_lt_timestamp",
    "date_le_timestamp", "date_gt_timestamp",
    "date_ge_timestamp",
    "timestamp_cmp_date", "timestamp_eq_date",
    "timestamp_ne_date", "timestamp_lt_date",
    "timestamp_le_date", "timestamp_gt_date",
    "timestamp_ge_date",
    # Cross-type date/timestamptz comparisons
    "date_cmp_timestamptz", "date_eq_timestamptz",
    "date_ne_timestamptz", "date_lt_timestamptz",
    "date_le_timestamptz", "date_gt_timestamptz",
    "date_ge_timestamptz",
    "timestamptz_cmp_date", "timestamptz_eq_date",
    "timestamptz_ne_date", "timestamptz_lt_date",
    "timestamptz_le_date", "timestamptz_gt_date",
    "timestamptz_ge_date",
    # Type conversions (STABLE because of timezone GUC)
    "timestamptz_timestamp", "timestamp_timestamptz",
    "date_timestamp", "date_timestamptz",
    "timestamptz_date", "timestamp_date",
    "timestamptz_time", "timestamptz_timetz",
    "timetz_timestamptz",
    # I/O functions (STABLE because of GUCs like DateStyle)
    "timestamp_in", "timestamp_out",
    "timestamptz_in", "timestamptz_out",
    "date_in", "date_out",
    "time_in", "time_out",
    "timetz_in", "timetz_out",
    "interval_in", "interval_out",
    "float4in", "float8in",
    # Date/time arithmetic (STABLE for timestamptz)
    "timestamp_pl_interval", "timestamp_mi_interval",
    "timestamptz_pl_interval", "timestamptz_mi_interval",
    "interval_pl_timestamp", "interval_pl_timestamptz",
    "date_pl_interval", "date_mi_interval",
    "interval_pl_date",
    "timestamp_mi", "timestamptz_mi",
    # Timestamp parts/extract
    "extract",
    "timestamp_part", "timestamptz_part",
    "date_part", "time_part", "interval_part",
    # Hashing (STABLE for some types)
    "timestamp_hash", "timestamptz_hash",
    "timestamp_hash_extended", "timestamptz_hash_extended",
    # Min/max support
    "timestamp_larger", "timestamp_smaller",
    "timestamptz_larger", "timestamptz_smaller",
    "date_larger", "date_smaller",
    # Date truncation
    "date_trunc",
    "timestamp_trunc", "timestamptz_trunc",
    "interval_trunc",
    # Age functions
    "timestamp_age", "timestamptz_age",
    # Sorting support
    "timestamp_sortsupport", "date_sortsupport",
    # Array-related STABLE functions
    "array_to_text", "array_to_string",
})


# ──────────────────────────────────────────────────────────────────────
#  AST-only checks (no DB connection needed)
# ──────────────────────────────────────────────────────────────────────

def check_ast(sql: str) -> AstCheckResult:
    """Parse SQL, run structural checks, strip comments, extract metadata.

    Raises UnsafeQueryError on any structural violation.
    """
    # Parse
    try:
        stmts = pglast.parse_sql(sql)
    except pglast.parser.ParseError as e:
        raise UnsafeQueryError(f"Parse error: {e}") from e

    if len(stmts) != 1:
        raise UnsafeQueryError(
            f"Multi-statement SQL not allowed (got {len(stmts)} statements)"
        )

    stmt = stmts[0].stmt
    if not isinstance(stmt, ast.SelectStmt):
        raise UnsafeQueryError(
            f"Only SELECT statements allowed, got {type(stmt).__name__}"
        )

    # Strip comments by deparsing through the parser
    clean_sql = prettify(sql)

    # Run Visitor over entire tree for structural checks + metadata collection
    collector = _AstCollector()
    collector(stmts[0])

    # Top-level ORDER BY detection (for equivalence comparison mode)
    has_order_by = _top_level_has_order_by(stmt)

    # CTE-aware relation extraction
    relations = _extract_relations(stmt)
    if not relations:
        raise UnsafeQueryError("No base tables referenced (zero-table query)")

    return AstCheckResult(
        clean_sql=clean_sql,
        function_names=collector.function_names,
        operator_names=collector.operator_names,
        relations=relations,
        has_order_by=has_order_by,
        cast_type_names=collector.cast_type_names,
    )


# ──────────────────────────────────────────────────────────────────────
#  Catalog checks (requires DB connection)
# ──────────────────────────────────────────────────────────────────────

def check_catalog(conn, ast_result: AstCheckResult) -> list[ResolvedRelation]:
    """Verify functions, operators, and relations against the DB catalog.

    Raises UnsafeQueryError on any catalog violation.
    Returns the list of resolved relations.
    """
    resolved = _resolve_relations(conn, ast_result.relations)
    _check_domain_enum_columns(conn, resolved)
    _check_function_namespaces(conn, ast_result.function_names)
    _check_operator_namespaces(conn, ast_result.operator_names)
    _check_cast_types(conn, ast_result.cast_type_names)
    return resolved


# ──────────────────────────────────────────────────────────────────────
#  Visitor for structural checks + metadata extraction
# ──────────────────────────────────────────────────────────────────────

class _AstCollector(Visitor):
    def __init__(self):
        self.function_names: set[str] = set()
        self.operator_names: set[str] = set()
        self.cast_type_names: list[tuple[str, ...]] = []

    # ── SelectStmt constraints ──

    def visit_SelectStmt(self, ancestors, node):
        if node.limitCount is not None or node.limitOffset is not None:
            raise UnsafeQueryError("LIMIT/OFFSET/FETCH FIRST not allowed")

        if node.distinctClause is not None:
            if any(item is not None for item in node.distinctClause):
                raise UnsafeQueryError("DISTINCT ON not allowed")

        if node.lockingClause is not None:
            locking_names = []
            for lc in node.lockingClause:
                s = lc.strength
                locking_names.append(s.name if hasattr(s, "name") else str(s))
            raise UnsafeQueryError(
                f"FOR UPDATE/SHARE/NO KEY UPDATE/KEY SHARE not allowed ({', '.join(locking_names)})"
            )

    # ── Reject non-SELECT in CTEs ──

    def visit_InsertStmt(self, ancestors, node):
        raise UnsafeQueryError("INSERT statements not allowed (writable CTE?)")

    def visit_UpdateStmt(self, ancestors, node):
        raise UnsafeQueryError("UPDATE statements not allowed (writable CTE?)")

    def visit_DeleteStmt(self, ancestors, node):
        raise UnsafeQueryError("DELETE statements not allowed (writable CTE?)")

    def visit_MergeStmt(self, ancestors, node):
        raise UnsafeQueryError("MERGE statements not allowed")

    # ── Parameter references ──

    def visit_ParamRef(self, ancestors, node):
        raise UnsafeQueryError(
            f"Parameter references (${node.number}) not allowed"
        )

    # ── FROM clause restrictions ──

    def visit_RangeFunction(self, ancestors, node):
        raise UnsafeQueryError(
            "Set-returning functions in FROM clause not allowed"
        )

    def visit_RangeTableSample(self, ancestors, node):
        raise UnsafeQueryError("TABLESAMPLE not allowed")

    def visit_RangeTableFunc(self, ancestors, node):
        raise UnsafeQueryError("XMLTABLE/JSON_TABLE in FROM not allowed")

    def visit_RangeSubselect(self, ancestors, node):
        if isinstance(node.subquery, ast.SelectStmt) and node.subquery.valuesLists:
            raise UnsafeQueryError("VALUES as FROM source not allowed")

    # ── Function calls (aggregates + window) ──

    def visit_FuncCall(self, ancestors, node):
        name = _func_name(node)
        self.function_names.add(name)

        if name in ORDER_DEPENDENT_AGGREGATES:
            raise UnsafeQueryError(
                f"Order-dependent aggregate '{name}' not allowed"
            )

        if node.over is not None:
            self._check_window(name, node)
        elif _is_aggregate_call(name, node):
            if name not in ALLOWED_AGGREGATES:
                raise UnsafeQueryError(
                    f"Aggregate function '{name}' not in allowlist"
                )

    def _check_window(self, name: str, node: ast.FuncCall):
        if name in REJECTED_WINDOW_FUNCTIONS:
            raise UnsafeQueryError(f"Window function '{name}' not allowed")
        if name in ALLOWED_WINDOW_FUNCTIONS:
            return
        if name in ALLOWED_AGGREGATES:
            wd = node.over
            if not wd.orderClause:
                return  # No ORDER BY in OVER → full partition → OK
            if (wd.frameOptions & _FULL_PARTITION_MASK) == _FULL_PARTITION_MASK:
                return  # Explicit full frame → OK
            raise UnsafeQueryError(
                f"Aggregate '{name}' as window function requires full-partition "
                f"form (no ORDER BY in OVER, or explicit ROWS/RANGE BETWEEN "
                f"UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)"
            )
        # Unknown window function — will be caught by catalog check

    # ── Operators ──

    def visit_A_Expr(self, ancestors, node):
        if node.name:
            for s in node.name:
                self.operator_names.add(s.sval)

    # ── Type casts ──

    def visit_TypeCast(self, ancestors, node):
        names = tuple(s.sval.lower() for s in node.typeName.names)
        self.cast_type_names.append(names)
        self._check_cast_type(names)

    def _check_cast_type(self, names: tuple[str, ...]):
        if len(names) == 2:
            schema, type_name = names
            if schema != "pg_catalog":
                raise UnsafeQueryError(
                    f"Cast to non-pg_catalog type '{schema}.{type_name}' not allowed"
                )
            if type_name not in BUILTIN_CAST_TYPE_NAMES:
                raise UnsafeQueryError(
                    f"Cast to unsupported type 'pg_catalog.{type_name}' not allowed"
                )
        elif len(names) == 1:
            type_name = names[0]
            if type_name not in BUILTIN_CAST_TYPE_NAMES:
                raise UnsafeQueryError(
                    f"Cast to unsupported type '{type_name}' not allowed"
                )
        else:
            raise UnsafeQueryError(f"Cast to unknown type {names!r} not allowed")


# ──────────────────────────────────────────────────────────────────────
#  Helpers
# ──────────────────────────────────────────────────────────────────────

def _func_name(node: ast.FuncCall) -> str:
    """Get the unqualified function name, lowercased."""
    return node.funcname[-1].sval.lower()


def _is_aggregate_call(name: str, node: ast.FuncCall) -> bool:
    if node.agg_star:
        return True
    if node.agg_within_group:
        return True
    if node.agg_order:
        return True
    if hasattr(node, "agg_filter") and node.agg_filter:
        return True
    return name in ALLOWED_AGGREGATES


def _top_level_has_order_by(stmt: ast.SelectStmt) -> bool:
    return stmt.sortClause is not None and len(stmt.sortClause) > 0


# ──────────────────────────────────────────────────────────────────────
#  CTE-aware relation extraction
# ──────────────────────────────────────────────────────────────────────

def _extract_relations(stmt: ast.SelectStmt) -> list[RelationRef]:
    """Scope-aware walk: collect RangeVar nodes, skipping CTE references."""
    out: list[RelationRef] = []
    _walk_select_rels(stmt, frozenset(), out)
    return out


def _walk_select_rels(
    node: ast.SelectStmt, cte_scope: frozenset[str], out: list[RelationRef]
) -> None:
    # Set operations (UNION/INTERSECT/EXCEPT)
    if node.op != SetOperation.SETOP_NONE:
        if node.larg:
            _walk_select_rels(node.larg, cte_scope, out)
        if node.rarg:
            _walk_select_rels(node.rarg, cte_scope, out)
        return

    # Process CTEs
    local_ctes: set[str] = set()
    if node.withClause:
        all_cte_names = {cte.ctename for cte in node.withClause.ctes}
        accumulated: set[str] = set()
        for cte in node.withClause.ctes:
            if node.withClause.recursive:
                inner_scope = cte_scope | all_cte_names
            else:
                inner_scope = cte_scope | accumulated
            if isinstance(cte.ctequery, ast.SelectStmt):
                _walk_select_rels(cte.ctequery, frozenset(inner_scope), out)
            accumulated.add(cte.ctename)
        local_ctes = all_cte_names

    effective = cte_scope | frozenset(local_ctes)

    # Walk FROM clause
    if node.fromClause:
        for item in node.fromClause:
            _walk_from_rels(item, effective, out)

    # Walk expressions for SubLinks (EXISTS, IN subqueries)
    _walk_exprs_for_sublinks(node, effective, out)


def _walk_from_rels(
    node, cte_scope: frozenset[str], out: list[RelationRef]
) -> None:
    if isinstance(node, ast.RangeVar):
        if node.relname not in cte_scope:
            out.append(RelationRef(
                schema=node.schemaname if node.schemaname else None,
                name=node.relname,
                inh=node.inh,
            ))
    elif isinstance(node, ast.JoinExpr):
        _walk_from_rels(node.larg, cte_scope, out)
        _walk_from_rels(node.rarg, cte_scope, out)
    elif isinstance(node, ast.RangeSubselect):
        if isinstance(node.subquery, ast.SelectStmt):
            _walk_select_rels(node.subquery, cte_scope, out)


def _walk_exprs_for_sublinks(
    node: ast.SelectStmt, cte_scope: frozenset[str], out: list[RelationRef]
) -> None:
    """Scan all expression trees in a SelectStmt for SubLink nodes."""
    exprs = []
    if node.targetList:
        for target in node.targetList:
            if target.val is not None:
                exprs.append(target.val)
    if node.whereClause is not None:
        exprs.append(node.whereClause)
    if node.havingClause is not None:
        exprs.append(node.havingClause)
    if node.groupClause:
        exprs.extend(node.groupClause)
    if node.sortClause:
        for sb in node.sortClause:
            if hasattr(sb, "node") and sb.node is not None:
                exprs.append(sb.node)
    for expr in exprs:
        _walk_node_sublinks(expr, cte_scope, out)


def _walk_node_sublinks(node, cte_scope: frozenset[str], out: list[RelationRef]) -> None:
    """Recursively walk an expression tree looking for SubLink nodes."""
    if node is None:
        return
    if isinstance(node, ast.SubLink):
        if isinstance(node.subselect, ast.SelectStmt):
            _walk_select_rels(node.subselect, cte_scope, out)
    if isinstance(node, tuple):
        for item in node:
            _walk_node_sublinks(item, cte_scope, out)
    elif isinstance(node, ast.Node):
        for field_name in type(node).__slots__:
            child = getattr(node, field_name, None)
            if child is not None and isinstance(child, (ast.Node, tuple)):
                _walk_node_sublinks(child, cte_scope, out)


# ──────────────────────────────────────────────────────────────────────
#  Catalog checks
# ──────────────────────────────────────────────────────────────────────

def _resolve_relations(
    conn, relations: list[RelationRef]
) -> list[ResolvedRelation]:
    """Resolve each RelationRef against pg_class. Check relkind, persistence,
    temp namespaces, and pg_inherits descendants."""
    resolved: list[ResolvedRelation] = []
    seen: set[tuple[str, str]] = set()

    for rel in relations:
        schema = rel.schema if rel.schema else "public"
        key = (schema, rel.name)
        if key in seen:
            continue
        seen.add(key)

        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT c.oid, n.nspname, c.relname, c.relkind, c.relpersistence
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE c.relname = %s AND n.nspname = %s
                """,
                (rel.name, schema),
            )
            row = cur.fetchone()

        if row is None:
            raise UnsafeQueryError(
                f"Relation '{schema}.{rel.name}' not found in catalog"
            )

        oid, nspname, relname, relkind, relpersistence = row

        # Unqualified name must resolve in public
        if rel.schema is None and nspname != "public":
            raise UnsafeQueryError(
                f"Unqualified relation '{rel.name}' resolves to "
                f"'{nspname}' (must resolve in 'public')"
            )

        # Temp namespace check
        if nspname.startswith("pg_temp_") or nspname.startswith("pg_toast_temp_"):
            raise UnsafeQueryError(
                f"Temporary table '{nspname}.{relname}' not allowed"
            )
        if relpersistence == "t":
            raise UnsafeQueryError(
                f"Temporary table '{relname}' not allowed (relpersistence='t')"
            )

        # relkind check
        if relkind == "p":
            raise UnsafeQueryError(
                f"Partitioned table '{nspname}.{relname}' not allowed"
            )
        if relkind != "r":
            kind_labels = {
                "v": "view", "m": "materialized view", "f": "foreign table",
                "c": "composite type", "S": "sequence", "I": "partitioned index",
            }
            label = kind_labels.get(relkind, f"relkind='{relkind}'")
            raise UnsafeQueryError(
                f"Relation '{nspname}.{relname}' is a {label}, "
                f"only ordinary tables (relkind='r') allowed"
            )

        # pg_inherits: reject if has descendants, unless ONLY
        with conn.cursor() as cur:
            cur.execute(
                "SELECT EXISTS(SELECT 1 FROM pg_inherits WHERE inhparent = %s)",
                (oid,),
            )
            has_descendants = cur.fetchone()[0]

        if has_descendants and rel.inh:
            raise UnsafeQueryError(
                f"Table '{nspname}.{relname}' has inheritance descendants. "
                f"Use ONLY to query without descendants, or drop this table from scope."
            )

        resolved.append(ResolvedRelation(schema=nspname, name=relname, oid=oid))

    return resolved


def _check_domain_enum_columns(conn, resolved: list[ResolvedRelation]) -> None:
    """Reject tables with domain or enum columns."""
    for rel in resolved:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT a.attname, t.typname, t.typtype
                FROM pg_attribute a
                JOIN pg_type t ON t.oid = a.atttypid
                WHERE a.attrelid = %s
                  AND a.attnum > 0
                  AND NOT a.attisdropped
                  AND t.typtype IN ('d', 'e')
                """,
                (rel.oid,),
            )
            row = cur.fetchone()
        if row is not None:
            attname, typname, typtype = row
            kind = "domain" if typtype == "d" else "enum"
            raise UnsafeQueryError(
                f"Table '{rel.schema}.{rel.name}' has {kind} column "
                f"'{attname}' (type '{typname}'). Domain/enum types not supported."
            )


def _check_function_namespaces(conn, function_names: set[str]) -> None:
    """Verify all functions resolve exclusively to pg_catalog.
    Also check volatility."""
    for fname in function_names:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT p.proname, p.provolatile, n.nspname
                FROM pg_proc p
                JOIN pg_namespace n ON n.oid = p.pronamespace
                WHERE p.proname = %s
                """,
                (fname,),
            )
            rows = cur.fetchall()

        if not rows:
            # Function not found — could be a keyword used as function syntax
            # (e.g., COALESCE, GREATEST). These don't appear in pg_proc.
            continue

        for proname, provolatile, nspname in rows:
            if nspname != "pg_catalog":
                raise UnsafeQueryError(
                    f"Function '{proname}' exists in namespace '{nspname}' "
                    f"(only pg_catalog functions allowed)"
                )

        # Volatility: take most volatile across all overloads
        volatilities = {row[1] for row in rows}
        if "v" in volatilities:
            raise UnsafeQueryError(
                f"Function '{fname}' has VOLATILE overload (not allowed)"
            )
        if "s" in volatilities:
            # Check STABLE allowlist
            stable_procs = [r for r in rows if r[1] == "s"]
            for proname, _, _ in stable_procs:
                if proname not in STABLE_FUNCTION_ALLOWLIST:
                    raise UnsafeQueryError(
                        f"Function '{proname}' is STABLE but not on the allowlist"
                    )


def _check_operator_namespaces(conn, operator_names: set[str]) -> None:
    """Verify all operators resolve exclusively to pg_catalog.
    Then check volatility of underlying functions via JOIN."""
    for opname in operator_names:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT o.oprname, n.nspname, p.proname, p.provolatile
                FROM pg_operator o
                JOIN pg_namespace n ON n.oid = o.oprnamespace
                LEFT JOIN pg_proc p ON p.oid = o.oprcode
                WHERE o.oprname = %s
                """,
                (opname,),
            )
            rows = cur.fetchall()

        if not rows:
            continue

        for oprname, nspname, proname, provolatile in rows:
            if nspname != "pg_catalog":
                raise UnsafeQueryError(
                    f"Operator '{oprname}' exists in namespace '{nspname}' "
                    f"(only pg_catalog operators allowed)"
                )
            if provolatile == "v":
                raise UnsafeQueryError(
                    f"Operator '{opname}' uses VOLATILE function '{proname}'"
                )
            if provolatile == "s" and proname not in STABLE_FUNCTION_ALLOWLIST:
                raise UnsafeQueryError(
                    f"Operator '{opname}' uses STABLE function '{proname}' "
                    f"not on the allowlist"
                )


def _check_cast_types(conn, cast_type_names: list[tuple[str, ...]]) -> None:
    """Verify explicit cast targets are built-in types (not domains/enums)."""
    for names in cast_type_names:
        if len(names) == 2:
            schema, type_name = names
        elif len(names) == 1:
            schema, type_name = "pg_catalog", names[0]
        else:
            raise UnsafeQueryError(f"Unknown cast type {names!r}")

        with conn.cursor() as cur:
            # Try pg_catalog first for unqualified names
            cur.execute(
                """
                SELECT t.oid, t.typname, t.typtype, n.nspname
                FROM pg_type t
                JOIN pg_namespace n ON n.oid = t.typnamespace
                WHERE t.typname = %s AND n.nspname = %s
                """,
                (type_name, schema),
            )
            row = cur.fetchone()

        if row is None:
            # For unqualified names not in pg_catalog, check public
            if len(names) == 1:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT t.oid, t.typname, t.typtype, n.nspname
                        FROM pg_type t
                        JOIN pg_namespace n ON n.oid = t.typnamespace
                        WHERE t.typname = %s
                        ORDER BY n.nspname = 'pg_catalog' DESC
                        LIMIT 1
                        """,
                        (type_name,),
                    )
                    row = cur.fetchone()

        if row is None:
            raise UnsafeQueryError(f"Cast target type '{type_name}' not found")

        _, typname, typtype, nspname = row
        if nspname != "pg_catalog":
            raise UnsafeQueryError(
                f"Cast target type '{nspname}.{typname}' is not in pg_catalog"
            )
        if typtype == "d":
            raise UnsafeQueryError(
                f"Cast target type '{typname}' is a domain (not allowed)"
            )
        if typtype == "e":
            raise UnsafeQueryError(
                f"Cast target type '{typname}' is an enum (not allowed)"
            )
