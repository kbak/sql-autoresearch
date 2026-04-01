from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from typing import Any

import anthropic

from sql_autoresearch.adapters import postgres as pg
from sql_autoresearch.benchmark import benchmark_pair
from sql_autoresearch.equivalence import compare_results
from sql_autoresearch.generate import generate_candidate
from sql_autoresearch.models import (
    MAX_BYTES,
    MAX_ROWS,
    CompareResult,
    IterationResult,
    IterationStatus,
    PromptBudgetError,
    QueryOutcome,
    QueryResult,
    UnsafeQueryError,
    UnsupportedQueryError,
)
from sql_autoresearch.safety import check_ast, check_catalog


def optimize_query(
    dsn: str,
    sql: str,
    api_key: str,
    max_iterations: int = 5,
    log_dir: str | None = None,
    max_rows: int = MAX_ROWS,
    max_bytes: int = MAX_BYTES,
    early_stop: bool = False,
) -> QueryResult:
    """Full ratchet optimization loop for a single query.

    1. Safety check original
    2. Connect to DB, resolve relations
    3. Probe original (execute, check types/size, record baseline timing)
    4. Gather table info + EXPLAIN
    5. Ratchet loop: generate candidate → safety check → verify → benchmark → keep/discard
    6. Log results to JSONL
    """
    iterations: list[IterationResult] = []

    # ── Step 1: AST safety check on original ──
    try:
        ast_result = check_ast(sql)
    except UnsafeQueryError as e:
        return _finalize(sql, sql, iterations, error=str(e),
                         forced_outcome=QueryOutcome.UNSUPPORTED_SAFETY,
                         log_dir=log_dir)

    clean_sql = ast_result.clean_sql

    # ── Step 2: Connect and catalog checks ──
    try:
        conn = pg.connect(dsn)
    except Exception as e:
        return _finalize(sql, sql, iterations, error=f"Connection error: {e}",
                         forced_outcome=QueryOutcome.ERROR, log_dir=log_dir)

    try:
        try:
            resolved = check_catalog(conn, ast_result)
        except UnsafeQueryError as e:
            return _finalize(sql, sql, iterations, error=str(e),
                             forced_outcome=QueryOutcome.UNSUPPORTED_SAFETY,
                             log_dir=log_dir)

        # ── Step 3: Probe original ──
        try:
            with pg.repeatable_read_txn(conn) as cur:
                orig_descs, orig_rows = pg.execute_query(
                    cur, clean_sql, max_rows=max_rows, max_bytes=max_bytes
                )
                pg.check_column_types(orig_descs)
                baseline_ms = pg.time_query(cur, clean_sql)
        except UnsupportedQueryError as e:
            etype = str(e)
            if "type OID" in etype:
                outcome = QueryOutcome.UNSUPPORTED_TYPES
            elif "limit" in etype.lower():
                outcome = QueryOutcome.UNSUPPORTED_TOO_LARGE
            else:
                outcome = QueryOutcome.UNSUPPORTED_TOO_LARGE
            return _finalize(sql, sql, iterations, error=str(e),
                             forced_outcome=outcome, log_dir=log_dir)
        except Exception as e:
            return _finalize(sql, sql, iterations, error=f"Probe error: {e}",
                             forced_outcome=QueryOutcome.ERROR, log_dir=log_dir)

        # ── Step 4: Table info + EXPLAIN ──
        try:
            tables = [pg.get_table_info(conn, r.oid) for r in resolved]
            with pg.repeatable_read_txn(conn) as cur:
                current_explain = pg.get_explain_json(cur, clean_sql)
        except Exception as e:
            return _finalize(sql, sql, iterations, error=f"Metadata error: {e}",
                             forced_outcome=QueryOutcome.ERROR, log_dir=log_dir)

        # ── Step 5: Ratchet loop ──
        client = anthropic.Anthropic(api_key=api_key)
        current_best = clean_sql
        current_best_timing = baseline_ms
        current_best_explain = current_explain
        original_resolved_oids = {r.oid for r in resolved}
        previous_failures: list[str] = []
        consecutive_stop_count = 0

        for iteration_num in range(1, max_iterations + 1):
            iter_result = _run_iteration(
                conn=conn,
                client=client,
                original_sql=clean_sql,
                original_descs=orig_descs,
                original_rows=orig_rows,
                original_resolved_oids=original_resolved_oids,
                current_best=current_best,
                current_best_timing=current_best_timing,
                current_best_explain=current_best_explain,
                tables=tables,
                has_order_by=ast_result.has_order_by,
                iteration_num=iteration_num,
                previous_failures=previous_failures,
                max_rows=max_rows,
                max_bytes=max_bytes,
            )
            iterations.append(iter_result)

            # Accumulate FAILED_SAFETY messages for next iteration
            if iter_result.status == IterationStatus.FAILED_SAFETY:
                if iter_result.explanation:
                    previous_failures.append(iter_result.explanation)

            # Early-stop: counting statuses increment, non-counting skip
            _COUNTING_STATUSES = {
                IterationStatus.DISCARDED_SLOWER,
                IterationStatus.FAILED_MISMATCH,
                IterationStatus.FAILED_TIE_REORDER,
                IterationStatus.FAILED_SCHEMA,
                IterationStatus.CANDIDATE_TOO_LARGE,
            }

            if iter_result.status == IterationStatus.KEPT:
                current_best = iter_result.candidate_sql
                current_best_timing = iter_result.candidate_timing_ms
                consecutive_stop_count = 0
                # Re-fetch EXPLAIN for new current_best
                try:
                    with pg.repeatable_read_txn(conn) as cur:
                        current_best_explain = pg.get_explain_json(cur, current_best)
                except Exception:
                    pass  # Keep old explain if re-fetch fails
            elif iter_result.status in _COUNTING_STATUSES:
                consecutive_stop_count += 1
            # FAILED_SAFETY, CANDIDATE_ERROR: invisible to counter

            _print_iteration(iteration_num, max_iterations, iter_result)

            if early_stop and consecutive_stop_count >= 2:
                break

    finally:
        conn.close()

    return _finalize(
        clean_sql, current_best, iterations,
        baseline_ms=baseline_ms,
        final_timing_ms=current_best_timing,
        log_dir=log_dir,
    )


def _run_iteration(
    *,
    conn,
    client,
    original_sql: str,
    original_descs,
    original_rows,
    original_resolved_oids: set[int],
    current_best: str,
    current_best_timing: float,
    current_best_explain,
    tables,
    has_order_by: bool,
    iteration_num: int,
    previous_failures: list[str] | None = None,
    max_rows: int = MAX_ROWS,
    max_bytes: int = MAX_BYTES,
) -> IterationResult:
    """Run a single ratchet iteration. Never raises — returns an IterationResult."""

    # ── Generate candidate ──
    try:
        gen_result = generate_candidate(
            client, current_best, tables, current_best_explain,
            previous_failures=previous_failures,
        )
        candidate_sql = gen_result.candidate_sql
        explanation = gen_result.explanation
    except PromptBudgetError:
        return IterationResult(
            iteration=iteration_num,
            status=IterationStatus.CANDIDATE_ERROR,
            explanation="Prompt budget exceeded",
        )
    except Exception as e:
        return IterationResult(
            iteration=iteration_num,
            status=IterationStatus.CANDIDATE_ERROR,
            explanation=f"Generation error: {e}",
        )

    # ── Safety check candidate ──
    try:
        cand_ast = check_ast(candidate_sql)
        cand_resolved = check_catalog(conn, cand_ast)
        # Candidate relation containment: must be subset of original's
        cand_oids = {r.oid for r in cand_resolved}
        if not cand_oids.issubset(original_resolved_oids):
            extra = cand_oids - original_resolved_oids
            return IterationResult(
                iteration=iteration_num,
                status=IterationStatus.FAILED_SAFETY,
                candidate_sql=candidate_sql,
                explanation=f"Candidate references extra tables (OIDs: {extra})",
            )
    except UnsafeQueryError as e:
        return IterationResult(
            iteration=iteration_num,
            status=IterationStatus.FAILED_SAFETY,
            candidate_sql=candidate_sql,
            explanation=f"Safety check failed: {e}",
        )

    # ── Execute original + candidate in same REPEATABLE READ txn ──
    try:
        with pg.repeatable_read_txn(conn) as cur:
            # Execute candidate, check schema BEFORE fetching rows
            cand_descs_raw = _execute_and_get_descs(cur, candidate_sql)
            schema_mismatch = pg.check_schema_equality(original_descs, cand_descs_raw)
            if schema_mismatch:
                return IterationResult(
                    iteration=iteration_num,
                    status=IterationStatus.FAILED_SCHEMA,
                    candidate_sql=candidate_sql,
                    explanation=f"Schema mismatch: {schema_mismatch}",
                )

            # Fetch candidate rows
            try:
                _, cand_rows = pg.execute_query(
                    cur, candidate_sql,
                    max_rows=max_rows, max_bytes=max_bytes,
                )
            except UnsupportedQueryError:
                return IterationResult(
                    iteration=iteration_num,
                    status=IterationStatus.CANDIDATE_TOO_LARGE,
                    candidate_sql=candidate_sql,
                    explanation="Candidate result too large",
                )

            # Compare against ORIGINAL (not current_best)
            cmp = compare_results(
                original_rows, cand_rows, original_descs, has_order_by
            )

    except Exception as e:
        return IterationResult(
            iteration=iteration_num,
            status=IterationStatus.CANDIDATE_ERROR,
            candidate_sql=candidate_sql,
            explanation=f"Execution error: {e}",
        )

    if cmp == CompareResult.MISMATCH:
        return IterationResult(
            iteration=iteration_num,
            status=IterationStatus.FAILED_MISMATCH,
            candidate_sql=candidate_sql,
            explanation=explanation,
        )

    if cmp == CompareResult.TIE_REORDER:
        return IterationResult(
            iteration=iteration_num,
            status=IterationStatus.FAILED_TIE_REORDER,
            candidate_sql=candidate_sql,
            explanation=explanation,
        )

    # ── Equivalent! Now benchmark against current_best ──
    try:
        with pg.repeatable_read_txn(conn) as cur:
            bench = benchmark_pair(cur, current_best, candidate_sql)
    except Exception as e:
        return IterationResult(
            iteration=iteration_num,
            status=IterationStatus.CANDIDATE_ERROR,
            candidate_sql=candidate_sql,
            explanation=f"Benchmark error: {e}",
        )

    if bench.is_faster:
        return IterationResult(
            iteration=iteration_num,
            status=IterationStatus.KEPT,
            candidate_sql=candidate_sql,
            explanation=explanation,
            candidate_timing_ms=bench.candidate_median_ms,
            current_best_timing_ms=bench.baseline_median_ms,
        )
    else:
        return IterationResult(
            iteration=iteration_num,
            status=IterationStatus.DISCARDED_SLOWER,
            candidate_sql=candidate_sql,
            explanation=explanation,
            candidate_timing_ms=bench.candidate_median_ms,
            current_best_timing_ms=bench.baseline_median_ms,
        )


def _execute_and_get_descs(cur, sql: str) -> list:
    """Execute query just to get cursor.description, then discard results."""
    from sql_autoresearch.models import ColumnDesc
    cur.execute(sql)
    if cur.description is None:
        return []
    descs = [
        ColumnDesc(name=col.name, type_oid=col.type_code)
        for col in cur.description
    ]
    # Drain rows to avoid "results not consumed" errors
    cur.fetchall()
    return descs


def _determine_outcome(
    original_sql: str,
    final_sql: str,
    iterations: list[IterationResult],
) -> QueryOutcome:
    """Determine QueryOutcome from iteration results (precedence order)."""
    if not iterations:
        return QueryOutcome.NO_VALID_CANDIDATE

    statuses = [it.status for it in iterations]

    # g. Ratchet advanced at least once
    if final_sql != original_sql:
        return QueryOutcome.OPTIMIZED

    # h. At least one iteration verified equivalent but none faster
    has_verified = any(
        s in (IterationStatus.DISCARDED_SLOWER, IterationStatus.KEPT)
        for s in statuses
    )
    if has_verified:
        return QueryOutcome.UNCHANGED

    # i. Any FAILED_MISMATCH
    if IterationStatus.FAILED_MISMATCH in statuses:
        return QueryOutcome.VERIFICATION_FAILED

    # j. All verified failures were FAILED_TIE_REORDER only
    verified_failures = [
        s for s in statuses
        if s in (IterationStatus.FAILED_MISMATCH, IterationStatus.FAILED_TIE_REORDER)
    ]
    if verified_failures and all(
        s == IterationStatus.FAILED_TIE_REORDER for s in verified_failures
    ):
        return QueryOutcome.VERIFICATION_TIE

    # f. All iterations failed to generate a parseable candidate
    if all(s == IterationStatus.CANDIDATE_ERROR for s in statuses):
        return QueryOutcome.NO_VALID_CANDIDATE

    # k. Mix of failures
    return QueryOutcome.NO_VERIFIED_CANDIDATE


def _finalize(
    original_sql: str,
    final_sql: str,
    iterations: list[IterationResult],
    *,
    error: str | None = None,
    forced_outcome: QueryOutcome | None = None,
    baseline_ms: float | None = None,
    final_timing_ms: float | None = None,
    log_dir: str | None = None,
) -> QueryResult:
    """Build QueryResult, compute improvement ratio, write JSONL log."""
    if forced_outcome is not None:
        outcome = forced_outcome
    else:
        outcome = _determine_outcome(original_sql, final_sql, iterations)

    improvement_ratio = None
    if baseline_ms and final_timing_ms and baseline_ms > 0:
        improvement_ratio = (baseline_ms - final_timing_ms) / baseline_ms

    result = QueryResult(
        original_sql=original_sql,
        final_sql=final_sql,
        outcome=outcome,
        iterations=iterations,
        improvement_ratio=improvement_ratio,
        original_timing_ms=baseline_ms,
        final_timing_ms=final_timing_ms,
        error=error,
    )

    if log_dir:
        _write_log(result, log_dir, error=error)

    return result


def _write_log(result: QueryResult, log_dir: str, error: str | None = None) -> None:
    """Append one JSONL line to the run log."""
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, "run.jsonl")

    entry: dict[str, Any] = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "original_sql": result.original_sql,
        "final_sql": result.final_sql,
        "outcome": result.outcome.value,
        "improvement_ratio": result.improvement_ratio,
        "original_timing_ms": result.original_timing_ms,
        "final_timing_ms": result.final_timing_ms,
        "iterations": [
            {
                "iteration": it.iteration,
                "status": it.status.value,
                "candidate_sql": it.candidate_sql,
                "explanation": it.explanation,
                "candidate_timing_ms": it.candidate_timing_ms,
                "current_best_timing_ms": it.current_best_timing_ms,
            }
            for it in result.iterations
        ],
    }
    if error:
        entry["error"] = error

    with open(log_file, "a") as f:
        f.write(json.dumps(entry) + "\n")


def _print_iteration(num: int, total: int, result: IterationResult) -> None:
    """Print iteration status to stderr."""
    status = result.status.value
    timing = ""
    if result.candidate_timing_ms is not None and result.current_best_timing_ms is not None:
        timing = (
            f" (candidate={result.candidate_timing_ms:.1f}ms, "
            f"best={result.current_best_timing_ms:.1f}ms)"
        )
    print(f"  [{num}/{total}] {status}{timing}", file=sys.stderr)
    if result.explanation:
        # Truncate long explanations
        expl = result.explanation[:200]
        if len(result.explanation) > 200:
            expl += "..."
        print(f"          {expl}", file=sys.stderr)
