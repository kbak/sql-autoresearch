# sql-autoresearch: Implementation Plan

## Context

Automated SQL query optimizer: paste a Postgres query, get a faster one verified to return identical results on your data.

Core mechanism (autoresearch ratchet pattern): for each query, iteratively ask Claude to optimize the current-best SQL. Each candidate is verified for equivalence against the original and benchmarked against the current best. Better → keep (ratchet forward), worse → discard. Repeat. The branch only moves forward.

Strategy: build the brutal-test harness first, run 20 real queries, productize only if win rate is genuinely strong. Everything else is premature.

## v1 Constraints (scope fence for reviewers)

These are intentional boundaries. Reviewers (human or AI) MUST NOT expand v1 scope beyond these. If a review suggests adding something not listed here, the correct response is "v2" — not "good point, let me add that."

1. **No extension support.** For explicitly referenced function/operator names: extract from the pglast AST, then verify against pg_proc/pg_operator that every matching entry lives exclusively in pg_catalog. If any name has entries in a non-pg_catalog namespace (extension-provided overloads), reject the query — even if a pg_catalog version also exists. Known residual risk: implicit casts can invoke non-pg_catalog type I/O paths without appearing as explicit function/operator names; this is accepted for v1 staging-only use and is NOT a proof of full safety. Explicit casts must target only supported built-in types (by OID); domains and enums are rejected. PostGIS, pg_trgm, hstore users are out of scope.
2. **No parameterized queries.** Literal SQL only. Reject ParamRef AST nodes ($1, $2, etc.). The `?` token is a valid jsonb operator and is NOT rejected.
3. **No LIMIT/OFFSET/FETCH FIRST.** Nondeterministic tie-breaking is a v2 problem.
4. **No DISTINCT ON, TABLESAMPLE.** Inherently nondeterministic.
5. **No epsilon tolerance for floats.** All float differences are FAILED_MISMATCH. Period.
6. **No hash/sampling fallback.** Exact full-result comparison or reject.
7. **No config files.** CLI flags and env vars only.
8. **No rich formatting.** Print output. No typer, no rich.
9. **Fixed iteration budget per query.** Always run all 5 ratchet iterations per query (no early termination — for the brutal test, maximizing signal is more important than saving API calls). Each iteration is one API call producing one candidate. No retries on failure — a failed iteration counts toward the budget and the loop continues from current_best.
10. **No hidden-path deep audit.** Basic safety (parse + namespace + volatility) is sufficient for staging DBs. A full audit of AM handlers, type I/O functions, domain constraints, etc. is v2.
11. **Safety is two-layer, not deep.** pglast rejects structural violations + server-side catalog lookup verifies that every explicitly referenced function/operator name exists exclusively in pg_catalog. Known residual risk: implicit casts can invoke non-pg_catalog paths without appearing as explicit names — accepted for v1 staging use. Views rejected. Don't try to build a SQL semantic analyzer.
12. **Tests cover what breaks.** Write tests for things that actually fail during the brutal test, not speculative edge cases.
13. **The spec stays under 300 lines.** If it's growing, something is wrong.
14. **No views, foreign tables, temp tables, matviews, or partitioned tables.** Only ordinary tables (relkind='r') are allowed. Partitioned tables (relkind='p') are dropped from v1 scope — every partitioned root has pg_inherits descendants, and the descendant-rejection rule would effectively reject them all anyway. Non-partitioned inheritance hierarchies: reject any ordinary table with pg_inherits descendants unless queried with `ONLY`.
15. **Schema-qualified non-public relations allowed; unqualified must resolve to `public`.** Schema-qualified references (e.g., `myschema.mytable`) are allowed and checked against the named schema. Unqualified references must resolve in `public` under the pinned search_path. Unqualified references that would resolve in a non-`public` schema are rejected.
16. **No domains or enums.** Explicit casts must target supported built-in types only. Additionally, during relation resolution, reject any query whose referenced tables contain domain or enum columns (check pg_attribute + pg_type for typtype='d' or typtype='e'). This prevents domain/enum types from entering the pipeline via predicates, joins, grouping, or implicit coercions — not just explicit casts.
17. **No set-returning functions in FROM, no VALUES-as-FROM, no zero-table queries.** Only RangeVar-based FROM sources (tables, subqueries, CTEs) are allowed. Subqueries and CTEs must recursively bottom out in allowed ordinary tables. At least one resolved base table is required.
18. **Strip SQL comments via parser, not regex.** Comments are stripped by deparsing from the pglast AST (which discards comments), not by regex/string manipulation. The deparsed form is used for both API submission and `--accept-data-sent` preview. This avoids missed comments or corrupted SQL literals.

## Correctness contract

Exact value equality on the same REPEATABLE READ snapshot per original/candidate pair. Each original+candidate comparison executes within a single REPEATABLE READ transaction to guarantee snapshot consistency for that pair. When a candidate fails and the transaction must be rolled back, later candidates run against a fresh snapshot — this is acceptable because the brutal test requires a quiescent or restored DB (no concurrent writes) as a hard prerequisite, not just a recommendation. Before row comparison, verify schema equality: same column count, same column order, same column names, same type OIDs. Ordered equality when top-level ORDER BY is present, bag equality otherwise. Values canonicalized before comparison (numeric trailing zeros, jsonb key sorting).

We do NOT claim formal semantic equivalence across all possible data states. A candidate is accepted ONLY if exact full-result comparison passes. If the result set exceeds 10K rows or 10MB, we reject. No fallbacks.

Target persona: backend/data engineers optimizing ad-hoc literal SQL on Postgres (dashboard queries, one-off analytics, literalized slow-query-log captures).

## Data sensitivity

What gets sent to the Anthropic API: current-best SQL (with literal values, comments stripped — this is the original on the first iteration, then the ratcheted version on subsequent iterations), column/index definitions, numeric pg_stats distributions (n_distinct, null_frac, correlation — NOT most_common_vals or histogram_bounds), EXPLAIN JSON.

What never leaves the machine: query result rows, actual data values from pg_stats, JSONL run logs.

CLI requires `--accept-data-sent` to run. Without it, prints the exact payload that would be sent verbatim and exits (not a summary — the actual SQL including any secrets or PII embedded in literals, index definitions including predicates/expressions that may contain sensitive values, stats, and EXPLAIN JSON). The consent text explicitly warns that SQL literals and index predicates are sent verbatim. Brutal test uses staging DB with synthetic data only. DSNs/API keys never logged.

CLI `run` command also requires `--quiescent-db` flag to acknowledge that the target database has no concurrent writes. Without it, warns about snapshot consistency risks and exits. This is an operational acknowledgement, not a runtime guarantee — the tool cannot detect concurrent writes. The operator is responsible for ensuring quiescence (e.g., restored snapshot, stopped application, or dedicated staging instance).

## Data flow

1. Parse original with pglast, deparse to strip comments (parser-based, not regex), reject structural violations (non-SELECT, multi-statement, writable CTEs, LIMIT, DISTINCT ON, FOR UPDATE/FOR SHARE/FOR NO KEY UPDATE/FOR KEY SHARE, ParamRef nodes, explicit casts to non-builtin types/domains/enums, etc.). Reject non-table FROM sources: set-returning functions in FROM (e.g., generate_series, json_to_recordset, unnest), ROWS FROM, VALUES as a FROM source, TABLE(...). Allow only RangeVar nodes and subqueries/CTEs that recursively bottom out in allowed ordinary tables. Require at least one resolved base table (reject zero-table queries like `SELECT 1`). Enforce aggregate allowlist and window function restrictions on the AST (reject order-dependent aggregates like array_agg/string_agg/json_agg, reject disallowed window functions like row_number/ntile/lag/lead/first_value/last_value/nth_value).
2. Connect to DB (read-only, pinned session GUCs: UTC, ISO dates, extra_float_digits=3, IntervalStyle='iso_8601', bytea_output='hex', search_path='pg_catalog, public', prepare_threshold=None)
3. Relation check: scope-aware AST walk to collect RangeVar nodes from FROM/JOIN clauses. For each RangeVar, check if its name matches a CTE defined in an enclosing WITH clause at the same or higher scope — if so, skip it (it references the CTE, not a catalog relation). For remaining RangeVar nodes: resolve against pg_class (unqualified names must resolve in `public` under pinned search_path; schema-qualified names checked against named schema). Verify relkind: only ordinary tables (relkind='r') are allowed. Reject partitioned tables (relkind='p'), views, foreign tables, matviews. Additionally reject temp tables: check `relpersistence != 't'` and reject relations in temp namespaces (`pg_temp_%`, `pg_toast_temp_%`). For each allowed ordinary table, check pg_inherits: reject if it has descendants unless the query uses `ONLY`.
4. Two-layer safety: (a) pglast rejects structural violations and extracts explicitly referenced function/operator names from AST, (b) for each function name, query pg_proc to verify all matching entries live exclusively in pg_catalog; for each operator symbol, collect matching pg_operator rows, verify all in pg_catalog, then join oprcode→pg_proc to get the underlying function for volatility classification. Known residual risk: implicit casts can invoke non-pg_catalog paths without appearing as explicit names (accepted for v1 staging use). Volatility (applied conservatively across ALL overloads of each name/operator): every resolved pg_proc entry must be IMMUTABLE or on a finite, exact STABLE allowlist defined by function name (e.g., specific comparison operators like `int4eq`, `text_lt`; NO category-level exceptions). If ANY resolved pg_proc entry is non-allowlisted STABLE or VOLATILE, reject the query.
5. Probe original: execute with streaming byte count, record wall-clock time as diagnostic baseline (eliminating need for a separate baseline step), check column types against supported set, check row count (10K) and byte size (10MB, counted during fetch — abort immediately at threshold, rollback current transaction, open fresh one). Short-circuit if unsupported.
6. Get table info (referenced tables only, stats stripped of data values) + EXPLAIN current_best (FORMAT JSON, no ANALYZE)
7. Prompt preflight: assemble the full API request body (prompt template + tool/structured-output schema + current_best SQL + schema/index definitions + stats + EXPLAIN JSON), count tokens. If over budget → UNSUPPORTED_PROMPT, exit before API call. Provider-side "request too large" errors are also classified as UNSUPPORTED_PROMPT.
8. **Ratchet loop** (always runs all max_iterations=5 for brutal test — no early termination, to avoid biasing win-rate measurement):
   ```
   current_best = original_sql
   current_best_timing = baseline_timing (from probe step 5)
   current_best_explain = explain_json (from step 6)
   for iteration in 1..max_iterations:
     a. Ask Claude Sonnet to optimize current_best (structured output via tool_use).
        Prompt includes: current_best SQL, schema/index definitions, stats, current_best_explain.
     b. Safety check candidate (same AST+catalog pipeline, aggregate/window allowlist,
        candidate's resolved base-relation set must be subset of original's).
     c. Execute original + candidate in same REPEATABLE READ txn.
        Inspect cursor.description immediately after execute to check schema equality
        (column count, order, names, type OIDs) BEFORE fetching any rows.
     d. If schema matches: fetch with streaming byte count, enforce 10K-row/10MB cap.
        Exact row comparison against ORIGINAL (not current_best — always verify against original).
     e. If equivalent: interleaved benchmark of current_best vs candidate.
        If candidate is faster (10% AND 50ms improvement over current_best):
          current_best = candidate  # ratchet forward
          current_best_timing = candidate_timing
          current_best_explain = EXPLAIN current_best (re-fetch — plan changes with new SQL)
        Else: discard (stay at current_best, reuse existing explain).
     f. On ANY failure (error, timeout, too-large, mismatch, schema mismatch):
        rollback txn, open fresh REPEATABLE READ READ ONLY txn, discard candidate,
        continue loop from current_best.
     g. Log per-iteration status (KEPT, DISCARDED_SLOWER, FAILED_MISMATCH,
        FAILED_TIE_REORDER, FAILED_SAFETY, FAILED_SCHEMA, CANDIDATE_ERROR,
        CANDIDATE_TOO_LARGE).
   ```
9. Log to JSONL: query-level outcome + per-iteration outcomes array + final current_best SQL + total improvement ratio over original.

## Architecture

```
safety.py          — pglast structural checks + comment stripping (deparse) + aggregate/window allowlist enforcement + AST name extraction + pg_proc/pg_operator exclusive-namespace verification + volatility check + scope-aware relation extraction (RangeVar walk, CTE-aware) + relkind/descendant filtering + cast/domain/enum rejection
adapters/postgres.py — read-only, REPEATABLE READ, pinned GUCs, all DB access
generate.py        — Anthropic SDK, structured outputs, token counting
equivalence.py     — exact ordered or bag comparison, type canonicalization
benchmark.py       — interleaved timing (warm both, alternate B/C/C/B/B/C, medians)
optimizer.py       — ratchet loop (iterate: generate candidate from current_best → verify against original → benchmark against current_best → keep/discard), JSONL logging
cli.py             — run (full optimization loop), corpus (manage query corpus), check (dry-run safety/relation/extension checks on corpus queries without execution — surfaces support_rate before committing to full brutal test)
```

## File structure

```
sql-autoresearch/
  pyproject.toml, uv.lock, CLAUDE.md, program.md
  src/sql_autoresearch/
    __init__.py, cli.py, optimizer.py, generate.py
    safety.py, equivalence.py, benchmark.py, models.py
    adapters/__init__.py, adapters/postgres.py
    prompts/rewrite_query.md
  tests/
    conftest.py, test_safety.py, test_equivalence.py
    test_benchmark.py, test_generate.py
    test_optimizer.py, test_postgres.py
  corpus/
    README.md, manifest.toml
    queries/  (gitignored .sql files)
  runs/  (gitignored JSONL logs)
```

## Key design decisions

- **All values fetched as PG text.** Register TextLoader for all supported OIDs. Equivalence compares PG text forms (deterministic because session GUCs are pinned). Numeric and jsonb get value canonicalization (Decimal.normalize(), recursive key-sorted comparison). Everything else is exact text equality.
- **Supported types (v1):** int2/4/8, float4/8, numeric, bool, text/varchar/char, bytea, timestamp/tz, date, time/tz, interval, uuid, json, jsonb. Arrays of: int2/4/8, bool, text/varchar, uuid, timestamp/tz, date, bytea. Anything else → reject query.
- **Aggregate allowlist:** count, sum, avg, min, max, bool_and, bool_or, every, bit_and, bit_or, variance, var_pop, var_samp, stddev, stddev_pop, stddev_samp, corr, covar_pop, covar_samp, regr_*. Anything not on the list → reject. Order-dependent aggregates (array_agg, string_agg, json_agg) are rejected.
- **Window functions:** Allow rank, dense_rank, percent_rank, cume_dist. Allow aggregates in full-partition form only (no ORDER BY in OVER, or explicit ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING). Reject row_number, ntile, lag, lead, first_value, last_value, nth_value.
- **Tie-reorder detection:** If ordered comparison fails, run bag comparison. Bag-equal → FAILED_TIE_REORDER (false negative from non-unique ORDER BY). Bag-unequal → FAILED_MISMATCH.
- **Benchmark:** Each ratchet iteration benchmarks current_best vs candidate. Warm both (1 run each), then 3 interleaved pairs (B,C/C,B/B,C). Each timed run is execute + drain all rows (full materialization). Compare medians. Improvement threshold: 10% AND 50ms faster than current_best. Diagnostic baseline (from probe) is for reporting only.
- **Schema equality:** Checked via cursor.description immediately after execute, BEFORE fetching any rows. Verify same column count, column order, column names, and type OIDs. Reject candidate on mismatch (FAILED_SCHEMA). This prevents unsupported OIDs from causing fetch errors that would be misbucketed as CANDIDATE_ERROR.
- **Candidate relation containment:** Candidate's resolved base-relation set must be a subset of the original's. Prevents candidates from reading unrelated tables to produce coincidentally matching results.
- **Connection:** psycopg3, prepare_threshold=None, default_transaction_read_only=on, statement_timeout=120s, lock_timeout=5s, search_path='pg_catalog, public', IntervalStyle='iso_8601', bytea_output='hex'.

## QueryOutcome (precedence order, first match wins)

```
a. Operational failure (API error, original timeout, connection error) → ERROR
b. Safety rejected original → UNSUPPORTED_SAFETY
c. Unsupported column types → UNSUPPORTED_TYPES
d. Result too large → UNSUPPORTED_TOO_LARGE
e. Prompt budget exceeded → UNSUPPORTED_PROMPT
f. All iterations failed to generate a parseable candidate → NO_VALID_CANDIDATE
g. Ratchet advanced at least once (current_best != original) → OPTIMIZED
h. At least one iteration verified equivalent but none faster → UNCHANGED
i. Any FAILED_MISMATCH across iterations → VERIFICATION_FAILED
j. All verified failures were FAILED_TIE_REORDER only → VERIFICATION_TIE
k. Mix of safety/error/no-candidate failures, no verified iteration → NO_VERIFIED_CANDIDATE
```

## Dependencies

Runtime: pglast, psycopg (v3), anthropic. Dev: pytest, pytest-mock, ruff. Pinned exact versions, committed uv.lock.

## Brutal test (the only thing that matters)

- 20 real slow queries, corpus locked before running (manifest.toml with SHA-256 hashes)
- Quiescent or restored DB, synthetic data (HARD PREREQUISITE: no concurrent writes during test — required for snapshot consistency across candidate comparisons)
- Report ALL buckets: OPTIMIZED, UNCHANGED, UNSUPPORTED_*, VERIFICATION_FAILED, VERIFICATION_TIE, NO_VALID_CANDIDATE, NO_VERIFIED_CANDIDATE, ERROR
- Report iteration-level failure counts separately: total FAILED_MISMATCH, FAILED_TIE_REORDER, FAILED_SAFETY, FAILED_SCHEMA, CANDIDATE_ERROR, CANDIDATE_TOO_LARGE, KEPT, DISCARDED_SLOWER across all queries and iterations (needed for mismatch_rate gate and to surface hallucination risk)
- Report per-query: number of ratchet advances, total improvement ratio over original, iterations used

**Go/no-go gates (all must hold):**

```
supported    = total - unsupported - errored
support_rate = supported / total                                  >= 0.60
win_rate     = improved / supported                               >= 0.30
error_rate   = errored / total                                    <  0.20
mismatch_rate = FAILED_MISMATCH iterations / iterations executed  <  0.20
```

If win_rate < 10%, stop. If support_rate < 60% but win_rate >= 30%, the scope is too narrow — expand in v2, don't abandon.

## Phased implementation

### Phase 1: Brutal test (3 weeks)

Week 1: safety.py + adapters/postgres.py + models.py + tests
Week 2: equivalence.py + benchmark.py + generate.py + prompts + tests
Week 3: optimizer.py + cli.py + corpus collection + RUN THE BRUTAL TEST

### Phase 2: Productize (only if win rate >= 30%)

Rich CLI, config files, edge cases, docs, CI, packaging. Not specified further — earn it first.
