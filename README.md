# sql-autoresearch

Automated SQL query optimizer for PostgreSQL. Paste a slow query, get a faster one — verified to return identical results on your data.

Uses an iterative "ratchet" pattern: each round asks Claude to optimize the current-best SQL, then verifies the candidate returns exactly the same rows and benchmarks it. Better candidates ratchet forward; worse ones are discarded.

## Install

```bash
uv sync
```

## Quick start (synthetic test database)

If you don't have a production database to test against, bootstrap a synthetic one:

```bash
uv sync
./scripts/bootstrap.sh          # creates DB, loads ~20M rows, registers 20 queries
```

This creates `autoresearch_test` with a SaaS-style schema (teams, users, orders, line_items, products, events) and 20 realistic queries as the corpus. Takes 2-4 minutes on an M-series Mac.

Then run the brutal test:

```bash
export ANTHROPIC_API_KEY=sk-ant-...

uv run sql-autoresearch check \
  --dsn "postgresql://localhost/autoresearch_test" \
  --corpus corpus/

uv run sql-autoresearch run \
  --dsn "postgresql://localhost/autoresearch_test" \
  --corpus corpus/ \
  --accept-data-sent \
  --quiescent-db
```

## Usage

### 1. Check if your queries are supported

```bash
sql-autoresearch check \
  --dsn "postgresql://user:pass@host:5432/dbname" \
  --sql "SELECT o.id, sum(li.amount) FROM orders o JOIN line_items li ON li.order_id = o.id WHERE o.created_at > '2024-01-01' GROUP BY o.id ORDER BY sum(li.amount) DESC"
```

Or check an entire corpus:

```bash
sql-autoresearch check --dsn "..." --corpus corpus/
```

### 2. Optimize a query

```bash
export ANTHROPIC_API_KEY=sk-ant-...

sql-autoresearch run \
  --dsn "postgresql://user:pass@host:5432/staging_db" \
  --sql "SELECT ..." \
  --accept-data-sent \
  --quiescent-db
```

The `--accept-data-sent` flag is required. Without it, the tool prints the exact payload that would be sent to the API (SQL with literals, index definitions, stats, EXPLAIN JSON) so you can review it first.

The `--quiescent-db` flag acknowledges that the target database has no concurrent writes — required for correct equivalence verification.

### 3. Run the brutal test (corpus of 20 queries)

```bash
# Add queries to the corpus
sql-autoresearch corpus add queries/slow_dashboard.sql --description "Dashboard user metrics"
sql-autoresearch corpus add queries/analytics_join.sql --description "Cross-table analytics"

# Verify corpus integrity
sql-autoresearch corpus verify

# Run all queries
sql-autoresearch run \
  --dsn "postgresql://user:pass@host:5432/staging_db" \
  --corpus corpus/ \
  --accept-data-sent \
  --quiescent-db \
  --log-dir runs/
```

Results are logged to `runs/run.jsonl` and a summary with go/no-go gates is printed:

```
Go/no-go gates:
  PASS  support_rate         = 75%  (>= 60%)
  PASS  win_rate             = 40%  (>= 30%)
  PASS  error_rate           = 5%   (< 20%)
  PASS  mismatch_rate        = 8%   (< 20%)
```

## What gets sent to the API

- Current-best SQL (with literal values, comments stripped)
- Column and index definitions (including predicates/expressions)
- Numeric pg_stats distributions (n_distinct, null_frac, correlation)
- EXPLAIN JSON (no ANALYZE)

What never leaves the machine: query result rows, actual data values, JSONL run logs.

## Requirements

- Python 3.12+
- A PostgreSQL database (staging/snapshot recommended)
- `ANTHROPIC_API_KEY` environment variable

## Development

```bash
uv sync
uv run pytest tests/ -v
uv run ruff check src/ tests/
```
