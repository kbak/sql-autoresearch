# sql-autoresearch

Automated SQL query optimizer for PostgreSQL. Paste a query, get a faster one verified to return identical results.

## Quick start

```bash
uv sync
uv run pytest tests/ -v
uv run sql-autoresearch check --dsn "postgresql://..." --sql "SELECT ..."
uv run sql-autoresearch run --dsn "postgresql://..." --sql "SELECT ..." --accept-data-sent --quiescent-db
```

## Architecture

```
safety.py          — pglast AST checks + catalog verification (pg_proc/pg_operator/pg_class)
adapters/postgres.py — read-only DB access, pinned GUCs, TextLoader
generate.py        — Anthropic SDK structured output via tool_use
equivalence.py     — exact ordered/bag comparison with canonicalization
benchmark.py       — interleaved timing (B/C/C/B/B/C pattern)
optimizer.py       — ratchet loop orchestration + JSONL logging
cli.py             — argparse CLI: run, check, corpus
```

## Key conventions

- All DB values fetched as PG text strings (RawTextLoader) for deterministic comparison
- Session GUCs pinned: UTC, ISO dates, extra_float_digits=3, iso_8601 intervals, hex bytea
- Safety is two-layer: pglast AST validation + pg_catalog namespace/volatility verification
- No LiteLLM — use Anthropic SDK directly
- No rich/typer — plain print output
- Tests: `uv run pytest tests/ -v`
- Lint: `uv run ruff check src/ tests/`
- Format: `uv run ruff format src/ tests/`
