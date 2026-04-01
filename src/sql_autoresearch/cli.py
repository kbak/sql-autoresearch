from __future__ import annotations

import argparse
import hashlib
import os
import sys
import tomllib
from pathlib import Path

from sql_autoresearch.generate import build_prompt
from sql_autoresearch.models import (
    IterationStatus,
    QueryOutcome,
    QueryResult,
    UnsafeQueryError,
)
from sql_autoresearch.optimizer import optimize_query
from sql_autoresearch.safety import check_ast, check_catalog


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="sql-autoresearch",
        description="Automated SQL query optimizer for PostgreSQL",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # ── run ──
    run_parser = subparsers.add_parser("run", help="Optimize a query or corpus")
    run_parser.add_argument("--dsn", help="PostgreSQL DSN")
    run_parser.add_argument("--sql", help="SQL query to optimize (or use --corpus)")
    run_parser.add_argument("--corpus", help="Path to corpus directory")
    run_parser.add_argument(
        "--accept-data-sent", action="store_true",
        help=(
            "Acknowledge that SQL literals, index predicates, stats, "
            "and EXPLAIN JSON are sent to the Anthropic API"
        ),
    )
    run_parser.add_argument(
        "--quiescent-db", action="store_true",
        help="Acknowledge that the target database has no concurrent writes",
    )
    run_parser.add_argument("--iterations", type=int, default=5, help="Max iterations per query")
    run_parser.add_argument("--log-dir", default="runs", help="JSONL log directory")
    run_parser.add_argument(
        "--max-rows", type=int, default=10000,
        help="Max result rows (default 10000)",
    )
    run_parser.add_argument(
        "--max-bytes", type=int, default=10 * 1024 * 1024,
        help="Max result bytes (default 10MB)",
    )
    run_parser.add_argument(
        "--early-stop", action="store_true",
        help="Stop after 2 consecutive non-improving iterations",
    )

    # ── check ──
    check_parser = subparsers.add_parser(
        "check", help="Dry-run safety/relation checks without execution"
    )
    check_parser.add_argument("--dsn", help="PostgreSQL DSN")
    check_parser.add_argument("--sql", help="SQL query to check (or use --corpus)")
    check_parser.add_argument("--corpus", help="Path to corpus directory")

    # ── corpus ──
    corpus_parser = subparsers.add_parser("corpus", help="Manage query corpus")
    corpus_sub = corpus_parser.add_subparsers(dest="corpus_command", required=True)

    add_parser = corpus_sub.add_parser("add", help="Add a query to the corpus")
    add_parser.add_argument("file", help="Path to .sql file")
    add_parser.add_argument("--description", required=True, help="Query description")
    add_parser.add_argument("--corpus-dir", default="corpus", help="Corpus directory")

    list_parser = corpus_sub.add_parser("list", help="List corpus queries")
    list_parser.add_argument("--corpus-dir", default="corpus", help="Corpus directory")

    verify_parser = corpus_sub.add_parser("verify", help="Verify corpus integrity")
    verify_parser.add_argument("--corpus-dir", default="corpus", help="Corpus directory")

    args = parser.parse_args()

    # Merge config file values for run/check commands
    if args.command in ("run", "check"):
        _merge_config(args)

    if args.command == "run":
        _cmd_run(args)
    elif args.command == "check":
        _cmd_check(args)
    elif args.command == "corpus":
        if args.corpus_command == "add":
            _cmd_corpus_add(args)
        elif args.corpus_command == "list":
            _cmd_corpus_list(args)
        elif args.corpus_command == "verify":
            _cmd_corpus_verify(args)


# ── config ──

_CONFIG_FILE = ".sql-autoresearch.toml"

# Keys that map config file -> argparse attribute
_CONFIG_KEYS = {
    "dsn": "dsn",
    "max_iterations": "iterations",
    "max_rows": "max_rows",
    "max_bytes": "max_bytes",
    "log_dir": "log_dir",
}


def _merge_config(args) -> None:
    """Load .sql-autoresearch.toml from CWD and merge into args.

    Precedence: CLI flag > config file > argparse default.
    """
    config_path = Path(_CONFIG_FILE)
    if not config_path.exists():
        # No config file — validate required fields
        if not getattr(args, "dsn", None):
            print(
                "Error: --dsn is required (or set dsn in "
                f"{_CONFIG_FILE})",
                file=sys.stderr,
            )
            sys.exit(1)
        return

    with open(config_path, "rb") as f:
        config = tomllib.load(f)

    for config_key, attr_name in _CONFIG_KEYS.items():
        if config_key in config and getattr(args, attr_name, None) is None:
            setattr(args, attr_name, config[config_key])

    if not getattr(args, "dsn", None):
        print(
            f"Error: --dsn is required (not found in CLI or {_CONFIG_FILE})",
            file=sys.stderr,
        )
        sys.exit(1)


# ── run command ──

def _cmd_run(args) -> None:
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("Error: ANTHROPIC_API_KEY environment variable not set", file=sys.stderr)
        sys.exit(1)

    queries = _load_queries(args)

    # Data-sent consent check
    if not args.accept_data_sent:
        print("The following data would be sent to the Anthropic API:\n", file=sys.stderr)
        _preview_payloads(queries, args.dsn)
        print(
            "\nWARNING: SQL literals (including any secrets or PII embedded in them), "
            "index definitions (including predicates/expressions that may contain "
            "sensitive values), numeric stats, and EXPLAIN JSON are sent verbatim.\n",
            file=sys.stderr,
        )
        print("Re-run with --accept-data-sent to proceed.", file=sys.stderr)
        sys.exit(1)

    if not args.quiescent_db:
        print(
            "WARNING: Snapshot consistency requires no concurrent writes to the target database.\n"
            "The tool cannot detect concurrent writes — you are responsible "
            "for ensuring quiescence\n"
            "(e.g., restored snapshot, stopped application, or dedicated staging instance).\n\n"
            "Re-run with --quiescent-db to acknowledge.",
            file=sys.stderr,
        )
        sys.exit(1)

    results: list[QueryResult] = []
    for i, (name, sql) in enumerate(queries, 1):
        print(f"\n{'='*60}", file=sys.stderr)
        print(f"Query {i}/{len(queries)}: {name}", file=sys.stderr)
        print(f"{'='*60}", file=sys.stderr)

        result = optimize_query(
            dsn=args.dsn,
            sql=sql,
            api_key=api_key,
            max_iterations=args.iterations,
            log_dir=args.log_dir,
            max_rows=args.max_rows,
            max_bytes=args.max_bytes,
            early_stop=args.early_stop,
        )
        results.append(result)
        _print_query_result(name, result)

    _print_summary(results)


def _preview_payloads(queries: list[tuple[str, str]], dsn: str) -> None:
    """Print the exact payload that would be sent to the API."""
    from sql_autoresearch.adapters import postgres as pg

    try:
        conn = pg.connect(dsn)
    except Exception as e:
        print(f"Cannot connect to preview payloads: {e}", file=sys.stderr)
        for name, sql in queries:
            print(f"\n--- {name} ---", file=sys.stderr)
            print(sql, file=sys.stderr)
        return

    try:
        for name, sql in queries:
            print(f"\n--- {name} ---", file=sys.stderr)
            try:
                ast_result = check_ast(sql)
                resolved = check_catalog(conn, ast_result)
                tables = [pg.get_table_info(conn, r.oid) for r in resolved]
                with pg.repeatable_read_txn(conn) as cur:
                    explain_json = pg.get_explain_json(cur, ast_result.clean_sql)
                prompt = build_prompt(ast_result.clean_sql, tables, explain_json)
                print(prompt, file=sys.stderr)
            except Exception as e:
                print(f"Error building preview: {e}", file=sys.stderr)
                print(f"Raw SQL: {sql}", file=sys.stderr)
    finally:
        conn.close()


# ── check command ──

def _cmd_check(args) -> None:
    from sql_autoresearch.adapters import postgres as pg

    queries = _load_queries(args)
    supported = 0
    total = len(queries)

    try:
        conn = pg.connect(args.dsn)
    except Exception as e:
        print(f"Connection error: {e}", file=sys.stderr)
        sys.exit(1)

    try:
        for name, sql in queries:
            try:
                ast_result = check_ast(sql)
                check_catalog(conn, ast_result)
                print(f"  PASS  {name}")
                supported += 1
            except (UnsafeQueryError, Exception) as e:
                print(f"  FAIL  {name}: {e}")
    finally:
        conn.close()

    rate = supported / total if total > 0 else 0
    print(f"\nSupport rate: {supported}/{total} ({rate:.0%})")
    if rate < 0.60:
        print("WARNING: support_rate < 60% — scope may be too narrow", file=sys.stderr)


# ── corpus commands ──

def _cmd_corpus_add(args) -> None:
    sql_path = Path(args.file)
    if not sql_path.exists():
        print(f"File not found: {sql_path}", file=sys.stderr)
        sys.exit(1)

    corpus_dir = Path(args.corpus_dir)
    queries_dir = corpus_dir / "queries"
    queries_dir.mkdir(parents=True, exist_ok=True)

    content = sql_path.read_text()
    sha = hashlib.sha256(content.encode()).hexdigest()

    # Copy to corpus
    dest = queries_dir / sql_path.name
    dest.write_text(content)

    # Update manifest
    manifest_path = corpus_dir / "manifest.toml"
    if manifest_path.exists():
        with open(manifest_path, "rb") as f:
            manifest = tomllib.load(f)
    else:
        manifest = {"queries": []}

    manifest.setdefault("queries", [])
    # Check for duplicates
    updated = False
    for entry in manifest["queries"]:
        if entry["file"] == sql_path.name:
            entry["sha256"] = sha
            entry["description"] = args.description
            updated = True
            break
    if not updated:
        manifest["queries"].append({
            "file": sql_path.name,
            "sha256": sha,
            "description": args.description,
        })

    _write_manifest(manifest_path, manifest["queries"])
    print(f"Added {sql_path.name} (SHA-256: {sha[:16]}...)")


def _cmd_corpus_list(args) -> None:
    manifest_path = Path(args.corpus_dir) / "manifest.toml"
    if not manifest_path.exists():
        print("No manifest.toml found", file=sys.stderr)
        sys.exit(1)

    with open(manifest_path, "rb") as f:
        manifest = tomllib.load(f)

    for entry in manifest.get("queries", []):
        print(f"  {entry['file']:30s} {entry.get('description', '')}")


def _cmd_corpus_verify(args) -> None:
    corpus_dir = Path(args.corpus_dir)
    manifest_path = corpus_dir / "manifest.toml"
    if not manifest_path.exists():
        print("No manifest.toml found", file=sys.stderr)
        sys.exit(1)

    with open(manifest_path, "rb") as f:
        manifest = tomllib.load(f)

    ok = 0
    fail = 0
    for entry in manifest.get("queries", []):
        sql_path = corpus_dir / "queries" / entry["file"]
        if not sql_path.exists():
            print(f"  MISSING  {entry['file']}")
            fail += 1
            continue

        content = sql_path.read_text()
        actual_sha = hashlib.sha256(content.encode()).hexdigest()
        if actual_sha == entry["sha256"]:
            print(f"  OK       {entry['file']}")
            ok += 1
        else:
            print(
                f"  MISMATCH {entry['file']} "
                f"(expected {entry['sha256'][:16]}..., "
                f"got {actual_sha[:16]}...)"
            )
            fail += 1

    print(f"\n{ok} OK, {fail} failed")
    if fail > 0:
        sys.exit(1)


# ── Helpers ──

def _load_queries(args) -> list[tuple[str, str]]:
    """Load queries from --sql or --corpus. Returns [(name, sql), ...]."""
    if args.sql:
        return [("inline", args.sql)]

    if hasattr(args, "corpus") and args.corpus:
        corpus_dir = Path(args.corpus)
        manifest_path = corpus_dir / "manifest.toml"
        if not manifest_path.exists():
            print(f"No manifest.toml in {corpus_dir}", file=sys.stderr)
            sys.exit(1)

        with open(manifest_path, "rb") as f:
            manifest = tomllib.load(f)

        queries = []
        for entry in manifest.get("queries", []):
            sql_path = corpus_dir / "queries" / entry["file"]
            if not sql_path.exists():
                print(f"Warning: {entry['file']} not found, skipping", file=sys.stderr)
                continue
            queries.append((entry["file"], sql_path.read_text()))

        if not queries:
            print("No queries found in corpus", file=sys.stderr)
            sys.exit(1)

        return queries

    print("Error: provide --sql or --corpus", file=sys.stderr)
    sys.exit(1)


def _print_query_result(name: str, result: QueryResult) -> None:
    """Print per-query result summary."""
    print(f"\n  Outcome: {result.outcome.value}", file=sys.stderr)
    if result.error:
        print(f"  Error: {result.error}", file=sys.stderr)
    if result.original_timing_ms is not None:
        print(f"  Original timing: {result.original_timing_ms:.1f}ms", file=sys.stderr)
    if result.final_timing_ms is not None:
        print(f"  Final timing: {result.final_timing_ms:.1f}ms", file=sys.stderr)
    if result.improvement_ratio is not None:
        print(f"  Improvement: {result.improvement_ratio:.1%}", file=sys.stderr)

    advances = sum(1 for it in result.iterations if it.status == IterationStatus.KEPT)
    print(f"  Ratchet advances: {advances}/{len(result.iterations)}", file=sys.stderr)

    if result.outcome == QueryOutcome.OPTIMIZED:
        print("\n  Optimized SQL:", file=sys.stderr)
        print(f"  {result.final_sql}", file=sys.stderr)


def _print_summary(results: list[QueryResult]) -> None:
    """Print the brutal test summary with go/no-go gates."""
    total = len(results)

    # Bucket counts
    buckets: dict[str, int] = {}
    for r in results:
        buckets[r.outcome.value] = buckets.get(r.outcome.value, 0) + 1

    # Iteration-level counts
    iter_counts: dict[str, int] = {}
    total_iterations = 0
    for r in results:
        for it in r.iterations:
            total_iterations += 1
            iter_counts[it.status.value] = iter_counts.get(it.status.value, 0) + 1

    print(f"\n{'='*60}", file=sys.stderr)
    print("BRUTAL TEST SUMMARY", file=sys.stderr)
    print(f"{'='*60}", file=sys.stderr)

    print(f"\nQuery-level outcomes ({total} queries):", file=sys.stderr)
    for outcome in QueryOutcome:
        count = buckets.get(outcome.value, 0)
        if count > 0:
            print(f"  {outcome.value:30s} {count}", file=sys.stderr)

    print(f"\nIteration-level outcomes ({total_iterations} iterations):", file=sys.stderr)
    for status in IterationStatus:
        count = iter_counts.get(status.value, 0)
        if count > 0:
            print(f"  {status.value:30s} {count}", file=sys.stderr)

    # Go/no-go gates
    unsupported = sum(
        buckets.get(o.value, 0)
        for o in QueryOutcome
        if o.value.startswith("UNSUPPORTED")
    )
    errored = buckets.get(QueryOutcome.ERROR.value, 0)
    supported = total - unsupported - errored
    improved = buckets.get(QueryOutcome.OPTIMIZED.value, 0)
    mismatch_iters = iter_counts.get(IterationStatus.FAILED_MISMATCH.value, 0)

    support_rate = supported / total if total > 0 else 0
    win_rate = improved / supported if supported > 0 else 0
    error_rate = errored / total if total > 0 else 0
    mismatch_rate = mismatch_iters / total_iterations if total_iterations > 0 else 0

    print("\nGo/no-go gates:", file=sys.stderr)
    _gate("support_rate", support_rate, ">=", 0.60)
    _gate("win_rate", win_rate, ">=", 0.30)
    _gate("error_rate", error_rate, "<", 0.20)
    _gate("mismatch_rate", mismatch_rate, "<", 0.20)

    # Per-query details
    print("\nPer-query details:", file=sys.stderr)
    for i, r in enumerate(results, 1):
        advances = sum(1 for it in r.iterations if it.status == IterationStatus.KEPT)
        ratio = f"{r.improvement_ratio:.1%}" if r.improvement_ratio else "N/A"
        print(
            f"  Q{i}: {r.outcome.value:25s} advances={advances} "
            f"improvement={ratio} iters={len(r.iterations)}",
            file=sys.stderr,
        )


def _gate(name: str, value: float, op: str, threshold: float) -> None:
    if op == ">=":
        passed = value >= threshold
    elif op == "<":
        passed = value < threshold
    else:
        passed = False
    status = "PASS" if passed else "FAIL"
    print(
        f"  {status}  {name:20s} = {value:.2%}  ({op} {threshold:.0%})",
        file=sys.stderr,
    )


def _write_manifest(path: Path, queries: list[dict]) -> None:
    """Write manifest.toml without requiring a TOML writing library."""
    lines = []
    for entry in queries:
        lines.append("[[queries]]")
        lines.append(f'file = "{entry["file"]}"')
        lines.append(f'sha256 = "{entry["sha256"]}"')
        desc = entry.get("description", "").replace('"', '\\"')
        lines.append(f'description = "{desc}"')
        lines.append("")
    path.write_text("\n".join(lines))


if __name__ == "__main__":
    main()
