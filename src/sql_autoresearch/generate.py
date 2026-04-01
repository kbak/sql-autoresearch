from __future__ import annotations

import importlib.resources
import json
import os
from dataclasses import dataclass
from typing import Any

import anthropic

from sql_autoresearch.models import (
    PromptBudgetError,
    TableInfo,
)

DEFAULT_MODEL = "claude-sonnet-4-20250514"
MODEL = os.environ.get("SQL_AUTORESEARCH_MODEL", DEFAULT_MODEL)
MAX_OUTPUT_TOKENS = 4096
# Token budget: leave headroom for output tokens
MAX_INPUT_TOKENS = 180_000

_TOOL_SCHEMA = {
    "name": "sql_optimization",
    "description": (
        "Provide an optimized version of the SQL query. "
        "The optimized query must return exactly the same results."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "optimized_sql": {
                "type": "string",
                "description": "The optimized SQL query",
            },
            "explanation": {
                "type": "string",
                "description": (
                    "Brief explanation of what was changed and why it should be faster"
                ),
            },
        },
        "required": ["optimized_sql", "explanation"],
    },
}


@dataclass
class GenerateResult:
    candidate_sql: str
    explanation: str
    input_tokens: int
    output_tokens: int


def _load_prompt_template() -> str:
    return (
        importlib.resources.files("sql_autoresearch.prompts")
        .joinpath("rewrite_query.md")
        .read_text()
    )


def _format_table_definitions(tables: list[TableInfo]) -> str:
    parts = []
    for t in tables:
        lines = [f"### {t.schema}.{t.name} (~{t.row_estimate:.0f} rows)"]
        lines.append("Columns:")
        for col in t.columns:
            nullable = "NOT NULL" if col.not_null else "NULL"
            lines.append(f"  - {col.name} {col.type_name} {nullable}")
        if t.indexes:
            lines.append("Indexes:")
            for idx in t.indexes:
                lines.append(f"  - {idx.definition}")
        parts.append("\n".join(lines))
    return "\n\n".join(parts)


def _format_table_stats(tables: list[TableInfo]) -> str:
    parts = []
    for t in tables:
        lines = [f"### {t.schema}.{t.name}"]
        stats = t.stats
        for col in t.columns:
            stat_parts = []
            if col.name in stats.n_distinct:
                stat_parts.append(f"n_distinct={stats.n_distinct[col.name]}")
            if col.name in stats.null_frac:
                stat_parts.append(f"null_frac={stats.null_frac[col.name]}")
            if col.name in stats.correlation:
                stat_parts.append(f"correlation={stats.correlation[col.name]}")
            if stat_parts:
                lines.append(f"  - {col.name}: {', '.join(stat_parts)}")
        parts.append("\n".join(lines))
    return "\n\n".join(parts)


def build_prompt(
    current_sql: str,
    tables: list[TableInfo],
    explain_json: list[dict[str, Any]],
    previous_failures: list[str] | None = None,
) -> str:
    """Assemble the full prompt from template + context."""
    template = _load_prompt_template()
    prompt = template.format(
        current_sql=current_sql,
        table_definitions=_format_table_definitions(tables),
        table_stats=_format_table_stats(tables),
        explain_json=json.dumps(explain_json, indent=2),
    )
    if previous_failures:
        # Cap at last 3 to avoid prompt bloat
        recent = previous_failures[-3:]
        lines = [
            "\n## Previous attempts that were rejected -- do NOT repeat these"
        ]
        for f in recent:
            lines.append(f"- {f}")
        prompt += "\n".join(lines)
    return prompt


def check_token_budget(
    client: anthropic.Anthropic,
    prompt_text: str,
) -> int:
    """Count tokens for the request. Raise PromptBudgetError if over budget."""
    messages = [{"role": "user", "content": prompt_text}]
    try:
        count_result = client.messages.count_tokens(
            model=MODEL,
            messages=messages,
            tools=[_TOOL_SCHEMA],
        )
        input_tokens = count_result.input_tokens
    except Exception:
        # Fallback: rough estimate (4 chars per token)
        input_tokens = len(prompt_text) // 4

    if input_tokens > MAX_INPUT_TOKENS:
        raise PromptBudgetError(
            f"Prompt has {input_tokens} tokens, exceeds budget of {MAX_INPUT_TOKENS}"
        )
    return input_tokens


def generate_candidate(
    client: anthropic.Anthropic,
    current_sql: str,
    tables: list[TableInfo],
    explain_json: list[dict[str, Any]],
    previous_failures: list[str] | None = None,
) -> GenerateResult:
    """Ask Claude to optimize the query. Returns structured output.

    Raises PromptBudgetError if prompt is too large.
    Raises ValueError if response doesn't contain valid tool use.
    """
    prompt_text = build_prompt(
        current_sql, tables, explain_json, previous_failures
    )
    check_token_budget(client, prompt_text)

    try:
        response = client.messages.create(
            model=MODEL,
            max_tokens=MAX_OUTPUT_TOKENS,
            tools=[_TOOL_SCHEMA],
            tool_choice={"type": "tool", "name": "sql_optimization"},
            messages=[{"role": "user", "content": prompt_text}],
        )
    except anthropic.BadRequestError as e:
        if "too large" in str(e).lower() or "token" in str(e).lower():
            raise PromptBudgetError(f"Request too large: {e}") from e
        raise

    # Extract tool use from response
    for block in response.content:
        if block.type == "tool_use" and block.name == "sql_optimization":
            tool_input = block.input
            return GenerateResult(
                candidate_sql=tool_input["optimized_sql"],
                explanation=tool_input.get("explanation", ""),
                input_tokens=response.usage.input_tokens,
                output_tokens=response.usage.output_tokens,
            )

    raise ValueError("No sql_optimization tool use in response")
