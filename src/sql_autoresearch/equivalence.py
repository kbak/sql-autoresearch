from __future__ import annotations

import json
from collections import Counter
from decimal import Decimal, InvalidOperation

from sql_autoresearch.models import SUPPORTED_TYPE_OIDS, ColumnDesc, CompareResult


def compare_results(
    original_rows: list[tuple[str | None, ...]],
    candidate_rows: list[tuple[str | None, ...]],
    col_descs: list[ColumnDesc],
    has_order_by: bool,
) -> CompareResult:
    """Compare original and candidate result sets.

    If has_order_by: ordered equality (row-by-row in order).
    Otherwise: bag equality (multiset comparison).

    Values are canonicalized before comparison (numeric trailing zeros,
    jsonb key sorting).

    Returns CompareResult.EQUAL, MISMATCH, or TIE_REORDER.
    """
    if len(original_rows) != len(candidate_rows):
        return CompareResult.MISMATCH

    canon_orig = _canonicalize_rows(original_rows, col_descs)
    canon_cand = _canonicalize_rows(candidate_rows, col_descs)

    if has_order_by:
        if canon_orig == canon_cand:
            return CompareResult.EQUAL
        # Ordered comparison failed — check if it's a tie-reorder
        if _bag_equal(canon_orig, canon_cand):
            return CompareResult.TIE_REORDER
        return CompareResult.MISMATCH
    else:
        if _bag_equal(canon_orig, canon_cand):
            return CompareResult.EQUAL
        return CompareResult.MISMATCH


def _canonicalize_rows(
    rows: list[tuple[str | None, ...]],
    col_descs: list[ColumnDesc],
) -> list[tuple[str | None, ...]]:
    """Canonicalize each value in each row based on its type OID."""
    type_oids = [col.type_oid for col in col_descs]
    return [
        tuple(
            _canonicalize_value(val, type_oids[i])
            for i, val in enumerate(row)
        )
        for row in rows
    ]


def _canonicalize_value(val: str | bool | None, type_oid: int) -> str | None:
    """Canonicalize a single PG text value for comparison."""
    if val is None:
        return None

    # Bool comes as native Python bool (not text-loaded)
    if isinstance(val, bool):
        return "t" if val else "f"

    type_name = SUPPORTED_TYPE_OIDS.get(type_oid, "")

    if type_name == "numeric":
        return _canonicalize_numeric(val)
    elif type_name in ("json", "jsonb"):
        return _canonicalize_json(val)
    else:
        return val


def _canonicalize_numeric(val: str) -> str:
    """Normalize numeric: remove trailing zeros, normalize representation."""
    try:
        d = Decimal(val)
        # Normalize removes trailing zeros: 1.500 -> 1.5, but keeps 0 as 0
        normalized = d.normalize()
        # Handle edge case: Decimal('0E+2').normalize() -> '0E+2'
        if normalized == 0:
            return "0"
        return str(normalized)
    except InvalidOperation:
        # NaN, Infinity, or unparseable — compare as-is
        return val


def _canonicalize_json(val: str) -> str:
    """Canonicalize JSON/JSONB: sort keys recursively, compact format."""
    try:
        obj = json.loads(val)
        return json.dumps(obj, sort_keys=True, separators=(",", ":"))
    except (json.JSONDecodeError, TypeError):
        return val


def _bag_equal(
    rows_a: list[tuple[str | None, ...]],
    rows_b: list[tuple[str | None, ...]],
) -> bool:
    """Multiset (bag) equality: same rows with same multiplicities."""
    if len(rows_a) != len(rows_b):
        return False
    return Counter(rows_a) == Counter(rows_b)
