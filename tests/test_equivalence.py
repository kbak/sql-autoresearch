
from sql_autoresearch.equivalence import (
    _canonicalize_json,
    _canonicalize_numeric,
    compare_results,
)
from sql_autoresearch.models import ColumnDesc, CompareResult


def _int_desc(name="a"):
    return ColumnDesc(name=name, type_oid=23)  # int4


def _numeric_desc(name="a"):
    return ColumnDesc(name=name, type_oid=1700)  # numeric


def _jsonb_desc(name="a"):
    return ColumnDesc(name=name, type_oid=3802)  # jsonb


def _text_desc(name="a"):
    return ColumnDesc(name=name, type_oid=25)  # text


class TestNumericCanonicalization:
    def test_trailing_zeros(self):
        assert _canonicalize_numeric("1.500") == "1.5"

    def test_integer(self):
        assert _canonicalize_numeric("42") == "42"

    def test_zero(self):
        assert _canonicalize_numeric("0.00") == "0"

    def test_negative(self):
        assert _canonicalize_numeric("-3.140") == "-3.14"

    def test_scientific(self):
        assert _canonicalize_numeric("1.5E+3") == "1.5E+3"

    def test_nan(self):
        assert _canonicalize_numeric("NaN") == "NaN"


class TestJsonCanonicalization:
    def test_key_sorting(self):
        result = _canonicalize_json('{"b": 1, "a": 2}')
        assert result == '{"a":2,"b":1}'

    def test_nested_key_sorting(self):
        result = _canonicalize_json('{"b": {"d": 1, "c": 2}, "a": 3}')
        assert result == '{"a":3,"b":{"c":2,"d":1}}'

    def test_array_order_preserved(self):
        result = _canonicalize_json('[3, 1, 2]')
        assert result == '[3,1,2]'

    def test_compact_format(self):
        result = _canonicalize_json('{ "a" : 1 }')
        assert result == '{"a":1}'

    def test_invalid_json(self):
        assert _canonicalize_json("not json") == "not json"


class TestOrderedComparison:
    def test_equal_ordered(self):
        rows_a = [("1", "a"), ("2", "b")]
        rows_b = [("1", "a"), ("2", "b")]
        descs = [_int_desc("x"), _text_desc("y")]
        assert compare_results(rows_a, rows_b, descs, has_order_by=True) == CompareResult.EQUAL

    def test_different_order_is_tie_reorder(self):
        rows_a = [("1", "a"), ("2", "b")]
        rows_b = [("2", "b"), ("1", "a")]
        descs = [_int_desc("x"), _text_desc("y")]
        result = compare_results(rows_a, rows_b, descs, has_order_by=True)
        assert result == CompareResult.TIE_REORDER

    def test_different_values_is_mismatch(self):
        rows_a = [("1", "a")]
        rows_b = [("1", "x")]
        descs = [_int_desc("x"), _text_desc("y")]
        assert compare_results(rows_a, rows_b, descs, has_order_by=True) == CompareResult.MISMATCH

    def test_different_row_count_is_mismatch(self):
        rows_a = [("1",)]
        rows_b = [("1",), ("2",)]
        descs = [_int_desc()]
        assert compare_results(rows_a, rows_b, descs, has_order_by=True) == CompareResult.MISMATCH


class TestBagComparison:
    def test_same_rows_different_order(self):
        rows_a = [("1", "a"), ("2", "b")]
        rows_b = [("2", "b"), ("1", "a")]
        descs = [_int_desc("x"), _text_desc("y")]
        assert compare_results(rows_a, rows_b, descs, has_order_by=False) == CompareResult.EQUAL

    def test_duplicate_rows(self):
        rows_a = [("1",), ("1",), ("2",)]
        rows_b = [("2",), ("1",), ("1",)]
        descs = [_int_desc()]
        assert compare_results(rows_a, rows_b, descs, has_order_by=False) == CompareResult.EQUAL

    def test_different_multiplicity(self):
        rows_a = [("1",), ("1",), ("2",)]
        rows_b = [("1",), ("2",), ("2",)]
        descs = [_int_desc()]
        assert compare_results(rows_a, rows_b, descs, has_order_by=False) == CompareResult.MISMATCH


class TestNullHandling:
    def test_null_equal(self):
        rows_a = [(None, "a")]
        rows_b = [(None, "a")]
        descs = [_int_desc("x"), _text_desc("y")]
        assert compare_results(rows_a, rows_b, descs, has_order_by=True) == CompareResult.EQUAL

    def test_null_vs_value(self):
        rows_a = [(None,)]
        rows_b = [("1",)]
        descs = [_int_desc()]
        assert compare_results(rows_a, rows_b, descs, has_order_by=True) == CompareResult.MISMATCH


class TestNumericComparison:
    def test_trailing_zero_equivalence(self):
        rows_a = [("1.500",)]
        rows_b = [("1.5",)]
        descs = [_numeric_desc()]
        assert compare_results(rows_a, rows_b, descs, has_order_by=True) == CompareResult.EQUAL

    def test_different_values(self):
        rows_a = [("1.5",)]
        rows_b = [("1.6",)]
        descs = [_numeric_desc()]
        assert compare_results(rows_a, rows_b, descs, has_order_by=True) == CompareResult.MISMATCH


class TestJsonbComparison:
    def test_key_order_equivalence(self):
        rows_a = [('{"b":1,"a":2}',)]
        rows_b = [('{"a":2,"b":1}',)]
        descs = [_jsonb_desc()]
        assert compare_results(rows_a, rows_b, descs, has_order_by=True) == CompareResult.EQUAL

    def test_different_values(self):
        rows_a = [('{"a":1}',)]
        rows_b = [('{"a":2}',)]
        descs = [_jsonb_desc()]
        assert compare_results(rows_a, rows_b, descs, has_order_by=True) == CompareResult.MISMATCH


class TestEmptyResults:
    def test_both_empty(self):
        descs = [_int_desc()]
        assert compare_results([], [], descs, has_order_by=True) == CompareResult.EQUAL
        assert compare_results([], [], descs, has_order_by=False) == CompareResult.EQUAL
