import pytest

from sql_autoresearch.models import UnsafeQueryError
from sql_autoresearch.safety import check_ast


class TestCommentStripping:
    def test_strips_block_comments(self):
        result = check_ast("SELECT /* secret */ a FROM t1")
        assert "secret" not in result.clean_sql
        assert "a" in result.clean_sql

    def test_strips_line_comments(self):
        result = check_ast("SELECT a -- password=hunter2\nFROM t1")
        assert "hunter2" not in result.clean_sql

    def test_preserves_string_literals(self):
        result = check_ast("SELECT a FROM t1 WHERE b = '/* not a comment */'")
        assert "not a comment" in result.clean_sql


class TestStatementTypeRejection:
    def test_rejects_insert(self):
        with pytest.raises(UnsafeQueryError, match="INSERT|SELECT"):
            check_ast("INSERT INTO t1 VALUES (1)")

    def test_rejects_update(self):
        with pytest.raises(UnsafeQueryError, match="UPDATE|SELECT"):
            check_ast("UPDATE t1 SET a = 1")

    def test_rejects_delete(self):
        with pytest.raises(UnsafeQueryError, match="DELETE|SELECT"):
            check_ast("DELETE FROM t1")

    def test_rejects_multi_statement(self):
        with pytest.raises(UnsafeQueryError, match="[Mm]ulti"):
            check_ast("SELECT 1 FROM t1; SELECT 2 FROM t2")

    def test_rejects_writable_cte(self):
        with pytest.raises(UnsafeQueryError, match="DELETE|INSERT|UPDATE"):
            check_ast(
                "WITH del AS (DELETE FROM t1 RETURNING *) SELECT * FROM del"
            )


class TestLimitRestriction:
    def test_rejects_limit(self):
        with pytest.raises(UnsafeQueryError, match="LIMIT"):
            check_ast("SELECT a FROM t1 LIMIT 10")

    def test_rejects_offset(self):
        with pytest.raises(UnsafeQueryError, match="LIMIT|OFFSET"):
            check_ast("SELECT a FROM t1 OFFSET 5")

    def test_rejects_fetch_first(self):
        with pytest.raises(UnsafeQueryError, match="LIMIT|FETCH"):
            check_ast("SELECT a FROM t1 FETCH FIRST 5 ROWS ONLY")

    def test_rejects_limit_in_subquery(self):
        with pytest.raises(UnsafeQueryError, match="LIMIT"):
            check_ast(
                "SELECT * FROM t1 WHERE a IN (SELECT a FROM t2 LIMIT 5)"
            )


class TestDistinctOn:
    def test_allows_distinct(self):
        result = check_ast("SELECT DISTINCT a FROM t1")
        assert result.clean_sql

    def test_rejects_distinct_on(self):
        with pytest.raises(UnsafeQueryError, match="DISTINCT ON"):
            check_ast("SELECT DISTINCT ON (a) a, b FROM t1 ORDER BY a, b")


class TestLockingClause:
    def test_rejects_for_update(self):
        with pytest.raises(UnsafeQueryError, match="FOR UPDATE"):
            check_ast("SELECT a FROM t1 FOR UPDATE")

    def test_rejects_for_share(self):
        with pytest.raises(UnsafeQueryError, match="FOR"):
            check_ast("SELECT a FROM t1 FOR SHARE")


class TestParamRef:
    def test_rejects_dollar_params(self):
        with pytest.raises(UnsafeQueryError, match="\\$1|[Pp]arameter"):
            check_ast("SELECT a FROM t1 WHERE a = $1")

    def test_allows_jsonb_question_mark(self):
        result = check_ast("SELECT a FROM t1 WHERE data ? 'key'")
        assert result.clean_sql


class TestFromRestrictions:
    def test_rejects_srf_in_from(self):
        with pytest.raises(UnsafeQueryError, match="[Ss]et-returning|FROM"):
            check_ast("SELECT * FROM generate_series(1, 10)")

    def test_rejects_tablesample(self):
        with pytest.raises(UnsafeQueryError, match="TABLESAMPLE"):
            check_ast("SELECT * FROM t1 TABLESAMPLE BERNOULLI(10)")

    def test_rejects_values_in_from(self):
        with pytest.raises(UnsafeQueryError, match="VALUES"):
            check_ast(
                "SELECT * FROM (VALUES (1, 2), (3, 4)) AS v(a, b)"
            )

    def test_allows_subquery_in_from(self):
        result = check_ast(
            "SELECT * FROM (SELECT a FROM t1) AS sub"
        )
        assert result.clean_sql

    def test_rejects_zero_table_query(self):
        with pytest.raises(UnsafeQueryError, match="[Nn]o base table"):
            check_ast("SELECT 1 AS x")


class TestAggregateAllowlist:
    def test_allows_count(self):
        result = check_ast("SELECT count(*) FROM t1")
        assert "count" in result.function_names

    def test_allows_sum(self):
        result = check_ast("SELECT sum(a) FROM t1")
        assert "sum" in result.function_names

    def test_rejects_array_agg(self):
        with pytest.raises(UnsafeQueryError, match="array_agg"):
            check_ast("SELECT array_agg(a) FROM t1")

    def test_rejects_string_agg(self):
        with pytest.raises(UnsafeQueryError, match="string_agg"):
            check_ast("SELECT string_agg(a, ',') FROM t1")

    def test_rejects_json_agg(self):
        with pytest.raises(UnsafeQueryError, match="json_agg"):
            check_ast("SELECT json_agg(a) FROM t1")


class TestWindowFunctions:
    def test_allows_rank(self):
        result = check_ast(
            "SELECT rank() OVER (ORDER BY a) FROM t1"
        )
        assert "rank" in result.function_names

    def test_allows_dense_rank(self):
        result = check_ast(
            "SELECT dense_rank() OVER (ORDER BY a) FROM t1"
        )
        assert "dense_rank" in result.function_names

    def test_rejects_row_number(self):
        with pytest.raises(UnsafeQueryError, match="row_number"):
            check_ast("SELECT row_number() OVER (ORDER BY a) FROM t1")

    def test_rejects_lag(self):
        with pytest.raises(UnsafeQueryError, match="lag"):
            check_ast("SELECT lag(a) OVER (ORDER BY a) FROM t1")

    def test_allows_aggregate_window_full_partition(self):
        result = check_ast("SELECT sum(a) OVER () FROM t1")
        assert result.clean_sql

    def test_allows_aggregate_window_explicit_full_frame(self):
        result = check_ast(
            "SELECT sum(a) OVER (ORDER BY b ROWS BETWEEN "
            "UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM t1"
        )
        assert result.clean_sql

    def test_rejects_aggregate_window_with_order_default_frame(self):
        with pytest.raises(UnsafeQueryError, match="full-partition"):
            check_ast("SELECT sum(a) OVER (ORDER BY b) FROM t1")


class TestFunctionExtraction:
    def test_extracts_function_names(self):
        result = check_ast(
            "SELECT count(*), max(a), min(b) FROM t1"
        )
        assert {"count", "max", "min"}.issubset(result.function_names)

    def test_extracts_nested_functions(self):
        result = check_ast(
            "SELECT count(*) FROM t1 WHERE a > avg(b) OVER ()"
        )
        assert "count" in result.function_names
        assert "avg" in result.function_names


class TestOperatorExtraction:
    def test_extracts_operators(self):
        result = check_ast(
            "SELECT a FROM t1 WHERE a > 1 AND b = 'x'"
        )
        assert ">" in result.operator_names
        assert "=" in result.operator_names

    def test_extracts_arithmetic_operators(self):
        result = check_ast(
            "SELECT a + b, c - d FROM t1"
        )
        assert "+" in result.operator_names
        assert "-" in result.operator_names


class TestCastTypeChecks:
    def test_allows_integer_cast(self):
        result = check_ast("SELECT a::integer FROM t1")
        assert result.clean_sql

    def test_allows_text_cast(self):
        result = check_ast("SELECT a::text FROM t1")
        assert result.clean_sql

    def test_allows_jsonb_cast(self):
        result = check_ast("SELECT a::jsonb FROM t1")
        assert result.clean_sql


class TestRelationExtraction:
    def test_simple_table(self):
        result = check_ast("SELECT a FROM t1")
        assert len(result.relations) == 1
        assert result.relations[0].name == "t1"
        assert result.relations[0].schema is None

    def test_schema_qualified(self):
        result = check_ast("SELECT a FROM myschema.t1")
        assert result.relations[0].schema == "myschema"
        assert result.relations[0].name == "t1"

    def test_join_extracts_both_tables(self):
        result = check_ast(
            "SELECT t1.a FROM t1 JOIN t2 ON t1.id = t2.id"
        )
        names = {r.name for r in result.relations}
        assert names == {"t1", "t2"}

    def test_only_keyword(self):
        result = check_ast("SELECT a FROM ONLY t1")
        assert result.relations[0].inh is False

    def test_cte_not_counted_as_table(self):
        result = check_ast(
            "WITH cte AS (SELECT a FROM t1) SELECT * FROM cte"
        )
        names = {r.name for r in result.relations}
        assert names == {"t1"}
        assert "cte" not in names

    def test_cte_reference_in_later_cte(self):
        result = check_ast(
            "WITH cte1 AS (SELECT a FROM t1), "
            "cte2 AS (SELECT a FROM cte1) "
            "SELECT * FROM cte2"
        )
        names = {r.name for r in result.relations}
        assert names == {"t1"}

    def test_subquery_tables_extracted(self):
        result = check_ast(
            "SELECT * FROM (SELECT a FROM t1) AS sub"
        )
        names = {r.name for r in result.relations}
        assert names == {"t1"}

    def test_exists_subquery_tables(self):
        result = check_ast(
            "SELECT a FROM t1 WHERE EXISTS (SELECT 1 FROM t2)"
        )
        names = {r.name for r in result.relations}
        assert names == {"t1", "t2"}

    def test_union_tables(self):
        result = check_ast(
            "SELECT a FROM t1 UNION ALL SELECT a FROM t2"
        )
        names = {r.name for r in result.relations}
        assert names == {"t1", "t2"}


class TestOrderByDetection:
    def test_detects_order_by(self):
        result = check_ast("SELECT a FROM t1 ORDER BY a")
        assert result.has_order_by is True

    def test_no_order_by(self):
        result = check_ast("SELECT a FROM t1")
        assert result.has_order_by is False

    def test_union_with_order_by(self):
        result = check_ast(
            "SELECT a FROM t1 UNION ALL SELECT a FROM t2 ORDER BY a"
        )
        assert result.has_order_by is True
