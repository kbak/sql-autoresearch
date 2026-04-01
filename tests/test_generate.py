from sql_autoresearch.generate import _format_table_definitions, _format_table_stats, build_prompt
from sql_autoresearch.models import ColumnInfo, IndexInfo, TableInfo, TableStats


def _make_table():
    return TableInfo(
        schema="public",
        name="users",
        oid=12345,
        columns=[
            ColumnInfo(name="id", type_name="int4", type_oid=23, not_null=True),
            ColumnInfo(name="name", type_name="text", type_oid=25, not_null=False),
        ],
        indexes=[
            IndexInfo(
                name="users_pkey",
                definition="CREATE UNIQUE INDEX users_pkey ON public.users USING btree (id)",
                is_unique=True,
                is_primary=True,
            ),
        ],
        stats=TableStats(
            n_distinct={"id": -1.0, "name": -0.5},
            null_frac={"id": 0.0, "name": 0.1},
            correlation={"id": 0.99, "name": 0.01},
        ),
        row_estimate=10000.0,
    )


class TestFormatTableDefinitions:
    def test_includes_columns(self):
        result = _format_table_definitions([_make_table()])
        assert "id int4 NOT NULL" in result
        assert "name text NULL" in result

    def test_includes_indexes(self):
        result = _format_table_definitions([_make_table()])
        assert "users_pkey" in result


class TestFormatTableStats:
    def test_includes_stats(self):
        result = _format_table_stats([_make_table()])
        assert "n_distinct=-1.0" in result
        assert "null_frac=0.0" in result


class TestBuildPrompt:
    def test_includes_sql(self):
        result = build_prompt("SELECT * FROM users", [_make_table()], [{"Plan": {}}])
        assert "SELECT * FROM users" in result

    def test_includes_explain(self):
        result = build_prompt("SELECT 1", [_make_table()], [{"Plan": {"Node Type": "Seq Scan"}}])
        assert "Seq Scan" in result
