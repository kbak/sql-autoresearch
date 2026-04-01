from sql_autoresearch.adapters.postgres import RawTextLoader, check_schema_equality
from sql_autoresearch.models import ColumnDesc


class TestCheckSchemaEquality:
    def test_equal_schemas(self):
        a = [ColumnDesc("id", 23), ColumnDesc("name", 25)]
        b = [ColumnDesc("id", 23), ColumnDesc("name", 25)]
        assert check_schema_equality(a, b) is None

    def test_different_column_count(self):
        a = [ColumnDesc("id", 23)]
        b = [ColumnDesc("id", 23), ColumnDesc("name", 25)]
        result = check_schema_equality(a, b)
        assert result is not None
        assert "count" in result.lower()

    def test_different_column_name(self):
        a = [ColumnDesc("id", 23)]
        b = [ColumnDesc("user_id", 23)]
        result = check_schema_equality(a, b)
        assert result is not None
        assert "name" in result.lower()

    def test_different_type_oid(self):
        a = [ColumnDesc("id", 23)]
        b = [ColumnDesc("id", 20)]
        result = check_schema_equality(a, b)
        assert result is not None
        assert "type" in result.lower()


class TestRawTextLoader:
    def test_loads_bytes(self):
        loader = RawTextLoader(0)
        assert loader.load(b"hello") == "hello"

    def test_loads_memoryview(self):
        loader = RawTextLoader(0)
        assert loader.load(memoryview(b"hello")) == "hello"

    def test_loads_utf8(self):
        loader = RawTextLoader(0)
        assert loader.load("café".encode("utf-8")) == "café"
