import pytest


@pytest.fixture
def simple_select():
    return "SELECT a, b FROM t1 WHERE a > 1 ORDER BY a"


@pytest.fixture
def join_select():
    return "SELECT t1.a, t2.b FROM t1 JOIN t2 ON t1.id = t2.id WHERE t1.a > 1"


@pytest.fixture
def cte_select():
    return "WITH cte AS (SELECT a, b FROM t1) SELECT * FROM cte"
