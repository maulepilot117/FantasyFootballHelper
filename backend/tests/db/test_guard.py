import pytest

from tests.db._guard import assert_test_database


def test_guard_rejects_non_test_database():
    with pytest.raises(RuntimeError, match=r"non-test database 'ffh'.*\*_test"):
        assert_test_database("postgresql+psycopg://ffh:ffh@localhost:5432/ffh")


def test_guard_rejects_empty_database_name():
    with pytest.raises(RuntimeError, match="non-test database ''"):
        assert_test_database("postgresql+psycopg://ffh:ffh@localhost:5432")


def test_guard_returns_url_for_test_database():
    url = "postgresql+psycopg://ffh:ffh@localhost:5432/ffh_test"
    assert assert_test_database(url) == url
