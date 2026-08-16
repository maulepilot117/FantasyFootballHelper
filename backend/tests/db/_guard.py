"""Safety guard for tests that DROP SCHEMA public CASCADE on FFH_TEST_DATABASE_URL."""

from sqlalchemy.engine import make_url


def assert_test_database(url: str) -> str:
    """Return ``url`` unchanged if its database name ends with ``_test``; else raise.

    Called before any schema-reset DDL so a misconfigured FFH_TEST_DATABASE_URL can
    never wipe a real database.
    """
    name = make_url(url).database or ""
    if not name.endswith("_test"):
        raise RuntimeError(
            f"Refusing to reset schema on non-test database '{name}' "
            "(FFH_TEST_DATABASE_URL must point at a *_test database)"
        )
    return url
