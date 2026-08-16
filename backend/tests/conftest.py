import subprocess
import sys

import pytest
from sqlalchemy import text
from sqlalchemy.orm import Session

from ffh.config import get_settings
from ffh.db.engine import make_engine


@pytest.fixture(autouse=True)
def _clear_settings_cache():
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


@pytest.fixture(scope="session")
def migrated_engine():
    url = get_settings().test_database_url
    engine = make_engine(url)
    with engine.begin() as conn:
        conn.execute(text("DROP SCHEMA public CASCADE; CREATE SCHEMA public;"))
    r = subprocess.run(
        [sys.executable, "-m", "alembic", "-x", f"url={url}", "upgrade", "head"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert r.returncode == 0, r.stderr
    yield engine
    engine.dispose()


@pytest.fixture
def db_session(migrated_engine):
    """A session inside a transaction that is always rolled back."""
    conn = migrated_engine.connect()
    trans = conn.begin()
    session = Session(bind=conn, join_transaction_mode="create_savepoint")
    try:
        yield session
    finally:
        session.close()
        trans.rollback()
        conn.close()
