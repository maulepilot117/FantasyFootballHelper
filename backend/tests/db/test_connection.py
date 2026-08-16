import pytest
from sqlalchemy import create_engine, text

from ffh.config import get_settings

pytestmark = pytest.mark.db


def test_test_database_is_reachable_and_is_postgres_17():
    engine = create_engine(get_settings().test_database_url)
    with engine.connect() as conn:
        version = conn.execute(text("SHOW server_version_num")).scalar_one()
        database = conn.execute(text("SELECT current_database()")).scalar_one()
    assert database == "ffh_test"
    assert 170000 <= int(version) < 180000
