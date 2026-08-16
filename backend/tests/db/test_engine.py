import pytest
from sqlalchemy import text
from sqlalchemy.engine import make_url

from ffh.config import get_settings
from ffh.db.base import Base
from ffh.db.engine import make_engine, make_session_factory

pytestmark = pytest.mark.db


def test_make_engine_defaults_to_settings_database_url(monkeypatch):
    test_url = get_settings().test_database_url
    monkeypatch.setenv("FFH_DATABASE_URL", test_url)
    get_settings.cache_clear()
    engine = make_engine()
    assert engine.url == make_url(test_url)
    with engine.connect() as conn:
        assert conn.execute(text("select 1")).scalar_one() == 1


def test_session_factory_yields_working_session():
    engine = make_engine(get_settings().test_database_url)
    factory = make_session_factory(engine)
    with factory() as session:
        assert session.execute(text("select 2")).scalar_one() == 2


def test_metadata_naming_convention_is_set():
    assert "fk" in Base.metadata.naming_convention
    assert "ix" in Base.metadata.naming_convention
