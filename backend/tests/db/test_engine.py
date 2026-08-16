import pytest
from sqlalchemy import text

from ffh.db.base import Base
from ffh.db.engine import make_engine, make_session_factory

pytestmark = pytest.mark.db


def test_make_engine_defaults_to_settings_database_url(monkeypatch):
    monkeypatch.setenv("FFH_DATABASE_URL", "postgresql+psycopg://ffh:ffh@localhost:5432/ffh_test")
    engine = make_engine()
    assert engine.url.database == "ffh_test"
    with engine.connect() as conn:
        assert conn.execute(text("select 1")).scalar_one() == 1


def test_session_factory_yields_working_session():
    engine = make_engine("postgresql+psycopg://ffh:ffh@localhost:5432/ffh_test")
    factory = make_session_factory(engine)
    with factory() as session:
        assert session.execute(text("select 2")).scalar_one() == 2


def test_metadata_naming_convention_is_set():
    assert "fk" in Base.metadata.naming_convention
    assert "ix" in Base.metadata.naming_convention
