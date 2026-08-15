import os
from pathlib import Path

from ffh.config import Settings, get_settings


def test_settings_read_env_with_prefix(monkeypatch):
    monkeypatch.setenv("FFH_DATABASE_URL", "postgresql+psycopg://u:p@h:5432/db")
    monkeypatch.setenv("FFH_LAKE_ROOT", "/tmp/lake")
    monkeypatch.setenv("FFH_SEASON", "2026")
    s = Settings()
    assert s.database_url == "postgresql+psycopg://u:p@h:5432/db"
    assert s.lake_root == Path("/tmp/lake")
    assert s.season == 2026


def test_settings_defaults_are_local_dev(monkeypatch):
    for key in [k for k in os.environ if k.startswith("FFH_")]:
        monkeypatch.delenv(key)
    s = Settings(_env_file=None)
    assert s.database_url == "postgresql+psycopg://ffh:ffh@localhost:5432/ffh"
    assert s.test_database_url.endswith("/ffh_test")
    assert s.redis_url == "redis://localhost:6379/0"
    assert s.sleeper_base_url == "https://api.sleeper.app/v1"
    assert s.season == 2026


def test_get_settings_is_cached():
    get_settings.cache_clear()
    assert get_settings() is get_settings()
