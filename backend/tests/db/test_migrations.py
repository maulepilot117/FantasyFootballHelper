import subprocess
import sys

import pytest
from sqlalchemy import inspect, text

from ffh.config import get_settings
from ffh.db.engine import make_engine

pytestmark = pytest.mark.db


def _alembic(*args: str) -> subprocess.CompletedProcess[str]:
    url = get_settings().test_database_url
    return subprocess.run(
        [sys.executable, "-m", "alembic", "-x", f"url={url}", *args],
        capture_output=True,
        text=True,
        check=False,
    )


def _reset_schema() -> None:
    engine = make_engine(get_settings().test_database_url)
    with engine.begin() as conn:
        conn.execute(text("DROP SCHEMA public CASCADE; CREATE SCHEMA public;"))
    engine.dispose()


@pytest.fixture(autouse=True)
def _leave_schema_at_head():
    """Other tests (db_session fixture) expect ffh_test at head after this module runs."""
    yield
    _reset_schema()
    assert _alembic("upgrade", "head").returncode == 0


def test_upgrade_head_creates_all_tables_and_check_reports_no_drift():
    _reset_schema()
    up = _alembic("upgrade", "head")
    assert up.returncode == 0, up.stderr
    engine = make_engine(get_settings().test_database_url)
    tables = set(inspect(engine).get_table_names())
    assert {
        "players",
        "player_external_ids",
        "games",
        "leagues",
        "draft_picks",
        "adp",
        "projections",
        "recommendations",
        "ai_debates",
        "ingest_runs",
    } <= tables
    check = _alembic("check")
    assert check.returncode == 0, check.stdout + check.stderr


def test_downgrade_base_removes_everything():
    _reset_schema()
    assert _alembic("upgrade", "head").returncode == 0
    down = _alembic("downgrade", "base")
    assert down.returncode == 0, down.stderr
    engine = make_engine(get_settings().test_database_url)
    assert set(inspect(engine).get_table_names()) == {"alembic_version"}
