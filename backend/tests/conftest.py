import json
import subprocess
import sys
from pathlib import Path

import httpx
import pytest
import respx
from sqlalchemy import text
from sqlalchemy.orm import Session

from ffh.adapters.sleeper.client import SleeperClient
from ffh.config import get_settings
from ffh.db.engine import make_engine
from tests.db._guard import assert_test_database

BACKEND_DIR = Path(__file__).resolve().parents[1]


@pytest.fixture(autouse=True)
def _clear_settings_cache():
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


@pytest.fixture(scope="session")
def migrated_engine():
    url = assert_test_database(get_settings().test_database_url)
    engine = make_engine(url)
    with engine.begin() as conn:
        conn.execute(text("DROP SCHEMA public CASCADE; CREATE SCHEMA public;"))
    r = subprocess.run(
        [sys.executable, "-m", "alembic", "-x", f"url={url}", "upgrade", "head"],
        cwd=BACKEND_DIR,
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


FIXTURE_LEAGUE_ID = "1000000000000000001"
FIXTURE_DRAFT_ID = "2000000000000000001"
SLEEPER_FIXTURES = BACKEND_DIR / "tests" / "fixtures" / "sleeper"


def load_sleeper_fixture(name: str):
    """Load one recorded Sleeper response by file stem."""
    return json.loads((SLEEPER_FIXTURES / f"{name}.json").read_text(encoding="utf-8"))


@pytest.fixture
def sleeper_fixture():
    return load_sleeper_fixture


@pytest.fixture
def sleeper_mock():
    """respx router serving the fixture league. CI never touches the network."""
    base = get_settings().sleeper_base_url
    lid, did = FIXTURE_LEAGUE_ID, FIXTURE_DRAFT_ID
    routes = {
        "/state/nfl": "state_nfl",
        f"/league/{lid}": "league",
        f"/league/{lid}/rosters": "rosters",
        f"/league/{lid}/users": "users",
        f"/league/{lid}/drafts": "league_drafts",
        f"/draft/{did}": "draft",
        f"/draft/{did}/picks": "draft_picks",
        f"/league/{lid}/matchups/1": "matchups_week1",
        f"/league/{lid}/transactions/1": "transactions_week1",
        "/players/nfl": "players_slice",
    }
    with respx.mock(base_url=base, assert_all_called=False) as router:
        for path, name in routes.items():
            router.get(path).mock(return_value=httpx.Response(200, json=load_sleeper_fixture(name)))
        yield router


@pytest.fixture
async def sleeper_client(sleeper_mock):
    """A SleeperClient bound to the fixture router; closed on teardown so no
    httpx.AsyncClient leaks out of a test.

    Retries use the REAL asyncio.sleep: a test that mocks 429/5xx on this client will
    sleep for real. Such tests should build their own client with an injected
    `retry_sleep` instead (see tests/adapters/sleeper/test_client.py).
    """
    client = SleeperClient(base_url=get_settings().sleeper_base_url)
    try:
        yield client
    finally:
        await client.aclose()
