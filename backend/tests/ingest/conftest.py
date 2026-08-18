"""Fixtures shared by the league-load tests (test_platform_sync, test_crosswalk_coverage).

They live here rather than in one of those modules so neither test file is a de facto
public API for the other. Every adapter fixture closes its `SleeperClient` in its own
short-lived event loop: `load_league` owns the one `asyncio.run`, so nothing here may hold
a loop open, and no httpx.AsyncClient may leak out of a test.
"""

import asyncio

import pytest
from sqlalchemy import func, select

from ffh.adapters.base import PlayerRef
from ffh.adapters.sleeper.adapter import SleeperAdapter, player_ref
from ffh.adapters.sleeper.client import SleeperClient
from ffh.adapters.sleeper.models import RawPlayer
from ffh.config import get_settings
from ffh.db.models import Player
from tests.conftest import load_sleeper_fixture
from tests.ingest._sleeper_seed import SEEDED_PLAYERS, seed_fixture_players


class FixtureCatalog:
    """The Sleeper blob slice as a PlayerCatalog — what LakePlayerCatalog serves in prod."""

    async def all_players(self) -> dict[str, PlayerRef]:
        blob = load_sleeper_fixture("players_slice")
        return {pid: player_ref(RawPlayer.model_validate(raw)) for pid, raw in blob.items()}


@pytest.fixture
def adapter(sleeper_mock):
    """A SYNC fixture on purpose: every test here drives the sync `load_league`, which owns
    the one `asyncio.run`. An async fixture would need an async test to hold the loop open.
    The client is closed in its own short-lived loop so no httpx.AsyncClient leaks.

    No catalog: a rostered id reaches the crosswalk as a bare id (a NON-numeric one as its
    team DST), which is the degraded shape — rung 1 or nothing for a human.
    """
    client = SleeperClient(base_url=get_settings().sleeper_base_url)
    try:
        yield SleeperAdapter(client, my_user_id="USER_ME")
    finally:
        asyncio.run(client.aclose())


@pytest.fixture
def catalog_adapter(sleeper_mock):
    """Same, with the player blob wired in — the production shape `ffh league load` builds,
    where a rostered id reaches the crosswalk with a name, a position, a team AND a
    gsis_id, so rungs 2, 3 and 4 are all reachable."""
    client = SleeperClient(base_url=get_settings().sleeper_base_url)
    try:
        yield SleeperAdapter(client, my_user_id="USER_ME", catalog=FixtureCatalog())
    finally:
        asyncio.run(client.aclose())


@pytest.fixture
def seeded(db_session):
    """db_session with the registry seeded the way `ffh crosswalk seed` would: a players
    row + sleeper id per fixture human (④ apply_playerids) and the 32 DSTs (④
    seed_dst_players). Tests that assert roster_slots / rostered counts take THIS instead
    of db_session; tests about unmatched reporting stay unseeded.

    Every human here gets a DynastyProcess sleeper mapping, so every one of them resolves
    at rung 1. Tests that need a lower rung seed with `seed_fixture_players(db_session,
    drop_sleeper_ids=..., drop_gsis_ids=...)` themselves.
    """
    seed_fixture_players(db_session)
    assert db_session.scalar(select(func.count()).select_from(Player)) == SEEDED_PLAYERS
    return db_session
