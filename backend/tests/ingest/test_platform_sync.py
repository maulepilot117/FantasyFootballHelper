import asyncio
from dataclasses import replace

import httpx
import pytest
from sqlalchemy import delete, func, select

from ffh.adapters.base import PlatformError, PlayerRef
from ffh.adapters.sleeper.adapter import SleeperAdapter, player_ref
from ffh.adapters.sleeper.client import SleeperClient
from ffh.adapters.sleeper.models import RawPlayer
from ffh.config import get_settings
from ffh.crosswalk.resolve import GSIS_METHOD, LIVE_MAPPING
from ffh.crosswalk.review import reject_mapping
from ffh.db.models import (
    CrosswalkUnmatched,
    Draft,
    DraftPick,
    League,
    LeagueTeam,
    Player,
    PlayerExternalId,
    RosterSlot,
)
from ffh.ingest.platform_sync import fetch_snapshot, load_league, persist_snapshot
from tests.conftest import load_sleeper_fixture
from tests.ingest._sleeper_seed import SEEDED_PLAYERS, seed_fixture_players

pytestmark = pytest.mark.db

LEAGUE = "1000000000000000001"
DRAFT = "2000000000000000001"


class FixtureCatalog:
    """The Sleeper blob slice as a PlayerCatalog — what LakePlayerCatalog serves in prod."""

    async def all_players(self) -> dict[str, PlayerRef]:
        blob = load_sleeper_fixture("players_slice")
        return {pid: player_ref(RawPlayer.model_validate(raw)) for pid, raw in blob.items()}


@pytest.fixture
def adapter(sleeper_mock):
    """A SYNC fixture on purpose: every test here drives the sync `load_league`, which owns
    the one `asyncio.run`. An async fixture would need an async test to hold the loop open.
    The client is closed in its own short-lived loop so no httpx.AsyncClient leaks."""
    client = SleeperClient(base_url=get_settings().sleeper_base_url)
    try:
        yield SleeperAdapter(client, my_user_id="USER_ME")
    finally:
        asyncio.run(client.aclose())


@pytest.fixture
def adapter_factory(sleeper_mock):
    """Builds a FRESH adapter (and client) per `load_league` call — `load_league`'s
    documented lifetime contract, because it runs and closes its own event loop and an
    httpx pool cannot be reused across loops. Every test that loads twice uses this.
    respx would happily hide the violation, so the tests model the contract instead."""
    clients: list[SleeperClient] = []

    def make() -> SleeperAdapter:
        client = SleeperClient(base_url=get_settings().sleeper_base_url)
        clients.append(client)
        return SleeperAdapter(client, my_user_id="USER_ME")

    try:
        yield make
    finally:
        asyncio.run(_aclose_all(clients))


async def _aclose_all(clients: list[SleeperClient]) -> None:
    for client in clients:
        await client.aclose()


@pytest.fixture
def catalog_adapter(sleeper_mock):
    """Same, with the player blob wired in — the production shape, where a rostered id
    reaches the crosswalk with a name, a position, a team AND a gsis_id."""
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
    of db_session; tests about unmatched reporting stay unseeded."""
    seed_fixture_players(db_session)
    assert db_session.scalar(select(func.count()).select_from(Player)) == SEEDED_PLAYERS
    return db_session


def test_load_league_persists_settings_verbatim(db_session, sleeper_fixture, adapter):
    report = load_league(db_session, adapter, LEAGUE, season=2026, week=1)
    row = db_session.get(League, report.league_id)
    expected = sleeper_fixture("league")["scoring_settings"]
    assert row.scoring_settings == expected
    assert set(row.scoring_settings) == set(expected)  # no key added or removed
    assert row.platform == "sleeper" and row.season == 2026
    assert row.num_teams == 2 and row.league_type == "redraft"
    assert row.faab_budget == 100
    assert row.playoff_teams == 2 and row.playoff_start_wk == 15


def test_is_superflex_is_derived_from_roster_positions(db_session, adapter):
    report = load_league(db_session, adapter, LEAGUE, season=2026, week=1)
    row = db_session.get(League, report.league_id)
    assert row.is_superflex is True
    assert "SUPER_FLEX" in row.roster_settings["starters"]


def test_teams_my_team_and_roster_slots(seeded, adapter):
    report = load_league(seeded, adapter, LEAGUE, season=2026, week=1)
    assert report.teams == 2
    assert report.unmatched == [] and report.pending_review == []
    league = seeded.get(League, report.league_id)
    mine = seeded.scalars(
        select(LeagueTeam).where(LeagueTeam.league_id == league.league_id, LeagueTeam.is_me)
    ).all()
    assert len(mine) == 1
    assert league.my_team_id == mine[0].league_team_id
    assert mine[0].faab_remaining == 75

    slots = seeded.scalars(
        select(RosterSlot).where(RosterSlot.league_team_id == mine[0].league_team_id)
    ).all()
    assert {s.week for s in slots} == {1}
    by_slot = {s.slot for s in slots}
    assert {"QB", "SUPER_FLEX", "DST", "BN", "IR", "TAXI"} <= by_slot
    assert sum(1 for s in slots if s.is_starter) == 10
    assert report.rostered == 23  # 13 on my roster + 10 on theirs


def test_drafts_and_picks_land(seeded, adapter):
    report = load_league(seeded, adapter, LEAGUE, season=2026, week=1)
    assert report.drafts == 1 and report.picks == 4
    draft = seeded.scalars(select(Draft)).one()
    assert draft.draft_type == "snake" and draft.rounds == 13 and draft.my_slot == 1
    picks = seeded.scalars(select(DraftPick).order_by(DraftPick.pick_no)).all()
    assert [p.pick_no for p in picks] == [1, 2, 3, 4]
    assert picks[2].is_keeper is True
    assert picks[3].auction_amount == 12
    assert all(p.league_team_id is not None for p in picks)
    assert all(p.player_id is not None for p in picks)  # every pick resolved via the seed
    assert all(p.picked_at is None for p in picks)  # Sleeper publishes no per-pick time


def _picks_plus(sleeper_mock, sleeper_fixture, player_id: str) -> None:
    """Serve the fixture draft with one EXTRA pick, of a player nobody rosters.

    Routine after week-1 waiver churn: the pick is history, the player is gone from every
    roster. `draft_picks.player_id` still has to point somewhere, so the id must reach the
    crosswalk even though no roster mentions it.
    """
    picks = sleeper_fixture("draft_picks")
    extra = dict(picks[-1])
    extra |= {"pick_no": 5, "round": 3, "draft_slot": 1, "player_id": player_id}
    extra["metadata"] = dict(extra["metadata"]) | {"player_id": player_id, "amount": "0"}
    sleeper_mock.get(f"/draft/{DRAFT}/picks").mock(
        return_value=httpx.Response(200, json=[*picks, extra])
    )


def test_a_drafted_but_no_longer_rostered_player_still_reaches_the_crosswalk(
    seeded, sleeper_mock, sleeper_fixture, adapter
):
    """Pick ids are unioned into the crosswalk batch, so a drafted-and-dropped player
    still resolves — rather than landing as a NULL player_id nobody ever hears about."""
    _picks_plus(sleeper_mock, sleeper_fixture, "90")  # a free agent: drafted, since dropped
    report = load_league(seeded, adapter, LEAGUE, season=2026, week=1)
    assert report.picks == 5
    assert report.unmatched == [] and report.pending_review == []
    # He is not on a roster, so the week's snapshot is unchanged...
    assert report.rostered == 23
    assert seeded.scalar(select(func.count()).select_from(RosterSlot)) == 23
    # ...but his pick is fully resolved.
    pick = seeded.scalars(select(DraftPick).where(DraftPick.pick_no == 5)).one()
    assert pick.player_id is not None


def test_a_drafted_player_the_crosswalk_cannot_resolve_is_reported_not_dropped(
    seeded, sleeper_mock, sleeper_fixture, adapter
):
    """The Global Constraint, applied to picks: an unresolvable drafted id lands in the
    report AND in crosswalk_unmatched, instead of vanishing behind a NULL FK."""
    _picks_plus(sleeper_mock, sleeper_fixture, "9999")  # nobody the registry has ever seen
    report = load_league(seeded, adapter, LEAGUE, season=2026, week=1)
    assert [u.external_id for u in report.unmatched] == ["9999"]
    assert report.rostered == 23  # the rostered snapshot is untouched
    assert (
        seeded.scalar(
            select(func.count())
            .select_from(CrosswalkUnmatched)
            .where(CrosswalkUnmatched.source == "sleeper", CrosswalkUnmatched.external_id == "9999")
        )
        == 1
    )
    pick = seeded.scalars(select(DraftPick).where(DraftPick.pick_no == 5)).one()
    assert pick.player_id is None  # reported, then left NULL — never silently dropped


def test_duplicate_pick_numbers_abort_before_any_write(
    db_session, sleeper_mock, sleeper_fixture, adapter
):
    """draft_picks is keyed (draft_id, pick_no): two picks sharing one would ON CONFLICT
    onto each other. Caught before the first write, like the same-league invariant."""
    picks = sleeper_fixture("draft_picks")
    sleeper_mock.get(f"/draft/{DRAFT}/picks").mock(
        return_value=httpx.Response(200, json=[*picks, dict(picks[0])])
    )
    with pytest.raises(PlatformError, match="pick numbers"):
        load_league(db_session, adapter, LEAGUE, season=2026, week=1)
    assert db_session.scalar(select(func.count()).select_from(League)) == 0


def test_duplicate_draft_ids_abort_before_any_write(
    db_session, sleeper_mock, sleeper_fixture, adapter
):
    """`snapshot.picks` is keyed by draft external_id, so a league listing the same draft
    twice collapses to ONE picks key while `snapshot.drafts` still has two entries —
    `_upsert_drafts` would then re-walk that key and double-count (and re-ON-CONFLICT)
    every pick. Counting rows afterwards cannot see it: the loop counted both."""
    drafts = sleeper_fixture("league_drafts")
    sleeper_mock.get(f"/league/{LEAGUE}/drafts").mock(
        return_value=httpx.Response(200, json=[*drafts, dict(drafts[0])])
    )
    with pytest.raises(PlatformError, match="external ids"):
        load_league(db_session, adapter, LEAGUE, season=2026, week=1)
    assert db_session.scalar(select(func.count()).select_from(League)) == 0


def test_picks_for_an_unlisted_draft_abort_before_any_write(db_session, adapter):
    """`_upsert_drafts` only walks LISTED drafts, so picks under any other key would be
    silently discarded — the one thing the Global Constraints forbid. `fetch_snapshot`
    cannot build such a snapshot (it derives the keys from the drafts), so the guard is
    proved against a hand-made one, the way `persist_snapshot` can be called directly."""
    snapshot = asyncio.run(fetch_snapshot(adapter, LEAGUE, week=1))
    orphaned = replace(snapshot, picks={**snapshot.picks, "DRAFT_GONE": snapshot.picks[DRAFT]})
    with pytest.raises(PlatformError, match="unlisted draft"):
        persist_snapshot(db_session, orphaned)
    assert db_session.scalar(select(func.count()).select_from(League)) == 0


def test_fetch_snapshot_describes_rostered_and_drafted_ids(sleeper_mock, sleeper_fixture, adapter):
    _picks_plus(sleeper_mock, sleeper_fixture, "9999")
    snapshot = asyncio.run(fetch_snapshot(adapter, LEAGUE, week=1))
    rostered = {e.player_external_id for r in snapshot.rosters for e in r.players}
    assert "9999" not in rostered
    assert set(snapshot.player_refs) == rostered | {"9999"}


def test_load_is_idempotent(seeded, adapter_factory):
    first = load_league(seeded, adapter_factory(), LEAGUE, season=2026, week=1)
    second = load_league(seeded, adapter_factory(), LEAGUE, season=2026, week=1)
    assert first.league_id == second.league_id
    assert seeded.scalar(select(func.count()).select_from(League)) == 1
    assert seeded.scalar(select(func.count()).select_from(LeagueTeam)) == 2
    assert seeded.scalar(select(func.count()).select_from(Draft)) == 1
    assert seeded.scalar(select(func.count()).select_from(DraftPick)) == 4
    assert seeded.scalar(select(func.count()).select_from(RosterSlot)) == 23


def test_reruns_replace_the_week_snapshot_rather_than_accumulating(
    seeded, sleeper_mock, sleeper_fixture, adapter_factory
):
    load_league(seeded, adapter_factory(), LEAGUE, season=2026, week=1)
    rosters = sleeper_fixture("rosters")
    rosters[0]["players"] = [p for p in rosters[0]["players"] if p != "10"]  # dropped a bench guy
    sleeper_mock.get(f"/league/{LEAGUE}/rosters").mock(
        return_value=httpx.Response(200, json=rosters)
    )
    report = load_league(seeded, adapter_factory(), LEAGUE, season=2026, week=1)
    assert report.rostered == 22
    assert seeded.scalar(select(func.count()).select_from(RosterSlot)) == 22


def test_a_different_week_keeps_the_earlier_snapshot(seeded, adapter_factory):
    load_league(seeded, adapter_factory(), LEAGUE, season=2026, week=1)
    load_league(seeded, adapter_factory(), LEAGUE, season=2026, week=2)
    assert seeded.scalar(select(func.count()).select_from(RosterSlot)) == 46
    assert set(seeded.scalars(select(RosterSlot.week))) == {1, 2}


def test_season_mismatch_raises(db_session, adapter):
    with pytest.raises(ValueError, match="season 2026"):
        load_league(db_session, adapter, LEAGUE, season=2025, week=1)


def test_same_league_invariant_on_draft_picks_raises(
    db_session, sleeper_mock, sleeper_fixture, adapter
):
    """A pick whose roster_id is not a team of THIS league must abort the load."""
    picks = sleeper_fixture("draft_picks")
    picks[0]["roster_id"] = 99
    sleeper_mock.get(f"/draft/{DRAFT}/picks").mock(return_value=httpx.Response(200, json=picks))
    with pytest.raises(PlatformError, match="not a team of league"):
        load_league(db_session, adapter, LEAGUE, season=2026, week=1)
    # The invariant is checked BEFORE the first write, so nothing landed to roll back.
    assert db_session.scalar(select(func.count()).select_from(League)) == 0


def test_unmatched_players_are_reported_not_dropped(db_session, adapter):
    """With no players seeded, every rostered id is unmatched and every one is reported."""
    report = load_league(db_session, adapter, LEAGUE, season=2026, week=1)
    assert len(report.unmatched) == 23
    assert {u.external_id for u in report.unmatched} >= {"1", "KC", "SF"}
    assert db_session.scalar(select(func.count()).select_from(RosterSlot)) == 0
    # ④'s resolve_many already queued every one of them; nothing was silently dropped.
    queued = set(
        db_session.scalars(
            select(CrosswalkUnmatched.external_id).where(CrosswalkUnmatched.source == "sleeper")
        )
    )
    assert queued == {u.external_id for u in report.unmatched}


def test_unmatched_rows_carry_raw_context_for_the_operator(db_session, catalog_adapter):
    """Ruling 6: ④'s _record_unmatched only stores the raw_* fields the CALLER supplied, so
    platform_sync must pass them or the review queue holds a bare id and an acknowledged
    entry silently re-opens on the next sync."""
    report = load_league(db_session, catalog_adapter, LEAGUE, season=2026, week=1)
    assert len(report.unmatched) == 23
    rows = {
        r.external_id: r
        for r in db_session.scalars(
            select(CrosswalkUnmatched).where(CrosswalkUnmatched.source == "sleeper")
        )
    }
    assert rows["1"].raw_name == "Fixture Quarterback"
    assert rows["1"].raw_position == "QB" and rows["1"].raw_team == "KC"
    assert rows["KC"].raw_name == "KC" and rows["KC"].raw_position == "DST"
    assert all(r.raw_name for r in rows.values())
    # The report mirrors the queue, not a bare id list.
    described = {u.external_id: u for u in report.unmatched}
    assert described["1"].name == "Fixture Quarterback" and described["1"].position == "QB"


def test_a_gsis_id_resolves_a_player_whose_sleeper_mapping_is_missing(seeded, catalog_adapter):
    """Ruling 7: PlayerRef.gsis_id is the crosswalk's strongest join key. Drop the seeded
    sleeper mapping for player "1" and only the gsis fact can still resolve him."""
    seeded.execute(
        delete(PlayerExternalId).where(
            PlayerExternalId.source == "sleeper", PlayerExternalId.external_id == "1"
        )
    )
    report = load_league(seeded, catalog_adapter, LEAGUE, season=2026, week=1)
    assert report.unmatched == [] and report.pending_review == []
    assert report.rostered == 23
    # LIVE_MAPPING (ruling 5), never a hand-rolled predicate: a tombstone is not a mapping.
    row = seeded.scalars(
        select(PlayerExternalId).where(
            PlayerExternalId.source == "sleeper",
            PlayerExternalId.external_id == "1",
            LIVE_MAPPING,
        )
    ).one()
    assert row.match_method == GSIS_METHOD


def test_a_rejected_tombstone_is_reported_unmatched_never_resolved(seeded, adapter):
    """Ruling 5: `match_method='rejected'` is a human's "no". It must not resolve, and the
    id must come back in the report instead of vanishing."""
    assert reject_mapping(seeded, "sleeper", "1") is True
    report = load_league(seeded, adapter, LEAGUE, season=2026, week=1)
    assert [u.external_id for u in report.unmatched] == ["1"]
    assert report.rostered == 22
    assert (
        seeded.scalars(
            select(PlayerExternalId).where(
                PlayerExternalId.source == "sleeper",
                PlayerExternalId.external_id == "1",
                LIVE_MAPPING,
            )
        ).first()
        is None
    )


def test_fetch_snapshot_and_persist_snapshot_are_separately_drivable(seeded, adapter):
    """The async half and the sync half are both public so a caller already inside an
    event loop can await one and hand the result to the other."""
    snapshot = asyncio.run(fetch_snapshot(adapter, LEAGUE, week=3))
    assert snapshot.week == 3
    assert len(snapshot.teams) == 2 and len(snapshot.rosters) == 2
    assert len(snapshot.drafts) == 1 and len(snapshot.picks[DRAFT]) == 4
    assert set(snapshot.player_refs) >= {"1", "KC", "SF"}
    report = persist_snapshot(seeded, snapshot)
    assert report.rostered == 23
    assert set(seeded.scalars(select(RosterSlot.week))) == {3}


def test_fetch_snapshot_fetches_the_week_when_none_is_given(adapter):
    snapshot = asyncio.run(fetch_snapshot(adapter, LEAGUE))
    assert snapshot.week == 1  # state_nfl fixture is season_type="regular", week 1


async def test_load_league_refuses_to_run_inside_an_event_loop(db_session, adapter):
    with pytest.raises(RuntimeError, match="synchronous"):
        load_league(db_session, adapter, LEAGUE, season=2026, week=1)
