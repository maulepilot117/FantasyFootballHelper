from datetime import UTC, datetime

import httpx
import polars as pl
import pytest

from ffh.adapters.base import FantasyPlatformAdapter, PlatformError, PlayerRef
from ffh.adapters.sleeper.adapter import SleeperAdapter, player_ref
from ffh.adapters.sleeper.catalog import LakePlayerCatalog
from ffh.adapters.sleeper.models import RawPlayer
from tests.conftest import FIXTURE_DRAFT_ID as DRAFT
from tests.conftest import FIXTURE_LEAGUE_ID as LEAGUE


class StubCatalog:
    def __init__(self, refs: dict[str, PlayerRef]) -> None:
        self._refs = refs

    async def all_players(self) -> dict[str, PlayerRef]:
        return self._refs


@pytest.fixture
def adapter(sleeper_client) -> SleeperAdapter:
    """Adapter over the fixture-bound client; the client is closed by its own fixture."""
    return SleeperAdapter(sleeper_client, my_user_id="USER_ME")


async def test_get_scoring_settings_is_verbatim(adapter, sleeper_fixture):
    s = await adapter.get_scoring_settings(LEAGUE)
    assert s.points == sleeper_fixture("league")["scoring_settings"]
    assert s.format == "half_ppr"


async def test_get_roster_settings_maps_tokens_and_capacity(adapter):
    r = await adapter.get_roster_settings(LEAGUE)
    assert r.starters == ["QB", "RB", "RB", "WR", "WR", "TE", "FLEX", "SUPER_FLEX", "K", "DST"]
    assert r.bench == 3 and r.ir == 1 and r.taxi == 1
    assert r.flex_composition == {
        "FLEX": ["RB", "WR", "TE"],
        "SUPER_FLEX": ["QB", "RB", "WR", "TE"],
    }
    assert r.is_superflex is True


@pytest.mark.parametrize("field", ["reserve_slots", "taxi_slots"])
async def test_get_roster_settings_raises_when_ir_or_taxi_capacity_is_missing(
    adapter, sleeper_mock, sleeper_fixture, field
):
    # No made-up 0: a league whose settings lack the field is a hard error naming it.
    raw = sleeper_fixture("league")
    raw["settings"][field] = None
    sleeper_mock.get(f"/league/{LEAGUE}").mock(return_value=httpx.Response(200, json=raw))
    with pytest.raises(PlatformError, match=field):
        await adapter.get_roster_settings(LEAGUE)


async def test_get_league_derives_type_faab_and_superflex(adapter):
    lg = await adapter.get_league(LEAGUE)
    assert lg.platform == "sleeper" and lg.external_id == LEAGUE and lg.season == 2026
    assert lg.num_teams == 2 and lg.league_type == "redraft"
    assert lg.is_superflex is True
    assert lg.faab_budget == 100  # waiver_type == 2
    assert lg.playoff_teams == 2 and lg.playoff_start_week == 15
    assert lg.my_team_external_id == "1"


async def test_faab_budget_is_none_when_waivers_are_priority_based(
    adapter, sleeper_mock, sleeper_fixture
):
    raw = sleeper_fixture("league")
    raw["settings"]["waiver_type"] = 0
    sleeper_mock.get(f"/league/{LEAGUE}").mock(return_value=httpx.Response(200, json=raw))
    lg = await adapter.get_league(LEAGUE)
    assert lg.faab_budget is None


async def test_unknown_league_type_raises_rather_than_defaulting(
    adapter, sleeper_mock, sleeper_fixture
):
    raw = sleeper_fixture("league")
    raw["settings"]["type"] = 7
    sleeper_mock.get(f"/league/{LEAGUE}").mock(return_value=httpx.Response(200, json=raw))
    with pytest.raises(PlatformError):
        await adapter.get_league(LEAGUE)


async def test_get_teams_maps_names_faab_and_is_me(adapter):
    teams = {t.external_id: t for t in await adapter.get_teams(LEAGUE)}
    assert set(teams) == {"1", "2"}
    assert teams["1"].display_name == "Fixture Me"
    assert teams["1"].manager_name == "chris"
    assert teams["1"].is_me is True
    assert teams["1"].faab_remaining == 75  # 100 budget - 25 used
    assert teams["1"].waiver_priority == 1
    assert teams["1"].draft_slot == 1
    # No metadata.team_name -> fall back to the manager's display name.
    assert teams["2"].display_name == "opponent"
    assert teams["2"].is_me is False


async def test_get_teams_faab_remaining_raises_when_budget_used_is_missing_in_a_faab_league(
    adapter, sleeper_mock, sleeper_fixture
):
    rosters = sleeper_fixture("rosters")
    rosters[0]["settings"]["waiver_budget_used"] = None
    sleeper_mock.get(f"/league/{LEAGUE}/rosters").mock(
        return_value=httpx.Response(200, json=rosters)
    )
    with pytest.raises(PlatformError, match="waiver_budget_used"):
        await adapter.get_teams(LEAGUE)


async def test_get_teams_faab_remaining_is_none_when_the_league_is_not_faab(
    adapter, sleeper_mock, sleeper_fixture
):
    # Priority waivers (waiver_type 0): waiver_budget is meaningless and a missing
    # waiver_budget_used is not an error — faab_remaining is simply None.
    league = sleeper_fixture("league")
    league["settings"]["waiver_type"] = 0
    sleeper_mock.get(f"/league/{LEAGUE}").mock(return_value=httpx.Response(200, json=league))
    rosters = sleeper_fixture("rosters")
    rosters[0]["settings"]["waiver_budget_used"] = None
    sleeper_mock.get(f"/league/{LEAGUE}/rosters").mock(
        return_value=httpx.Response(200, json=rosters)
    )
    teams = {t.external_id: t for t in await adapter.get_teams(LEAGUE)}
    assert teams["1"].faab_remaining is None and teams["2"].faab_remaining is None
    assert teams["1"].waiver_priority == 1


async def test_get_teams_raises_when_two_draft_slots_map_to_one_roster(
    adapter, sleeper_mock, sleeper_fixture
):
    raw = sleeper_fixture("draft")
    raw["slot_to_roster_id"] = {"1": 1, "2": 1}
    sleeper_mock.get(f"/draft/{DRAFT}").mock(return_value=httpx.Response(200, json=raw))
    with pytest.raises(PlatformError):
        await adapter.get_teams(LEAGUE)


async def test_get_rosters_assigns_every_slot_kind_and_drops_the_zero_placeholder(adapter):
    rosters = {r.team_external_id: r for r in await adapter.get_rosters(LEAGUE, 1)}
    mine = {e.player_external_id: e for e in rosters["1"].players}
    assert rosters["1"].week == 1
    assert mine["1"].slot == "QB" and mine["1"].is_starter is True
    assert mine["8"].slot == "SUPER_FLEX" and mine["8"].is_starter is True
    assert mine["KC"].slot == "DST" and mine["KC"].is_starter is True
    assert mine["12"].slot == "IR" and mine["12"].is_starter is False
    assert mine["11"].slot == "TAXI" and mine["11"].is_starter is False
    assert mine["10"].slot == "BN" and mine["10"].is_starter is False
    assert len(mine) == 13

    theirs = {e.player_external_id: e for e in rosters["2"].players}
    assert "0" not in theirs  # the empty SUPER_FLEX slot is not a player
    assert len(theirs) == 10
    assert theirs["21"].slot == "BN"


async def test_starter_length_mismatch_raises(adapter, sleeper_mock, sleeper_fixture):
    rosters = sleeper_fixture("rosters")
    rosters[0]["starters"] = rosters[0]["starters"][:5]
    sleeper_mock.get(f"/league/{LEAGUE}/rosters").mock(
        return_value=httpx.Response(200, json=rosters)
    )
    with pytest.raises(PlatformError):
        await adapter.get_rosters(LEAGUE, 1)


async def test_null_starters_means_every_slot_is_empty_and_everyone_sits_on_the_bench(
    adapter, sleeper_mock, sleeper_fixture
):
    # Sleeper sends `starters: null` for a roster that never set a lineup. Not ambiguous:
    # every starting slot is empty, so all of `players` land on BN (IR/TAXI still win).
    rosters = sleeper_fixture("rosters")
    rosters[1]["starters"] = None
    rosters[1]["reserve"] = None
    rosters[1]["taxi"] = None
    sleeper_mock.get(f"/league/{LEAGUE}/rosters").mock(
        return_value=httpx.Response(200, json=rosters)
    )
    by_team = {r.team_external_id: r for r in await adapter.get_rosters(LEAGUE, 1)}
    theirs = by_team["2"].players
    assert {e.player_external_id for e in theirs} == set(rosters[1]["players"])
    assert all(e.slot == "BN" and e.is_starter is False for e in theirs)
    # Roster 1 (untouched) still maps its starters.
    assert any(e.is_starter for e in by_team["1"].players)


async def test_get_matchups_pairs_by_matchup_id(adapter):
    matchups = await adapter.get_matchups(LEAGUE, 1)
    assert len(matchups) == 1
    m = matchups[0]
    assert m.week == 1 and m.matchup_no == 1
    assert m.home_team_external_id == "1" and m.away_team_external_id == "2"
    assert m.home_points == pytest.approx(100.5) and m.away_points == pytest.approx(88.0)


async def test_get_matchups_emits_a_bye_for_a_null_matchup_id(
    adapter, sleeper_mock, sleeper_fixture
):
    raw = sleeper_fixture("matchups_week1")
    raw[1]["matchup_id"] = None
    sleeper_mock.get(f"/league/{LEAGUE}/matchups/1").mock(
        return_value=httpx.Response(200, json=raw)
    )
    matchups = await adapter.get_matchups(LEAGUE, 1)
    assert len(matchups) == 2
    assert all(m.away_team_external_id is None for m in matchups)
    by_home = {m.home_team_external_id: m for m in matchups}
    assert by_home["1"].matchup_no == 1  # kept its platform matchup_id
    assert by_home["2"].matchup_no == 2  # bye, synthesised after max(groups)
    assert by_home["2"].home_points == pytest.approx(88.0)


async def test_get_matchups_prefers_commissioner_custom_points(
    adapter, sleeper_mock, sleeper_fixture
):
    raw = sleeper_fixture("matchups_week1")
    raw[1]["custom_points"] = 95.25  # commissioner override on roster 2; roster 1 stays null
    sleeper_mock.get(f"/league/{LEAGUE}/matchups/1").mock(
        return_value=httpx.Response(200, json=raw)
    )
    (m,) = await adapter.get_matchups(LEAGUE, 1)
    assert m.home_team_external_id == "1" and m.home_points == pytest.approx(100.5)
    assert m.away_team_external_id == "2" and m.away_points == pytest.approx(95.25)


async def test_get_transactions_normalizes_type_faab_and_epoch_ms(adapter):
    txns = {t.external_id: t for t in await adapter.get_transactions(LEAGUE, 1)}
    assert txns["TXN1"].type == "waiver"
    assert txns["TXN1"].faab_spent == 25
    assert txns["TXN1"].week == 1
    assert txns["TXN1"].adds == {"90": "1"} and txns["TXN1"].drops == {"10": "1"}
    assert txns["TXN1"].executed_at == datetime.fromtimestamp(1758698028886 / 1000, tz=UTC)
    # free_agent with only drops normalizes to "drop"
    assert txns["TXN2"].type == "drop" and txns["TXN2"].faab_spent is None


async def test_failed_waiver_claim_spent_no_faab(adapter, sleeper_mock, sleeper_fixture):
    raw = sleeper_fixture("transactions_week1")
    raw[0]["status"] = "failed"  # TXN1 keeps settings.waiver_bid == 25
    sleeper_mock.get(f"/league/{LEAGUE}/transactions/1").mock(
        return_value=httpx.Response(200, json=raw)
    )
    txns = {t.external_id: t for t in await adapter.get_transactions(LEAGUE, 1)}
    assert txns["TXN1"].type == "waiver" and txns["TXN1"].status == "failed"
    assert txns["TXN1"].faab_spent is None


async def test_commissioner_transaction_maps_to_its_own_type(
    adapter, sleeper_mock, sleeper_fixture
):
    raw = sleeper_fixture("transactions_week1")
    raw.append(
        {
            "transaction_id": "TXN3",
            "type": "commissioner",
            "status": "complete",
            "leg": 1,
            "created": 1758684254016,
            "status_updated": 1758684254016,
            "creator": "USER_ME",
            "adds": {"91": 2},
            "drops": {"20": 2},
            "roster_ids": [2],
            "consenter_ids": [2],
            "settings": None,
            "metadata": None,
        }
    )
    sleeper_mock.get(f"/league/{LEAGUE}/transactions/1").mock(
        return_value=httpx.Response(200, json=raw)
    )
    txns = {t.external_id: t for t in await adapter.get_transactions(LEAGUE, 1)}
    assert txns["TXN3"].type == "commissioner"
    assert txns["TXN3"].adds == {"91": "2"} and txns["TXN3"].drops == {"20": "2"}
    assert txns["TXN3"].faab_spent is None


async def test_get_free_agents_is_the_catalog_minus_everyone_rostered(sleeper_client):
    catalog = StubCatalog(
        {
            "1": PlayerRef(external_id="1", name="Fixture Quarterback", position="QB", team="KC"),
            "90": PlayerRef(
                external_id="90", name="Fixture Freeagentwr", position="WR", team="DET"
            ),
            "91": PlayerRef(
                external_id="91", name="Fixture Freeagentqb", position="QB", team="DET"
            ),
            "KC": PlayerRef(external_id="KC", name="KC", position="DST", team="KC"),
            "92": PlayerRef(external_id="92", name="No Position", position=None, team=None),
        }
    )
    adapter = SleeperAdapter(sleeper_client, my_user_id="USER_ME", catalog=catalog)
    free = await adapter.get_free_agents(LEAGUE)
    assert [p.external_id for p in free] == ["90", "91", "92"]
    assert free[2].position is None


async def test_get_free_agents_without_a_catalog_raises_rather_than_returning_empty(adapter):
    with pytest.raises(PlatformError):
        await adapter.get_free_agents(LEAGUE)


async def test_get_draft_maps_slot_and_epoch_ms(adapter):
    d = await adapter.get_draft(DRAFT)
    assert d.external_id == DRAFT and d.league_external_id == LEAGUE
    assert d.draft_type == "snake" and d.rounds == 13 and d.status == "complete"
    assert d.my_slot == 1
    assert d.last_picked_ms == 1756083970192
    assert d.started_at == datetime.fromtimestamp(1756074607722 / 1000, tz=UTC)


async def test_get_draft_raises_when_the_wire_has_no_league_id(
    adapter, sleeper_mock, sleeper_fixture
):
    raw = sleeper_fixture("draft")
    raw["league_id"] = None
    sleeper_mock.get(f"/draft/{DRAFT}").mock(return_value=httpx.Response(200, json=raw))
    with pytest.raises(PlatformError, match="no league_id"):
        await adapter.get_draft(DRAFT)


async def test_get_league_drafts_maps_every_row_without_slot_to_roster_id(adapter, sleeper_fixture):
    # /league/{id}/drafts rows carry no slot_to_roster_id; the mapping must not need it.
    raws = sleeper_fixture("league_drafts")
    assert all("slot_to_roster_id" not in r for r in raws)
    drafts = await adapter.get_league_drafts(LEAGUE)
    assert len(drafts) == len(raws) == 1
    (d,) = drafts
    assert d.external_id == DRAFT and d.league_external_id == LEAGUE
    assert d.draft_type == "snake" and d.rounds == 13 and d.status == "complete"
    assert d.my_slot == 1 and d.last_picked_ms == 1756083970192


async def test_get_draft_picks_maps_roster_keeper_and_auction_amount(adapter):
    picks = {p.pick_no: p for p in await adapter.get_draft_picks(DRAFT)}
    assert picks[1].team_external_id == "1" and picks[1].player_external_id == "1"
    assert picks[1].is_keeper is False
    assert picks[1].auction_amount is None  # metadata amount "0" -> not an auction bid
    assert picks[3].is_keeper is True
    assert picks[4].auction_amount == 12
    assert all(p.picked_at is None for p in picks.values())  # Sleeper has no pick timestamps


@pytest.mark.parametrize(
    ("cursor", "expected_changed"),
    [
        (None, True),
        ("1756083970192:complete", False),
        ("1756083970191:complete", True),
        ("1756083970193:complete", True),
        # Same pick timestamp, different status: a status-only transition is a change.
        ("1756083970192:drafting", True),
        ("1756083970192:paused", True),
        # Legacy bare-numeric cursors compare on the epoch-ms part only.
        ("1756083970192", False),
        ("1756083970191", True),
        ("garbage", True),
    ],
)
async def test_draft_changed_since_compares_epoch_ms_and_status(adapter, cursor, expected_changed):
    changed, new_cursor = await adapter.draft_changed_since(DRAFT, cursor)
    assert changed is expected_changed
    assert new_cursor == "1756083970192:complete"


async def test_draft_changed_since_sees_status_only_transitions(
    adapter, sleeper_mock, sleeper_fixture
):
    raw = sleeper_fixture("draft")
    raw["last_picked"] = None
    raw["status"] = "pre_draft"
    route = sleeper_mock.get(f"/draft/{DRAFT}").mock(return_value=httpx.Response(200, json=raw))
    changed, cursor = await adapter.draft_changed_since(DRAFT, None)
    assert changed is True and cursor == "0:pre_draft"
    changed, cursor = await adapter.draft_changed_since(DRAFT, cursor)
    assert changed is False and cursor == "0:pre_draft"
    # The draft opens: no pick yet (last_picked still null) but status flipped.
    raw["status"] = "drafting"
    route.mock(return_value=httpx.Response(200, json=raw))
    changed, cursor = await adapter.draft_changed_since(DRAFT, cursor)
    assert changed is True and cursor == "0:drafting"
    # First pick lands.
    raw["last_picked"] = 1756083970100
    route.mock(return_value=httpx.Response(200, json=raw))
    changed, cursor = await adapter.draft_changed_since(DRAFT, cursor)
    assert changed is True and cursor == "1756083970100:drafting"
    # Commissioner pauses: same last_picked, new status.
    raw["status"] = "paused"
    route.mock(return_value=httpx.Response(200, json=raw))
    changed, cursor = await adapter.draft_changed_since(DRAFT, cursor)
    assert changed is True and cursor == "1756083970100:paused"
    changed, cursor = await adapter.draft_changed_since(DRAFT, cursor)
    assert changed is False


def test_player_ref_maps_a_defense_to_dst_named_by_team_abbreviation():
    raw = RawPlayer(
        player_id="KC",
        position="DEF",
        team="KC",
        first_name="Kansas City",
        last_name="Chiefs",
        fantasy_positions=["DEF"],
    )
    ref = player_ref(raw)
    assert ref.external_id == "KC"
    assert ref.position == "DST"
    assert ref.name == "KC"  # the one form the crosswalk's normalize_dst canonicalizes
    assert ref.team == "KC"
    assert ref.gsis_id is None  # DEF entries carry no gsis_id


def test_player_ref_strips_sleepers_stray_leading_space_from_gsis_id():
    raw = RawPlayer(
        player_id="4046",
        full_name="Patrick Mahomes",
        position="QB",
        team="KC",
        gsis_id=" 00-0033873",
    )
    assert player_ref(raw).gsis_id == "00-0033873"


@pytest.mark.parametrize("gsis", [None, "", "   "])
def test_player_ref_never_emits_an_empty_gsis_id(gsis):
    raw = RawPlayer(player_id="1", full_name="X Y", position="QB", team=None, gsis_id=gsis)
    assert player_ref(raw).gsis_id is None


def test_player_ref_builds_a_human_name_from_first_and_last_when_full_name_is_missing():
    raw = RawPlayer(
        player_id="4046", first_name="Patrick", last_name="Mahomes", position="QB", team="KC"
    )
    ref = player_ref(raw)
    assert ref.external_id == "4046"
    assert ref.name == "Patrick Mahomes"
    assert ref.position == "QB" and ref.team == "KC"


@pytest.mark.parametrize(
    "names",
    [
        {},
        {"full_name": None, "first_name": None, "last_name": None},
        {"full_name": "", "first_name": "", "last_name": ""},
        {"full_name": None, "first_name": " ", "last_name": ""},
    ],
)
def test_player_ref_raises_when_a_non_def_entry_has_no_name_at_all(names):
    raw = RawPlayer(player_id="424242", position="WR", team=None, **names)
    with pytest.raises(PlatformError, match="sleeper player 424242: no name on the wire"):
        player_ref(raw)


def test_player_ref_passes_a_null_position_through_as_none_never_empty_string():
    raw = RawPlayer(player_id="7", full_name="Retired Guy", position=None, team=None)
    ref = player_ref(raw)
    assert ref.position is None and ref.name == "Retired Guy"


def test_player_ref_prefers_full_name_when_present():
    raw = RawPlayer(
        player_id="1",
        full_name="Fixture Quarterback",
        first_name="X",
        last_name="Y",
        position="QB",
        team=None,
    )
    ref = player_ref(raw)
    assert ref.name == "Fixture Quarterback" and ref.team is None


async def test_lake_player_catalog_reads_the_newest_partition(tmp_path):
    old = tmp_path / "raw" / "sleeper" / "players" / "scrape_date=2026-08-01"
    new = tmp_path / "raw" / "sleeper" / "players" / "scrape_date=2026-08-15"
    for d, name in ((old, "Stale Player"), (new, "Fresh Player")):
        d.mkdir(parents=True)
        pl.DataFrame(
            {"player_id": ["1"], "name": [name], "position": ["QB"], "team": ["KC"]}
        ).write_parquet(d / "players.parquet")
    refs = await LakePlayerCatalog(tmp_path).all_players()
    assert refs["1"].name == "Fresh Player"
    # A partition without a gsis_id column (older lake) still loads, with gsis_id=None.
    assert refs["1"].gsis_id is None


async def test_lake_player_catalog_round_trips_gsis_id(tmp_path):
    part = tmp_path / "raw" / "sleeper" / "players" / "scrape_date=2026-08-15"
    part.mkdir(parents=True)
    pl.DataFrame(
        {
            "player_id": ["1", "2", "KC"],
            "name": ["A", "B", "KC"],
            "position": ["QB", "RB", "DST"],
            "team": ["KC", None, "KC"],
            "gsis_id": ["00-0033873", " 00-0099999", None],
        }
    ).write_parquet(part / "players.parquet")
    refs = await LakePlayerCatalog(tmp_path).all_players()
    assert refs["1"].gsis_id == "00-0033873"
    assert refs["2"].gsis_id == "00-0099999"  # same strip rule as player_ref
    assert refs["KC"].gsis_id is None


async def test_lake_player_catalog_round_trips_a_null_position(tmp_path):
    part = tmp_path / "raw" / "sleeper" / "players" / "scrape_date=2026-08-15"
    part.mkdir(parents=True)
    pl.DataFrame(
        {
            "player_id": ["1", "2"],
            "name": ["Has Position", "No Position"],
            "position": ["QB", None],
            "team": ["KC", None],
        }
    ).write_parquet(part / "players.parquet")
    refs = await LakePlayerCatalog(tmp_path).all_players()
    assert refs["1"].position == "QB"
    assert refs["2"].position is None and refs["2"].name == "No Position"


async def test_lake_player_catalog_translates_a_null_name_row_into_platform_error(tmp_path):
    part = tmp_path / "raw" / "sleeper" / "players" / "scrape_date=2026-08-15"
    part.mkdir(parents=True)
    pl.DataFrame(
        {
            "player_id": ["1", "2"],
            "name": ["Ok", None],
            "position": ["QB", "RB"],
            "team": ["KC", None],
        }
    ).write_parquet(part / "players.parquet")
    with pytest.raises(PlatformError, match="scrape_date=2026-08-15"):
        await LakePlayerCatalog(tmp_path).all_players()


async def test_lake_player_catalog_raises_when_the_lake_is_empty(tmp_path):
    with pytest.raises(PlatformError):
        await LakePlayerCatalog(tmp_path).all_players()


async def test_adapter_satisfies_the_protocol(adapter):
    assert isinstance(adapter, FantasyPlatformAdapter)


async def test_current_week_is_zero_outside_the_regular_season(
    adapter, sleeper_mock, sleeper_fixture
):
    assert await adapter.current_week() == 1
    pre = sleeper_fixture("state_nfl") | {"season_type": "pre", "week": 2}
    sleeper_mock.get("/state/nfl").mock(return_value=httpx.Response(200, json=pre))
    assert await adapter.current_week() == 0


async def test_get_player_refs_treats_a_non_numeric_id_as_a_defense(adapter):
    refs = await adapter.get_player_refs({"1", "KC"})
    assert refs["KC"].position == "DST" and refs["KC"].team == "KC" and refs["KC"].name == "KC"
    # `position=""` would masquerade as a real (empty) position; base.PlayerRef says None.
    assert refs["1"].position is None and refs["1"].name == "1" and refs["1"].team is None


async def test_get_player_refs_prefers_the_catalog(sleeper_client):
    catalog = StubCatalog(
        {
            "1": PlayerRef(
                external_id="1",
                name="Fixture Quarterback",
                position="QB",
                team="KC",
                gsis_id="00-0090001",
            )
        }
    )
    adapter = SleeperAdapter(sleeper_client, my_user_id="USER_ME", catalog=catalog)
    refs = await adapter.get_player_refs({"1", "KC"})
    assert refs["1"].name == "Fixture Quarterback" and refs["1"].position == "QB"
    assert refs["1"].gsis_id == "00-0090001"  # the crosswalk's strongest join key survives
    assert refs["KC"].position == "DST"


async def test_get_player_refs_covers_every_id_it_was_asked_for(adapter):
    ids = {"1", "2", "KC", "SF"}
    assert set(await adapter.get_player_refs(ids)) == ids
    assert await adapter.get_player_refs(set()) == {}
