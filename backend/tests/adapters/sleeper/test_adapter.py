from datetime import UTC, datetime

import httpx
import polars as pl
import pytest

from ffh.adapters.base import FantasyPlatformAdapter, PlatformError, PlayerRef
from ffh.adapters.sleeper.adapter import SleeperAdapter
from ffh.adapters.sleeper.catalog import LakePlayerCatalog
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


async def test_get_transactions_normalizes_type_faab_and_epoch_ms(adapter):
    txns = {t.external_id: t for t in await adapter.get_transactions(LEAGUE, 1)}
    assert txns["TXN1"].type == "waiver"
    assert txns["TXN1"].faab_spent == 25
    assert txns["TXN1"].week == 1
    assert txns["TXN1"].adds == {"90": "1"} and txns["TXN1"].drops == {"10": "1"}
    assert txns["TXN1"].executed_at == datetime.fromtimestamp(1758698028886 / 1000, tz=UTC)
    # free_agent with only drops normalizes to "drop"
    assert txns["TXN2"].type == "drop" and txns["TXN2"].faab_spent is None


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
        }
    )
    adapter = SleeperAdapter(sleeper_client, my_user_id="USER_ME", catalog=catalog)
    free = await adapter.get_free_agents(LEAGUE)
    assert [p.external_id for p in free] == ["90", "91"]


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
    [(None, True), ("1756083970192", False), ("1756083970191", True), ("1756083970193", True)],
)
async def test_draft_changed_since_compares_epoch_ms_exactly_in_both_directions(
    adapter, cursor, expected_changed
):
    changed, new_cursor = await adapter.draft_changed_since(DRAFT, cursor)
    assert changed is expected_changed
    assert new_cursor == "1756083970192"


async def test_draft_changed_since_handles_a_predraft_null_last_picked(
    adapter, sleeper_mock, sleeper_fixture
):
    raw = sleeper_fixture("draft")
    raw["last_picked"] = None
    raw["status"] = "pre_draft"
    sleeper_mock.get(f"/draft/{DRAFT}").mock(return_value=httpx.Response(200, json=raw))
    changed, cursor = await adapter.draft_changed_since(DRAFT, None)
    assert changed is True and cursor == "0"
    changed, cursor = await adapter.draft_changed_since(DRAFT, "0")
    assert changed is False and cursor == "0"


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


async def test_lake_player_catalog_raises_when_the_lake_is_empty(tmp_path):
    with pytest.raises(PlatformError):
        await LakePlayerCatalog(tmp_path).all_players()


async def test_adapter_satisfies_the_protocol(adapter):
    assert isinstance(adapter, FantasyPlatformAdapter)
