"""Raw Sleeper wire-model tests. Payloads mirror the live-verified record (2026-08-16)."""

from ffh.adapters.sleeper.models import (
    RawDraft,
    RawDraftPick,
    RawLeague,
    RawMatchup,
    RawPlayer,
    RawRoster,
    RawState,
    RawTransaction,
    RawUser,
)


def test_state_parses_preseason_payload():
    s = RawState.model_validate(
        {
            "week": 2,
            "leg": 0,
            "season": "2026",
            "season_type": "pre",
            "display_week": 2,
            "league_season": "2026",
            "previous_season": "2025",
            "season_start_date": "2026-08-06",
            "season_has_scores": True,
        }
    )
    assert s.week == 2 and s.season_type == "pre" and s.season == "2026"


def test_league_keeps_scoring_settings_verbatim_and_ignores_unknown_keys():
    raw = {
        "league_id": "1",
        "name": "L",
        "season": "2026",
        "status": "in_season",
        "total_rosters": 2,
        "draft_id": "d1",
        "roster_positions": ["QB", "SUPER_FLEX", "BN"],
        "scoring_settings": {"rec": 0.5, "fgm_yds_over_30": 0.0},
        "settings": {"num_teams": 2, "type": 0, "waiver_type": 2, "waiver_budget": 100},
        "metadata": {"auto_continue": "on"},
        "shard": 497,
        "some_new_key_sleeper_added": 1,
    }
    lg = RawLeague.model_validate(raw)
    assert lg.scoring_settings == {"rec": 0.5, "fgm_yds_over_30": 0.0}
    assert lg.settings.num_teams == 2 and lg.settings.type == 0
    assert lg.roster_positions == ["QB", "SUPER_FLEX", "BN"]
    assert "some_new_key_sleeper_added" not in lg.model_dump()
    assert "shard" not in lg.model_dump()


def test_roster_null_collections_become_empty():
    r = RawRoster.model_validate(
        {
            "roster_id": 1,
            "owner_id": None,
            "league_id": "1",
            "players": None,
            "starters": None,
            "reserve": None,
            "taxi": None,
            "co_owners": None,
            "metadata": None,
            "settings": {"wins": 0, "losses": 0, "waiver_budget_used": 0, "waiver_position": 3},
        }
    )
    assert r.players == [] and r.starters == [] and r.reserve == [] and r.taxi == []
    assert r.co_owners == [] and r.metadata == {}
    assert r.settings.waiver_position == 3


def test_roster_null_settings_become_defaults():
    r = RawRoster.model_validate({"roster_id": 1, "settings": None})
    assert r.settings.waiver_budget_used == 0
    assert r.settings.wins is None and r.settings.waiver_position is None


def test_user_team_name_is_optional():
    u = RawUser.model_validate(
        {"user_id": "u1", "display_name": "chris", "league_id": "1", "metadata": {}}
    )
    assert u.metadata == {} and u.display_name == "chris"


def test_draft_predraft_nulls():
    d = RawDraft.model_validate(
        {
            "draft_id": "d1",
            "league_id": "l1",
            "type": "snake",
            "status": "pre_draft",
            "season": "2026",
            "settings": {"rounds": 22, "teams": 12, "slots_super_flex": 1},
            "draft_order": None,
            "slot_to_roster_id": None,
            "last_picked": None,
            "start_time": None,
            "metadata": {},
        }
    )
    assert d.last_picked is None and d.start_time is None
    assert d.draft_order == {} and d.slot_to_roster_id == {}
    assert d.settings.rounds == 22 and d.settings.slots_super_flex == 1


def test_draft_pick_auction_amount_is_a_string_in_metadata():
    p = RawDraftPick.model_validate(
        {
            "draft_id": "d1",
            "pick_no": 1,
            "round": 1,
            "draft_slot": 11,
            "roster_id": 3,
            "picked_by": "u1",
            "player_id": "4866",
            "is_keeper": None,
            "reactions": None,
            "metadata": {
                "amount": "64",
                "first_name": "Saquon",
                "last_name": "Barkley",
                "position": "RB",
                "team": "PHI",
                "status": "Active",
            },
        }
    )
    assert p.is_keeper is None and p.metadata["amount"] == "64"


def test_metadata_is_opaque_and_tolerates_non_string_values():
    # Sleeper metadata values are not always strings; a single int must not fail the parse.
    p = RawDraftPick.model_validate(
        {
            "pick_no": 2,
            "round": 1,
            "draft_slot": 1,
            "metadata": {"amount": "42", "years_exp": 8, "number": 26, "slot": 1},
        }
    )
    assert p.metadata["amount"] == "42" and p.metadata["years_exp"] == 8
    u = RawUser.model_validate({"user_id": "u1", "metadata": {"mention_pn": True}})
    assert u.metadata["mention_pn"] is True
    lg = RawLeague.model_validate(
        {"league_id": "1", "season": "2026", "settings": {"num_teams": 2}, "metadata": None}
    )
    assert lg.metadata == {}


def test_matchup_per_roster_shape_with_nulls():
    m = RawMatchup.model_validate(
        {
            "roster_id": 4,
            "matchup_id": None,
            "points": 0.0,
            "custom_points": None,
            "starters": ["4866", "0"],
            "starters_points": None,
            "players": ["4866"],
            "players_points": {"4866": 21.3},
        }
    )
    assert m.matchup_id is None and m.starters_points == []
    assert m.players_points == {"4866": 21.3} and m.starters == ["4866", "0"]
    m2 = RawMatchup.model_validate(
        {
            "roster_id": 5,
            "matchup_id": 1,
            "points": 101.2,
            "starters_points": [20.1, 9.0],
            "players_points": None,
        }
    )
    assert m2.starters_points == [20.1, 9.0] and m2.players_points == {}


def test_transaction_shape():
    t = RawTransaction.model_validate(
        {
            "transaction_id": "t1",
            "type": "waiver",
            "status": "complete",
            "leg": 3,
            "created": 1758684054016,
            "status_updated": 1758698028886,
            "adds": {"8188": 8},
            "drops": {"9506": 8},
            "roster_ids": [8],
            "settings": {"seq": 3, "waiver_bid": 24},
            "metadata": {"notes": "ok"},
            "draft_picks": [],
            "waiver_budget": [],
        }
    )
    assert t.settings is not None and t.settings.waiver_bid == 24
    assert t.adds == {"8188": 8} and t.drops == {"9506": 8}


def test_transaction_null_adds_and_drops_become_empty():
    t = RawTransaction.model_validate(
        {
            "transaction_id": "t2",
            "type": "trade",
            "status": "complete",
            "leg": 4,
            "adds": None,
            "drops": None,
            "settings": None,
        }
    )
    assert t.adds == {} and t.drops == {} and t.settings is None


def test_player_def_entry_has_no_full_name_or_gsis():
    p = RawPlayer.model_validate(
        {
            "player_id": "KC",
            "position": "DEF",
            "first_name": "Kansas City",
            "last_name": "Chiefs",
            "team": "KC",
            "fantasy_positions": ["DEF"],
            "injury_status": None,
            "active": True,
            "sport": "nfl",
        }
    )
    assert p.full_name is None and p.gsis_id is None and p.position == "DEF"


def test_player_human_entry_ids_are_ints_on_the_wire():
    p = RawPlayer.model_validate(
        {
            "player_id": "4866",
            "full_name": "Saquon Barkley",
            "first_name": "Saquon",
            "last_name": "Barkley",
            "position": "RB",
            "fantasy_positions": ["RB"],
            "team": "PHI",
            "status": "Active",
            "active": True,
            "gsis_id": "00-0034844",
            "espn_id": 3929630,
            "yahoo_id": 30972,
            "rotowire_id": 12507,
            "sportradar_id": "9811b753-347c-467a-b3cb-85937e71e2b9",
            "birth_date": "1997-02-09",
            "college": "Penn State",
            "injury_status": None,
            "years_exp": 8,
            "number": 26,
            "search_rank": 13,
        }
    )
    assert p.espn_id == 3929630 and p.gsis_id == "00-0034844"
