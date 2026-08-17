from typing import get_args

import pytest
from pydantic import ValidationError

from ffh.adapters.base import (
    Draft,
    DraftPick,
    FantasyPlatformAdapter,
    League,
    LeagueTeam,
    PlatformAuthError,
    PlatformError,
    PlatformNotFound,
    PlayerRef,
    RosterSettings,
    ScoringSettings,
    TransactionType,
)


def _roster() -> RosterSettings:
    return RosterSettings(
        starters=["QB", "RB", "RB", "WR", "WR", "TE", "FLEX", "SUPER_FLEX", "K", "DST"],
        bench=3,
        ir=1,
        taxi=1,
        flex_composition={
            "FLEX": ["RB", "WR", "TE"],
            "SUPER_FLEX": ["QB", "RB", "WR", "TE"],
        },
    )


@pytest.mark.parametrize(
    ("rec", "expected"),
    [(1.0, "ppr"), (0.5, "half_ppr"), (0.0, "standard"), (1.5, "custom")],
)
def test_scoring_format_is_derived_from_rec(rec, expected):
    assert ScoringSettings(points={"rec": rec, "rush_yd": 0.1}).format == expected


def test_scoring_format_is_custom_when_rec_absent():
    # No `rec` key means we do not know what a reception is worth: never assume standard.
    assert ScoringSettings(points={"rush_yd": 0.1}).format == "custom"


@pytest.mark.parametrize(
    ("points", "expected"),
    [
        ({"rec": 1.0, "rec_td": 6.0, "pass_yd": 0.04}, "ppr"),
        ({"rec": 0.5, "rec_td": 6.0, "bonus_rec_te": 0.0}, "half_ppr"),
        ({"rec": 0.0, "rec_td": 6.0, "pts_allow_14_20": 1.0}, "standard"),
        ({"rec": 0.25, "rec_td": 6.0}, "custom"),
    ],
)
def test_scoring_format_for_sleeper_shaped_settings(points, expected):
    # Sleeper always publishes `rec` (verified live: 132-key scoring_settings), so the
    # explicit-rec branches are unchanged by the "absent -> custom" rule.
    assert ScoringSettings(points=points).format == expected


def test_player_ref_position_is_optional_and_name_is_not():
    ref = PlayerRef(external_id="1", name="No Position", team=None)
    assert ref.position is None
    with pytest.raises(ValidationError):
        PlayerRef(external_id="1", position="QB", team=None)  # type: ignore[call-arg]


def test_transaction_type_includes_commissioner():
    assert "commissioner" in get_args(TransactionType)


def test_scoring_settings_keep_every_platform_key_verbatim():
    raw = {"rec": 0.5, "bonus_rec_te": 0.0, "pts_allow_14_20": 1.0, "fgm_yds_over_30": 0.0}
    assert ScoringSettings(points=raw).points == raw


def test_models_are_frozen():
    s = ScoringSettings(points={"rec": 1.0})
    with pytest.raises(ValidationError):
        s.points = {"rec": 0.0}


def test_roster_settings_superflex_detection():
    assert _roster().is_superflex is True
    plain = _roster().model_copy(update={"starters": ["QB", "RB", "WR", "FLEX"]})
    assert plain.is_superflex is False


def test_league_requires_settings_and_has_no_defaults_for_them():
    with pytest.raises(ValidationError):
        League(
            external_id="1",
            platform="sleeper",
            season=2026,
            name="x",
            num_teams=12,
            league_type="redraft",
            is_superflex=False,
            playoff_teams=6,
            playoff_start_week=15,
            faab_budget=100,
            my_team_external_id=None,
        )


def test_league_round_trips():
    lg = League(
        external_id="1",
        platform="sleeper",
        season=2026,
        name="x",
        num_teams=12,
        scoring=ScoringSettings(points={"rec": 0.5}),
        roster=_roster(),
        league_type="redraft",
        is_superflex=True,
        playoff_teams=6,
        playoff_start_week=15,
        faab_budget=100,
        my_team_external_id="1",
    )
    assert League.model_validate(lg.model_dump()) == lg


def test_league_team_defaults_is_me_false():
    t = LeagueTeam(external_id="1", display_name="A", manager_name="a")
    assert t.is_me is False
    assert t.draft_slot is None and t.faab_remaining is None and t.waiver_priority is None


def test_draft_and_pick_nullable_fields():
    d = Draft(
        external_id="d1",
        league_external_id="l1",
        draft_type="snake",
        rounds=13,
        status="pre_draft",
        my_slot=None,
        started_at=None,
        last_picked_ms=None,
    )
    assert d.last_picked_ms is None
    p = DraftPick(
        pick_no=1,
        round=1,
        draft_slot=1,
        team_external_id="1",
        player_external_id=None,
        is_keeper=False,
        auction_amount=None,
        picked_at=None,
    )
    assert p.player_external_id is None


def test_error_hierarchy():
    assert issubclass(PlatformAuthError, PlatformError)
    assert issubclass(PlatformNotFound, PlatformError)


_METHODS = (
    "get_league",
    "get_scoring_settings",
    "get_roster_settings",
    "get_teams",
    "get_rosters",
    "get_matchups",
    "get_transactions",
    "get_free_agents",
    "get_draft",
    "get_draft_picks",
    "draft_changed_since",
)


def _stub() -> object:
    """A structural implementation of every Protocol member, no inheritance."""

    async def _method(self, *args, **kwargs):
        raise NotImplementedError

    ns = {name: _method for name in _METHODS}
    ns["platform"] = "sleeper"
    return type("StubAdapter", (), ns)()


def test_protocol_is_runtime_checkable_and_lists_every_method():
    for name in _METHODS:
        assert hasattr(FantasyPlatformAdapter, name), name
    assert isinstance(_stub(), FantasyPlatformAdapter)
    assert not isinstance(object(), FantasyPlatformAdapter)
