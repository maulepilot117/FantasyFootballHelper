"""Raw Sleeper wire models.

Shapes verified live 2026-08-16 against https://api.sleeper.app/v1 — see the verification
record in docs/superpowers/plans/2026-08-15-phase0-05-adapter-sleeper.md.

Sleeper's API is READ-ONLY and its data is licensed for NON-COMMERCIAL use only.
extra="ignore" everywhere: Sleeper adds keys without notice, and a 132-key
`scoring_settings` must never be enumerated field-by-field.

Sleeper sends JSON `null` (not an absent key) for `players`, `starters`, `reserve`,
`taxi`, `co_owners`, `keepers`, `adds`, `drops`, `draft_order`, `slot_to_roster_id`,
`metadata` and roster `settings`. `default_factory` only fires when a key is ABSENT, so
nullable collections use a `BeforeValidator`. Downstream code must never branch on
`None` vs `[]` for a collection.
"""

from __future__ import annotations

from typing import Annotated, Any

from pydantic import BaseModel, BeforeValidator, ConfigDict, Field


def _none_to(default: Any) -> BeforeValidator:
    def _v(value: Any) -> Any:
        return default() if value is None else value

    return BeforeValidator(_v)


StrList = Annotated[list[str], _none_to(list)]
IntList = Annotated[list[int], _none_to(list)]
FloatList = Annotated[list[float], _none_to(list)]
# Sleeper `metadata` objects are OPAQUE payloads: values may be str, int, float or bool and
# Pydantic lax mode does not coerce non-str -> str. Consumers coerce at point of use
# (e.g. `str(metadata["amount"])`).
Metadata = Annotated[dict[str, Any], _none_to(dict)]
StrIntDict = Annotated[dict[str, int], _none_to(dict)]
StrFloatDict = Annotated[dict[str, float], _none_to(dict)]


class _Raw(BaseModel):
    model_config = ConfigDict(extra="ignore")


class RawState(_Raw):
    # `week` is 2 while `season_type` is "pre" — meaningless outside the regular season.
    week: int
    leg: int | None = None
    season: str
    season_type: str
    display_week: int | None = None
    league_season: str | None = None
    previous_season: str | None = None
    season_start_date: str | None = None
    season_has_scores: bool | None = None


class RawUser(_Raw):
    user_id: str
    username: str | None = None
    display_name: str | None = None
    avatar: str | None = None
    is_bot: bool | None = None
    # Commissioner flag on /league/{id}/users; bool | null.
    is_owner: bool | None = None
    league_id: str | None = None
    # metadata["team_name"] is OPTIONAL — absent for several verified users.
    metadata: Metadata = Field(default_factory=dict)


class RawLeagueSettings(_Raw):
    num_teams: int
    playoff_teams: int | None = None
    playoff_week_start: int | None = None
    # Present (and meaningless) even when waiver_type != 2.
    waiver_budget: int | None = None
    # 0 = rolling priority, 1 = reverse standings, 2 = FAAB
    waiver_type: int | None = None
    # 0 = redraft, 1 = keeper, 2 = dynasty
    type: int | None = None
    # Verified present on every live league; None means Sleeper omitted or nulled the key,
    # and the adapter refuses to invent a 0 for it.
    taxi_slots: int | None = None
    reserve_slots: int | None = None
    draft_rounds: int | None = None
    max_keepers: int | None = None
    start_week: int | None = None
    leg: int | None = None
    trade_deadline: int | None = None


class RawLeague(_Raw):
    league_id: str
    name: str | None = None
    season: str
    season_type: str | None = None
    status: str | None = None
    total_rosters: int | None = None
    draft_id: str | None = None
    previous_league_id: str | None = None
    avatar: str | None = None
    # Ordered; e.g. ["QB","RB","RB","WR","WR","TE","FLEX","K","DEF","BN",...].
    roster_positions: StrList = Field(default_factory=list)
    # Stored VERBATIM in leagues.scoring_settings. Never mutate, never fill in.
    scoring_settings: StrFloatDict = Field(default_factory=dict)
    # Required: a league with no settings is a hard error, never a default.
    settings: RawLeagueSettings
    metadata: Metadata = Field(default_factory=dict)


class RawRosterSettings(_Raw):
    wins: int | None = None
    losses: int | None = None
    ties: int | None = None
    fpts: int | None = None
    fpts_decimal: int | None = None
    # None means Sleeper omitted or nulled the key (e.g. `settings: null` on a brand-new
    # roster). Never a made-up 0: the adapter decides what "unknown" means per league.
    waiver_budget_used: int | None = None
    waiver_position: int | None = None
    total_moves: int | None = None


class RawRoster(_Raw):
    roster_id: int
    league_id: str | None = None
    owner_id: str | None = None
    co_owners: StrList = Field(default_factory=list)
    # Ordered to match roster_positions minus non-starting tokens.
    # "0" is an EMPTY SLOT PLACEHOLDER, not a player id.
    starters: StrList = Field(default_factory=list)
    players: StrList = Field(default_factory=list)
    reserve: StrList = Field(default_factory=list)
    taxi: StrList = Field(default_factory=list)
    keepers: StrList = Field(default_factory=list)
    # Sleeper may send an explicit null here (e.g. brand-new rosters); tolerate it.
    settings: Annotated[RawRosterSettings, _none_to(RawRosterSettings)] = Field(
        default_factory=RawRosterSettings
    )
    metadata: Metadata = Field(default_factory=dict)


class RawDraftSettings(_Raw):
    rounds: int
    teams: int | None = None
    budget: int | None = None
    pick_timer: int | None = None
    slots_qb: int = 0
    slots_rb: int = 0
    slots_wr: int = 0
    slots_te: int = 0
    slots_flex: int = 0
    slots_super_flex: int = 0
    slots_k: int = 0
    slots_def: int = 0
    slots_bn: int = 0


class RawDraft(_Raw):
    draft_id: str
    league_id: str | None = None
    # Verified: "snake" and "auction".
    type: str
    status: str
    season: str | None = None
    season_type: str | None = None
    # Required: a draft with no settings is a hard error, never a default.
    settings: RawDraftSettings
    metadata: Metadata = Field(default_factory=dict)
    # user_id -> draft slot. Null before the order is set.
    draft_order: StrIntDict = Field(default_factory=dict)
    # draft slot (STRING key) -> roster_id. Only present on GET /draft/{id}.
    slot_to_roster_id: StrIntDict = Field(default_factory=dict)
    # EPOCH MILLISECONDS. Null before the first pick.
    last_picked: int | None = None
    # EPOCH MILLISECONDS. Null before the draft opens.
    start_time: int | None = None
    created: int | None = None
    creators: StrList = Field(default_factory=list)


class RawDraftPick(_Raw):
    draft_id: str | None = None
    pick_no: int
    round: int
    draft_slot: int
    roster_id: int | None = None
    # user_id; may be "".
    picked_by: str | None = None
    player_id: str | None = None
    # null | true on the wire.
    is_keeper: bool | None = None
    # metadata["amount"] is the auction bid AS A STRING. There is no per-pick timestamp.
    metadata: Metadata = Field(default_factory=dict)


class RawMatchup(_Raw):
    # One row PER ROSTER, not per pairing; pair by matchup_id.
    roster_id: int
    matchup_id: int | None = None
    points: float | None = None
    custom_points: float | None = None
    starters: StrList = Field(default_factory=list)
    players: StrList = Field(default_factory=list)
    starters_points: FloatList = Field(default_factory=list)
    players_points: StrFloatDict = Field(default_factory=dict)


class RawTransactionSettings(_Raw):
    seq: int | None = None
    waiver_bid: int | None = None


class RawTransaction(_Raw):
    transaction_id: str
    # "free_agent" | "waiver" | "trade" | "commissioner"
    type: str
    status: str | None = None
    # leg == week
    leg: int | None = None
    # EPOCH MILLISECONDS.
    created: int | None = None
    status_updated: int | None = None
    creator: str | None = None
    # player_id -> roster_id
    adds: StrIntDict = Field(default_factory=dict)
    drops: StrIntDict = Field(default_factory=dict)
    roster_ids: IntList = Field(default_factory=list)
    consenter_ids: IntList = Field(default_factory=list)
    settings: RawTransactionSettings | None = None
    metadata: Metadata = Field(default_factory=dict)


class RawPlayer(_Raw):
    player_id: str
    # DEF entries (keyed by team abbreviation) have NO full_name and NO gsis_id.
    full_name: str | None = None
    first_name: str | None = None
    last_name: str | None = None
    search_full_name: str | None = None
    position: str | None = None
    fantasy_positions: StrList = Field(default_factory=list)
    team: str | None = None
    status: str | None = None
    active: bool | None = None
    injury_status: str | None = None
    # Null for 8,326 of 12,219 verified entries.
    gsis_id: str | None = None
    espn_id: int | None = None
    yahoo_id: int | None = None
    rotowire_id: int | None = None
    fantasy_data_id: int | None = None
    sportradar_id: str | None = None
    birth_date: str | None = None
    college: str | None = None
    years_exp: int | None = None
    number: int | None = None
    depth_chart_order: int | None = None
    depth_chart_position: str | None = None
    search_rank: int | None = None
