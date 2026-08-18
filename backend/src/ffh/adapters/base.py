"""Platform-agnostic adapter interface.

Nothing outside `ffh.adapters` may know which platform is in use
(docs/ARCHITECTURE.md § Module boundaries).

All three platforms are READ-ONLY. Sleeper's API cannot write at all; Yahoo removed
write access in 2026. The app recommends; a human executes in the platform UI.

Scoring and roster settings are ALWAYS fetched, NEVER hardcoded. A hardcoded default
is a bug even when it happens to be right (docs/ARCHITECTURE.md).
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal, Protocol, runtime_checkable

from pydantic import BaseModel, ConfigDict

Platform = Literal["sleeper", "espn", "yahoo"]
ScoringFormat = Literal["ppr", "half_ppr", "standard", "custom"]
LeagueType = Literal["redraft", "keeper", "dynasty"]
DraftType = Literal["snake", "linear", "auction"]
DraftStatus = Literal["pre_draft", "drafting", "paused", "complete"]
TransactionType = Literal["add", "drop", "trade", "waiver", "commissioner"]
# Mirrors docs/DATABASE.md §4 roster_slots.slot.
RosterSlotName = str


class PlatformError(Exception):
    """Any failure talking to a fantasy platform."""


class PlatformAuthError(PlatformError):
    """Credentials missing, expired, or rejected (ESPN cookies, Yahoo OAuth)."""


class PlatformNotFound(PlatformError):
    """The platform returned 404 for the requested resource."""


class _Frozen(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class ScoringSettings(_Frozen):
    """Flat stat -> points, exactly as the platform published it.

    `points` is stored verbatim in `leagues.scoring_settings`; no key is added,
    removed, renamed or rounded. `format` is a DERIVED convenience for downstream
    baselines — it never replaces `points` and never becomes an input default.
    """

    points: dict[str, float]

    @property
    def format(self) -> ScoringFormat:
        # No `rec` key at all is NOT standard scoring — we do not know what the platform
        # awards per reception, so we refuse to guess.
        rec = self.points.get("rec")
        if rec is None:
            return "custom"
        if rec == 1.0:
            return "ppr"
        if rec == 0.5:
            return "half_ppr"
        if rec == 0.0:
            return "standard"
        return "custom"


class RosterSettings(_Frozen):
    """Starter slots in platform order, plus non-starting capacity."""

    starters: list[RosterSlotName]
    bench: int
    ir: int
    taxi: int
    # Only the flex tokens actually present in `starters` appear here.
    flex_composition: dict[str, list[str]]

    @property
    def is_superflex(self) -> bool:
        return "SUPER_FLEX" in self.starters


class League(_Frozen):
    external_id: str
    platform: Platform
    season: int
    name: str | None
    num_teams: int
    scoring: ScoringSettings
    roster: RosterSettings
    league_type: LeagueType
    is_superflex: bool
    playoff_teams: int | None
    playoff_start_week: int | None
    faab_budget: int | None
    my_team_external_id: str | None


class LeagueTeam(_Frozen):
    external_id: str
    display_name: str | None = None
    manager_name: str | None = None
    draft_slot: int | None = None
    faab_remaining: int | None = None
    waiver_priority: int | None = None
    is_me: bool = False


class RosterEntry(_Frozen):
    player_external_id: str
    slot: RosterSlotName
    is_starter: bool


class Roster(_Frozen):
    team_external_id: str
    week: int
    players: list[RosterEntry]


class Matchup(_Frozen):
    week: int
    matchup_no: int
    home_team_external_id: str
    away_team_external_id: str | None
    home_points: float | None
    away_points: float | None


class Transaction(_Frozen):
    external_id: str
    type: TransactionType
    week: int | None
    executed_at: datetime | None
    faab_spent: int | None
    status: str
    # player_external_id -> team_external_id
    adds: dict[str, str]
    drops: dict[str, str]


class PlayerRef(_Frozen):
    external_id: str
    name: str
    # None when the platform publishes no position (Sleeper: some retired/unsigned blob
    # entries). Never "".
    position: str | None = None
    team: str | None
    # NFL GSIS id when the platform publishes one (Sleeper: null for DEF and ~2/3 of the
    # blob). Whitespace-stripped; never "". The crosswalk's strongest join key.
    gsis_id: str | None = None


class Draft(_Frozen):
    external_id: str
    league_external_id: str
    draft_type: DraftType
    rounds: int
    status: DraftStatus
    my_slot: int | None
    started_at: datetime | None
    # Sleeper's cheap change detector. EPOCH MILLISECONDS. None before the draft opens.
    last_picked_ms: int | None


class DraftPick(_Frozen):
    pick_no: int
    round: int
    draft_slot: int
    team_external_id: str | None
    player_external_id: str | None
    is_keeper: bool
    auction_amount: int | None
    # Sleeper publishes no per-pick timestamp; always None for that platform.
    picked_at: datetime | None


@runtime_checkable
class PlayerCatalog(Protocol):
    """The platform's full player universe, sourced off the request path."""

    async def all_players(self) -> dict[str, PlayerRef]: ...


@runtime_checkable
class FantasyPlatformAdapter(Protocol):
    platform: Literal["sleeper", "espn", "yahoo"]

    async def get_league(self, league_id: str) -> League: ...
    async def get_scoring_settings(self, league_id: str) -> ScoringSettings: ...
    async def get_roster_settings(self, league_id: str) -> RosterSettings: ...
    async def get_teams(self, league_id: str) -> list[LeagueTeam]: ...
    async def get_rosters(self, league_id: str, week: int) -> list[Roster]: ...
    async def get_matchups(self, league_id: str, week: int) -> list[Matchup]: ...
    async def get_transactions(self, league_id: str, week: int) -> list[Transaction]: ...
    async def get_free_agents(self, league_id: str) -> list[PlayerRef]: ...

    # Draft
    async def get_draft(self, draft_id: str) -> Draft: ...
    async def get_draft_picks(self, draft_id: str) -> list[DraftPick]: ...
    async def draft_changed_since(self, draft_id: str, cursor: str | None) -> tuple[bool, str]:
        """Cheap change detector. Sleeper: last_picked epoch ms.
        ESPN: inProgress + count of picks with playerId != -1.
        Returns (changed, new_cursor). Called at 1-2s; must be cheap."""


# ---------------------------------------------------------------------------
# The league-sync surface — everything `ffh.ingest.platform_sync.load_league` drives
# BEYOND `FantasyPlatformAdapter`. It lives here, next to the Protocol it extends,
# because it is part of the adapter interface: `ffh.ingest` owning it meant the author
# of the next adapter (ESPN, Phase 2) discovered the requirement as a runtime
# `ValueError` from a module they never read, instead of statically from this file.
#
# The pieces are separate Protocols because `fetch_snapshot` checks them separately and
# grades them differently — `current_week` is only needed when no `week=` is passed, and
# a missing `get_league_drafts` is a logged degradation, not an error. `LeagueSyncAdapter`
# below is the aggregate to declare a new adapter against.
# ---------------------------------------------------------------------------


@runtime_checkable
class WeekAware(Protocol):
    """Can answer "what week is it?" — required only when the caller passes no `week=`."""

    async def current_week(self) -> int: ...


@runtime_checkable
class RefAware(Protocol):
    """Can describe arbitrary platform player ids for the crosswalk. Always required."""

    async def get_player_refs(self, external_ids: set[str]) -> dict[str, PlayerRef]: ...


@runtime_checkable
class DraftListing(Protocol):
    """Can list a league's drafts. OPTIONAL: `FantasyPlatformAdapter` exposes
    `get_draft(draft_id)`, not "the league's drafts", and ESPN lands in Phase 2."""

    async def get_league_drafts(self, league_id: str) -> list[Draft]: ...


@runtime_checkable
class IdentityAware(Protocol):
    """Can say whether it was configured with an identity AT ALL.

    The distinction this exists to preserve is *unknown* vs *nobody*. `LeagueTeam.is_me`
    is a bool: with no identity configured every team comes back `is_me=False`, which
    reads as the platform saying "none of these is yours" — and a sync that believes that
    NULLs `leagues.my_team_id` and clears every `league_teams.is_me`, erasing the pointer
    the draft and lineup modules depend on. An adapter that returns False here is saying
    "I could not identify anyone", and `persist_snapshot` leaves the stored pointer alone.
    """

    def identifies_me(self) -> bool: ...


@runtime_checkable
class LeagueSyncAdapter(
    FantasyPlatformAdapter, WeekAware, RefAware, DraftListing, IdentityAware, Protocol
):
    """The FULL surface `ffh.ingest.platform_sync.load_league` drives.

    Declare a new adapter against this one — not against `FantasyPlatformAdapter` alone —
    if it is meant to be loadable by `ffh league load`. `SleeperAdapter` satisfies it.
    """
