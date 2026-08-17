"""Sleeper -> normalized model mapping.

Scoring and roster settings are ALWAYS what the platform returned. Nothing here supplies
a default for them; an unrecognised setting raises rather than guessing.

Every method issues a fresh fetch (no memoisation): at 300 req/min that is cheap, and the
draft hot path is `draft_changed_since`, which is one call.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Literal

from ffh.adapters.base import (
    Draft,
    DraftPick,
    League,
    LeagueTeam,
    LeagueType,
    Matchup,
    PlatformError,
    PlayerCatalog,
    PlayerRef,
    Roster,
    RosterEntry,
    RosterSettings,
    ScoringSettings,
    Transaction,
)
from ffh.adapters.sleeper.client import SleeperClient
from ffh.adapters.sleeper.models import (
    RawDraft,
    RawDraftPick,
    RawLeague,
    RawMatchup,
    RawPlayer,
    RawRoster,
    RawTransaction,
    RawUser,
)

# Sleeper's `settings.type`. Unknown values raise — never default to redraft.
LEAGUE_TYPES: dict[int, LeagueType] = {0: "redraft", 1: "keeper", 2: "dynasty"}
DRAFT_TYPES = frozenset({"snake", "linear", "auction"})
DRAFT_STATUSES = frozenset({"pre_draft", "drafting", "paused", "complete"})
# Roster-position tokens that are not starting slots.
NON_STARTER_TOKENS = frozenset({"BN", "IR", "TAXI"})
# Sleeper's DEF is our DST (docs/DATABASE.md §4 roster_slots.slot).
SLOT_ALIASES = {"DEF": "DST"}
FLEX_COMPOSITION: dict[str, list[str]] = {
    "FLEX": ["RB", "WR", "TE"],
    "SUPER_FLEX": ["QB", "RB", "WR", "TE"],
    "REC_FLEX": ["WR", "TE"],
    "WRRB_FLEX": ["RB", "WR"],
    "IDP_FLEX": ["DL", "LB", "DB"],
}
# Sleeper writes "0" into an unfilled starter slot. It is NOT a player id.
EMPTY_SLOT = "0"
# waiver_type 2 == FAAB. 0/1 are priority-based and waiver_budget is meaningless there.
FAAB_WAIVER_TYPE = 2


def _ms_to_dt(ms: int | None) -> datetime | None:
    """Sleeper timestamps are EPOCH MILLISECONDS (AGENTS.md Tier 1)."""
    return None if ms is None else datetime.fromtimestamp(ms / 1000, tz=UTC)


def player_ref(raw: RawPlayer) -> PlayerRef:
    """Normalize one blob entry.

    Defenses have no full_name in the blob, and their Sleeper player_id IS the team
    abbreviation — the one form the crosswalk's normalize_dst is guaranteed to canonicalize.
    """
    if raw.position == "DEF":
        return PlayerRef(
            external_id=raw.player_id,
            name=raw.player_id,
            position="DST",
            team=raw.team or raw.player_id,
        )
    name = raw.full_name or f"{raw.first_name or ''} {raw.last_name or ''}".strip()
    return PlayerRef(
        external_id=raw.player_id,
        name=name,
        position=raw.position or "",
        team=raw.team,
    )


def to_scoring_settings(raw: RawLeague) -> ScoringSettings:
    if not raw.scoring_settings:
        raise PlatformError(f"league {raw.league_id} returned no scoring_settings")
    return ScoringSettings(points=dict(raw.scoring_settings))


def to_roster_settings(raw: RawLeague) -> RosterSettings:
    tokens = raw.roster_positions
    if not tokens:
        raise PlatformError(f"league {raw.league_id} returned no roster_positions")
    starters = [SLOT_ALIASES.get(t, t) for t in tokens if t not in NON_STARTER_TOKENS]
    unknown_flex = [s for s in starters if s.endswith("FLEX") and s not in FLEX_COMPOSITION]
    if unknown_flex:
        raise PlatformError(f"unknown flex slot(s) {unknown_flex} in league {raw.league_id}")
    # IR / taxi capacity is a league setting, not a roster_positions token (verified live:
    # reserve_slots=1 with no "IR" token). The settings ints are authoritative; an
    # explicit 0 there is a real 0.
    return RosterSettings(
        starters=starters,
        bench=tokens.count("BN"),
        ir=raw.settings.reserve_slots,
        taxi=raw.settings.taxi_slots,
        flex_composition={s: FLEX_COMPOSITION[s] for s in starters if s in FLEX_COMPOSITION},
    )


class SleeperAdapter:
    platform: Literal["sleeper", "espn", "yahoo"] = "sleeper"

    def __init__(
        self,
        client: SleeperClient,
        *,
        my_user_id: str | None = None,
        catalog: PlayerCatalog | None = None,
    ) -> None:
        self._client = client
        self._my_user_id = my_user_id
        self._catalog = catalog

    # --- helpers ---------------------------------------------------------------
    def _is_mine(self, roster: RawRoster) -> bool:
        if self._my_user_id is None:
            return False
        return roster.owner_id == self._my_user_id or self._my_user_id in roster.co_owners

    # --- league ----------------------------------------------------------------
    async def get_league(self, league_id: str) -> League:
        raw = await self._client.get_league(league_id)
        rosters = await self._client.get_rosters(league_id)
        settings = raw.settings
        if settings.type not in LEAGUE_TYPES:
            raise PlatformError(
                f"league {league_id} has unrecognised settings.type={settings.type!r}"
            )
        if raw.total_rosters is not None and raw.total_rosters != settings.num_teams:
            raise PlatformError(
                f"league {league_id}: total_rosters={raw.total_rosters} but "
                f"settings.num_teams={settings.num_teams}"
            )
        roster_settings = to_roster_settings(raw)
        mine = [r for r in rosters if self._is_mine(r)]
        if len(mine) > 1:
            raise PlatformError(f"league {league_id}: {len(mine)} rosters match my_user_id")
        return League(
            external_id=raw.league_id,
            platform="sleeper",
            season=int(raw.season),
            name=raw.name,
            num_teams=settings.num_teams,
            scoring=to_scoring_settings(raw),
            roster=roster_settings,
            league_type=LEAGUE_TYPES[settings.type],
            # Single source of truth: derived from the starter tokens, never stored twice.
            is_superflex=roster_settings.is_superflex,
            playoff_teams=settings.playoff_teams,
            playoff_start_week=settings.playoff_week_start,
            faab_budget=(
                settings.waiver_budget if settings.waiver_type == FAAB_WAIVER_TYPE else None
            ),
            my_team_external_id=str(mine[0].roster_id) if mine else None,
        )

    async def get_scoring_settings(self, league_id: str) -> ScoringSettings:
        return to_scoring_settings(await self._client.get_league(league_id))

    async def get_roster_settings(self, league_id: str) -> RosterSettings:
        return to_roster_settings(await self._client.get_league(league_id))

    async def get_teams(self, league_id: str) -> list[LeagueTeam]:
        raw = await self._client.get_league(league_id)
        rosters = await self._client.get_rosters(league_id)
        users = {u.user_id: u for u in await self._client.get_users(league_id)}
        is_faab = raw.settings.waiver_type == FAAB_WAIVER_TYPE
        budget = raw.settings.waiver_budget if is_faab else None
        slots = await self._draft_slots(raw)
        teams = [self._team(r, users.get(r.owner_id or ""), budget, slots) for r in rosters]
        if len(teams) != len(rosters):
            raise PlatformError(f"league {league_id}: dropped a roster while mapping teams")
        return teams

    def _team(
        self,
        roster: RawRoster,
        user: RawUser | None,
        faab_budget: int | None,
        slots: dict[int, int],
    ) -> LeagueTeam:
        # metadata is opaque dict[str, Any]; coerce at point of use.
        raw_team_name = user.metadata.get("team_name") if user else None
        team_name = str(raw_team_name) if raw_team_name else None
        manager = user.display_name if user else None
        return LeagueTeam(
            external_id=str(roster.roster_id),
            display_name=team_name or manager,
            manager_name=manager,
            draft_slot=slots.get(roster.roster_id),
            faab_remaining=(
                None if faab_budget is None else faab_budget - roster.settings.waiver_budget_used
            ),
            waiver_priority=roster.settings.waiver_position,
            is_me=self._is_mine(roster),
        )

    async def _draft_slots(self, raw: RawLeague) -> dict[int, int]:
        """roster_id -> draft slot. GET /draft/{id}.slot_to_roster_id maps slot STRING -> roster_id."""
        if not raw.draft_id:
            return {}
        draft = await self._client.get_draft(raw.draft_id)
        return {roster_id: int(slot) for slot, roster_id in draft.slot_to_roster_id.items()}

    # --- rosters ---------------------------------------------------------------
    async def get_rosters(self, league_id: str, week: int) -> list[Roster]:
        raw = await self._client.get_league(league_id)
        starter_slots = to_roster_settings(raw).starters
        rosters = await self._client.get_rosters(league_id)
        return [self._roster(r, starter_slots, week, league_id) for r in rosters]

    def _roster(
        self, raw: RawRoster, starter_slots: list[str], week: int, league_id: str
    ) -> Roster:
        if len(raw.starters) != len(starter_slots):
            raise PlatformError(
                f"league {league_id} roster {raw.roster_id}: {len(raw.starters)} starters "
                f"but {len(starter_slots)} starting slots — refusing to guess the alignment"
            )
        entries: list[RosterEntry] = []
        seen: set[str] = set()
        for slot, pid in zip(starter_slots, raw.starters, strict=True):
            if pid == EMPTY_SLOT or not pid:
                continue
            entries.append(RosterEntry(player_external_id=pid, slot=slot, is_starter=True))
            seen.add(pid)
        for pid, slot in [(p, "IR") for p in raw.reserve] + [(p, "TAXI") for p in raw.taxi]:
            if pid == EMPTY_SLOT or pid in seen:
                continue
            entries.append(RosterEntry(player_external_id=pid, slot=slot, is_starter=False))
            seen.add(pid)
        for pid in raw.players:
            if pid == EMPTY_SLOT or pid in seen:
                continue
            entries.append(RosterEntry(player_external_id=pid, slot="BN", is_starter=False))
            seen.add(pid)
        expected = {
            p
            for p in (*raw.players, *raw.starters, *raw.reserve, *raw.taxi)
            if p and p != EMPTY_SLOT
        }
        if seen != expected or len(entries) != len(expected):
            raise PlatformError(
                f"league {league_id} roster {raw.roster_id}: slot assignment lost or "
                f"duplicated players ({len(entries)} entries vs {len(expected)} ids)"
            )
        return Roster(team_external_id=str(raw.roster_id), week=week, players=entries)

    # --- matchups / transactions -----------------------------------------------
    async def get_matchups(self, league_id: str, week: int) -> list[Matchup]:
        raws = await self._client.get_matchups(league_id, week)
        groups: dict[int, list[RawMatchup]] = {}
        byes: list[RawMatchup] = []
        for m in raws:
            if m.matchup_id is None:
                byes.append(m)
            else:
                groups.setdefault(m.matchup_id, []).append(m)
        out: list[Matchup] = []
        for matchup_id in sorted(groups):
            group = sorted(groups[matchup_id], key=lambda m: m.roster_id)
            if len(group) > 2:
                raise PlatformError(
                    f"league {league_id} week {week}: matchup {matchup_id} has {len(group)} teams"
                )
            home = group[0]
            away = group[1] if len(group) == 2 else None
            out.append(
                Matchup(
                    week=week,
                    matchup_no=matchup_id,
                    home_team_external_id=str(home.roster_id),
                    away_team_external_id=None if away is None else str(away.roster_id),
                    home_points=home.points,
                    away_points=None if away is None else away.points,
                )
            )
        # Byes (null matchup_id) get synthetic matchup numbers after the platform's own.
        next_no = (max(groups) if groups else 0) + 1
        for m in sorted(byes, key=lambda m: m.roster_id):
            out.append(
                Matchup(
                    week=week,
                    matchup_no=next_no,
                    home_team_external_id=str(m.roster_id),
                    away_team_external_id=None,
                    home_points=m.points,
                    away_points=None,
                )
            )
            next_no += 1
        covered = sum(2 if m.away_team_external_id else 1 for m in out)
        if covered != len(raws):
            raise PlatformError(
                f"league {league_id} week {week}: {len(raws)} roster rows mapped to {covered}"
            )
        return out

    async def get_transactions(self, league_id: str, week: int) -> list[Transaction]:
        raws = await self._client.get_transactions(league_id, week)
        return [self._transaction(t, week) for t in raws]

    def _transaction(self, raw: RawTransaction, week: int) -> Transaction:
        if raw.type in ("waiver", "trade"):
            kind = raw.type
        elif raw.type == "free_agent":
            kind = "add" if raw.adds else "drop"
        else:
            raise PlatformError(f"unrecognised Sleeper transaction type {raw.type!r}")
        return Transaction(
            external_id=raw.transaction_id,
            type=kind,  # type: ignore[arg-type]
            week=raw.leg if raw.leg is not None else week,
            executed_at=_ms_to_dt(raw.status_updated or raw.created),
            faab_spent=raw.settings.waiver_bid if raw.settings else None,
            status=raw.status or "unknown",
            adds={pid: str(rid) for pid, rid in raw.adds.items()},
            drops={pid: str(rid) for pid, rid in raw.drops.items()},
        )

    # --- free agents -----------------------------------------------------------
    async def get_free_agents(self, league_id: str) -> list[PlayerRef]:
        if self._catalog is None:
            raise PlatformError(
                "SleeperAdapter has no PlayerCatalog; pass LakePlayerCatalog(lake_root) "
                "after running `ffh ingest run sleeper_players`"
            )
        catalog = await self._catalog.all_players()
        rosters = await self._client.get_rosters(league_id)
        rostered = {
            pid
            for r in rosters
            for pid in (*r.players, *r.starters, *r.reserve, *r.taxi)
            if pid and pid != EMPTY_SLOT
        }
        free = [ref for pid, ref in catalog.items() if pid not in rostered]
        if len(free) != len(catalog) - len(rostered & catalog.keys()):
            raise PlatformError("free-agent filter lost rows")
        return sorted(free, key=lambda p: p.external_id)

    # --- draft -----------------------------------------------------------------
    async def get_draft(self, draft_id: str) -> Draft:
        return self._draft(await self._client.get_draft(draft_id))

    def _draft(self, raw: RawDraft) -> Draft:
        if raw.type not in DRAFT_TYPES:
            raise PlatformError(f"draft {raw.draft_id}: unrecognised type {raw.type!r}")
        if raw.status not in DRAFT_STATUSES:
            raise PlatformError(f"draft {raw.draft_id}: unrecognised status {raw.status!r}")
        my_slot = raw.draft_order.get(self._my_user_id) if self._my_user_id else None
        return Draft(
            external_id=raw.draft_id,
            league_external_id=raw.league_id or "",
            draft_type=raw.type,  # type: ignore[arg-type]
            rounds=raw.settings.rounds,
            status=raw.status,  # type: ignore[arg-type]
            my_slot=my_slot,
            started_at=_ms_to_dt(raw.start_time),
            last_picked_ms=raw.last_picked,
        )

    async def get_draft_picks(self, draft_id: str) -> list[DraftPick]:
        raws = await self._client.get_draft_picks(draft_id)
        picks = [self._pick(p) for p in raws]
        if len(picks) != len(raws):
            raise PlatformError(f"draft {draft_id}: dropped picks while mapping")
        return picks

    def _pick(self, raw: RawDraftPick) -> DraftPick:
        # metadata is opaque dict[str, Any]; `amount` is a STRING on the wire, coerce here.
        amount_raw = raw.metadata.get("amount")
        amount = None
        if amount_raw not in (None, ""):
            try:
                parsed = int(str(amount_raw))
            except ValueError as exc:
                raise PlatformError(
                    f"draft pick {raw.pick_no}: non-integer auction amount {amount_raw!r}"
                ) from exc
            # "0" means "not an auction"; a real auction bid is >= 1.
            amount = parsed if parsed > 0 else None
        return DraftPick(
            pick_no=raw.pick_no,
            round=raw.round,
            draft_slot=raw.draft_slot,
            team_external_id=None if raw.roster_id is None else str(raw.roster_id),
            player_external_id=raw.player_id or None,
            is_keeper=bool(raw.is_keeper),
            auction_amount=amount,
            # Sleeper publishes no per-pick timestamp (verified 2026-08-16).
            picked_at=None,
        )

    async def draft_changed_since(self, draft_id: str, cursor: str | None) -> tuple[bool, str]:
        raw = await self._client.get_draft(draft_id)
        # Pre-draft last_picked is null; "0" is the stable cursor for "nothing picked yet".
        new_ms = raw.last_picked or 0
        new_cursor = str(new_ms)
        if cursor is None:
            return True, new_cursor
        try:
            previous = int(cursor)
        except ValueError:
            return True, new_cursor
        return previous != new_ms, new_cursor
