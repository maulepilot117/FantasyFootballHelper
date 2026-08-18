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
from ffh.adapters.sleeper.gsis import normalize_gsis_id
from ffh.adapters.sleeper.models import (
    RawDraft,
    RawDraftPick,
    RawLeague,
    RawLeagueSettings,
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


def _matchup_points(m: RawMatchup) -> float | None:
    """A non-null custom_points is a commissioner override that REPLACES the computed score."""
    return m.custom_points if m.custom_points is not None else m.points


def _ms_to_dt(ms: int | None) -> datetime | None:
    """Sleeper timestamps are EPOCH MILLISECONDS (AGENTS.md Tier 1)."""
    return None if ms is None else datetime.fromtimestamp(ms / 1000, tz=UTC)


def _faab_budget(league_id: str, settings: RawLeagueSettings) -> int | None:
    """THE FAAB policy, for `get_league` and `get_teams` alike.

    None means "not a FAAB league" (`waiver_type` 0/1 are priority-based and
    `waiver_budget` is meaningless there). A FAAB league that publishes no budget raises:
    the two callers must not disagree about the same payload, and `faab_remaining` is
    computed as `budget - used`, so a guessed budget is a wrong number in every team row.
    """
    if settings.waiver_type != FAAB_WAIVER_TYPE:
        return None
    if settings.waiver_budget is None:
        raise PlatformError(f"league {league_id}: FAAB league without settings.waiver_budget")
    return settings.waiver_budget


def player_ref(raw: RawPlayer) -> PlayerRef:
    """Normalize one blob entry.

    Defenses have no `full_name` in the blob, but they DO carry `first_name` ("Kansas
    City") and `last_name` ("Chiefs"), and their Sleeper player_id is the team
    abbreviation. The name is the real one, not the abbreviation: ④'s `normalize_dst`
    canonicalizes "KC", "Kansas City", "Chiefs" and "Kansas City Chiefs" alike to
    `kc dst` (and `canonical_dst_key` is name-first), so the abbreviation buys the
    crosswalk nothing — while costing the operator, who otherwise meets an unmatched
    defense in the review queue as a bare `raw_name="KC"`. `team` stays the abbreviation.
    """
    if raw.position == "DEF":
        full = f"{raw.first_name or ''} {raw.last_name or ''}".strip()
        return PlayerRef(
            external_id=raw.player_id,
            # Fallback to the id (== the abbreviation) only when the blob carries neither
            # name part: normalize_dst canonicalizes that too, it is just less legible.
            name=full or raw.player_id,
            position="DST",
            team=raw.team or raw.player_id,
            gsis_id=None,
        )
    name = raw.full_name or f"{raw.first_name or ''} {raw.last_name or ''}".strip()
    if not name:
        raise PlatformError(f"sleeper player {raw.player_id}: no name on the wire")
    return PlayerRef(
        external_id=raw.player_id,
        name=name,
        # A null position stays null; "" would masquerade as a real (empty) position.
        position=raw.position,
        team=raw.team,
        gsis_id=normalize_gsis_id(raw.gsis_id),
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
    # explicit 0 there is a real 0. RawLeagueSettings.reserve_slots / taxi_slots are
    # `int | None` (None == Sleeper omitted or nulled the key); a missing value is a hard
    # error rather than a guessed 0 or a fallback to counting "IR"/"TAXI" tokens.
    ir = raw.settings.reserve_slots
    taxi = raw.settings.taxi_slots
    if ir is None:
        raise PlatformError(f"league {raw.league_id}: settings.reserve_slots is missing")
    if taxi is None:
        raise PlatformError(f"league {raw.league_id}: settings.taxi_slots is missing")
    return RosterSettings(
        starters=starters,
        bench=tokens.count("BN"),
        ir=ir,
        taxi=taxi,
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
    def identifies_me(self) -> bool:
        """base.IdentityAware: was this adapter given an identity to match against at all?

        False means every `is_me=False` below is ignorance, not observation — see
        `IdentityAware`'s docstring and `platform_sync._set_my_team`.
        """
        return self._my_user_id is not None

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
        # ONE FAAB policy, shared with get_teams: waiver_type 2 says this league runs on
        # FAAB, so a null budget is Sleeper contradicting itself and we refuse to guess.
        # This used to pass `faab_budget=None` here while `get_teams` raised on the very
        # same payload — `leagues.faab_budget` would have read "not a FAAB league" for a
        # league whose teams could not be loaded at all.
        faab_budget = _faab_budget(league_id, settings)
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
            faab_budget=faab_budget,
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
        budget = _faab_budget(league_id, raw.settings)
        slots = await self._draft_slots(raw)
        # No `len(teams) != len(rosters)` guard: the comprehension is over `rosters` and
        # emits exactly one element per element, so it could never fire.
        return [self._team(r, users.get(r.owner_id or ""), budget, slots) for r in rosters]

    def _team(
        self,
        roster: RawRoster,
        user: RawUser | None,
        faab_budget: int | None,
        slots: dict[int, int],
    ) -> LeagueTeam:
        """`faab_budget` is None for a non-FAAB league (-> faab_remaining None)."""
        # metadata is opaque dict[str, Any]; coerce at point of use.
        raw_team_name = user.metadata.get("team_name") if user else None
        team_name = str(raw_team_name) if raw_team_name else None
        manager = user.display_name if user else None
        faab_remaining: int | None = None
        if faab_budget is not None:
            used = roster.settings.waiver_budget_used
            if used is None:
                # A FAAB league must say how much each roster has spent; 0 is a guess.
                raise PlatformError(
                    f"roster {roster.roster_id}: FAAB league but settings.waiver_budget_used "
                    "is missing"
                )
            faab_remaining = faab_budget - used
        return LeagueTeam(
            external_id=str(roster.roster_id),
            display_name=team_name or manager,
            manager_name=manager,
            draft_slot=slots.get(roster.roster_id),
            faab_remaining=faab_remaining,
            waiver_priority=roster.settings.waiver_position,
            is_me=self._is_mine(roster),
        )

    async def _draft_slots(self, raw: RawLeague) -> dict[int, int]:
        """roster_id -> draft slot. GET /draft/{id}.slot_to_roster_id maps slot STRING -> roster_id."""
        if not raw.draft_id:
            return {}
        draft = await self._client.get_draft(raw.draft_id)
        slots = {roster_id: int(slot) for slot, roster_id in draft.slot_to_roster_id.items()}
        if len(slots) != len(draft.slot_to_roster_id):
            raise PlatformError(
                f"draft {raw.draft_id}: slot_to_roster_id maps {len(draft.slot_to_roster_id)} "
                f"slots onto {len(slots)} rosters — a roster holds more than one slot"
            )
        return slots

    async def current_week(self) -> int:
        """Platform week for a roster snapshot. FETCHED, never assumed.

        /state/nfl returns week=2 with season_type="pre" (verified 2026-08-16), so its
        `week` is meaningless outside the regular season. Week 0 is our explicit
        "pre-season / post-draft snapshot" marker.
        """
        state = await self._client.get_state()
        return state.week if state.season_type == "regular" else 0

    async def get_player_refs(self, external_ids: set[str]) -> dict[str, PlayerRef]:
        """Name/position/team/gsis for arbitrary Sleeper ids, for crosswalk resolution.

        Uses the lake catalog when one is configured — and it always is under `ffh league
        load`, so a defense reaches the crosswalk under its real name ("Kansas City
        Chiefs"; see `player_ref`). Without a catalog, a NON-NUMERIC Sleeper id is a team
        defense — verified: the 32 DEF entries in /players/nfl are keyed by team
        abbreviation ("KC", "SF", ...) — and the abbreviation is all we have to name it
        with. ④'s `normalize_dst` canonicalizes that form too, it is merely the less
        legible one in an operator's review queue. Numeric ids fall back to the id alone
        with a NULL position (never "", which would masquerade as a real position); rung 1
        — the DynastyProcess sleeper_id lookup, the primary rung for Sleeper regardless —
        resolves those.
        """
        catalog: dict[str, PlayerRef] = {}
        if self._catalog is not None:
            catalog = await self._catalog.all_players()
        out: dict[str, PlayerRef] = {}
        for ext in external_ids:
            known = catalog.get(ext)
            if known is not None:
                out[ext] = known
            elif not ext.isdigit():
                out[ext] = PlayerRef(external_id=ext, name=ext, position="DST", team=ext)
            else:
                out[ext] = PlayerRef(external_id=ext, name=ext, position=None, team=None)
        if len(out) != len(external_ids):
            raise PlatformError("get_player_refs lost ids")
        return out

    # --- rosters ---------------------------------------------------------------
    async def get_rosters(self, league_id: str, week: int) -> list[Roster]:
        raw = await self._client.get_league(league_id)
        starter_slots = to_roster_settings(raw).starters
        rosters = await self._client.get_rosters(league_id)
        return [self._roster(r, starter_slots, week, league_id) for r in rosters]

    def _roster(
        self, raw: RawRoster, starter_slots: list[str], week: int, league_id: str
    ) -> Roster:
        # Sleeper sends `starters: null` (-> []) for a roster that has never set a lineup.
        # That is unambiguous — every starting slot is empty — so treat it as
        # [EMPTY_SLOT] * len(starter_slots) and let everyone fall to BN. Only a NON-empty
        # list whose length disagrees with the league's starter tokens is ambiguous.
        starters = raw.starters or [EMPTY_SLOT] * len(starter_slots)
        if len(starters) != len(starter_slots):
            raise PlatformError(
                f"league {league_id} roster {raw.roster_id}: {len(starters)} starters "
                f"but {len(starter_slots)} starting slots — refusing to guess the alignment"
            )
        entries: list[RosterEntry] = []
        seen: set[str] = set()
        for slot, pid in zip(starter_slots, starters, strict=True):
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
                    home_points=_matchup_points(home),
                    away_points=None if away is None else _matchup_points(away),
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
                    home_points=_matchup_points(m),
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
        if raw.type in ("waiver", "trade", "commissioner"):
            # "commissioner" is a commish-forced move; its adds/drops map exactly like a
            # free_agent's, but the type is kept so downstream never mistakes it for a
            # manager's own claim.
            kind = raw.type
        elif raw.type == "free_agent":
            kind = "add" if raw.adds else "drop"
        else:
            raise PlatformError(f"unrecognised Sleeper transaction type {raw.type!r}")
        # A waiver_bid on a failed/pending claim was never charged: only a completed
        # transaction actually spent FAAB.
        faab_spent = raw.settings.waiver_bid if raw.settings and raw.status == "complete" else None
        return Transaction(
            external_id=raw.transaction_id,
            type=kind,  # type: ignore[arg-type]
            week=raw.leg if raw.leg is not None else week,
            executed_at=_ms_to_dt(raw.status_updated or raw.created),
            faab_spent=faab_spent,
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

    async def get_league_drafts(self, league_id: str) -> list[Draft]:
        """Every draft attached to a league (GET /league/{id}/drafts).

        Those rows carry no `slot_to_roster_id` (only GET /draft/{id} does); `_draft` never
        reads it, so the mapping is identical.
        """
        raws = await self._client.get_league_drafts(league_id)
        # One Draft per raw row by construction — a count guard over the comprehension's
        # own source can never fire. `_validate_snapshot` is what actually catches a league
        # listing the same draft twice, before the first write.
        return [self._draft(raw) for raw in raws]

    def _draft(self, raw: RawDraft) -> Draft:
        if raw.type not in DRAFT_TYPES:
            raise PlatformError(f"draft {raw.draft_id}: unrecognised type {raw.type!r}")
        if raw.status not in DRAFT_STATUSES:
            raise PlatformError(f"draft {raw.draft_id}: unrecognised status {raw.status!r}")
        if not raw.league_id:
            raise PlatformError(f"draft {raw.draft_id}: no league_id on the wire")
        my_slot = raw.draft_order.get(self._my_user_id) if self._my_user_id else None
        return Draft(
            external_id=raw.draft_id,
            league_external_id=raw.league_id,
            draft_type=raw.type,  # type: ignore[arg-type]
            rounds=raw.settings.rounds,
            status=raw.status,  # type: ignore[arg-type]
            my_slot=my_slot,
            started_at=_ms_to_dt(raw.start_time),
            last_picked_ms=raw.last_picked,
        )

    async def get_draft_picks(self, draft_id: str) -> list[DraftPick]:
        raws = await self._client.get_draft_picks(draft_id)
        # Same as get_league_drafts: one pick per raw row by construction. Duplicate pick
        # NUMBERS are the real hazard and are caught in `_validate_snapshot`, pre-write.
        return [self._pick(p) for p in raws]

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
        """Cursor is ``"{last_picked_ms}:{status}"``.

        `last_picked` alone misses status-only transitions (pre_draft -> drafting,
        drafting -> paused, last pick -> complete), so the status rides along. Pre-draft
        `last_picked` is null; `0` is the stable "nothing picked yet" value. A legacy bare
        numeric cursor (no ":") is compared on the epoch-ms part only.
        """
        raw = await self._client.get_draft(draft_id)
        new_ms = raw.last_picked or 0
        new_cursor = f"{new_ms}:{raw.status}"
        if cursor is None:
            return True, new_cursor
        prev_ms_text, sep, prev_status = cursor.partition(":")
        try:
            previous_ms = int(prev_ms_text)
        except ValueError:
            return True, new_cursor
        if previous_ms != new_ms:
            return True, new_cursor
        if not sep:
            # Legacy cursor without a status: epoch-ms parity is all we can compare.
            return False, new_cursor
        return prev_status != raw.status, new_cursor
