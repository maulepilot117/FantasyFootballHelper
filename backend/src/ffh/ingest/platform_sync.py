"""Load a fantasy league into Postgres.

ARCHITECTURE.md's module map has no home for "land a league in Postgres"; this is ingest's
fetch -> validate -> land, landing in Postgres rather than Parquet. Recorded as a deviation
in ARCHITECTURE.md by this PR.

SYNCHRONOUS by design: it takes an orm.Session, matching the sync engine, the db_session
test fixture, and the crosswalk's resolve_many. The async adapter boundary is crossed
exactly once, in load_league — which therefore owns an event loop for the length of one
call and requires a per-call adapter (see its docstring's lifetime contract).
"""

from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol, runtime_checkable

import structlog
from sqlalchemy import delete
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from ffh.adapters.base import (
    Draft,
    DraftPick,
    FantasyPlatformAdapter,
    League,
    LeagueTeam,
    PlatformError,
    PlayerRef,
    Roster,
)
from ffh.crosswalk.resolve import ResolveInput, resolve_many
from ffh.db.models import Draft as DraftRow
from ffh.db.models import DraftPick as DraftPickRow
from ffh.db.models import League as LeagueRow
from ffh.db.models import LeagueTeam as LeagueTeamRow
from ffh.db.models import RosterSlot as RosterSlotRow

log = structlog.get_logger(__name__)


@runtime_checkable
class _WeekAware(Protocol):
    async def current_week(self) -> int: ...


@runtime_checkable
class _RefAware(Protocol):
    async def get_player_refs(self, external_ids: set[str]) -> dict[str, PlayerRef]: ...


@runtime_checkable
class _DraftListing(Protocol):
    async def get_league_drafts(self, league_id: str) -> list[Draft]: ...


@dataclass(frozen=True, slots=True)
class UnmatchedPlayer:
    external_id: str
    name: str
    position: str | None
    team: str | None


@dataclass(frozen=True, slots=True)
class LeagueLoadReport:
    league_id: uuid.UUID
    teams: int
    rostered: int
    #: ④ rung 5 — already upserted into crosswalk_unmatched by resolve_many.
    unmatched: list[UnmatchedPlayer]
    #: ④ rung 4 — fuzzy hit persisted UNVERIFIED in player_external_ids; not in
    #: crosswalk_unmatched. Usable only after `ffh crosswalk verify <source> <id>`.
    pending_review: list[UnmatchedPlayer]
    drafts: int
    picks: int


@dataclass(frozen=True, slots=True)
class LeagueSnapshot:
    league: League
    teams: list[LeagueTeam]
    rosters: list[Roster]
    drafts: list[Draft]
    picks: dict[str, list[DraftPick]]
    week: int
    # external_id -> name/position/team/gsis_id, for crosswalk resolution.
    player_refs: dict[str, PlayerRef]


async def fetch_snapshot(
    adapter: FantasyPlatformAdapter, external_id: str, week: int | None = None
) -> LeagueSnapshot:
    """Every network call for one league load. No DB access."""
    if week is None:
        if not isinstance(adapter, _WeekAware):
            raise ValueError(
                f"{type(adapter).__name__} cannot resolve the current week; pass week="
            )
        week = await adapter.current_week()
    league = await adapter.get_league(external_id)
    teams = await adapter.get_teams(external_id)
    rosters = await adapter.get_rosters(external_id, week)
    if len(rosters) != len(teams):
        raise PlatformError(f"league {external_id}: {len(teams)} teams but {len(rosters)} rosters")
    drafts = await _league_drafts(adapter, external_id)
    picks = {d.external_id: await adapter.get_draft_picks(d.external_id) for d in drafts}
    rostered_ids = {e.player_external_id for r in rosters for e in r.players}
    # A drafted player who has since been dropped is NOT on any roster, and `draft_picks`
    # has its own `player_id` FK. Describing only the rostered ids left every such pick
    # with a NULL player_id, no crosswalk_unmatched row and no report bucket — a silent
    # drop, in the one module the Global Constraints name. The crosswalk batch is
    # therefore rosters UNION picks; `get_player_refs` takes arbitrary ids.
    drafted_ids = {
        pick.player_external_id
        for draft_picks in picks.values()
        for pick in draft_picks
        if pick.player_external_id
    }
    wanted_ids = rostered_ids | drafted_ids
    if not isinstance(adapter, _RefAware):
        raise ValueError(f"{type(adapter).__name__} cannot describe players for the crosswalk")
    player_refs = await adapter.get_player_refs(wanted_ids)
    if set(player_refs) != wanted_ids:
        raise PlatformError(
            f"player refs cover {len(player_refs)} of {len(wanted_ids)} rostered/drafted ids"
        )
    log.info(
        "platform_sync.fetch_snapshot",
        platform=league.platform,
        league=external_id,
        week=week,
        teams=len(teams),
        rostered=len(rostered_ids),
        drafted_not_rostered=len(drafted_ids - rostered_ids),
        drafts=len(drafts),
        picks=sum(len(v) for v in picks.values()),
    )
    return LeagueSnapshot(
        league=league,
        teams=teams,
        rosters=rosters,
        drafts=drafts,
        picks=picks,
        week=week,
        player_refs=player_refs,
    )


async def _league_drafts(adapter: FantasyPlatformAdapter, external_id: str) -> list[Draft]:
    """The Protocol exposes get_draft(draft_id), not "the league's drafts"."""
    if not isinstance(adapter, _DraftListing):
        # Not an error — ESPN lands in Phase 2 — but `drafts=0` must never look like
        # "this league has no draft" when it really means "this adapter cannot say".
        log.warning(
            "platform_sync.no_draft_listing",
            adapter=type(adapter).__name__,
            league=external_id,
        )
        return []
    return list(await adapter.get_league_drafts(external_id))


def _validate_snapshot(snapshot: LeagueSnapshot) -> None:
    """Cross-object invariants, checked BEFORE the first write.

    `leagues`, `league_teams` and `drafts` are written with plain INSERT ... ON CONFLICT,
    so a raise midway through `_upsert_drafts` would leave the caller's transaction holding
    a half-loaded league — visible to any query the caller runs before it rolls back. Every
    invariant that can be decided from the snapshot alone is therefore decided here.
    """
    known = {t.external_id for t in snapshot.teams}
    mine = [t for t in snapshot.teams if t.is_me]
    if len(mine) > 1:
        raise PlatformError(
            f"league {snapshot.league.external_id}: {len(mine)} teams flagged is_me"
        )
    for roster in snapshot.rosters:
        if roster.team_external_id not in known:
            raise PlatformError(
                f"roster names team {roster.team_external_id!r}, which is "
                f"not a team of league {snapshot.league.external_id}"
            )
    listed = {d.external_id for d in snapshot.drafts}
    orphaned = set(snapshot.picks) - listed
    if orphaned:
        raise PlatformError(
            f"league {snapshot.league.external_id}: picks for unlisted draft(s) "
            f"{sorted(orphaned)} would never be persisted"
        )
    for draft in snapshot.drafts:
        draft_picks = snapshot.picks.get(draft.external_id, [])
        # `draft_picks` is keyed (draft_id, pick_no): two picks sharing a pick_no would
        # ON CONFLICT onto each other and one would vanish. Counting rows after the fact
        # cannot see that — the loop counted both — so it is a pre-write check.
        numbers = [p.pick_no for p in draft_picks]
        if len(set(numbers)) != len(numbers):
            raise PlatformError(
                f"draft {draft.external_id}: {len(numbers)} picks share "
                f"{len(set(numbers))} pick numbers"
            )
        for pick in draft_picks:
            # Same-league invariant: DATABASE.md §5 leaves this to us, with a test.
            if pick.team_external_id is not None and pick.team_external_id not in known:
                raise PlatformError(
                    f"draft pick {pick.pick_no} names team {pick.team_external_id!r}, "
                    f"which is not a team of league {snapshot.league.external_id}"
                )


def _resolve_refs(
    session: Session, source: str, refs: dict[str, PlayerRef]
) -> tuple[dict[str, uuid.UUID], list[UnmatchedPlayer], list[UnmatchedPlayer]]:
    """The ONLY place PR 5 touches PR 4's crosswalk contract (ffh.crosswalk.resolve).

    ④'s shapes: `resolve_many(session, Iterable[ResolveInput])` returns a
    `ResolveManyReport` whose `resolved` is keyed by `(source, external_id)` — NOT by bare
    external_id — plus `unmatched` (rung 5, already in crosswalk_unmatched) and
    `pending_review` (rung 4 fuzzy, persisted unverified, NOT in crosswalk_unmatched).
    `resolve_many` runs two internal passes and REORDERS its input, and both list buckets
    come back in processing order — so they are read as sets of keys and re-described from
    our own refs, never zipped against the input sequence.

    Every input carries `raw_name` / `raw_position` / `raw_team`: ④'s `_record_unmatched`
    stores exactly what the caller supplied, so omitting them leaves the operator's review
    queue holding a bare id, and `upsert_unmatched` re-opens an acknowledged entry whenever
    those fields change. `gsis_id` rides along too — it is the crosswalk's strongest join
    key (rung 2, confidence 1.0), and withholding it pushes a player whose platform id the
    id file has not caught up with down into fuzzy matching.

    Returns (external_id -> player_id, unmatched, pending_review). If the merged ④ code
    differs, fix it here and nowhere else.
    """
    ordered = sorted(refs.values(), key=lambda r: r.external_id)
    inputs = [
        ResolveInput(
            source=source,
            external_id=r.external_id,
            raw_name=r.name,
            raw_position=r.position,
            raw_team=r.team,
            gsis_id=r.gsis_id,
        )
        for r in ordered
    ]
    report = resolve_many(session, inputs)

    foreign = {key for key in report.resolved if key[0] != source}
    if foreign:
        raise PlatformError(f"crosswalk returned resolutions for other sources: {sorted(foreign)}")
    resolved = {key[1]: res.player_id for key, res in report.resolved.items()}
    unmatched_ids = {key[1] for key in report.unmatched}
    pending_ids = {key[1] for key in report.pending_review}

    def _as_unmatched(r: PlayerRef) -> UnmatchedPlayer:
        return UnmatchedPlayer(
            external_id=r.external_id, name=r.name, position=r.position, team=r.team
        )

    unmatched = [_as_unmatched(r) for r in ordered if r.external_id in unmatched_ids]
    pending = [_as_unmatched(r) for r in ordered if r.external_id in pending_ids]
    accounted = len(resolved) + len(unmatched) + len(pending)
    if accounted != len(refs):
        raise PlatformError(f"crosswalk accounted for {accounted} of {len(refs)} players")
    return resolved, unmatched, pending


def persist_snapshot(session: Session, snapshot: LeagueSnapshot) -> LeagueLoadReport:
    """All DB writes for one league load. No network. One transaction (caller commits)."""
    _validate_snapshot(snapshot)
    league_id = _upsert_league(session, snapshot.league)
    team_ids = _upsert_teams(session, league_id, snapshot.teams)
    _set_my_team(session, league_id, snapshot.teams, team_ids)

    resolved, unmatched, pending_review = _resolve_refs(
        session, snapshot.league.platform, snapshot.player_refs
    )

    rostered = _replace_roster_slots(session, snapshot, team_ids, resolved)
    drafts, picks = _upsert_drafts(session, league_id, snapshot, team_ids, resolved)
    session.flush()
    log.info(
        "platform_sync.persist_snapshot",
        platform=snapshot.league.platform,
        league=snapshot.league.external_id,
        league_id=str(league_id),
        week=snapshot.week,
        teams=len(team_ids),
        rostered=rostered,
        unmatched=len(unmatched),
        pending_review=len(pending_review),
        drafts=drafts,
        picks=picks,
    )
    return LeagueLoadReport(
        league_id=league_id,
        teams=len(team_ids),
        rostered=rostered,
        unmatched=unmatched,
        pending_review=pending_review,
        drafts=drafts,
        picks=picks,
    )


def _upsert_league(session: Session, league: League) -> uuid.UUID:
    values = {
        "platform": league.platform,
        "external_id": league.external_id,
        "season": league.season,
        "name": league.name,
        "num_teams": league.num_teams,
        # VERBATIM. Never normalized, never defaulted.
        "scoring_settings": dict(league.scoring.points),
        # RosterSettings.model_dump() — {"starters": [...], "bench": n, "ir": n, "taxi": n,
        # "flex_composition": {...}}. `is_superflex` is a property, not a field, and lives
        # in its own column. NOTE: ③'s sentinel generic league stores a plain COUNT MAP
        # ({"QB": 1, "RB": 2, ...}) in this same column, so leagues.roster_settings has two
        # shapes; consumers must branch on `"starters" in roster_settings`, never assume one.
        "roster_settings": league.roster.model_dump(),
        "league_type": league.league_type,
        "is_superflex": league.is_superflex,
        "playoff_teams": league.playoff_teams,
        "playoff_start_wk": league.playoff_start_week,
        "faab_budget": league.faab_budget,
    }
    keys = ("platform", "external_id", "season")
    stmt = insert(LeagueRow).values(**values)
    stmt = stmt.on_conflict_do_update(
        index_elements=list(keys),
        set_={k: stmt.excluded[k] for k in values if k not in keys},
    ).returning(LeagueRow.league_id)
    return session.execute(stmt).scalar_one()


def _upsert_teams(
    session: Session, league_id: uuid.UUID, teams: list[LeagueTeam]
) -> dict[str, uuid.UUID]:
    out: dict[str, uuid.UUID] = {}
    for team in teams:
        values = {
            "league_id": league_id,
            "external_id": team.external_id,
            "display_name": team.display_name,
            "manager_name": team.manager_name,
            "draft_slot": team.draft_slot,
            "faab_remaining": team.faab_remaining,
            "waiver_priority": team.waiver_priority,
            "is_me": team.is_me,
        }
        keys = ("league_id", "external_id")
        stmt = insert(LeagueTeamRow).values(**values)
        stmt = stmt.on_conflict_do_update(
            index_elements=list(keys),
            set_={k: stmt.excluded[k] for k in values if k not in keys},
        ).returning(LeagueTeamRow.league_team_id)
        out[team.external_id] = session.execute(stmt).scalar_one()
    if len(out) != len(teams):
        raise PlatformError(f"upserted {len(out)} of {len(teams)} teams")
    return out


def _set_my_team(
    session: Session,
    league_id: uuid.UUID,
    teams: list[LeagueTeam],
    team_ids: dict[str, uuid.UUID],
) -> None:
    """`leagues.my_team_id` carries a composite FK onto (league_id, league_team_id), so it
    can only be set once this league's teams exist."""
    mine = [t for t in teams if t.is_me]
    # populate_existing: the upsert above went through Core, so an instance already in
    # the identity map (a previous load in this same transaction) would otherwise be
    # returned with its pre-upsert column values.
    row = session.get(LeagueRow, league_id, populate_existing=True)
    if row is None:  # pragma: no cover — the upsert above returned this id
        raise PlatformError(f"league {league_id} vanished mid-load")
    row.my_team_id = team_ids[mine[0].external_id] if mine else None


def _replace_roster_slots(
    session: Session,
    snapshot: LeagueSnapshot,
    team_ids: dict[str, uuid.UUID],
    resolved: dict[str, uuid.UUID],
) -> int:
    """Delete-then-insert THIS WEEK's snapshot so a dropped player leaves no stale row.
    Other weeks are history and are never touched."""
    session.execute(
        delete(RosterSlotRow).where(
            RosterSlotRow.league_team_id.in_(list(team_ids.values())),
            RosterSlotRow.week == snapshot.week,
        )
    )
    captured = datetime.now(tz=UTC)
    rows = [
        {
            "league_team_id": team_ids[roster.team_external_id],
            "week": snapshot.week,
            "player_id": resolved[entry.player_external_id],
            "slot": entry.slot,
            "is_starter": entry.is_starter,
            "captured_at": captured,
        }
        for roster in snapshot.rosters
        for entry in roster.players
        # An id the crosswalk could not resolve has no players row to point at. It is NOT
        # dropped: resolve_many queued it and it rides back in LeagueLoadReport.unmatched.
        if entry.player_external_id in resolved
    ]
    if rows:
        session.execute(insert(RosterSlotRow), rows)
    return len(rows)


def _upsert_drafts(
    session: Session,
    league_id: uuid.UUID,
    snapshot: LeagueSnapshot,
    team_ids: dict[str, uuid.UUID],
    resolved: dict[str, uuid.UUID],
) -> tuple[int, int]:
    drafts = 0
    picks = 0
    for draft in snapshot.drafts:
        values = {
            "league_id": league_id,
            "external_id": draft.external_id,
            "draft_type": draft.draft_type,
            "rounds": draft.rounds,
            "status": draft.status,
            "my_slot": draft.my_slot,
            "started_at": draft.started_at,
        }
        keys = ("league_id", "external_id")
        stmt = insert(DraftRow).values(**values)
        stmt = stmt.on_conflict_do_update(
            index_elements=list(keys),
            set_={k: stmt.excluded[k] for k in values if k not in keys},
        ).returning(DraftRow.draft_id)
        draft_pk = session.execute(stmt).scalar_one()
        drafts += 1
        for pick in snapshot.picks.get(draft.external_id, []):
            pvalues = {
                "draft_id": draft_pk,
                "pick_no": pick.pick_no,
                "round": pick.round,
                "draft_slot": pick.draft_slot,
                "league_team_id": (
                    None if pick.team_external_id is None else team_ids[pick.team_external_id]
                ),
                "player_id": resolved.get(pick.player_external_id or ""),
                "is_keeper": pick.is_keeper,
                "auction_amount": pick.auction_amount,
                # Sleeper publishes no per-pick timestamp; always None for that platform.
                "picked_at": pick.picked_at,
            }
            pkeys = ("draft_id", "pick_no")
            pstmt = insert(DraftPickRow).values(**pvalues)
            pstmt = pstmt.on_conflict_do_update(
                index_elements=list(pkeys),
                set_={k: pstmt.excluded[k] for k in pvalues if k not in pkeys},
            )
            session.execute(pstmt)
            picks += 1
    # No post-write count guard: `_validate_snapshot` already proved every picks key
    # belongs to a listed draft and that pick numbers are unique per draft, so one row
    # per pick is landed. A guard here could only raise AFTER the writes it guards.
    return drafts, picks


def load_league(
    session: Session,
    adapter: FantasyPlatformAdapter,
    external_id: str,
    season: int,
    week: int | None = None,
) -> LeagueLoadReport:
    """Fetch a league and land it in Postgres. The caller commits.

    **Adapter lifetime contract — one adapter per call.** This function runs its own
    event loop (`asyncio.run`), which is closed before it returns. An
    `httpx.AsyncClient` keep-alive connection belongs to the loop that opened it, so an
    adapter whose client outlives this call and is passed to a second `load_league` is
    driving a pool across two dead-and-reborn loops. Tests cannot catch it — respx
    replaces the transport — so it is a contract, not an assertion: **build the client
    (and the adapter) inside each invocation and close it after.** Task 8's CLI does
    exactly that, and `tests/ingest/test_platform_sync.py::adapter_factory` models it.

    Already inside your own event loop, or holding a long-lived client on purpose? Do
    not call this: `await fetch_snapshot(...)` and then call `persist_snapshot(...)`.
    Calling `load_league` from a running loop raises rather than nesting.
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        pass
    else:
        raise RuntimeError(
            "load_league() is synchronous and cannot run inside an event loop; "
            "await fetch_snapshot() then call persist_snapshot()"
        )
    snapshot = asyncio.run(fetch_snapshot(adapter, external_id, week))
    if snapshot.league.season != season:
        raise ValueError(
            f"league {external_id} is season {snapshot.league.season}, asked for {season}"
        )
    return persist_snapshot(session, snapshot)
