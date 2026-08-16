# PR ⑤ `feat/adapter-sleeper` — Platform Adapter Interface + Sleeper + League Sync Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** One platform-agnostic adapter interface, a complete read-only Sleeper implementation behind it, and `ffh.ingest.platform_sync.load_league` that lands a Sleeper league (settings, teams, roster snapshot, draft, picks) into Postgres with every rostered player resolved through the crosswalk.

**Architecture:** `ffh.adapters.base` owns the `FantasyPlatformAdapter` Protocol and the normalized frozen Pydantic v2 models — nothing above `adapters/` learns which platform is in use. `ffh.adapters.sleeper` is a thin in-house httpx client (async, token-bucketed, tenacity-retried) plus raw response models and a mapping layer. `ffh.ingest.platform_sync` is **synchronous** (`sqlalchemy.orm.Session`); it crosses the async boundary exactly once at the top of `load_league` via `asyncio.run`, then performs all persistence in a single sync transaction. The 14.6 MB `/players/nfl` blob never touches the request path: it is landed to the Parquet lake by an `IngestJob` at most once a day, and `get_free_agents` reads the latest partition.

**Tech Stack:** Python 3.13 · httpx 0.28.1 (async) · tenacity 9.1.4 · Pydantic 2.13.4 · Polars 1.43.2 · SQLAlchemy 2.0.51 · typer 0.27.1 · pytest 9.1.1 + pytest-asyncio 1.4.0 (`asyncio_mode = "auto"`) + respx 0.23.1. **No new dependencies.**

**Spec:** `docs/superpowers/specs/2026-08-15-phase0-foundation-design.md` §5 (and the cross-cutting rules section)

**Overview / locked scope:** `docs/superpowers/plans/2026-08-15-phase0-00-overview.md` § "⑤ `feat/adapter-sleeper`"

---

## Live verification record (2026-08-16)

Everything below was fetched live against `https://api.sleeper.app/v1` while writing this plan. Where a shape here disagrees with your priors, this record wins. Task 11 copies the new facts into `docs/DATA_SOURCES.md`.

| Endpoint | Verified result |
|---|---|
| `GET /state/nfl` | `{"week":2,"leg":0,"season":"2026","season_type":"pre","league_season":"2026","previous_season":"2025","season_start_date":"2026-08-06","display_week":2,"league_create_season":"2026","season_has_scores":true}` — **`week` is 2 while `season_type` is `pre`.** It is *not* a regular-season week during the preseason. |
| `GET /user/{username}` | `{"user_id","username","display_name","avatar","is_bot",...}` — all other keys null. |
| `GET /user/{user_id}/leagues/nfl/{season}` | Array of full league objects (same shape as `/league/{id}` minus `slot_to_roster_id`). Returns `[]` for users with no leagues. |
| `GET /league/{id}` | Top-level keys: `league_id, name, season (str), season_type, sport, status, total_rosters, draft_id, previous_league_id, avatar, metadata, roster_positions, scoring_settings, settings, shard, group_id, bracket_id, loser_bracket_id, bracket_overrides_id, loser_bracket_overrides_id, company_id, last_*`. |
| `roster_positions` | Ordered list, e.g. `["QB","RB","RB","WR","WR","TE","FLEX","K","DEF","BN","BN","BN","BN","BN","BN"]`. A **superflex** league verified live (`league_id=1389393510163042306`): `["QB","RB","RB","WR","WR","WR","TE","FLEX","FLEX","SUPER_FLEX","BN","BN"]`. |
| `scoring_settings` | Flat `dict[str, float]`, **132 keys** in the verified league. `rec` is `0.5` (half PPR) there and `1.0` in another live league. There is no `format` field — format must be derived, never assumed. |
| `settings` | `num_teams, playoff_teams, playoff_week_start, waiver_budget, waiver_type, type, taxi_slots, reserve_slots, draft_rounds, max_keepers, leg, start_week, trade_deadline, ...`. Verified values: `type=0`, `waiver_type=2` (FAAB, `waiver_budget=100`) and `waiver_type=0` (priority, `waiver_budget` **still present and meaningless**). `reserve_slots=1`, `taxi_slots=0`. |
| `GET /league/{id}/rosters` | Keys: `roster_id (int), owner_id (str\|null), co_owners (list\|null), players (list[str]\|null), starters (list[str]\|null), reserve (list[str]\|null), taxi (list[str]\|null), keepers, metadata, settings, league_id, player_map`. `settings` carries `wins, losses, ties, fpts, fpts_decimal, ppts, waiver_budget_used, waiver_position, total_moves`. **`len(starters) == len(roster_positions) - count("BN")`** — verified 9 == 15 − 6. |
| Pre-draft rosters | **`starters` is `["0","0",...]` and `players` is `[]`.** The string `"0"` is an *empty slot placeholder*, not a player id. Resolving it would create a bogus `crosswalk_unmatched` row. It must be filtered. |
| `GET /league/{id}/users` | Keys: `user_id, display_name, avatar, is_bot, is_owner (bool\|null — commissioner), league_id, settings, metadata`. `metadata.team_name` is **optional** (absent for several verified users). |
| `GET /league/{id}/drafts` | Array. Keys: `draft_id, league_id, type, status, season, season_type, sport, rounds (inside settings), settings, metadata, draft_order, last_picked, start_time, created, creators, last_message_*`. **No `slot_to_roster_id`.** |
| `GET /draft/{id}` | Same keys **plus `slot_to_roster_id`** (`{"1": 11, "2": 5, ...}` — draft slot → roster_id, keys are strings). `type` verified as `"auction"` and `"snake"`. `settings.rounds`, `settings.budget`, `settings.slots_qb/rb/wr/te/flex/super_flex/k/def/bn`. |
| Pre-draft `/draft/{id}` | **`last_picked`, `start_time` and `draft_order` are all `null`.** |
| `GET /draft/{id}/picks` | 210 rows in the verified draft. Keys: `draft_id, pick_no, round, draft_slot, roster_id (int), picked_by (user_id, may be ""), player_id, is_keeper (null\|true), reactions, metadata`. `metadata` carries `amount` (auction bid, **string**), `first_name, last_name, position, team, status, injury_status, number, years_exp, news_updated, slot, sport, player_id`. **There is no per-pick timestamp** — `picked_at` is unavailable from Sleeper. |
| `GET /league/{id}/matchups/{wk}` | One row **per roster**, not per pairing: `{roster_id, matchup_id, points, custom_points, starters, starters_points, players, players_points}`. Pairing is by `matchup_id`. |
| `GET /league/{id}/transactions/{round}` | Keys: `transaction_id, type ("free_agent"\|"waiver"\|"trade"), status, status_updated (epoch ms), created (epoch ms), leg (week), adds ({player_id: roster_id}\|null), drops (same\|null), roster_ids, consenter_ids, draft_picks, waiver_budget, creator, settings ({seq, waiver_bid}), metadata`. |
| `GET /players/nfl` | Top-level `dict` keyed by Sleeper player id. **12,219 entries.** Human entry has 53 keys incl. `player_id, full_name, first_name, last_name, search_full_name, position, fantasy_positions, team, status, active, injury_status, gsis_id, espn_id (int), yahoo_id (int), rotowire_id, sportradar_id, fantasy_data_id, birth_date (str), college, years_exp, number, depth_chart_order, depth_chart_position, search_rank`. **8,326 of 12,219 have a null `gsis_id`.** |
| `/players/nfl` DEF entries | **32 entries keyed by team abbreviation** (`"KC"`, `"SF"`, …) with only `{active, position:"DEF", first_name:"Kansas City", last_name:"Chiefs", sport, team, player_id:"KC", fantasy_positions:["DEF"], injury_status}`. **No `full_name`, no `gsis_id`.** |
| `/players/nfl` caching | Response carries `ETag: W/"…"` and `cache-control: public, s-maxage=600`, **but sending `If-None-Match` returns `200` with the full body** (verified: `status=200 size=14639113`). Conditional GET does **not** work here. The ≤1×/day partition guard plus a content hash comparison are the only real protections. Body is **14.6 MB uncompressed** (~5 MB gzipped, which is what DATA_SOURCES.md records). |

---

## Global Constraints

Every task's requirements implicitly include this section.

- **Scoring and roster settings are ALWAYS fetched from the platform, NEVER hardcoded** (`docs/ARCHITECTURE.md`, `docs/DATABASE.md` §4). A default is a bug even when it happens to be right. `leagues.scoring_settings` stores the platform payload **verbatim** — no key added, removed, renamed or rounded. There is a test that asserts exactly that.
- **All three platforms are read-only.** No adapter method writes to a platform. Never design toward automated lineup submission.
- **Rate limit:** Sleeper's ceiling is 1000 req/min and **IP-based** (no key identifies you — a block hits the whole household). This client stays at **≤ 300 req/min with a burst of 30**, and backs off on 429/5xx with exponential jitter.
- **`last_picked` is epoch milliseconds** (`AGENTS.md` Tier 1 timezone rule). Convert with `datetime.fromtimestamp(ms / 1000, tz=UTC)`. Never seconds. Never naive.
- **Never call a live API in a test.** Every test drives `respx` mounted on `settings.sleeper_base_url`. The only network-touching file is `backend/scripts/record_sleeper_fixtures.py`, which is marked/guarded and never runs in CI (`addopts = "-m 'not network'"`).
- **Nothing is silently dropped.** Every join, filter, and grouping asserts a count. Every unresolved player becomes a `crosswalk_unmatched` row (via ④) *and* appears in the returned report.
- **No `import pandas`, no `nfl_data_py`, no `nflreadpy`** (ruff banned-api enforces it).
- **No new dependencies.** `httpx`, `tenacity`, `pydantic`, `polars`, `typer`, `respx` are already pinned in `backend/pyproject.toml` under `exclude-newer = "2026-08-09T00:00:00Z"`. If you believe you need a new package, stop and check its publish date is ≥ 7 days old first, and say so in the PR.
- **No secrets in the repo.** `FFH_SLEEPER_MOCK_LEAGUE_ID` / `FFH_SLEEPER_USER_ID` are not secrets but still live in gitignored `backend/.env`; they are never committed and never required by CI.
- **Sleeper's license is non-commercial.** Self-hosted personal use only — note it wherever the client is documented.
- **Branch `feat/adapter-sleeper`** off `main`. Conventional commits scoped to the module (`feat(adapters):`, `feat(ingest):`, `test(adapters):`, `docs(adapters):`). One commit per task.
- **Docs ship in this PR** (`docs/ARCHITECTURE.md`, `docs/DATA_SOURCES.md`, `docs/ROADMAP.md`) — Task 11.
- **Implementers do not push.** Task 11 prepares the commits and the PR body; Chris pushes and runs the Codex adversarial review.

## Decisions this plan makes (the overview left them open)

1. **Roster-snapshot week.** `/state/nfl` returns `week=2` while `season_type="pre"`, so `state["week"]` is meaningless outside the regular season. Rule, implemented in `resolve_week()`:
   `week = state["week"] if state["season_type"] == "regular" else 0`.
   **Week 0 means "pre-season / post-draft roster snapshot."** `roster_slots.week` is `SMALLINT NOT NULL` and part of the PK, so 0 is a legal, meaningful, non-colliding value. An explicit `--week` on the CLI always wins. The platform week is **fetched**, never assumed.
2. **Sync vs async.** The Protocol mandates `async def` on every adapter method, so **`ffh.adapters` is async**. **`ffh.ingest.platform_sync` is synchronous** and takes `sqlalchemy.orm.Session` — matching the existing `db_session` fixture (`backend/tests/conftest.py`), the sync `make_engine`, and ④'s `resolve_many(session, rows)`. `load_league` crosses the boundary once, at the top, with `asyncio.run(fetch_snapshot(...))`, then persists synchronously. `fetch_snapshot` (async, pure network) and `persist_snapshot` (sync, pure DB) are both public so tests can drive either half alone.
3. **No `WindowsSelectorEventLoopPolicy` conftest hook.** That policy is only needed when **psycopg's async driver** runs on Windows. Nothing in this PR opens an async DB connection: persistence is sync, and the async surface is httpx-only, which is happy on the default Proactor loop. Do **not** add the hook. If a future PR introduces async psycopg in tests, that PR adds `if sys.platform == "win32": asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())` to `backend/tests/conftest.py` — and Task 11 records this note in `docs/ARCHITECTURE.md`.
4. **`get_free_agents` player source.** It needs the full player universe, which lives in the 14.6 MB blob. The adapter therefore takes an injected `PlayerCatalog`; the default implementation `LakePlayerCatalog` reads the newest `raw/sleeper/players/scrape_date=*/` Parquet partition with Polars and `pathlib` only — **no import of `ffh.ingest`**, so Task 5 stays independent of ③. With no catalog configured, `get_free_agents` raises `PlatformError` rather than silently returning an empty list.
5. **Sleeper `DEF` → `DST` at the adapter boundary.** `roster_slots.slot` and the crosswalk both use `DST` (`docs/DATABASE.md` §3–4). The adapter translates the roster-position token `DEF` → slot `DST` and the player position `DEF` → `DST`. For a defense, `PlayerRef.name` is set to the **team abbreviation** (which is also its Sleeper `player_id`), because that is the one form ④'s `normalize_dst` is guaranteed to canonicalize — the blob has no `full_name` for defenses.
6. **`matchups` and `transactions` tables are not written in this PR.** ⑤'s locked scope persists `leagues`, `league_teams`, `roster_slots`, `drafts`, `draft_picks`. `get_matchups` / `get_transactions` are implemented and contract-tested because the Protocol requires them; their persistence lands with the lineup module in Phase 2.
7. **Draft pick timestamps.** Sleeper publishes no per-pick time (verified). `DraftPick.picked_at` is always `None` from this adapter and `draft_picks.picked_at` stays NULL. `drafts.started_at` comes from `start_time` (epoch ms, nullable before the draft opens).

---

### Task 1: `ffh.adapters.base` — Protocol, normalized models, error hierarchy, config

**Files:**
- Create: `backend/src/ffh/adapters/base.py`
- Modify: `backend/src/ffh/config.py`
- Create: `backend/tests/adapters/__init__.py` (empty)
- Test: `backend/tests/adapters/test_base_models.py`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - Exceptions `PlatformError(Exception)`, `PlatformAuthError(PlatformError)`, `PlatformNotFound(PlatformError)`.
  - Type aliases `Platform`, `ScoringFormat`, `LeagueType`, `DraftType`, `DraftStatus`, `TransactionType`, `RosterSlotName`.
  - Frozen models `ScoringSettings`, `RosterSettings`, `League`, `LeagueTeam`, `RosterEntry`, `Roster`, `Matchup`, `Transaction`, `PlayerRef`, `Draft`, `DraftPick`.
  - `FantasyPlatformAdapter` Protocol (verbatim from `docs/ARCHITECTURE.md`), `PlayerCatalog` Protocol.
  - `Settings.sleeper_mock_league_id: str | None`, `Settings.sleeper_user_id: str | None`, `Settings.sleeper_username: str | None`.

- [ ] **Step 1: Write the failing test `backend/tests/adapters/test_base_models.py`**

```python
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
    RosterSettings,
    ScoringSettings,
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


def test_scoring_format_is_standard_when_rec_absent():
    assert ScoringSettings(points={"rush_yd": 0.1}).format == "standard"


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


def test_protocol_is_runtime_checkable_and_lists_every_method():
    for name in (
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
    ):
        assert hasattr(FantasyPlatformAdapter, name), name
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd backend && uv run pytest tests/adapters/test_base_models.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'ffh.adapters.base'`.

- [ ] **Step 3: Write `backend/src/ffh/adapters/base.py`**

The Protocol block is copied **verbatim** from `docs/ARCHITECTURE.md` § "The platform adapter interface". Do not reorder, rename, or change a signature.

```python
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
TransactionType = Literal["add", "drop", "trade", "waiver"]
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
        rec = self.points.get("rec", 0.0)
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
    position: str
    team: str | None


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
```

- [ ] **Step 4: Extend `backend/src/ffh/config.py`**

Add three fields after `sleeper_base_url` (env `FFH_SLEEPER_MOCK_LEAGUE_ID`, `FFH_SLEEPER_USER_ID`, `FFH_SLEEPER_USERNAME`; all live in gitignored `backend/.env`, none required by CI):

```python
    sleeper_base_url: str = "https://api.sleeper.app/v1"
    # Chris's mock-draft league; used by the fixture recorder and manual smoke runs.
    # Not a secret, but it is personal — .env only, never committed.
    sleeper_mock_league_id: str | None = None
    # Identifies "my" team on a Sleeper league. Either is enough; user_id wins.
    sleeper_user_id: str | None = None
    sleeper_username: str | None = None
```

- [ ] **Step 5: Create `backend/src/ffh/adapters/__init__.py`** if it does not already exist (PR ① created the package; verify with `ls backend/src/ffh/adapters/`) and `backend/tests/adapters/__init__.py` (empty file).

- [ ] **Step 6: Run tests**

Run: `cd backend && uv run pytest tests/adapters/test_base_models.py -v`
Expected: PASS (12 tests).

- [ ] **Step 7: Lint**

Run: `cd backend && uv run ruff check . && uv run ruff format --check .`
Expected: clean.

- [ ] **Step 8: Commit**

```bash
git add backend/src/ffh/adapters/base.py backend/src/ffh/config.py backend/tests/adapters/
git commit -m "feat(adapters): platform adapter Protocol and normalized models"
```

---

### Task 2: `ffh.adapters.sleeper.models` — raw response models

**Files:**
- Create: `backend/src/ffh/adapters/sleeper/__init__.py` (empty)
- Create: `backend/src/ffh/adapters/sleeper/models.py`
- Test: `backend/tests/adapters/sleeper/__init__.py` (empty), `backend/tests/adapters/sleeper/test_raw_models.py`

**Interfaces:**
- Consumes: nothing from Task 1 — these mirror the wire, not the normalized layer.
- Produces: `RawState`, `RawUser`, `RawLeague`, `RawLeagueSettings`, `RawRoster`, `RawRosterSettings`, `RawDraft`, `RawDraftSettings`, `RawDraftPick`, `RawMatchup`, `RawTransaction`, `RawTransactionSettings`, `RawPlayer` — all with `model_config = ConfigDict(extra="ignore")`.

Every field below matches the live-verified record at the top of this plan. `extra="ignore"` is deliberate: Sleeper adds keys without notice, and a 132-key `scoring_settings` must never be enumerated field-by-field.

- [ ] **Step 1: Write the failing test `backend/tests/adapters/sleeper/test_raw_models.py`**

```python
from ffh.adapters.sleeper.models import (
    RawDraft,
    RawDraftPick,
    RawLeague,
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
            "metadata": {"amount": "64", "first_name": "Saquon", "last_name": "Barkley",
                         "position": "RB", "team": "PHI", "status": "Active"},
        }
    )
    assert p.is_keeper is None and p.metadata["amount"] == "64"


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
        {"transaction_id": "t2", "type": "trade", "status": "complete", "leg": 4,
         "adds": None, "drops": None, "settings": None}
    )
    assert t.adds == {} and t.drops == {} and t.settings is None


def test_player_def_entry_has_no_full_name_or_gsis():
    p = RawPlayer.model_validate(
        {"player_id": "KC", "position": "DEF", "first_name": "Kansas City",
         "last_name": "Chiefs", "team": "KC", "fantasy_positions": ["DEF"],
         "injury_status": None, "active": True, "sport": "nfl"}
    )
    assert p.full_name is None and p.gsis_id is None and p.position == "DEF"


def test_player_human_entry_ids_are_ints_on_the_wire():
    p = RawPlayer.model_validate(
        {"player_id": "4866", "full_name": "Saquon Barkley", "first_name": "Saquon",
         "last_name": "Barkley", "position": "RB", "fantasy_positions": ["RB"],
         "team": "PHI", "status": "Active", "active": True, "gsis_id": "00-0034844",
         "espn_id": 3929630, "yahoo_id": 30972, "rotowire_id": 12507,
         "sportradar_id": "9811b753-347c-467a-b3cb-85937e71e2b9",
         "birth_date": "1997-02-09", "college": "Penn State", "injury_status": None,
         "years_exp": 8, "number": 26, "search_rank": 13}
    )
    assert p.espn_id == 3929630 and p.gsis_id == "00-0034844"
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd backend && uv run pytest tests/adapters/sleeper/test_raw_models.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'ffh.adapters.sleeper'`.

- [ ] **Step 3: Write `backend/src/ffh/adapters/sleeper/models.py`**

Sleeper sends JSON `null` (not an absent key) for `players`, `starters`, `reserve`, `taxi`,
`co_owners`, `keepers`, `adds`, `drops`, `draft_order`, `slot_to_roster_id` and `metadata`.
`default_factory` only fires when a key is **absent**, so nullable collections use a
`BeforeValidator`. Downstream code must never branch on `None` vs `[]` for a collection.

```python
"""Raw Sleeper wire models.

Shapes verified live 2026-08-16 against https://api.sleeper.app/v1 — see the verification
record in docs/superpowers/plans/2026-08-15-phase0-05-adapter-sleeper.md.

Sleeper's API is READ-ONLY and its data is licensed for NON-COMMERCIAL use only.
extra="ignore" everywhere: Sleeper adds keys without notice.
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
StrDict = Annotated[dict[str, str], _none_to(dict)]
StrIntDict = Annotated[dict[str, int], _none_to(dict)]
StrFloatDict = Annotated[dict[str, float], _none_to(dict)]


class _Raw(BaseModel):
    model_config = ConfigDict(extra="ignore")


class RawState(_Raw):
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
    is_owner: bool | None = None
    league_id: str | None = None
    metadata: StrDict = Field(default_factory=dict)


class RawLeagueSettings(_Raw):
    num_teams: int
    playoff_teams: int | None = None
    playoff_week_start: int | None = None
    waiver_budget: int | None = None
    # 0 = rolling priority, 1 = reverse standings, 2 = FAAB
    waiver_type: int | None = None
    # 0 = redraft, 1 = keeper, 2 = dynasty
    type: int | None = None
    taxi_slots: int = 0
    reserve_slots: int = 0
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
    roster_positions: StrList = Field(default_factory=list)
    # Stored VERBATIM in leagues.scoring_settings. Never mutate, never fill in.
    scoring_settings: StrFloatDict = Field(default_factory=dict)
    settings: RawLeagueSettings
    metadata: StrDict = Field(default_factory=dict)


class RawRosterSettings(_Raw):
    wins: int | None = None
    losses: int | None = None
    ties: int | None = None
    fpts: int | None = None
    fpts_decimal: int | None = None
    waiver_budget_used: int = 0
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
    settings: RawRosterSettings = Field(default_factory=RawRosterSettings)
    metadata: StrDict = Field(default_factory=dict)


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
    type: str
    status: str
    season: str | None = None
    season_type: str | None = None
    settings: RawDraftSettings
    metadata: StrDict = Field(default_factory=dict)
    # user_id -> draft slot. Null before the order is set.
    draft_order: StrIntDict = Field(default_factory=dict)
    # draft slot (STRING key) -> roster_id. Only present on GET /draft/{id}.
    slot_to_roster_id: StrIntDict = Field(default_factory=dict)
    # EPOCH MILLISECONDS. Null before the first pick.
    last_picked: int | None = None
    start_time: int | None = None
    created: int | None = None
    creators: StrList = Field(default_factory=list)


class RawDraftPick(_Raw):
    draft_id: str | None = None
    pick_no: int
    round: int
    draft_slot: int
    roster_id: int | None = None
    picked_by: str | None = None
    player_id: str | None = None
    is_keeper: bool | None = None
    # metadata["amount"] is the auction bid AS A STRING.
    metadata: StrDict = Field(default_factory=dict)


class RawMatchup(_Raw):
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
    type: str
    status: str | None = None
    leg: int | None = None
    created: int | None = None
    status_updated: int | None = None
    creator: str | None = None
    # player_id -> roster_id
    adds: StrIntDict = Field(default_factory=dict)
    drops: StrIntDict = Field(default_factory=dict)
    roster_ids: IntList = Field(default_factory=list)
    consenter_ids: IntList = Field(default_factory=list)
    settings: RawTransactionSettings | None = None
    metadata: StrDict = Field(default_factory=dict)


class RawPlayer(_Raw):
    player_id: str
    # DEF entries have NO full_name and NO gsis_id.
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
```

- [ ] **Step 4: Run tests**

Run: `cd backend && uv run pytest tests/adapters/sleeper/test_raw_models.py -v`
Expected: PASS (10 tests). If `test_roster_null_collections_become_empty` fails, a field is
missing its `_none_to` annotation.

- [ ] **Step 5: Lint and commit**

```bash
cd backend && uv run ruff check . && uv run ruff format .
cd .. && git add backend/src/ffh/adapters/sleeper backend/tests/adapters/sleeper
git commit -m "feat(adapters): Sleeper raw response models from live-verified shapes"
```

---

### Task 3: `ffh.adapters.ratelimit` — deterministic async token bucket

**Files:**
- Create: `backend/src/ffh/adapters/ratelimit.py`
- Test: `backend/tests/adapters/test_ratelimit.py`

**Interfaces:**
- Consumes: nothing.
- Produces: `TokenBucket(rate_per_min: int = 300, burst: int = 30, *, clock: Callable[[], float] = time.monotonic, sleep: Callable[[float], Awaitable[None]] = asyncio.sleep)`; `async def acquire(self, n: int = 1) -> None`; read-only property `tokens: float`.

Lives at `ffh/adapters/ratelimit.py` rather than under `sleeper/` because ESPN needs the
same thing in Phase 2. It has no Sleeper import.

- [ ] **Step 1: Write the failing test `backend/tests/adapters/test_ratelimit.py`**

The clock and sleep are injected, so this test is deterministic and consumes no wall time.

```python
import pytest

from ffh.adapters.ratelimit import TokenBucket


class FakeClock:
    """Monotonic clock that only advances when the fake sleep is awaited."""

    def __init__(self) -> None:
        self.now = 1000.0
        self.slept: list[float] = []

    def __call__(self) -> float:
        return self.now

    async def sleep(self, seconds: float) -> None:
        assert seconds >= 0
        self.slept.append(seconds)
        self.now += seconds


def _bucket(clock: FakeClock, rate: int = 300, burst: int = 30) -> TokenBucket:
    return TokenBucket(rate_per_min=rate, burst=burst, clock=clock, sleep=clock.sleep)


async def test_burst_is_served_without_sleeping():
    clock = FakeClock()
    bucket = _bucket(clock)
    for _ in range(30):
        await bucket.acquire()
    assert clock.slept == []
    assert bucket.tokens == pytest.approx(0.0)


async def test_the_31st_call_in_a_burst_waits_for_one_refill():
    clock = FakeClock()
    bucket = _bucket(clock)
    for _ in range(31):
        await bucket.acquire()
    # 300/min == 5 tokens/s == 0.2s per token
    assert clock.slept == [pytest.approx(0.2)]


async def test_never_exceeds_budget_over_a_sustained_burst():
    """100 calls at 300/min with burst 30 must span exactly (100-30)/5 seconds."""
    clock = FakeClock()
    bucket = _bucket(clock)
    start = clock.now
    for _ in range(100):
        await bucket.acquire()
    elapsed = clock.now - start
    assert elapsed == pytest.approx((100 - 30) / 5.0)


async def test_tokens_refill_while_idle_and_cap_at_burst():
    clock = FakeClock()
    bucket = _bucket(clock)
    for _ in range(30):
        await bucket.acquire()
    clock.now += 3600.0  # an hour idle
    assert bucket.tokens == pytest.approx(30.0)  # capped, not 18000
    for _ in range(30):
        await bucket.acquire()
    assert clock.slept == []


async def test_acquire_more_than_one_token():
    clock = FakeClock()
    bucket = _bucket(clock)
    await bucket.acquire(30)
    assert bucket.tokens == pytest.approx(0.0)
    await bucket.acquire(5)
    assert clock.slept == [pytest.approx(1.0)]


async def test_rejects_a_request_larger_than_the_bucket():
    clock = FakeClock()
    bucket = _bucket(clock)
    with pytest.raises(ValueError):
        await bucket.acquire(31)


def test_rejects_nonsense_construction():
    with pytest.raises(ValueError):
        TokenBucket(rate_per_min=300, burst=0)
    with pytest.raises(ValueError):
        TokenBucket(rate_per_min=0, burst=30)
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd backend && uv run pytest tests/adapters/test_ratelimit.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'ffh.adapters.ratelimit'`.

- [ ] **Step 3: Write `backend/src/ffh/adapters/ratelimit.py`**

```python
"""Client-side rate limiting for read-only platform APIs.

Sleeper's ceiling is 1000 req/min and IP-BASED — no key identifies us, so a block hits
everyone behind the same address. We hold 300 req/min (30% of ceiling) with a burst of 30,
still ~150x the 1-2s draft-poll budget (docs/ARCHITECTURE.md § Latency budgets).
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable


class TokenBucket:
    """Async token bucket. Clock and sleep are injected so tests are deterministic."""

    def __init__(
        self,
        rate_per_min: int = 300,
        burst: int = 30,
        *,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        if rate_per_min <= 0:
            raise ValueError("rate_per_min must be positive")
        if burst <= 0:
            raise ValueError("burst must be positive")
        self._rate_per_sec = rate_per_min / 60.0
        self._capacity = float(burst)
        self._clock = clock
        self._sleep = sleep
        self._tokens = float(burst)
        self._updated = clock()
        self._lock = asyncio.Lock()

    def _refill(self) -> None:
        now = self._clock()
        elapsed = max(0.0, now - self._updated)
        self._updated = now
        self._tokens = min(self._capacity, self._tokens + elapsed * self._rate_per_sec)

    @property
    def tokens(self) -> float:
        """Current allowance, refilled to `now`. Capped at `burst`."""
        self._refill()
        return self._tokens

    async def acquire(self, n: int = 1) -> None:
        """Block until `n` tokens are available, then consume them."""
        if n <= 0:
            raise ValueError("n must be positive")
        if n > self._capacity:
            raise ValueError(f"cannot acquire {n} tokens from a bucket of {self._capacity}")
        async with self._lock:
            self._refill()
            deficit = n - self._tokens
            if deficit > 0:
                await self._sleep(deficit / self._rate_per_sec)
                self._refill()
            self._tokens -= n
```

`asyncio.Lock()` serialises `acquire` so concurrent callers cannot observe the same tokens
and double-spend the budget. Constructing it in `__init__` needs no running loop on 3.13.

- [ ] **Step 4: Run tests**

Run: `cd backend && uv run pytest tests/adapters/test_ratelimit.py -v`
Expected: PASS (7 tests), in well under a second — no real sleeping happens.

- [ ] **Step 5: Lint and commit**

```bash
cd backend && uv run ruff check . && uv run ruff format .
cd .. && git add backend/src/ffh/adapters/ratelimit.py backend/tests/adapters/test_ratelimit.py
git commit -m "feat(adapters): deterministic async token bucket capped at 300 req/min"
```

---

### Task 4: Fixture corpus + `ffh.adapters.sleeper.client`

**Files:**
- Create: `backend/tests/fixtures/sleeper/README.md`
- Create: `backend/tests/fixtures/sleeper/{state_nfl,league,rosters,users,league_drafts,draft,draft_picks,matchups_week1,transactions_week1,players_slice}.json`
- Create: `backend/src/ffh/adapters/sleeper/client.py`
- Modify: `backend/tests/conftest.py`
- Test: `backend/tests/adapters/sleeper/test_client.py`

**Interfaces:**
- Consumes: `ffh.adapters.base.{PlatformError, PlatformAuthError, PlatformNotFound}` (Task 1); every `Raw*` model (Task 2); `ffh.adapters.ratelimit.TokenBucket` (Task 3).
- Produces:
  - `SleeperClient(base_url: str | None = None, http: httpx.AsyncClient | None = None, rate: TokenBucket | None = None, *, timeout: float = 10.0, retry_sleep: Callable[[float], Awaitable[None]] | None = None)`.
  - `async get_json(path: str) -> Any`; `get_state() -> RawState`; `get_user(username_or_id: str) -> RawUser`; `get_user_leagues(user_id: str, season: int) -> list[RawLeague]`; `get_league(league_id: str) -> RawLeague`; `get_rosters(league_id: str) -> list[RawRoster]`; `get_users(league_id: str) -> list[RawUser]`; `get_matchups(league_id: str, week: int) -> list[RawMatchup]`; `get_transactions(league_id: str, week: int) -> list[RawTransaction]`; `get_league_drafts(league_id: str) -> list[RawDraft]`; `get_draft(draft_id: str) -> RawDraft`; `get_draft_picks(draft_id: str) -> list[RawDraftPick]`; `aclose()`; async context manager.
  - Test helpers in `backend/tests/conftest.py`: `FIXTURE_LEAGUE_ID`, `FIXTURE_DRAFT_ID`, `sleeper_fixture(name)` fixture, `sleeper_mock` fixture (a `respx.MockRouter` with every endpoint wired to the fixture corpus).

**`/players/nfl` is deliberately absent from this client.** It is 14.6 MB and is only ever
fetched by the `sleeper_players` `IngestJob` (Task 6). Adding a client method for it would
invite a caller to put it on the request path.

- [ ] **Step 1: Create the hand-written minimal fixture corpus**

These are authored now so every test in Tasks 4–10 runs before Chris records the real mock
league (Task 9 overwrites them with real recordings). The shapes match the live-verified
record exactly. The league is deliberately tiny (2 teams) but exercises every branch:
`SUPER_FLEX`, the `"0"` empty-starter placeholder, `reserve` (IR), `taxi`, a `DEF`, a team
with no `metadata.team_name`, an auction amount, and a keeper.

Create `backend/tests/fixtures/sleeper/state_nfl.json`:

```json
{"week": 1, "leg": 1, "season": "2026", "season_type": "regular", "display_week": 1,
 "league_season": "2026", "previous_season": "2025", "season_start_date": "2026-08-06",
 "season_has_scores": true}
```

Create `backend/tests/fixtures/sleeper/league.json`:

```json
{
  "league_id": "1000000000000000001",
  "name": "FFH Fixture League",
  "season": "2026",
  "season_type": "regular",
  "sport": "nfl",
  "status": "in_season",
  "total_rosters": 2,
  "draft_id": "2000000000000000001",
  "previous_league_id": null,
  "avatar": null,
  "shard": 1,
  "metadata": {"auto_continue": "on"},
  "roster_positions": ["QB","RB","RB","WR","WR","TE","FLEX","SUPER_FLEX","K","DEF","BN","BN","BN"],
  "scoring_settings": {"pass_yd": 0.04, "pass_td": 4.0, "pass_int": -1.0, "rush_yd": 0.1,
    "rush_td": 6.0, "rec": 0.5, "rec_yd": 0.1, "rec_td": 6.0, "fum_lost": -2.0,
    "pass_2pt": 2.0, "rec_2pt": 2.0, "rush_2pt": 2.0, "fgm_0_19": 3.0, "fgm_30_39": 3.0,
    "xpm": 1.0, "sack": 1.0, "int": 2.0, "pts_allow_0": 10.0, "fgm_yds_over_30": 0.0},
  "settings": {"num_teams": 2, "playoff_teams": 2, "playoff_week_start": 15,
    "waiver_budget": 100, "waiver_type": 2, "type": 0, "taxi_slots": 1, "reserve_slots": 1,
    "draft_rounds": 13, "max_keepers": 1, "start_week": 1, "leg": 1, "trade_deadline": 11}
}
```

Create `backend/tests/fixtures/sleeper/rosters.json`:

```json
[
  {"roster_id": 1, "league_id": "1000000000000000001", "owner_id": "USER_ME",
   "co_owners": null, "keepers": null, "player_map": null,
   "starters": ["1","2","3","4","5","6","7","8","9","KC"],
   "players": ["1","2","3","4","5","6","7","8","9","KC","10","11","12"],
   "reserve": ["12"], "taxi": ["11"],
   "metadata": {"record": "W", "streak": "1W"},
   "settings": {"wins": 1, "losses": 0, "ties": 0, "fpts": 100, "fpts_decimal": 5,
     "waiver_budget_used": 25, "waiver_position": 1, "total_moves": 2}},
  {"roster_id": 2, "league_id": "1000000000000000001", "owner_id": "USER_OPP",
   "co_owners": null, "keepers": null, "player_map": null,
   "starters": ["13","14","15","16","17","18","19","0","20","SF"],
   "players": ["13","14","15","16","17","18","19","20","SF","21"],
   "reserve": null, "taxi": null, "metadata": null,
   "settings": {"wins": 0, "losses": 1, "ties": 0, "fpts": 88, "fpts_decimal": 0,
     "waiver_budget_used": 0, "waiver_position": 2, "total_moves": 0}}
]
```

Create `backend/tests/fixtures/sleeper/users.json`:

```json
[
  {"user_id": "USER_ME", "display_name": "chris", "avatar": null, "is_bot": null,
   "is_owner": true, "league_id": "1000000000000000001", "settings": null,
   "metadata": {"team_name": "Fixture Me"}},
  {"user_id": "USER_OPP", "display_name": "opponent", "avatar": null, "is_bot": null,
   "is_owner": false, "league_id": "1000000000000000001", "settings": null, "metadata": {}}
]
```

Create `backend/tests/fixtures/sleeper/draft.json` (this is `GET /draft/{id}` — note
`slot_to_roster_id`):

```json
{
  "draft_id": "2000000000000000001",
  "league_id": "1000000000000000001",
  "type": "snake",
  "status": "complete",
  "season": "2026",
  "season_type": "regular",
  "sport": "nfl",
  "created": 1745797560914,
  "creators": ["USER_ME"],
  "start_time": 1756074607722,
  "last_picked": 1756083970192,
  "draft_order": {"USER_ME": 1, "USER_OPP": 2},
  "slot_to_roster_id": {"1": 1, "2": 2},
  "metadata": {"name": "FFH Fixture League", "scoring_type": "half_ppr"},
  "settings": {"rounds": 13, "teams": 2, "budget": 0, "pick_timer": 30,
    "slots_qb": 1, "slots_rb": 2, "slots_wr": 2, "slots_te": 1, "slots_flex": 1,
    "slots_super_flex": 1, "slots_k": 1, "slots_def": 1, "slots_bn": 3}
}
```

Create `backend/tests/fixtures/sleeper/league_drafts.json` — a one-element array holding
exactly the object above **with `slot_to_roster_id` removed** (that is what
`GET /league/{id}/drafts` returns).

Create `backend/tests/fixtures/sleeper/draft_picks.json`:

```json
[
  {"draft_id": "2000000000000000001", "pick_no": 1, "round": 1, "draft_slot": 1,
   "roster_id": 1, "picked_by": "USER_ME", "player_id": "1", "is_keeper": null,
   "reactions": null,
   "metadata": {"first_name": "Fixture", "last_name": "Quarterback", "position": "QB",
     "team": "KC", "status": "Active", "injury_status": "", "number": "1",
     "player_id": "1", "slot": "1", "sport": "nfl", "years_exp": "3", "amount": "0"}},
  {"draft_id": "2000000000000000001", "pick_no": 2, "round": 1, "draft_slot": 2,
   "roster_id": 2, "picked_by": "USER_OPP", "player_id": "13", "is_keeper": null,
   "reactions": null,
   "metadata": {"first_name": "Fixture", "last_name": "Quarterbacktwo", "position": "QB",
     "team": "SF", "status": "Active", "player_id": "13", "amount": "0"}},
  {"draft_id": "2000000000000000001", "pick_no": 3, "round": 2, "draft_slot": 2,
   "roster_id": 2, "picked_by": "USER_OPP", "player_id": "14", "is_keeper": true,
   "reactions": null,
   "metadata": {"first_name": "Fixture", "last_name": "Runningbackfour", "position": "RB",
     "team": "SF", "status": "Active", "player_id": "14", "amount": "0"}},
  {"draft_id": "2000000000000000001", "pick_no": 4, "round": 2, "draft_slot": 1,
   "roster_id": 1, "picked_by": "USER_ME", "player_id": "2", "is_keeper": null,
   "reactions": null,
   "metadata": {"first_name": "Fixture", "last_name": "Runningback", "position": "RB",
     "team": "KC", "status": "Active", "player_id": "2", "amount": "12"}}
]
```

Create `backend/tests/fixtures/sleeper/matchups_week1.json`:

```json
[
  {"roster_id": 1, "matchup_id": 1, "points": 100.5, "custom_points": null,
   "starters": ["1","2","3","4","5","6","7","8","9","KC"],
   "starters_points": [20.1, 15.0, 10.0, 12.4, 9.0, 6.0, 8.0, 12.0, 5.0, 3.0],
   "players": ["1","2","3","4","5","6","7","8","9","KC","10","11","12"],
   "players_points": {"1": 20.1, "10": 0.0}},
  {"roster_id": 2, "matchup_id": 1, "points": 88.0, "custom_points": null,
   "starters": ["13","14","15","16","17","18","19","0","20","SF"],
   "starters_points": [18.0, 14.0, 9.0, 11.0, 8.0, 5.0, 7.0, 0.0, 4.0, 12.0],
   "players": ["13","14","15","16","17","18","19","20","SF","21"],
   "players_points": {"13": 18.0}}
]
```

Create `backend/tests/fixtures/sleeper/transactions_week1.json`:

```json
[
  {"transaction_id": "TXN1", "type": "waiver", "status": "complete", "leg": 1,
   "created": 1758684054016, "status_updated": 1758698028886, "creator": "USER_ME",
   "adds": {"90": 1}, "drops": {"10": 1}, "roster_ids": [1], "consenter_ids": [1],
   "draft_picks": [], "waiver_budget": [],
   "settings": {"seq": 1, "waiver_bid": 25}, "metadata": {"notes": "ok"}},
  {"transaction_id": "TXN2", "type": "free_agent", "status": "complete", "leg": 1,
   "created": 1758684154016, "status_updated": 1758684154016, "creator": "USER_OPP",
   "adds": null, "drops": {"21": 2}, "roster_ids": [2], "consenter_ids": [2],
   "draft_picks": [], "waiver_budget": [], "settings": null, "metadata": null}
]
```

Create `backend/tests/fixtures/sleeper/players_slice.json` by running this from `backend/`
(it emits all 25 entries from an explicit table — 21 humans, 2 defenses, 2 free agents):

```bash
uv run python - <<'PY'
import json, pathlib

HUMANS = [
    ("1", "Fixture", "Quarterback", "QB", "KC"),
    ("2", "Fixture", "Runningback", "RB", "KC"),
    ("3", "Fixture", "Runningbacktwo", "RB", "BUF"),
    ("4", "Fixture", "Receiver", "WR", "KC"),
    ("5", "Fixture", "Receivertwo", "WR", "BUF"),
    ("6", "Fixture", "Tightend", "TE", "KC"),
    ("7", "Fixture", "Runningbackthree", "RB", "DAL"),
    ("8", "Fixture", "Quarterbacktwoflex", "QB", "BUF"),
    ("9", "Fixture", "Kicker", "K", "KC"),
    ("10", "Fixture", "Receiverthree", "WR", "DAL"),
    ("11", "Fixture", "Runningbacktaxi", "RB", "DAL"),
    ("12", "Fixture", "Receiverinjured", "WR", "BUF"),
    ("13", "Fixture", "Quarterbacktwo", "QB", "SF"),
    ("14", "Fixture", "Runningbackfour", "RB", "SF"),
    ("15", "Fixture", "Runningbackfive", "RB", "PHI"),
    ("16", "Fixture", "Receiverfour", "WR", "SF"),
    ("17", "Fixture", "Receiverfive", "WR", "PHI"),
    ("18", "Fixture", "Tightendtwo", "TE", "SF"),
    ("19", "Fixture", "Receiversix", "WR", "DET"),
    ("20", "Fixture", "Kickertwo", "K", "SF"),
    ("21", "Fixture", "Tightendthree", "TE", "PHI"),
    ("90", "Fixture", "Freeagentwr", "WR", "DET"),
    ("91", "Fixture", "Freeagentqb", "QB", "DET"),
]
DEFENSES = [("KC", "Kansas City", "Chiefs"), ("SF", "San Francisco", "49ers")]

out = {}
for i, (pid, first, last, pos, team) in enumerate(HUMANS, start=1):
    out[pid] = {
        "player_id": pid, "first_name": first, "last_name": last,
        "full_name": f"{first} {last}", "search_full_name": f"{first}{last}".lower(),
        "position": pos, "fantasy_positions": [pos], "team": team,
        "status": "Active", "active": True, "injury_status": None,
        "gsis_id": f"00-009{i:04d}", "espn_id": 9000000 + i, "yahoo_id": 90000 + i,
        "rotowire_id": 91000 + i, "fantasy_data_id": 92000 + i,
        "sportradar_id": f"aaaaaaaa-0000-0000-0000-{i:012d}",
        "birth_date": "1998-01-01", "college": "Fixture State",
        "years_exp": 3, "number": i, "depth_chart_order": 1,
        "depth_chart_position": pos, "search_rank": i,
    }
for abbr, first, last in DEFENSES:
    out[abbr] = {
        "player_id": abbr, "position": "DEF", "first_name": first, "last_name": last,
        "team": abbr, "fantasy_positions": ["DEF"], "injury_status": None,
        "active": True, "sport": "nfl",
    }

p = pathlib.Path("tests/fixtures/sleeper/players_slice.json")
p.parent.mkdir(parents=True, exist_ok=True)
p.write_text(json.dumps(out, indent=1, sort_keys=True) + "\n", encoding="utf-8")
print(len(out), "players ->", p)
PY
```

Expected output: `25 players -> tests/fixtures/sleeper/players_slice.json`.

Create `backend/tests/fixtures/sleeper/README.md`:

```markdown
# Sleeper fixtures

Recorded responses from `https://api.sleeper.app/v1`. **CI never touches the network** —
every test drives these through `respx` mounted on `settings.sleeper_base_url`.

Source league: hand-written placeholder (`league_id=1000000000000000001`,
`draft_id=2000000000000000001`), authored 2026-08-16 to match live-verified shapes.
Replace with a real recording by running, from `backend/`:

    FFH_SLEEPER_MOCK_LEAGUE_ID=<id> uv run python scripts/record_sleeper_fixtures.py

That script rewrites every file here and updates the "Source league" line above.

Sleeper data is licensed for **non-commercial use only**. These fixtures exist solely to
test this self-hosted personal project.

| File | Endpoint |
|---|---|
| `state_nfl.json` | `GET /state/nfl` |
| `league.json` | `GET /league/{id}` |
| `rosters.json` | `GET /league/{id}/rosters` |
| `users.json` | `GET /league/{id}/users` |
| `league_drafts.json` | `GET /league/{id}/drafts` |
| `draft.json` | `GET /draft/{id}` |
| `draft_picks.json` | `GET /draft/{id}/picks` |
| `matchups_week1.json` | `GET /league/{id}/matchups/1` |
| `transactions_week1.json` | `GET /league/{id}/transactions/1` |
| `players_slice.json` | `GET /players/nfl`, restricted to rostered players plus two free agents |
```

- [ ] **Step 2: Write the failing test `backend/tests/adapters/sleeper/test_client.py`**

```python
import httpx
import pytest
import respx

from ffh.adapters.base import PlatformAuthError, PlatformError, PlatformNotFound
from ffh.adapters.ratelimit import TokenBucket
from ffh.adapters.sleeper.client import SleeperClient

BASE = "https://api.sleeper.app/v1"


async def _noop_sleep(_seconds: float) -> None:
    return None


def _client(**kw) -> SleeperClient:
    kw.setdefault("base_url", BASE)
    kw.setdefault("retry_sleep", _noop_sleep)
    return SleeperClient(**kw)


async def test_get_state_parses(sleeper_mock):
    async with _client() as client:
        state = await client.get_state()
    assert state.season == "2026" and state.season_type == "regular" and state.week == 1


async def test_league_rosters_users_drafts_picks(sleeper_mock, sleeper_fixture):
    async with _client() as client:
        league = await client.get_league("1000000000000000001")
        rosters = await client.get_rosters("1000000000000000001")
        users = await client.get_users("1000000000000000001")
        drafts = await client.get_league_drafts("1000000000000000001")
        draft = await client.get_draft("2000000000000000001")
        picks = await client.get_draft_picks("2000000000000000001")
    assert league.settings.num_teams == 2
    assert league.scoring_settings == sleeper_fixture("league")["scoring_settings"]
    assert [r.roster_id for r in rosters] == [1, 2]
    assert {u.user_id for u in users} == {"USER_ME", "USER_OPP"}
    assert len(drafts) == 1 and drafts[0].slot_to_roster_id == {}
    assert draft.slot_to_roster_id == {"1": 1, "2": 2}
    assert draft.last_picked == 1756083970192
    assert [p.pick_no for p in picks] == [1, 2, 3, 4]


async def test_matchups_and_transactions(sleeper_mock):
    async with _client() as client:
        matchups = await client.get_matchups("1000000000000000001", 1)
        txns = await client.get_transactions("1000000000000000001", 1)
    assert [m.roster_id for m in matchups] == [1, 2]
    assert {t.transaction_id for t in txns} == {"TXN1", "TXN2"}


@respx.mock
async def test_404_raises_platform_not_found():
    respx.get(f"{BASE}/league/nope").mock(return_value=httpx.Response(404))
    async with _client() as client:
        with pytest.raises(PlatformNotFound):
            await client.get_json("/league/nope")


@respx.mock
async def test_401_raises_platform_auth_error():
    respx.get(f"{BASE}/league/x").mock(return_value=httpx.Response(401))
    async with _client() as client:
        with pytest.raises(PlatformAuthError):
            await client.get_json("/league/x")


@respx.mock
async def test_retries_a_500_then_succeeds():
    route = respx.get(f"{BASE}/state/nfl").mock(
        side_effect=[httpx.Response(500), httpx.Response(200, json={"ok": True})]
    )
    async with _client() as client:
        assert await client.get_json("/state/nfl") == {"ok": True}
    assert route.call_count == 2


@respx.mock
async def test_retries_a_429_then_succeeds():
    route = respx.get(f"{BASE}/state/nfl").mock(
        side_effect=[
            httpx.Response(429, headers={"Retry-After": "1"}),
            httpx.Response(200, json={"ok": True}),
        ]
    )
    async with _client() as client:
        assert await client.get_json("/state/nfl") == {"ok": True}
    assert route.call_count == 2


@respx.mock
async def test_gives_up_after_five_attempts_and_raises_platform_error():
    route = respx.get(f"{BASE}/state/nfl").mock(return_value=httpx.Response(503))
    async with _client() as client:
        with pytest.raises(PlatformError):
            await client.get_json("/state/nfl")
    assert route.call_count == 5


@respx.mock
async def test_every_request_spends_a_rate_limit_token():
    respx.get(f"{BASE}/state/nfl").mock(return_value=httpx.Response(200, json={}))
    bucket = TokenBucket(rate_per_min=300, burst=30)
    async with _client(rate=bucket) as client:
        before = bucket.tokens
        await client.get_json("/state/nfl")
        assert bucket.tokens < before


def test_default_rate_is_300_per_minute_burst_30():
    client = _client()
    assert client.rate.tokens == pytest.approx(30.0)
```

- [ ] **Step 3: Extend `backend/tests/conftest.py` with the shared fixture loader and respx router**

Two edits to the existing file (do not remove `migrated_engine` / `db_session`).

First, the **imports go into the existing import block at the top of the file** — never
after code, or ruff `E402` (module-level import not at top) fails the lint gate. The
existing block already has `subprocess`, `sys`, `Path`, `pytest`, `text`, `Session`,
`get_settings`, `make_engine`, `assert_test_database`; merge these in, keeping ruff's
isort grouping (stdlib / third-party / first-party):

```python
import json
import subprocess
import sys
from pathlib import Path

import httpx
import pytest
import respx
from sqlalchemy import text
from sqlalchemy.orm import Session

from ffh.config import get_settings
from ffh.db.engine import make_engine
from tests.db._guard import assert_test_database
```

Second, append the fixture loader and router **below** the existing fixtures:

```python
FIXTURE_LEAGUE_ID = "1000000000000000001"
FIXTURE_DRAFT_ID = "2000000000000000001"
SLEEPER_FIXTURES = BACKEND_DIR / "tests" / "fixtures" / "sleeper"


def load_sleeper_fixture(name: str):
    """Load one recorded Sleeper response by file stem."""
    return json.loads((SLEEPER_FIXTURES / f"{name}.json").read_text(encoding="utf-8"))


@pytest.fixture
def sleeper_fixture():
    return load_sleeper_fixture


@pytest.fixture
def sleeper_mock():
    """respx router serving the fixture league. CI never touches the network."""
    base = get_settings().sleeper_base_url
    lid, did = FIXTURE_LEAGUE_ID, FIXTURE_DRAFT_ID
    routes = {
        "/state/nfl": "state_nfl",
        f"/league/{lid}": "league",
        f"/league/{lid}/rosters": "rosters",
        f"/league/{lid}/users": "users",
        f"/league/{lid}/drafts": "league_drafts",
        f"/draft/{did}": "draft",
        f"/draft/{did}/picks": "draft_picks",
        f"/league/{lid}/matchups/1": "matchups_week1",
        f"/league/{lid}/transactions/1": "transactions_week1",
        "/players/nfl": "players_slice",
    }
    with respx.mock(base_url=base, assert_all_called=False) as router:
        for path, name in routes.items():
            router.get(path).mock(
                return_value=httpx.Response(200, json=load_sleeper_fixture(name))
            )
        yield router
```

- [ ] **Step 4: Run to verify it fails**

Run: `cd backend && uv run pytest tests/adapters/sleeper/test_client.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'ffh.adapters.sleeper.client'`.

- [ ] **Step 5: Write `backend/src/ffh/adapters/sleeper/client.py`**

```python
"""Thin in-house async Sleeper client.

No auth, no key — and therefore an IP-BASED 1000 req/min ceiling. We hold 300 req/min.
Sleeper is READ-ONLY and non-commercial-use-only (docs/DATA_SOURCES.md §3).

GET /players/nfl is deliberately NOT exposed here: it is 14.6 MB and belongs to the
`sleeper_players` IngestJob, at most once a day.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

import httpx
from tenacity import (
    AsyncRetrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)

from ffh.adapters.base import PlatformAuthError, PlatformError, PlatformNotFound
from ffh.adapters.ratelimit import TokenBucket
from ffh.adapters.sleeper.models import (
    RawDraft,
    RawDraftPick,
    RawLeague,
    RawMatchup,
    RawRoster,
    RawState,
    RawTransaction,
    RawUser,
)
from ffh.config import get_settings


class _Retryable(PlatformError):
    """429 or 5xx — worth another attempt. Never escapes get_json()."""


class SleeperClient:
    def __init__(
        self,
        base_url: str | None = None,
        http: httpx.AsyncClient | None = None,
        rate: TokenBucket | None = None,
        *,
        timeout: float = 10.0,
        retry_sleep: Callable[[float], Awaitable[None]] | None = None,
    ) -> None:
        self.base_url = (base_url or get_settings().sleeper_base_url).rstrip("/")
        self._owns_http = http is None
        self._http = http or httpx.AsyncClient(base_url=self.base_url, timeout=timeout)
        self.rate = rate or TokenBucket(rate_per_min=300, burst=30)
        self._retry_sleep = retry_sleep

    async def __aenter__(self) -> SleeperClient:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        if self._owns_http:
            await self._http.aclose()

    async def _get_once(self, path: str) -> Any:
        await self.rate.acquire()
        resp = await self._http.get(path)
        code = resp.status_code
        if code == 404:
            raise PlatformNotFound(f"sleeper 404 for {path}")
        if code in (401, 403):
            raise PlatformAuthError(f"sleeper {code} for {path}")
        if code == 429 or code >= 500:
            raise _Retryable(f"sleeper {code} for {path}")
        if code >= 400:
            raise PlatformError(f"sleeper {code} for {path}")
        return resp.json()

    async def get_json(self, path: str) -> Any:
        retry_kwargs: dict[str, Any] = {
            "retry": retry_if_exception_type((_Retryable, httpx.TransportError)),
            "stop": stop_after_attempt(5),
            "wait": wait_exponential_jitter(initial=0.5, max=8.0),
            "reraise": True,
        }
        if self._retry_sleep is not None:
            retry_kwargs["sleep"] = self._retry_sleep
        retryer = AsyncRetrying(**retry_kwargs)
        try:
            return await retryer(self._get_once, path)
        except _Retryable as exc:
            raise PlatformError(f"sleeper unavailable after 5 attempts: {path}") from exc
        except httpx.TransportError as exc:
            raise PlatformError(f"sleeper transport failure: {path}") from exc

    # --- endpoints -------------------------------------------------------------
    async def get_state(self) -> RawState:
        return RawState.model_validate(await self.get_json("/state/nfl"))

    async def get_user(self, username_or_id: str) -> RawUser:
        return RawUser.model_validate(await self.get_json(f"/user/{username_or_id}"))

    async def get_user_leagues(self, user_id: str, season: int) -> list[RawLeague]:
        payload = await self.get_json(f"/user/{user_id}/leagues/nfl/{season}")
        return [RawLeague.model_validate(x) for x in payload]

    async def get_league(self, league_id: str) -> RawLeague:
        return RawLeague.model_validate(await self.get_json(f"/league/{league_id}"))

    async def get_rosters(self, league_id: str) -> list[RawRoster]:
        payload = await self.get_json(f"/league/{league_id}/rosters")
        return [RawRoster.model_validate(x) for x in payload]

    async def get_users(self, league_id: str) -> list[RawUser]:
        payload = await self.get_json(f"/league/{league_id}/users")
        return [RawUser.model_validate(x) for x in payload]

    async def get_matchups(self, league_id: str, week: int) -> list[RawMatchup]:
        payload = await self.get_json(f"/league/{league_id}/matchups/{week}")
        return [RawMatchup.model_validate(x) for x in payload]

    async def get_transactions(self, league_id: str, week: int) -> list[RawTransaction]:
        payload = await self.get_json(f"/league/{league_id}/transactions/{week}")
        return [RawTransaction.model_validate(x) for x in payload]

    async def get_league_drafts(self, league_id: str) -> list[RawDraft]:
        payload = await self.get_json(f"/league/{league_id}/drafts")
        return [RawDraft.model_validate(x) for x in payload]

    async def get_draft(self, draft_id: str) -> RawDraft:
        return RawDraft.model_validate(await self.get_json(f"/draft/{draft_id}"))

    async def get_draft_picks(self, draft_id: str) -> list[RawDraftPick]:
        payload = await self.get_json(f"/draft/{draft_id}/picks")
        return [RawDraftPick.model_validate(x) for x in payload]
```

- [ ] **Step 6: Run tests**

Run: `cd backend && uv run pytest tests/adapters -v`
Expected: PASS (all of Tasks 1–4). The retry tests must complete instantly — if one hangs,
`retry_sleep` is not being passed through to `AsyncRetrying(sleep=...)`.

- [ ] **Step 7: Lint and commit**

```bash
cd backend && uv run ruff check . && uv run ruff format .
cd .. && git add backend/src/ffh/adapters/sleeper/client.py backend/tests/conftest.py \
  backend/tests/adapters/sleeper/test_client.py backend/tests/fixtures/sleeper
git commit -m "feat(adapters): async Sleeper HTTP client with backoff and fixture corpus"
```

---

### Task 5: `ffh.adapters.sleeper.adapter` — raw → normalized mapping

**Files:**
- Create: `backend/src/ffh/adapters/sleeper/catalog.py`
- Create: `backend/src/ffh/adapters/sleeper/adapter.py`
- Test: `backend/tests/adapters/sleeper/test_adapter.py`

**Interfaces:**
- Consumes: everything from Tasks 1–4.
- Produces:
  - `LakePlayerCatalog(lake_root: Path)` implementing `PlayerCatalog`; `PLAYERS_LAKE_GLOB = "raw/sleeper/players/scrape_date=*"`.
  - `SleeperAdapter(client: SleeperClient, *, my_user_id: str | None = None, catalog: PlayerCatalog | None = None)` with `platform: Literal["sleeper"] = "sleeper"` and every `FantasyPlatformAdapter` method.
  - Module constants `LEAGUE_TYPES: dict[int, LeagueType]`, `FLEX_COMPOSITION: dict[str, list[str]]`, `NON_STARTER_TOKENS: frozenset[str]`, `EMPTY_SLOT = "0"`.

`LakePlayerCatalog` reads Parquet with Polars and `pathlib` only — **it must not import
`ffh.ingest`**, which is why this task can be completed before ③ merges.

Each method issues a fresh fetch (no memoisation). At 300 req/min that is cheap, and the
draft hot path is `draft_changed_since`, which is one call.

- [ ] **Step 1: Write the failing test `backend/tests/adapters/sleeper/test_adapter.py`**

```python
from datetime import UTC, datetime

import pytest

from ffh.adapters.base import PlatformError, PlayerRef
from ffh.adapters.sleeper.adapter import SleeperAdapter
from ffh.adapters.sleeper.client import SleeperClient

LEAGUE = "1000000000000000001"
DRAFT = "2000000000000000001"


class StubCatalog:
    def __init__(self, refs: dict[str, PlayerRef]) -> None:
        self._refs = refs

    async def all_players(self) -> dict[str, PlayerRef]:
        return self._refs


def _adapter(catalog=None) -> SleeperAdapter:
    return SleeperAdapter(SleeperClient(), my_user_id="USER_ME", catalog=catalog)


async def test_get_scoring_settings_is_verbatim(sleeper_mock, sleeper_fixture):
    s = await _adapter().get_scoring_settings(LEAGUE)
    assert s.points == sleeper_fixture("league")["scoring_settings"]
    assert s.format == "half_ppr"


async def test_get_roster_settings_maps_tokens_and_capacity(sleeper_mock):
    r = await _adapter().get_roster_settings(LEAGUE)
    assert r.starters == ["QB", "RB", "RB", "WR", "WR", "TE", "FLEX", "SUPER_FLEX", "K", "DST"]
    assert r.bench == 3 and r.ir == 1 and r.taxi == 1
    assert r.flex_composition == {
        "FLEX": ["RB", "WR", "TE"],
        "SUPER_FLEX": ["QB", "RB", "WR", "TE"],
    }
    assert r.is_superflex is True


async def test_get_league_derives_type_faab_and_superflex(sleeper_mock):
    lg = await _adapter().get_league(LEAGUE)
    assert lg.platform == "sleeper" and lg.external_id == LEAGUE and lg.season == 2026
    assert lg.num_teams == 2 and lg.league_type == "redraft"
    assert lg.is_superflex is True
    assert lg.faab_budget == 100  # waiver_type == 2
    assert lg.playoff_teams == 2 and lg.playoff_start_week == 15
    assert lg.my_team_external_id == "1"


async def test_faab_budget_is_none_when_waivers_are_priority_based(sleeper_mock, sleeper_fixture):
    import httpx

    raw = sleeper_fixture("league")
    raw["settings"]["waiver_type"] = 0
    sleeper_mock.get(f"/league/{LEAGUE}").mock(return_value=httpx.Response(200, json=raw))
    lg = await _adapter().get_league(LEAGUE)
    assert lg.faab_budget is None


async def test_unknown_league_type_raises_rather_than_defaulting(sleeper_mock, sleeper_fixture):
    import httpx

    raw = sleeper_fixture("league")
    raw["settings"]["type"] = 7
    sleeper_mock.get(f"/league/{LEAGUE}").mock(return_value=httpx.Response(200, json=raw))
    with pytest.raises(PlatformError):
        await _adapter().get_league(LEAGUE)


async def test_get_teams_maps_names_faab_and_is_me(sleeper_mock):
    teams = {t.external_id: t for t in await _adapter().get_teams(LEAGUE)}
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


async def test_get_rosters_assigns_every_slot_kind_and_drops_the_zero_placeholder(sleeper_mock):
    rosters = {r.team_external_id: r for r in await _adapter().get_rosters(LEAGUE, 1)}
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


async def test_starter_length_mismatch_raises(sleeper_mock, sleeper_fixture):
    import httpx

    rosters = sleeper_fixture("rosters")
    rosters[0]["starters"] = rosters[0]["starters"][:5]
    sleeper_mock.get(f"/league/{LEAGUE}/rosters").mock(
        return_value=httpx.Response(200, json=rosters)
    )
    with pytest.raises(PlatformError):
        await _adapter().get_rosters(LEAGUE, 1)


async def test_get_matchups_pairs_by_matchup_id(sleeper_mock):
    matchups = await _adapter().get_matchups(LEAGUE, 1)
    assert len(matchups) == 1
    m = matchups[0]
    assert m.week == 1 and m.matchup_no == 1
    assert m.home_team_external_id == "1" and m.away_team_external_id == "2"
    assert m.home_points == pytest.approx(100.5) and m.away_points == pytest.approx(88.0)


async def test_get_matchups_emits_a_bye_for_a_null_matchup_id(sleeper_mock, sleeper_fixture):
    import httpx

    raw = sleeper_fixture("matchups_week1")
    raw[1]["matchup_id"] = None
    sleeper_mock.get(f"/league/{LEAGUE}/matchups/1").mock(
        return_value=httpx.Response(200, json=raw)
    )
    matchups = await _adapter().get_matchups(LEAGUE, 1)
    assert len(matchups) == 2
    bye = [m for m in matchups if m.away_team_external_id is None]
    assert len(bye) == 1 and bye[0].home_team_external_id == "2"


async def test_get_transactions_normalizes_type_faab_and_epoch_ms(sleeper_mock):
    txns = {t.external_id: t for t in await _adapter().get_transactions(LEAGUE, 1)}
    assert txns["TXN1"].type == "waiver"
    assert txns["TXN1"].faab_spent == 25
    assert txns["TXN1"].week == 1
    assert txns["TXN1"].adds == {"90": "1"} and txns["TXN1"].drops == {"10": "1"}
    assert txns["TXN1"].executed_at == datetime.fromtimestamp(1758698028886 / 1000, tz=UTC)
    # free_agent with only drops normalizes to "drop"
    assert txns["TXN2"].type == "drop" and txns["TXN2"].faab_spent is None


async def test_get_free_agents_is_the_catalog_minus_everyone_rostered(sleeper_mock):
    catalog = StubCatalog(
        {
            "1": PlayerRef(external_id="1", name="Fixture Quarterback", position="QB", team="KC"),
            "90": PlayerRef(external_id="90", name="Fixture Freeagentwr", position="WR", team="DET"),
            "91": PlayerRef(external_id="91", name="Fixture Freeagentqb", position="QB", team="DET"),
            "KC": PlayerRef(external_id="KC", name="KC", position="DST", team="KC"),
        }
    )
    free = await _adapter(catalog).get_free_agents(LEAGUE)
    assert [p.external_id for p in free] == ["90", "91"]


async def test_get_free_agents_without_a_catalog_raises_rather_than_returning_empty(sleeper_mock):
    with pytest.raises(PlatformError):
        await _adapter().get_free_agents(LEAGUE)


async def test_get_draft_maps_slot_and_epoch_ms(sleeper_mock):
    d = await _adapter().get_draft(DRAFT)
    assert d.external_id == DRAFT and d.league_external_id == LEAGUE
    assert d.draft_type == "snake" and d.rounds == 13 and d.status == "complete"
    assert d.my_slot == 1
    assert d.last_picked_ms == 1756083970192
    assert d.started_at == datetime.fromtimestamp(1756074607722 / 1000, tz=UTC)


async def test_get_draft_picks_maps_roster_keeper_and_auction_amount(sleeper_mock):
    picks = {p.pick_no: p for p in await _adapter().get_draft_picks(DRAFT)}
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
    sleeper_mock, cursor, expected_changed
):
    changed, new_cursor = await _adapter().draft_changed_since(DRAFT, cursor)
    assert changed is expected_changed
    assert new_cursor == "1756083970192"


async def test_draft_changed_since_handles_a_predraft_null_last_picked(
    sleeper_mock, sleeper_fixture
):
    import httpx

    raw = sleeper_fixture("draft")
    raw["last_picked"] = None
    raw["status"] = "pre_draft"
    sleeper_mock.get(f"/draft/{DRAFT}").mock(return_value=httpx.Response(200, json=raw))
    changed, cursor = await _adapter().draft_changed_since(DRAFT, None)
    assert changed is True and cursor == "0"
    changed, cursor = await _adapter().draft_changed_since(DRAFT, "0")
    assert changed is False and cursor == "0"


async def test_lake_player_catalog_reads_the_newest_partition(tmp_path):
    import polars as pl

    from ffh.adapters.sleeper.catalog import LakePlayerCatalog

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
    from ffh.adapters.sleeper.catalog import LakePlayerCatalog

    with pytest.raises(PlatformError):
        await LakePlayerCatalog(tmp_path).all_players()


def test_adapter_satisfies_the_protocol():
    from ffh.adapters.base import FantasyPlatformAdapter

    assert isinstance(_adapter(), FantasyPlatformAdapter)
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd backend && uv run pytest tests/adapters/sleeper/test_adapter.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'ffh.adapters.sleeper.adapter'`.

- [ ] **Step 3: Write `backend/src/ffh/adapters/sleeper/catalog.py`**

```python
"""The Sleeper player universe, read off the request path.

GET /players/nfl is 14.6 MB. It is landed to the Parquet lake at most once a day by the
`sleeper_players` IngestJob; this reads the newest partition. Polars + pathlib only —
no ffh.ingest import, so the adapter package stands alone.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from ffh.adapters.base import PlatformError, PlayerRef

PLAYERS_LAKE_GLOB = "raw/sleeper/players/scrape_date=*"
REQUIRED_COLUMNS = ("player_id", "name", "position", "team")


class LakePlayerCatalog:
    """PlayerCatalog backed by the newest raw/sleeper/players Parquet partition."""

    def __init__(self, lake_root: Path) -> None:
        self._root = Path(lake_root)

    def _newest_partition(self) -> Path:
        parts = sorted(p for p in self._root.glob(PLAYERS_LAKE_GLOB) if p.is_dir())
        if not parts:
            raise PlatformError(
                f"no Sleeper player partition under {self._root / PLAYERS_LAKE_GLOB}; "
                "run `ffh ingest run sleeper_players` first"
            )
        return parts[-1]

    async def all_players(self) -> dict[str, PlayerRef]:
        partition = self._newest_partition()
        files = sorted(partition.glob("*.parquet"))
        if not files:
            raise PlatformError(f"partition {partition} contains no parquet files")
        df = pl.read_parquet(files)
        missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
        if missing:
            raise PlatformError(f"{partition} is missing columns {missing}")
        refs = {
            row["player_id"]: PlayerRef(
                external_id=row["player_id"],
                name=row["name"],
                position=row["position"],
                team=row["team"],
            )
            for row in df.select(REQUIRED_COLUMNS).iter_rows(named=True)
        }
        if len(refs) != df.height:
            raise PlatformError(
                f"{partition} has duplicate player_id rows ({df.height} rows, {len(refs)} ids)"
            )
        return refs
```

- [ ] **Step 4: Write `backend/src/ffh/adapters/sleeper/adapter.py`**

```python
"""Sleeper -> normalized model mapping.

Scoring and roster settings are ALWAYS what the platform returned. Nothing here supplies
a default for them; an unrecognised setting raises rather than guessing.
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
    return RosterSettings(
        starters=starters,
        bench=tokens.count("BN"),
        ir=tokens.count("IR") or raw.settings.reserve_slots,
        taxi=tokens.count("TAXI") or raw.settings.taxi_slots,
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
        budget = raw.settings.waiver_budget
        is_faab = raw.settings.waiver_type == FAAB_WAIVER_TYPE
        slots = await self._draft_slots(raw)
        teams = [
            self._team(r, users.get(r.owner_id or ""), budget if is_faab else None, slots)
            for r in rosters
        ]
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
        team_name = (user.metadata or {}).get("team_name") if user else None
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
        """roster_id -> draft slot, via GET /draft/{id}.slot_to_roster_id."""
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
        return [self._transaction(t, week) for t in await self._client.get_transactions(league_id, week)]

    def _transaction(self, raw: RawTransaction, week: int) -> Transaction:
        if raw.type in ("waiver", "trade"):
            kind = raw.type
        elif raw.type == "free_agent":
            kind = "add" if raw.adds else "drop"
        else:
            raise PlatformError(f"unrecognised Sleeper transaction type {raw.type!r}")
        return Transaction(
            external_id=raw.transaction_id,
            type=kind,
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
        amount_raw = raw.metadata.get("amount")
        amount = None
        if amount_raw:
            parsed = int(amount_raw)
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
        new_ms = raw.last_picked or 0
        new_cursor = str(new_ms)
        if cursor is None:
            return True, new_cursor
        try:
            previous = int(cursor)
        except ValueError:
            return True, new_cursor
        return previous != new_ms, new_cursor
```

- [ ] **Step 5: Run tests**

Run: `cd backend && uv run pytest tests/adapters -v`
Expected: PASS. If `test_get_teams_maps_names_faab_and_is_me` fails on `draft_slot`,
`_draft_slots` is inverting `slot_to_roster_id` the wrong way — its keys are **slot
strings** and its values are roster ids.

- [ ] **Step 6: Lint and commit**

```bash
cd backend && uv run ruff check . && uv run ruff format .
cd .. && git add backend/src/ffh/adapters/sleeper/adapter.py \
  backend/src/ffh/adapters/sleeper/catalog.py backend/tests/adapters/sleeper/test_adapter.py
git commit -m "feat(adapters): Sleeper adapter mapping raw responses to normalized models"
```

---

### Task 6: `sleeper_players` IngestJob — land the player blob to the lake

> **Requires ③ (`feat/ingest-nflverse-games`) merged. Rebase onto `main` first:**
> `git fetch origin && git rebase origin/main`. Then read
> `backend/src/ffh/ingest/base.py` and `backend/src/ffh/ingest/lake.py` and match the real
> signatures — the contract below is ③'s locked scope, and where the merged code differs,
> the merged code wins.

**Files:**
- Create: `backend/src/ffh/adapters/sleeper/players_job.py`
- Modify: `backend/src/ffh/cli.py` — one import line next to ③'s `from ffh.ingest import games as _games  # noqa: F401` block (that is where ③ eagerly imports job modules so `@register` fills `JOBS`), see Step 4. `ffh/ingest/base.py` itself is read-only here.
- Test: `backend/tests/adapters/sleeper/test_players_job.py`

**Interfaces:**
- Consumes (from ③, exact names): `ffh.ingest.base.{IngestJob, Fetched, NotModified, IngestValidationError, register, get_job}` (`NotModified(etag: str | None)` — no default; `register` is a class decorator keyed on the job's `name` ClassVar; `IngestJob.run` calls `self.fetch(etag)` **synchronously** and maps `PartitionExistsError` → `skipped`); `ffh.ingest.lake.scrape_date`. Consumes `ffh.adapters.sleeper.models.RawPlayer` and `ffh.adapters.sleeper.adapter.player_ref` (Tasks 2, 5).
- Produces: `SleeperPlayersJob` (`name="sleeper_players"`, `source="sleeper"`, `asset="players"`, `REQUIRED_COLUMNS = frozenset({"player_id", "name", "position", "team"})`), registered via `@register`; `players_to_frame(payload: dict[str, dict]) -> pl.DataFrame`; `PLAYER_COLUMNS: tuple[str, ...]`.

**Two live-verified facts drive this design:**
1. `GET /players/nfl` returns an `ETag` but **ignores `If-None-Match`** — it replies `200`
   with all 14.6 MB. Conditional GET is not available, so freshness is decided by a
   **sha256 of the body** stored in `ingest_runs.source_etag` as `sha256:<hex>`.
2. The response is a **dict keyed by player id**, not a list, and 32 of its entries are
   team defenses with no `full_name` and no `gsis_id`.

Every column lands as `pl.Utf8` — `espn_id`/`yahoo_id` arrive as ints but the crosswalk
joins them as text, and a mixed-type column would silently coerce.

- [ ] **Step 1: Write the failing test `backend/tests/adapters/sleeper/test_players_job.py`**

```python
import json

import httpx
import polars as pl
import pytest
import respx

from ffh.adapters.sleeper.players_job import PLAYER_COLUMNS, SleeperPlayersJob, players_to_frame
from ffh.ingest.base import Fetched, IngestValidationError, NotModified, get_job
from ffh.ingest.lake import scrape_date

BASE = "https://api.sleeper.app/v1"


def test_job_is_registered_under_its_name():
    # ③'s @register decorator keys JOBS on the class's `name` ClassVar.
    assert SleeperPlayersJob.name == "sleeper_players"
    assert get_job("sleeper_players") is SleeperPlayersJob


def test_partition_is_todays_utc_scrape_date():
    # Same clock as every other lake partition (③ `ffh.ingest.lake.scrape_date`, UTC).
    assert SleeperPlayersJob().partition() == {"scrape_date": scrape_date()}


def test_frame_is_all_utf8_with_normalized_name_and_position(sleeper_fixture):
    df = players_to_frame(sleeper_fixture("players_slice"))
    assert df.columns == list(PLAYER_COLUMNS)
    assert set(df.schema.values()) == {pl.Utf8}
    assert df.height == 25
    row = df.filter(pl.col("player_id") == "1").row(0, named=True)
    assert row["name"] == "Fixture Quarterback" and row["position"] == "QB"
    assert row["espn_id"] == "9000001"
    # DEF entries: position becomes DST and the name is the team abbreviation, which is
    # the one form the crosswalk's normalize_dst can canonicalize.
    dst = df.filter(pl.col("player_id") == "KC").row(0, named=True)
    assert dst["position"] == "DST" and dst["name"] == "KC" and dst["gsis_id"] is None


def test_frame_rejects_a_duplicate_or_empty_payload():
    with pytest.raises(ValueError):
        players_to_frame({})


def test_validate_requires_the_crosswalk_columns_and_rows(sleeper_fixture):
    job = SleeperPlayersJob()
    good = players_to_frame(sleeper_fixture("players_slice"))
    job.validate(good)
    # ③'s contract: validate() raises IngestValidationError (never a bare assert), so
    # IngestJob.run maps it to status="failed" with the message in ingest_runs.error.
    with pytest.raises(IngestValidationError, match="missing required columns"):
        job.validate(pl.DataFrame({"player_id": []}, schema={"player_id": pl.Utf8}))
    with pytest.raises(IngestValidationError, match="0 rows"):
        job.validate(good.head(0))
    with pytest.raises(IngestValidationError, match="duplicate player_id"):
        job.validate(pl.concat([good, good.head(1)]))


@respx.mock
def test_fetch_returns_not_modified_when_the_body_hash_is_unchanged(sleeper_fixture):
    # fetch() is SYNC: ③'s IngestJob.run calls `self.fetch(etag)` directly. respx mocks
    # the sync httpx.Client just as well as the async one.
    payload = sleeper_fixture("players_slice")
    body = json.dumps(payload).encode()
    respx.get(f"{BASE}/players/nfl").mock(return_value=httpx.Response(200, content=body))
    job = SleeperPlayersJob()
    first = job.fetch(None)
    assert isinstance(first, Fetched) and first.etag.startswith("sha256:")
    respx.get(f"{BASE}/players/nfl").mock(return_value=httpx.Response(200, content=body))
    second = job.fetch(first.etag)
    assert isinstance(second, NotModified) and second.etag == first.etag


@respx.mock
def test_second_run_on_the_same_day_is_skipped_never_overwritten(
    tmp_path, db_session, sleeper_fixture
):
    """③'s lifecycle owns the guard: write_parquet raises PartitionExistsError for today's
    file and IngestJob.run maps that to status="skipped" (an ingest_runs row is still
    written). No run() override here — the base lifecycle is the contract."""
    from ffh.ingest.lake import parquet_file, write_parquet

    body = json.dumps(sleeper_fixture("players_slice")).encode()
    respx.get(f"{BASE}/players/nfl").mock(return_value=httpx.Response(200, content=body))
    job = SleeperPlayersJob()
    today = job.partition()["scrape_date"]
    landed = parquet_file(tmp_path, "sleeper", "players", scrape_date=today)
    write_parquet(pl.DataFrame({"player_id": ["sentinel"]}), landed)

    result = job.run(db_session, tmp_path)
    assert result.status == "skipped" and result.rows_written is None
    assert "already exists" in result.error
    # The original partition survives untouched (DATABASE.md §1: never overwrite).
    assert pl.read_parquet(landed)["player_id"].to_list() == ["sentinel"]
    assert len(list(tmp_path.rglob("*.parquet"))) == 1


def test_landed_partition_is_readable_by_the_lake_player_catalog(tmp_path, sleeper_fixture):
    """Task 5's LakePlayerCatalog and this job must agree on path and columns."""
    import asyncio

    from ffh.adapters.sleeper.catalog import LakePlayerCatalog
    from ffh.ingest.lake import partition_path, write_parquet

    df = players_to_frame(sleeper_fixture("players_slice"))
    path = partition_path(tmp_path, "sleeper", "players", scrape_date="2026-08-16")
    write_parquet(df, path / "players.parquet")
    refs = asyncio.run(LakePlayerCatalog(tmp_path).all_players())
    assert refs["KC"].position == "DST" and refs["1"].name == "Fixture Quarterback"
    assert len(refs) == 25
```

③'s `IngestJob.run` writes an `ingest_runs` row, so the `db_session` test needs Postgres.
Either mark it individually with `@pytest.mark.db`, or move it (alone) into a
`tests/adapters/sleeper/test_players_job_db.py` split with `pytestmark = pytest.mark.db`.
Either is acceptable; keep the pure tests runnable without Postgres.

- [ ] **Step 2: Run to verify it fails**

Run: `cd backend && uv run pytest tests/adapters/sleeper/test_players_job.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'ffh.adapters.sleeper.players_job'`.

- [ ] **Step 3: Write `backend/src/ffh/adapters/sleeper/players_job.py`**

```python
"""Land GET /players/nfl to the Parquet lake, at most once a day.

The response is 14.6 MB and a dict keyed by Sleeper player id. Sleeper returns an ETag but
IGNORES If-None-Match (verified 2026-08-16: sending it still yields 200 + full body), so
freshness is a sha256 of the body stored in ingest_runs.source_etag as "sha256:<hex>".

The once-a-day guarantee is ③'s: `IngestJob.run` writes through `write_parquet`, which
refuses to overwrite today's `scrape_date=` partition (PartitionExistsError -> "skipped",
still recorded in ingest_runs). No run() override lives here.

Every column lands as Utf8: espn_id/yahoo_id arrive as ints, but the crosswalk joins them
as text and a mixed-type column would silently coerce.
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from typing import Any, ClassVar

import httpx
import polars as pl

from ffh.adapters.sleeper.adapter import player_ref
from ffh.adapters.sleeper.models import RawPlayer
from ffh.config import get_settings
from ffh.ingest.base import Fetched, IngestJob, IngestValidationError, NotModified, register
from ffh.ingest.lake import scrape_date

PLAYER_COLUMNS: tuple[str, ...] = (
    "player_id",
    "name",
    "position",
    "team",
    "first_name",
    "last_name",
    "fantasy_positions",
    "status",
    "active",
    "injury_status",
    "gsis_id",
    "espn_id",
    "yahoo_id",
    "rotowire_id",
    "fantasy_data_id",
    "sportradar_id",
    "birth_date",
    "college",
    "years_exp",
    "number",
    "depth_chart_order",
    "depth_chart_position",
    "search_rank",
)


def _s(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def players_to_frame(payload: dict[str, dict[str, Any]]) -> pl.DataFrame:
    if not payload:
        raise ValueError("GET /players/nfl returned an empty payload")
    rows: list[dict[str, str | None]] = []
    for entry in payload.values():
        raw = RawPlayer.model_validate(entry)
        ref = player_ref(raw)
        rows.append(
            {
                "player_id": ref.external_id,
                "name": ref.name,
                "position": ref.position,
                "team": ref.team,
                "first_name": raw.first_name,
                "last_name": raw.last_name,
                "fantasy_positions": "|".join(raw.fantasy_positions) or None,
                "status": raw.status,
                "active": _s(raw.active),
                "injury_status": raw.injury_status,
                "gsis_id": raw.gsis_id,
                "espn_id": _s(raw.espn_id),
                "yahoo_id": _s(raw.yahoo_id),
                "rotowire_id": _s(raw.rotowire_id),
                "fantasy_data_id": _s(raw.fantasy_data_id),
                "sportradar_id": raw.sportradar_id,
                "birth_date": raw.birth_date,
                "college": raw.college,
                "years_exp": _s(raw.years_exp),
                "number": _s(raw.number),
                "depth_chart_order": _s(raw.depth_chart_order),
                "depth_chart_position": raw.depth_chart_position,
                "search_rank": _s(raw.search_rank),
            }
        )
    df = pl.DataFrame(rows, schema={c: pl.Utf8 for c in PLAYER_COLUMNS})
    if df.height != len(payload):
        raise ValueError(f"lost rows building the frame: {len(payload)} in, {df.height} out")
    if df["player_id"].n_unique() != df.height:
        raise ValueError("duplicate player_id in /players/nfl payload")
    return df


@register
class SleeperPlayersJob(IngestJob):
    """Registered as `sleeper_players`. Non-commercial use only (Sleeper licence).

    Sync `fetch` on purpose: ③'s `IngestJob.run` calls `self.fetch(etag)` directly. This
    module never touches the async adapter client — the blob is not on the request path.
    """

    name: ClassVar[str] = "sleeper_players"
    source: ClassVar[str] = "sleeper"
    asset: ClassVar[str] = "players"
    # The crosswalk cannot work without these; ③'s base validate() checks them first.
    REQUIRED_COLUMNS: ClassVar[frozenset[str]] = frozenset({"player_id", "name", "position", "team"})

    def partition(self) -> dict[str, str]:
        # ③'s UTC clock — the same key every other lake partition uses.
        return {"scrape_date": scrape_date()}

    def fetch(self, etag: str | None) -> Fetched | NotModified:
        base = get_settings().sleeper_base_url.rstrip("/")
        with httpx.Client(base_url=base, timeout=60.0) as http:
            resp = http.get("/players/nfl")
            resp.raise_for_status()
            body = resp.content
        digest = f"sha256:{hashlib.sha256(body).hexdigest()}"
        if etag is not None and etag == digest:
            return NotModified(etag=digest)
        return Fetched(content=body, etag=digest, mtime=datetime.now(tz=UTC))

    def parse(self, payload: bytes) -> pl.DataFrame:
        return players_to_frame(json.loads(payload))

    def validate(self, df: pl.DataFrame) -> None:
        # ③'s contract: raise IngestValidationError, never a bare assert. The base checks
        # REQUIRED_COLUMNS and the empty frame; the two extra checks are ours.
        super().validate(df)
        if df["player_id"].null_count() != 0:
            raise IngestValidationError("sleeper_players: null player_id")
        if df["player_id"].n_unique() != df.height:
            raise IngestValidationError("sleeper_players: duplicate player_id")
```

No `run()` override: ③'s lifecycle inserts the `ingest_runs` row, fetches, validates, and
maps `write_parquet`'s `PartitionExistsError` to `skipped` when today's file already exists.
An override that short-circuits before `super().run()` would leave no `ingest_runs` row —
do not add one. (`write_parquet` is therefore not imported here; ruff `F401` will flag it if
you do.)

- [ ] **Step 4: Make sure the registry sees the job**

`@register` populates ③'s `ffh.ingest.base.JOBS` at import time, so the defining module must
be imported eagerly. Add the import to ③'s CLI, next to its `from ffh.ingest import games as
_games  # noqa: F401` / `nflverse` / `reference` lines in `backend/src/ffh/cli.py` (that is
where ③ collects jobs; if ③ moved the block to `ffh/ingest/__init__.py`, put it there):

```python
from ffh.adapters.sleeper import players_job as _sleeper_players_job  # noqa: F401
```

- [ ] **Step 5: Run tests**

Run: `cd backend && uv run pytest tests/adapters/sleeper/test_players_job.py -v`
Expected: PASS.

- [ ] **Step 6: Lint and commit**

```bash
cd backend && uv run ruff check . && uv run ruff format .
cd .. && git add backend/src/ffh/adapters/sleeper/players_job.py \
  backend/tests/adapters/sleeper/test_players_job.py backend/src/ffh/cli.py
git commit -m "feat(ingest): sleeper_players job lands the player blob at most once a day"
```

---

### Task 7: `ffh.ingest.platform_sync` — persist a league into Postgres

> **Requires ③ and ④ merged. Rebase onto `main` first:**
> `git fetch origin && git rebase origin/main`. Read
> `backend/src/ffh/crosswalk/resolve.py` before writing `_resolve_refs` — that function is
> the **only** place ⑤ touches ④'s contract, so if ④'s merged `ResolveInput` /
> `ResolveManyReport` differ from the shapes below, change `_resolve_refs` and nothing else.

**Files:**
- Create: `backend/src/ffh/ingest/platform_sync.py`
- Modify: `backend/src/ffh/adapters/sleeper/adapter.py` — add `current_week()`
- Create: `backend/tests/ingest/_sleeper_seed.py` — registry seeding shared with Task 10
- Test: `backend/tests/ingest/test_platform_sync.py`

**Interfaces:**
- Consumes: `ffh.adapters.base.{League, LeagueTeam, Roster, Draft, DraftPick, FantasyPlatformAdapter, PlatformError}` (Task 1); `SleeperAdapter` (Task 5); ④'s `ffh.crosswalk.resolve` — exact names:
  - `ResolveInput(source, external_id, raw_name=None, raw_position=None, raw_team=None, gsis_id=None, birth_date=None, college=None)` (frozen dataclass) with `.key -> (source, external_id)`;
  - `resolve_many(session, rows: Iterable[ResolveInput]) -> ResolveManyReport` where `ResolveManyReport.resolved: dict[tuple[str, str], Resolution]` is keyed by **`(source, external_id)`** (not by bare external_id), `unmatched: list[tuple[str, str]]` (rung 5 — ④ has already upserted `crosswalk_unmatched`), and `pending_review: list[tuple[str, str]]` (rung 4 fuzzy hits persisted **unverified** in `player_external_ids`; **not** in `crosswalk_unmatched`; a human runs `ffh crosswalk verify <source> <id>`). `Resolution(player_id, method, confidence)`.
  This module never writes `crosswalk_unmatched` itself. Tests seed the registry via ④'s `apply_playerids` + `seed_dst_players`.
- Produces:
  - `UnmatchedPlayer(external_id, name, position, team)` (frozen dataclass; also used for pending-review rows).
  - `LeagueLoadReport(league_id: uuid.UUID, teams: int, rostered: int, unmatched: list[UnmatchedPlayer], pending_review: list[UnmatchedPlayer], drafts: int, picks: int)`. `unmatched` = ④ rung 5; `pending_review` = ④ rung 4 awaiting `ffh crosswalk verify`. Neither kind gets a `roster_slots` row this load; both are reported, never dropped.
  - `LeagueSnapshot(league, teams, rosters, drafts, picks, week)` (frozen dataclass).
  - `async fetch_snapshot(adapter, external_id: str, week: int | None = None) -> LeagueSnapshot`.
  - `persist_snapshot(session: Session, snapshot: LeagueSnapshot) -> LeagueLoadReport`.
  - `load_league(session, adapter, external_id: str, season: int, week: int | None = None) -> LeagueLoadReport`.
  - `SleeperAdapter.current_week() -> int`; `SleeperAdapter.get_player_refs(external_ids: set[str]) -> dict[str, PlayerRef]`.

**This module is synchronous.** It crosses into async exactly once, in `load_league`.
`fetch_snapshot` is pure network; `persist_snapshot` is pure DB and needs no event loop —
which is why no `WindowsSelectorEventLoopPolicy` hook is required anywhere in this PR.

- [ ] **Step 1: Add `current_week()` to `backend/src/ffh/adapters/sleeper/adapter.py`**

The Protocol has no week accessor and is copied verbatim, so this is an *additional*
method (structural typing is unaffected). ESPN implements the same name in Phase 2.

```python
    async def current_week(self) -> int:
        """Platform week for a roster snapshot. FETCHED, never assumed.

        /state/nfl returns week=2 with season_type="pre" (verified 2026-08-16), so its
        `week` is meaningless outside the regular season. Week 0 is our explicit
        "pre-season / post-draft snapshot" marker.
        """
        state = await self._client.get_state()
        return state.week if state.season_type == "regular" else 0
```

Add `get_player_refs` in the same edit. Without it, a rostered team defense would reach the
crosswalk with no position and no team and could only ever be unmatched.

```python
    async def get_player_refs(self, external_ids: set[str]) -> dict[str, PlayerRef]:
        """Name/position/team for arbitrary Sleeper ids, for crosswalk resolution.

        Uses the lake catalog when one is configured. Without it, a NON-NUMERIC Sleeper id
        is a team defense — verified: the 32 DEF entries in /players/nfl are keyed by team
        abbreviation ("KC", "SF", ...), and that abbreviation is the one form the
        crosswalk's normalize_dst is guaranteed to canonicalize. Numeric ids fall back to
        the id alone, which rung 1 (the DynastyProcess sleeper_id lookup) resolves — the
        primary rung for Sleeper regardless.
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
                out[ext] = PlayerRef(external_id=ext, name=ext, position="", team=None)
        if len(out) != len(external_ids):
            raise PlatformError("get_player_refs lost ids")
        return out
```

Add to `backend/tests/adapters/sleeper/test_adapter.py`:

```python
async def test_current_week_is_zero_outside_the_regular_season(sleeper_mock, sleeper_fixture):
    import httpx

    assert await _adapter().current_week() == 1
    pre = sleeper_fixture("state_nfl") | {"season_type": "pre", "week": 2}
    sleeper_mock.get("/state/nfl").mock(return_value=httpx.Response(200, json=pre))
    assert await _adapter().current_week() == 0


async def test_get_player_refs_treats_a_non_numeric_id_as_a_defense(sleeper_mock):
    refs = await _adapter().get_player_refs({"1", "KC"})
    assert refs["KC"].position == "DST" and refs["KC"].team == "KC" and refs["KC"].name == "KC"
    assert refs["1"].position == "" and refs["1"].name == "1"


async def test_get_player_refs_prefers_the_catalog(sleeper_mock):
    catalog = StubCatalog(
        {"1": PlayerRef(external_id="1", name="Fixture Quarterback", position="QB", team="KC")}
    )
    refs = await _adapter(catalog).get_player_refs({"1", "KC"})
    assert refs["1"].name == "Fixture Quarterback" and refs["1"].position == "QB"
    assert refs["KC"].position == "DST"
```

- [ ] **Step 2a: Write the shared seeding helper `backend/tests/ingest/_sleeper_seed.py`**

The tests that assert `rostered == 23` need a `players` row and a
`player_external_ids(source='sleeper')` row for every fixture human, and the 32 DST rows —
exactly what `ffh crosswalk seed` produces in production. Both this task's tests and Task
10's coverage test seed the same way, so the recipe lives once, in a non-test helper module
(same pattern as `tests/db/_guard.py`). It is ⑤'s **only** dependency on ④'s
`apply_playerids` frame contract; if ④'s merged `DP_REQUIRED_COLUMNS` differs, fix it here.

```python
"""Seed the players registry for the Sleeper fixture league (Tasks 7 and 10).

Mirrors `ffh crosswalk seed`, from the fixture blob instead of the lake: ④'s
`seed_dst_players` creates the 32 team defenses that rung 3 resolves by `<abbr> dst`, and
④'s `apply_playerids` creates one `players` row + `player_external_ids(sleeper=...)` per
fixture human (its rookie path: a gsis_id not yet in the registry becomes a new player).
"""

import json
from pathlib import Path
from typing import Any

import polars as pl
from sqlalchemy.orm import Session

from ffh.crosswalk.dynastyprocess import DP_REQUIRED_COLUMNS, apply_playerids
from ffh.crosswalk.registry import seed_dst_players

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "sleeper"
FIXTURE_HUMANS = 23  # 21 rostered + 2 free agents in players_slice.json
DST_ROWS = 32
SEEDED_PLAYERS = FIXTURE_HUMANS + DST_ROWS

# ④'s DP_REQUIRED_COLUMNS, spelled out so a drift in either direction fails loudly below.
_PLAYERIDS_SCHEMA: dict[str, type[pl.DataType]] = {
    "mfl_id": pl.Utf8,
    "gsis_id": pl.Utf8,
    "sleeper_id": pl.Utf8,
    "espn_id": pl.Utf8,
    "yahoo_id": pl.Utf8,
    "pfr_id": pl.Utf8,
    "fantasypros_id": pl.Utf8,
    "sportradar_id": pl.Utf8,
    "rotowire_id": pl.Utf8,
    "name": pl.Utf8,
    "position": pl.Utf8,
    "team": pl.Utf8,
    "birthdate": pl.Utf8,
    "draft_year": pl.Int64,
    "college": pl.Utf8,
}


def _text(value: Any) -> str | None:
    """None-preserving str(): `str(None)` would store the literal id "None"."""
    return None if value is None else str(value)


def playerids_frame() -> pl.DataFrame:
    """A DynastyProcess `db_playerids` slice covering every human in the fixture league.

    `mfl_id` must be non-null and UNIQUE per row: ④ keys gsis-less rows on `mfl:<mfl_id>`,
    so a null column would collapse every rookie into one placeholder. The Sleeper id doubles
    as the mfl id here. `pfr_id`, `fantasypros_id` and `draft_year` are required columns
    that may be null.
    """
    blob = json.loads((FIXTURES / "players_slice.json").read_text(encoding="utf-8"))
    humans = [p for p in blob.values() if p.get("position") != "DEF"]
    n = len(humans)
    frame = pl.DataFrame(
        {
            "mfl_id": [p["player_id"] for p in humans],
            "gsis_id": [p.get("gsis_id") for p in humans],
            "sleeper_id": [p["player_id"] for p in humans],
            "espn_id": [_text(p.get("espn_id")) for p in humans],
            "yahoo_id": [_text(p.get("yahoo_id")) for p in humans],
            "pfr_id": [None] * n,
            "fantasypros_id": [None] * n,
            "sportradar_id": [p.get("sportradar_id") for p in humans],
            "rotowire_id": [_text(p.get("rotowire_id")) for p in humans],
            "name": [p["full_name"] for p in humans],
            "position": [p["position"] for p in humans],
            "team": [p.get("team") for p in humans],
            "birthdate": [p.get("birth_date") for p in humans],
            "draft_year": [None] * n,
            "college": [p.get("college") for p in humans],
        },
        schema=_PLAYERIDS_SCHEMA,
    )
    missing = DP_REQUIRED_COLUMNS - set(frame.columns)
    assert not missing, f"fixture frame lags ④'s DP_REQUIRED_COLUMNS: {sorted(missing)}"
    assert frame.height == FIXTURE_HUMANS, frame.height
    assert frame["mfl_id"].n_unique() == frame.height, "mfl_id must be unique per row"
    return frame


def seed_fixture_players(session: Session) -> None:
    """32 DST rows + one player (and its sleeper id) per fixture human. Flushes only —
    the caller's transaction owns the commit/rollback."""
    created_dst = seed_dst_players(session)
    assert created_dst == DST_ROWS, created_dst
    report = apply_playerids(session, playerids_frame())
    assert report.created_players == FIXTURE_HUMANS, report
    assert report.ambiguous == (), report
    session.flush()
```

- [ ] **Step 2b: Write the failing test `backend/tests/ingest/test_platform_sync.py`**

```python
import uuid

import pytest
from sqlalchemy import func, select

from ffh.adapters.base import PlatformError
from ffh.adapters.sleeper.adapter import SleeperAdapter
from ffh.adapters.sleeper.client import SleeperClient
from ffh.db.models import Draft, DraftPick, League, LeagueTeam, Player, RosterSlot
from ffh.ingest.platform_sync import load_league
from tests.ingest._sleeper_seed import SEEDED_PLAYERS, seed_fixture_players

pytestmark = pytest.mark.db

LEAGUE = "1000000000000000001"


@pytest.fixture
def adapter():
    return SleeperAdapter(SleeperClient(), my_user_id="USER_ME")


@pytest.fixture
def seeded(db_session):
    """db_session with the registry seeded the way `ffh crosswalk seed` would: a players
    row + sleeper id per fixture human (④ apply_playerids) and the 32 DSTs (④
    seed_dst_players). Tests that assert roster_slots / rostered counts take THIS instead
    of db_session; tests about unmatched reporting stay unseeded."""
    seed_fixture_players(db_session)
    assert db_session.scalar(select(func.count()).select_from(Player)) == SEEDED_PLAYERS
    return db_session


def test_load_league_persists_settings_verbatim(db_session, sleeper_mock, sleeper_fixture, adapter):
    report = load_league(db_session, adapter, LEAGUE, season=2026, week=1)
    row = db_session.get(League, report.league_id)
    expected = sleeper_fixture("league")["scoring_settings"]
    assert row.scoring_settings == expected
    assert set(row.scoring_settings) == set(expected)  # no key added or removed
    assert row.platform == "sleeper" and row.season == 2026
    assert row.num_teams == 2 and row.league_type == "redraft"
    assert row.faab_budget == 100
    assert row.playoff_teams == 2 and row.playoff_start_wk == 15


def test_is_superflex_is_derived_from_roster_positions(db_session, sleeper_mock, adapter):
    report = load_league(db_session, adapter, LEAGUE, season=2026, week=1)
    row = db_session.get(League, report.league_id)
    assert row.is_superflex is True
    assert "SUPER_FLEX" in row.roster_settings["starters"]


def test_teams_my_team_and_roster_slots(seeded, sleeper_mock, adapter):
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


def test_drafts_and_picks_land(seeded, sleeper_mock, adapter):
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


def test_load_is_idempotent(seeded, sleeper_mock, adapter):
    first = load_league(seeded, adapter, LEAGUE, season=2026, week=1)
    second = load_league(seeded, adapter, LEAGUE, season=2026, week=1)
    assert first.league_id == second.league_id
    assert seeded.scalar(select(func.count()).select_from(League)) == 1
    assert seeded.scalar(select(func.count()).select_from(LeagueTeam)) == 2
    assert seeded.scalar(select(func.count()).select_from(Draft)) == 1
    assert seeded.scalar(select(func.count()).select_from(DraftPick)) == 4
    assert seeded.scalar(select(func.count()).select_from(RosterSlot)) == 23


def test_reruns_replace_the_week_snapshot_rather_than_accumulating(
    seeded, sleeper_mock, sleeper_fixture, adapter
):
    import httpx

    load_league(seeded, adapter, LEAGUE, season=2026, week=1)
    rosters = sleeper_fixture("rosters")
    rosters[0]["players"] = [p for p in rosters[0]["players"] if p != "10"]  # dropped a bench guy
    sleeper_mock.get(f"/league/{LEAGUE}/rosters").mock(
        return_value=httpx.Response(200, json=rosters)
    )
    report = load_league(seeded, adapter, LEAGUE, season=2026, week=1)
    assert report.rostered == 22
    assert seeded.scalar(select(func.count()).select_from(RosterSlot)) == 22


def test_season_mismatch_raises(db_session, sleeper_mock, adapter):
    with pytest.raises(ValueError):
        load_league(db_session, adapter, LEAGUE, season=2025, week=1)


def test_same_league_invariant_on_draft_picks_raises(
    db_session, sleeper_mock, sleeper_fixture, adapter
):
    """A pick whose roster_id is not a team of THIS league must abort the load."""
    import httpx

    picks = sleeper_fixture("draft_picks")
    picks[0]["roster_id"] = 99
    sleeper_mock.get("/draft/2000000000000000001/picks").mock(
        return_value=httpx.Response(200, json=picks)
    )
    with pytest.raises(PlatformError, match="not a team of league"):
        load_league(db_session, adapter, LEAGUE, season=2026, week=1)
    assert db_session.scalar(select(func.count()).select_from(League)) == 0


def test_unmatched_players_are_reported_not_dropped(db_session, sleeper_mock, adapter):
    """With no players seeded, every rostered id is unmatched and every one is reported."""
    report = load_league(db_session, adapter, LEAGUE, season=2026, week=1)
    assert len(report.unmatched) == 23
    assert {u.external_id for u in report.unmatched} >= {"1", "KC", "SF"}
    assert db_session.scalar(select(func.count()).select_from(RosterSlot)) == 0
```

Note the last test: with nothing seeded, `roster_slots` cannot be written (its `player_id`
FK requires a `players` row), so the count is 0 and *all 23* are reported. Task 10 is the
mirror case where all 23 resolve.

- [ ] **Step 3: Run to verify it fails**

Run: `cd backend && uv run pytest tests/ingest/test_platform_sync.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'ffh.ingest.platform_sync'`.

- [ ] **Step 4: Write `backend/src/ffh/ingest/platform_sync.py`**

```python
"""Load a fantasy league into Postgres.

ARCHITECTURE.md's module map has no home for "land a league in Postgres"; this is ingest's
fetch -> validate -> land, landing in Postgres rather than Parquet. Recorded as a deviation
in ARCHITECTURE.md by this PR.

SYNCHRONOUS by design: it takes an orm.Session, matching the sync engine, the db_session
test fixture, and the crosswalk's resolve_many. The async adapter boundary is crossed
exactly once, in load_league.
"""

from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol

from sqlalchemy import delete, select
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


class _WeekAware(Protocol):
    async def current_week(self) -> int: ...


class _RefAware(Protocol):
    async def get_player_refs(self, external_ids: set[str]) -> dict[str, PlayerRef]: ...


@dataclass(frozen=True, slots=True)
class UnmatchedPlayer:
    external_id: str
    name: str
    position: str
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
    # external_id -> name/position/team, for crosswalk resolution.
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
    if not isinstance(adapter, _RefAware):
        raise ValueError(f"{type(adapter).__name__} cannot describe players for the crosswalk")
    player_refs = await adapter.get_player_refs(rostered_ids)
    if set(player_refs) != rostered_ids:
        raise PlatformError(
            f"player refs cover {len(player_refs)} of {len(rostered_ids)} rostered ids"
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
    lister = getattr(adapter, "get_league_drafts", None)
    if lister is not None:
        return list(await lister(external_id))
    return []


def _resolve_refs(
    session: Session, source: str, refs: dict[str, PlayerRef]
) -> tuple[dict[str, uuid.UUID], list[UnmatchedPlayer], list[UnmatchedPlayer]]:
    """The ONLY place PR 5 touches PR 4's crosswalk contract (ffh.crosswalk.resolve).

    ④'s shapes: `resolve_many(session, Iterable[ResolveInput])` returns a
    `ResolveManyReport` whose `resolved` is keyed by `(source, external_id)` — NOT by bare
    external_id — plus `unmatched` (rung 5, already in crosswalk_unmatched) and
    `pending_review` (rung 4 fuzzy, persisted unverified, NOT in crosswalk_unmatched).
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
        )
        for r in ordered
    ]
    report = resolve_many(session, inputs)

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
    league_id = _upsert_league(session, snapshot.league)
    team_ids = _upsert_teams(session, league_id, snapshot.teams)
    _set_my_team(session, league_id, snapshot.teams, team_ids)

    resolved, unmatched, pending_review = _resolve_refs(
        session, snapshot.league.platform, snapshot.player_refs
    )

    rostered = _replace_roster_slots(session, snapshot, team_ids, resolved)
    drafts, picks = _upsert_drafts(session, league_id, snapshot, team_ids, resolved)
    session.flush()
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
        # "flex_composition": {...}, "is_superflex": bool}. NOTE: ③'s sentinel generic
        # league stores a plain COUNT MAP ({"QB": 1, "RB": 2, ...}) in the same column, so
        # leagues.roster_settings has two shapes; consumers must not assume one.
        "roster_settings": league.roster.model_dump(),
        "league_type": league.league_type,
        "is_superflex": league.is_superflex,
        "playoff_teams": league.playoff_teams,
        "playoff_start_wk": league.playoff_start_week,
        "faab_budget": league.faab_budget,
    }
    stmt = insert(LeagueRow).values(**values)
    stmt = stmt.on_conflict_do_update(
        index_elements=["platform", "external_id", "season"],
        set_={k: stmt.excluded[k] for k in values if k not in ("platform", "external_id", "season")},
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
        stmt = insert(LeagueTeamRow).values(**values)
        stmt = stmt.on_conflict_do_update(
            index_elements=["league_id", "external_id"],
            set_={
                k: stmt.excluded[k] for k in values if k not in ("league_id", "external_id")
            },
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
    mine = [t for t in teams if t.is_me]
    if len(mine) > 1:
        raise PlatformError(f"league {league_id}: {len(mine)} teams flagged is_me")
    row = session.get(LeagueRow, league_id)
    row.my_team_id = team_ids[mine[0].external_id] if mine else None


def _replace_roster_slots(
    session: Session,
    snapshot: LeagueSnapshot,
    team_ids: dict[str, uuid.UUID],
    resolved: dict[str, uuid.UUID],
) -> int:
    """Delete-then-insert this week's snapshot so a dropped player leaves no stale row."""
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
        stmt = insert(DraftRow).values(**values)
        stmt = stmt.on_conflict_do_update(
            index_elements=["league_id", "external_id"],
            set_={k: stmt.excluded[k] for k in values if k not in ("league_id", "external_id")},
        ).returning(DraftRow.draft_id)
        draft_pk = session.execute(stmt).scalar_one()
        drafts += 1
        for pick in snapshot.picks.get(draft.external_id, []):
            # Same-league invariant: DATABASE.md §5 leaves this to us, with a test.
            if pick.team_external_id is not None and pick.team_external_id not in team_ids:
                raise PlatformError(
                    f"draft pick {pick.pick_no} names team {pick.team_external_id!r}, "
                    f"which is not a team of league {snapshot.league.external_id}"
                )
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
                "picked_at": pick.picked_at,
            }
            pstmt = insert(DraftPickRow).values(**pvalues)
            pstmt = pstmt.on_conflict_do_update(
                index_elements=["draft_id", "pick_no"],
                set_={k: pstmt.excluded[k] for k in pvalues if k not in ("draft_id", "pick_no")},
            )
            session.execute(pstmt)
            picks += 1
    expected_picks = sum(len(v) for v in snapshot.picks.values())
    if picks != expected_picks:
        raise PlatformError(f"persisted {picks} of {expected_picks} draft picks")
    return drafts, picks


def load_league(
    session: Session,
    adapter: FantasyPlatformAdapter,
    external_id: str,
    season: int,
    week: int | None = None,
) -> LeagueLoadReport:
    """Fetch a league and land it in Postgres. The caller commits."""
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
```

`leagues`, `league_teams`, `drafts` and `draft_picks` have **no `updated_at` column**
(`docs/DATABASE.md` §4–5), so `ON CONFLICT DO UPDATE` sets only data columns.
`roster_slots.captured_at` is set explicitly because the delete-then-insert must stamp the
new snapshot time rather than lean on the server default.

> **`leagues.roster_settings` has two shapes.** ③'s sentinel generic league
> (`seed_generic_league`, `GENERIC_ROSTER`) stores a **count map** — `{"QB": 1, "RB": 2,
> "WR": 2, "TE": 1, "FLEX": 1, "K": 1, "DST": 1, "BN": …}`. Platform-loaded leagues (this
> task) store `RosterSettings.model_dump()` — `{"starters": [...], "bench", "ir", "taxi",
> "flex_composition", "is_superflex"}`. Nothing in this PR reads the column back, but any
> consumer (lineup optimizer, draft engine) must branch on shape — e.g. `"starters" in
> roster_settings` — and never assume one. Task 11 records this in `docs/DATABASE.md` §4
> next to the `leagues` DDL (③'s Task 10 leaves the matching note in §6 beside the sentinel).

- [ ] **Step 5: Run tests**

Run: `cd backend && uv run pytest tests/ingest/test_platform_sync.py -v`
Expected: PASS. `test_same_league_invariant_on_draft_picks_raises` asserts the transaction
is left with no `leagues` row — the `db_session` fixture rolls back, and the raise happens
before any commit.

- [ ] **Step 6: Lint and commit**

```bash
cd backend && uv run ruff check . && uv run ruff format .
cd .. && git add backend/src/ffh/ingest/platform_sync.py \
  backend/src/ffh/adapters/sleeper/adapter.py backend/tests/ingest/_sleeper_seed.py \
  backend/tests/ingest/test_platform_sync.py backend/tests/adapters/sleeper/test_adapter.py
git commit -m "feat(ingest): platform_sync.load_league persists a league into Postgres"
```

---

### Task 8: CLI — `ffh league load sleeper <league_id>`

> **Requires ③ and ④ merged (do this after Task 7). Rebase onto `main` first:**
> `git fetch origin && git rebase origin/main`. This task imports
> `ffh.ingest.platform_sync` (Task 7, which needs ③ + ④) and **reuses ③'s
> `_session_scope()` context manager in `backend/src/ffh/cli.py`** — after the rebase that
> helper and the shared imports (`get_settings`, `make_engine`, `make_session_factory`,
> `Session`, `contextmanager`) already exist in `cli.py`. Do **not** redefine them and do
> not add a second session helper; add only the `ffh.adapters.*` / `platform_sync` imports
> and the new command.

**Files:**
- Modify: `backend/src/ffh/cli.py`
- Test: `backend/tests/test_cli_league.py`

**Interfaces:**
- Consumes: `load_league`, `LeagueLoadReport` (Task 7); `SleeperAdapter`, `LakePlayerCatalog` (Task 5); `SleeperClient` (Task 4); `get_settings` (Task 1); ③'s `ffh.cli._session_scope()` (context manager yielding a sync `Session`; tests monkeypatch it).
- Produces: `ffh league load <platform> <league_id> [--season N] [--week N]`. Exits **1** when `unmatched > 0` **or** `pending_review > 0`, **0** otherwise. `ffh league platforms` is unchanged.

- [ ] **Step 1: Write the failing test `backend/tests/test_cli_league.py`**

```python
import uuid

from typer.testing import CliRunner

from ffh.cli import app
from ffh.ingest.platform_sync import LeagueLoadReport, UnmatchedPlayer

runner = CliRunner()


def test_platforms_command_still_works():
    result = runner.invoke(app, ["league", "platforms"])
    assert result.exit_code == 0 and "sleeper" in result.stdout


def _report(**overrides):
    base = dict(
        league_id=uuid.UUID(int=1),
        teams=2,
        rostered=23,
        unmatched=[],
        pending_review=[],
        drafts=1,
        picks=4,
    )
    return LeagueLoadReport(**(base | overrides))


def test_load_prints_the_report_and_exits_zero_when_everything_resolves(monkeypatch):
    import ffh.cli as cli

    captured = {}

    def fake_load(session, adapter, external_id, season, week=None):
        captured["args"] = (external_id, season, week)
        return _report()

    monkeypatch.setattr(cli, "load_league", fake_load)
    monkeypatch.setattr(cli, "_session_scope", lambda: _NullSession())
    result = runner.invoke(app, ["league", "load", "sleeper", "L1", "--season", "2026", "--week", "1"])
    assert result.exit_code == 0
    assert captured["args"] == ("L1", 2026, 1)
    assert "teams=2" in result.stdout and "rostered=23" in result.stdout
    assert "unmatched=0" in result.stdout and "pending_review=0" in result.stdout


def test_load_exits_one_and_lists_unmatched(monkeypatch):
    import ffh.cli as cli

    def fake_load(session, adapter, external_id, season, week=None):
        return _report(unmatched=[UnmatchedPlayer("9999", "Mystery Person", "WR", "KC")])

    monkeypatch.setattr(cli, "load_league", fake_load)
    monkeypatch.setattr(cli, "_session_scope", lambda: _NullSession())
    result = runner.invoke(app, ["league", "load", "sleeper", "L1"])
    assert result.exit_code == 1
    assert "UNMATCHED 9999 Mystery Person" in result.stdout


def test_load_exits_one_and_lists_pending_review(monkeypatch):
    """④ rung 4: a fuzzy hit is persisted unverified and is NOT in crosswalk_unmatched, so
    the CLI must surface it separately and still refuse a clean exit."""
    import ffh.cli as cli

    def fake_load(session, adapter, external_id, season, week=None):
        return _report(pending_review=[UnmatchedPlayer("4881", "Lamarr Jackson", "QB", "BAL")])

    monkeypatch.setattr(cli, "load_league", fake_load)
    monkeypatch.setattr(cli, "_session_scope", lambda: _NullSession())
    result = runner.invoke(app, ["league", "load", "sleeper", "L1"])
    assert result.exit_code == 1
    assert "PENDING_REVIEW 4881 Lamarr Jackson" in result.stdout
    assert "ffh crosswalk verify sleeper 4881" in result.stdout


def test_load_rejects_an_unknown_platform():
    result = runner.invoke(app, ["league", "load", "espn", "L1"])
    assert result.exit_code != 0
    assert "espn" in result.stdout


class _NullSession:
    """Stands in for ③'s `_session_scope()` context manager."""

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def commit(self):
        return None
```

(`uuid` joins the imports at the top of the module.)

- [ ] **Step 2: Run to verify it fails**

Run: `cd backend && uv run pytest tests/test_cli_league.py -v`
Expected: FAIL — `AttributeError: module 'ffh.cli' has no attribute 'load_league'`.

- [ ] **Step 3: Extend `backend/src/ffh/cli.py`**

Keep the existing `version`, `ingest_*`, `league_platforms` and `crosswalk_*` commands and
③'s `_session_scope()` helper exactly as they are. **Do not** add `get_settings`,
`make_engine`, `make_session_factory`, `Session`, `contextmanager` or a second session
helper — ③ owns those lines and they are already in the file after the rebase (`ruff
check` flags duplicate imports). Add only these imports at module level (so
`monkeypatch.setattr(cli, "load_league", ...)` works) and the new command:

```python
from ffh.adapters.sleeper.adapter import SleeperAdapter
from ffh.adapters.sleeper.catalog import LakePlayerCatalog
from ffh.adapters.sleeper.client import SleeperClient
from ffh.ingest.platform_sync import load_league


@league_app.command("load")
def league_load(
    platform: str = typer.Argument(..., help="Only 'sleeper' is implemented."),
    league_id: str = typer.Argument(..., help="Platform league id."),
    season: int | None = typer.Option(None, "--season", help="Defaults to FFH_SEASON."),
    week: int | None = typer.Option(
        None, "--week", help="Roster snapshot week. Defaults to the platform's current week."
    ),
) -> None:
    """Load a league into Postgres. Exits 1 if any rostered player is unmatched or
    awaiting crosswalk review."""
    if platform != "sleeper":
        typer.echo(f"platform {platform!r} is not implemented; only 'sleeper' is available")
        raise typer.Exit(code=2)
    settings = get_settings()
    adapter = SleeperAdapter(
        SleeperClient(),
        my_user_id=settings.sleeper_user_id,
        catalog=LakePlayerCatalog(settings.lake_root),
    )
    # ③'s _session_scope() (cli.py) — one sync Session per invocation; it does not commit.
    with _session_scope() as session:
        report = load_league(
            session, adapter, league_id, season or settings.season, week
        )
        session.commit()
    typer.echo(
        f"league {report.league_id} teams={report.teams} rostered={report.rostered} "
        f"drafts={report.drafts} picks={report.picks} unmatched={len(report.unmatched)} "
        f"pending_review={len(report.pending_review)}"
    )
    for u in report.unmatched:
        typer.echo(f"  UNMATCHED {u.external_id} {u.name} {u.position} {u.team}")
    for u in report.pending_review:
        # ④ rung 4: persisted unverified in player_external_ids, not in crosswalk_unmatched.
        typer.echo(
            f"  PENDING_REVIEW {u.external_id} {u.name} {u.position} {u.team} "
            f"-> run: ffh crosswalk verify {platform} {u.external_id}"
        )
    if report.unmatched or report.pending_review:
        raise typer.Exit(code=1)
```

The `LakePlayerCatalog` is always attached: if the lake has no partition, the catalog
raises with the exact `ffh ingest run sleeper_players` remedy rather than degrading to
id-only refs behind Chris's back.

- [ ] **Step 4: Run tests**

Run: `cd backend && uv run pytest tests/test_cli_league.py tests/test_cli_ingest.py tests/test_cli.py -v`
Expected: PASS (5 new tests, plus ③'s CLI tests still green — the shared `_session_scope`
is untouched).

- [ ] **Step 5: Lint and commit**

```bash
cd backend && uv run ruff check . && uv run ruff format .
cd .. && git add backend/src/ffh/cli.py backend/tests/test_cli_league.py
git commit -m "feat(cli): ffh league load sleeper exits non-zero on unmatched players"
```

---

### Task 9: `scripts/record_sleeper_fixtures.py` — record the real mock league

**Files:**
- Create: `backend/scripts/record_sleeper_fixtures.py`
- Test: `backend/tests/adapters/sleeper/test_record_fixtures.py`

**Interfaces:**
- Consumes: `SleeperClient` (Task 4), `get_settings` (Task 1).
- Produces: `record(league_id: str, out_dir: Path, client: SleeperClient) -> dict[str, int]` (file stem → bytes written) and a `python -m`/`uv run python` entrypoint.

This script is the **only** thing in the repo that talks to the live API. It never runs in
CI: `addopts = "-m 'not network'"` already excludes the marker, and the script is not
imported by any collected test except the one below, which uses `respx`.

- [ ] **Step 1: Write the failing test `backend/tests/adapters/sleeper/test_record_fixtures.py`**

```python
import json

import pytest

from ffh.adapters.sleeper.client import SleeperClient

LEAGUE = "1000000000000000001"


async def test_record_writes_every_fixture_file(tmp_path, sleeper_mock):
    from scripts.record_sleeper_fixtures import EXPECTED_FILES, record

    written = await record(LEAGUE, tmp_path, SleeperClient())
    assert set(written) == set(EXPECTED_FILES)
    for stem in EXPECTED_FILES:
        assert (tmp_path / f"{stem}.json").exists()
    assert (tmp_path / "README.md").exists()
    assert LEAGUE in (tmp_path / "README.md").read_text(encoding="utf-8")


async def test_players_slice_is_restricted_to_rostered_players_plus_extras(
    tmp_path, sleeper_mock
):
    from scripts.record_sleeper_fixtures import EXTRA_FREE_AGENTS, record

    await record(LEAGUE, tmp_path, SleeperClient())
    sliced = json.loads((tmp_path / "players_slice.json").read_text(encoding="utf-8"))
    # 23 rostered ids in the fixture league, plus the free-agent extras that exist.
    assert "1" in sliced and "KC" in sliced
    assert len(sliced) <= 23 + EXTRA_FREE_AGENTS
    assert all(isinstance(v, dict) for v in sliced.values())


@pytest.mark.network
async def test_live_recording_is_opt_in():
    """Placeholder that documents the marker; excluded from CI by addopts."""
    pytest.skip("run manually: uv run pytest -m network")
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd backend && uv run pytest tests/adapters/sleeper/test_record_fixtures.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'scripts'`.
Fix by adding `backend/scripts/__init__.py` (empty) so pytest's `src = ["src", "tests"]`
rootdir import works; `[tool.ruff] src` already covers it.

- [ ] **Step 3: Write `backend/scripts/record_sleeper_fixtures.py`**

```python
"""Record Sleeper fixtures from a real league.

  FFH_SLEEPER_MOCK_LEAGUE_ID=<id> uv run python scripts/record_sleeper_fixtures.py

Rewrites backend/tests/fixtures/sleeper/. The only code in this repo that talks to the
live API — every test uses respx instead. Sleeper data is NON-COMMERCIAL use only.

/players/nfl is 14.6 MB; only the rostered players plus a few free agents are kept, so the
committed fixture stays small.
"""

from __future__ import annotations

import asyncio
import json
import sys
from datetime import date
from pathlib import Path

from ffh.adapters.sleeper.client import SleeperClient
from ffh.config import get_settings

FIXTURES = Path(__file__).resolve().parents[1] / "tests" / "fixtures" / "sleeper"
EXTRA_FREE_AGENTS = 5
EXPECTED_FILES = (
    "state_nfl",
    "league",
    "rosters",
    "users",
    "league_drafts",
    "draft",
    "draft_picks",
    "matchups_week1",
    "transactions_week1",
    "players_slice",
)

README = """# Sleeper fixtures

Recorded responses from `https://api.sleeper.app/v1`. **CI never touches the network** —
every test drives these through `respx` mounted on `settings.sleeper_base_url`.

Source league: `{league_id}` (draft `{draft_id}`), recorded {recorded_on}.
Re-record from `backend/`:

    FFH_SLEEPER_MOCK_LEAGUE_ID=<id> uv run python scripts/record_sleeper_fixtures.py

Sleeper data is licensed for **non-commercial use only**. These fixtures exist solely to
test this self-hosted personal project.

| File | Endpoint |
|---|---|
| `state_nfl.json` | `GET /state/nfl` |
| `league.json` | `GET /league/{{id}}` |
| `rosters.json` | `GET /league/{{id}}/rosters` |
| `users.json` | `GET /league/{{id}}/users` |
| `league_drafts.json` | `GET /league/{{id}}/drafts` |
| `draft.json` | `GET /draft/{{id}}` |
| `draft_picks.json` | `GET /draft/{{id}}/picks` |
| `matchups_week1.json` | `GET /league/{{id}}/matchups/1` |
| `transactions_week1.json` | `GET /league/{{id}}/transactions/1` |
| `players_slice.json` | `GET /players/nfl`, restricted to rostered players plus {extras} free agents |
"""


def _dump(out_dir: Path, stem: str, payload: object) -> int:
    path = out_dir / f"{stem}.json"
    text = json.dumps(payload, indent=1, sort_keys=True) + "\n"
    path.write_text(text, encoding="utf-8")
    return len(text)


async def record(league_id: str, out_dir: Path, client: SleeperClient) -> dict[str, int]:
    out_dir.mkdir(parents=True, exist_ok=True)
    written: dict[str, int] = {}

    written["state_nfl"] = _dump(out_dir, "state_nfl", await client.get_json("/state/nfl"))
    league = await client.get_json(f"/league/{league_id}")
    written["league"] = _dump(out_dir, "league", league)
    rosters = await client.get_json(f"/league/{league_id}/rosters")
    written["rosters"] = _dump(out_dir, "rosters", rosters)
    written["users"] = _dump(out_dir, "users", await client.get_json(f"/league/{league_id}/users"))
    drafts = await client.get_json(f"/league/{league_id}/drafts")
    written["league_drafts"] = _dump(out_dir, "league_drafts", drafts)

    draft_id = league.get("draft_id") or (drafts[0]["draft_id"] if drafts else None)
    if draft_id is None:
        raise SystemExit(f"league {league_id} has no draft")
    written["draft"] = _dump(out_dir, "draft", await client.get_json(f"/draft/{draft_id}"))
    written["draft_picks"] = _dump(
        out_dir, "draft_picks", await client.get_json(f"/draft/{draft_id}/picks")
    )
    written["matchups_week1"] = _dump(
        out_dir, "matchups_week1", await client.get_json(f"/league/{league_id}/matchups/1")
    )
    written["transactions_week1"] = _dump(
        out_dir,
        "transactions_week1",
        await client.get_json(f"/league/{league_id}/transactions/1"),
    )

    rostered = {
        pid
        for r in rosters
        for pid in (
            *(r.get("players") or []),
            *(r.get("starters") or []),
            *(r.get("reserve") or []),
            *(r.get("taxi") or []),
        )
        if pid and pid != "0"
    }
    blob = await client.get_json("/players/nfl")
    sliced = {pid: blob[pid] for pid in sorted(rostered) if pid in blob}
    extras = [
        pid
        for pid in sorted(blob, key=lambda p: (blob[p].get("search_rank") or 10**9, p))
        if pid not in rostered
    ][:EXTRA_FREE_AGENTS]
    for pid in extras:
        sliced[pid] = blob[pid]
    missing = rostered - set(sliced)
    if missing:
        raise SystemExit(f"/players/nfl is missing rostered ids {sorted(missing)}")
    written["players_slice"] = _dump(out_dir, "players_slice", sliced)

    (out_dir / "README.md").write_text(
        README.format(
            league_id=league_id,
            draft_id=draft_id,
            recorded_on=date.today().isoformat(),
            extras=EXTRA_FREE_AGENTS,
        ),
        encoding="utf-8",
    )
    return written


async def _main() -> int:
    settings = get_settings()
    league_id = settings.sleeper_mock_league_id
    if not league_id:
        print("set FFH_SLEEPER_MOCK_LEAGUE_ID in backend/.env first", file=sys.stderr)
        return 2
    async with SleeperClient() as client:
        written = await record(league_id, FIXTURES, client)
    for stem, size in sorted(written.items()):
        print(f"{stem}.json  {size:>9,} bytes")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(_main()))
```

- [ ] **Step 4: Run tests**

Run: `cd backend && uv run pytest tests/adapters/sleeper/test_record_fixtures.py -v`
Expected: PASS (2 run, 1 skipped by the `network` marker exclusion).

Note the recorder writes into `tmp_path` in tests — it never rewrites the committed
fixtures during a test run.

- [ ] **Step 5: Commit** (the recorded fixtures themselves land later, when Chris runs the
script against his mock league; this commit is the tool only)

```bash
cd backend && uv run ruff check . && uv run ruff format .
cd .. && git add backend/scripts backend/tests/adapters/sleeper/test_record_fixtures.py
git commit -m "test(adapters): network-marked recorder for real Sleeper fixtures"
```

---

### Task 10: `test_crosswalk_covers_all_rostered_players`

> **Requires ③ and ④ merged (after Task 7).** The registry is seeded by
> `backend/tests/ingest/_sleeper_seed.py` (Task 7, Step 2a) — ④'s `apply_playerids` over a
> `DP_REQUIRED_COLUMNS`-shaped frame built from the fixture blob, plus ④'s
> `seed_dst_players`. Nothing here hand-builds `players` rows.

**Files:**
- Test: `backend/tests/ingest/test_crosswalk_coverage.py`

**Interfaces:**
- Consumes: `tests.ingest._sleeper_seed.{seed_fixture_players, SEEDED_PLAYERS}` (Task 7; wraps ④'s `apply_playerids` and `seed_dst_players`); Task 7's `load_league`; Task 5's `SleeperAdapter`.
- Produces: the `docs/DATABASE.md` §3 mandatory test — *"every player on every roster in the league resolves. Failing this = the app is wrong."*

- [ ] **Step 1: Write the test `backend/tests/ingest/test_crosswalk_coverage.py`**

```python
"""docs/DATABASE.md §3: test_crosswalk_covers_all_rostered_players.

Failing this means the app is wrong: a crosswalk gap presents as a MISSING PLAYER, which
looks exactly like a player who isn't rostered, and quietly moves every VORP baseline.
"""

import pytest
from sqlalchemy import func, select

from ffh.adapters.sleeper.adapter import SleeperAdapter
from ffh.adapters.sleeper.client import SleeperClient
from ffh.db.models import CrosswalkUnmatched, Player, RosterSlot
from ffh.ingest.platform_sync import load_league
from tests.ingest._sleeper_seed import SEEDED_PLAYERS, seed_fixture_players

pytestmark = pytest.mark.db

LEAGUE = "1000000000000000001"


@pytest.fixture
def seeded(db_session):
    """Same recipe as Task 7's `seeded`: ④ apply_playerids (23 humans, sleeper ids at rung 1)
    + ④ seed_dst_players (32 DSTs, resolved at rung 3 by `<abbr> dst`)."""
    seed_fixture_players(db_session)
    assert db_session.scalar(select(func.count()).select_from(Player)) == SEEDED_PLAYERS  # 23 + 32
    return db_session


def test_crosswalk_covers_all_rostered_players(seeded, sleeper_mock):
    adapter = SleeperAdapter(SleeperClient(), my_user_id="USER_ME")
    report = load_league(seeded, adapter, LEAGUE, season=2026, week=1)

    assert report.unmatched == [], (
        "unmatched rostered players: "
        + ", ".join(f"{u.external_id}/{u.name}/{u.position}" for u in report.unmatched)
    )
    assert report.pending_review == [], (
        "rostered players awaiting crosswalk review: "
        + ", ".join(f"{u.external_id}/{u.name}/{u.position}" for u in report.pending_review)
    )
    assert report.rostered == 23
    assert seeded.scalar(select(func.count()).select_from(RosterSlot)) == 23
    assert seeded.scalar(select(func.count()).select_from(CrosswalkUnmatched)) == 0


def test_defenses_resolve_through_the_dst_canonical_form(seeded, sleeper_mock):
    adapter = SleeperAdapter(SleeperClient(), my_user_id="USER_ME")
    load_league(seeded, adapter, LEAGUE, season=2026, week=1)
    dst_slots = seeded.scalars(select(RosterSlot).where(RosterSlot.slot == "DST")).all()
    assert len(dst_slots) == 2
    positions = {seeded.get(Player, s.player_id).position for s in dst_slots}
    assert positions == {"DST"}
```

- [ ] **Step 2: Run it**

Run: `cd backend && uv run pytest tests/ingest/test_crosswalk_coverage.py -v`
Expected: PASS.

If `test_crosswalk_covers_all_rostered_players` reports the two defenses as unmatched,
④'s `normalize_dst` is not canonicalizing a bare team abbreviation — that is a real ④ bug;
fix it there (it is in ④'s locked scope: *"`KC`, `KC DST`, `Chiefs D/ST`, `Kansas City` →
`kc dst`"*) rather than special-casing here.

If a numeric id is unmatched, `apply_playerids` did not write a
`player_external_ids(source='sleeper')` row for it — check `playerids_frame()` in
`tests/ingest/_sleeper_seed.py` against ④'s merged `DP_REQUIRED_COLUMNS` (the helper
asserts the column set, so a drift fails there first) and make sure `mfl_id` is non-null and
unique per row (④ keys gsis-less rows on `mfl:<mfl_id>`).

- [ ] **Step 3: Full suite**

Run: `cd backend && uv run pytest -v`
Expected: all green (the `network`-marked test stays deselected).

- [ ] **Step 4: Commit**

```bash
git add backend/tests/ingest/test_crosswalk_coverage.py
git commit -m "test(crosswalk): every rostered player in the fixture league resolves"
```

---

### Task 11: Docs, ROADMAP, PR body

**Files:**
- Modify: `docs/ARCHITECTURE.md`
- Modify: `docs/DATA_SOURCES.md`
- Modify: `docs/DATABASE.md`
- Modify: `docs/ROADMAP.md`

- [ ] **Step 1: `docs/ARCHITECTURE.md`**

1. In the `## Module boundaries` code block, extend the `ingest/` line so the map has a
   home for league loading:

   ```
     ingest/        Fetch → validate → land as Parquet. Idempotent, watermarked.
                    No business logic. `ingest/platform_sync.py` is the one exception
                    to "land as Parquet": it lands a league in Postgres (fetch →
                    validate → land is still the shape). It is SYNCHRONOUS and takes an
                    orm.Session; adapters are async and the boundary is crossed once,
                    in load_league().
   ```

2. Immediately after the Protocol code block, add:

   > **Async boundary.** Adapter methods are `async`; `ffh.ingest.platform_sync` is
   > synchronous and takes a `Session`. `load_league()` crosses the boundary once with
   > `asyncio.run(fetch_snapshot(...))` and refuses to run inside a live event loop — a
   > caller already in async land awaits `fetch_snapshot()` then calls
   > `persist_snapshot()`. Because nothing here opens an **async psycopg** connection, no
   > `WindowsSelectorEventLoopPolicy` hook is needed in tests. The first PR that does
   > introduce async psycopg in tests must add
   > `if sys.platform == "win32": asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())`
   > to `backend/tests/conftest.py`.

3. Under **"Scoring and roster settings are always fetched, never hardcoded"**, append:
   "`leagues.scoring_settings` stores the platform payload verbatim — a test asserts no key
   is added or removed. `ScoringSettings.format` is derived from `rec` for downstream
   convenience and is never an input."

- [ ] **Step 2: `docs/DATA_SOURCES.md` §3 Sleeper** — replace the endpoint list's trailing
notes with the verified facts. Add, after the "Live draft" paragraph:

```markdown
**Verified live 2026-08-16** (shapes recorded in
`docs/superpowers/plans/2026-08-15-phase0-05-adapter-sleeper.md`):

- `/state/nfl` returns `week` **even during the preseason** (`{"week":2,"season_type":"pre"}`
  on 2026-08-16). `week` is only a regular-season week when `season_type == "regular"`;
  FFH uses week **0** for a pre-season roster snapshot.
- `/players/nfl` is **14.6 MB uncompressed** (~5 MB gzipped), a **dict keyed by player id**
  with 12,219 entries, of which **32 are team defenses keyed by team abbreviation**
  (`"KC"`) with no `full_name` and no `gsis_id`. **8,326 entries have a null `gsis_id`.**
- `/players/nfl` sends an `ETag` but **ignores `If-None-Match`** — a conditional GET still
  returns 200 with the full body. Freshness must be a content hash, and the ≤1×/day
  partition guard is the real protection.
- `rosters[].starters` is ordered to match `roster_positions` minus `BN`; an unfilled slot
  is the string **`"0"`**, which is not a player id. Pre-draft rosters are all `"0"` with
  `players: []`.
- `settings.type` is `0` redraft / `1` keeper / `2` dynasty. `settings.waiver_type == 2`
  means FAAB; `waiver_budget` is present but meaningless for `0`/`1`.
- Superflex is detected by `SUPER_FLEX` in `roster_positions` — there is no boolean.
- `/draft/{id}` adds `slot_to_roster_id` (keys are slot **strings**); `/league/{id}/drafts`
  does not. `last_picked`, `start_time` and `draft_order` are **null before the draft opens**.
- `/draft/{id}/picks` carries **no per-pick timestamp**. `metadata.amount` is the auction
  bid **as a string**, and `"0"` in a snake draft.
- `/league/{id}/matchups/{wk}` returns one row **per roster**; pairs are grouped by
  `matchup_id`, which is `null` for a bye.
```

Also update the client note: replace *"The API is simple enough that a thin in-house client
is defensible"* with *"FFH ships a thin in-house async client (`ffh.adapters.sleeper.client`)
with a 300 req/min token bucket; no third-party wrapper is a dependency."*

- [ ] **Step 3: `docs/DATABASE.md` §4** — append one paragraph under the `leagues` DDL block
(③'s Task 10 leaves the matching note in §6 beside the sentinel row):

```markdown
*Phase 0 note — `roster_settings` has two shapes.* The sentinel generic league (③,
`seed_generic_league`) stores a **count map** `{"QB": 1, "RB": 2, …}`; platform-loaded
leagues (⑤, `ffh.ingest.platform_sync`) store `RosterSettings.model_dump()` —
`{"starters": [...], "bench", "ir", "taxi", "flex_composition", "is_superflex"}`.
Consumers must branch on shape (e.g. `"starters" in roster_settings`), never assume one.
```

- [ ] **Step 4: `docs/ROADMAP.md`** — tick the Phase 0 item:

```markdown
- [x] Platform adapter interface + **Sleeper implementation** (no auth, no approval
      dependency — the one that can't block us)
```

> ③ and ④ tick the adjacent Phase 0 lines (crosswalk, nflverse ingest, games.csv). Expect a
> trivial rebase conflict in this block when ③/④ land first — **keep every tick**, resolve
> by hand, do not drop theirs.

- [ ] **Step 5: Full verification before claiming done**

```bash
cd backend
uv run ruff check .
uv run ruff format --check .
uv run pytest -v
```
Expected: all three clean/green. Do not proceed on a partial run.

- [ ] **Step 6: Commit docs**

```bash
git add docs/ARCHITECTURE.md docs/DATA_SOURCES.md docs/DATABASE.md docs/ROADMAP.md
git commit -m "docs(adapters): verified Sleeper shapes, platform_sync module home, roster_settings shapes, roadmap tick"
```

- [ ] **Step 7: Prepare the PR body — do NOT push.** Chris pushes and runs Codex.

Write this to `backend/.pr-body.md` (gitignored) or paste it directly:

```markdown
## Summary
- `ffh.adapters.base`: `FantasyPlatformAdapter` Protocol (verbatim from ARCHITECTURE.md)
  + frozen Pydantic v2 normalized models + `PlatformError` hierarchy
- `ffh.adapters.sleeper`: raw wire models, async client (token bucket 300 req/min, tenacity
  backoff on 429/5xx), adapter mapping, lake-backed player catalog, `sleeper_players` job
- `ffh.ingest.platform_sync.load_league`: leagues / league_teams / roster_slots / drafts /
  draft_picks, crosswalk-resolved, idempotent, same-league invariant enforced
- CLI `ffh league load sleeper <id> [--season] [--week]`, exit 1 on unmatched > 0 or
  pending_review > 0 (④ rung-4 rows awaiting `ffh crosswalk verify`)
- Fixtures + `scripts/record_sleeper_fixtures.py`; CI never touches the network

## Verified live 2026-08-16
`/state/nfl` returns `week` during the preseason (so week 0 is our pre-season snapshot);
`/players/nfl` ignores `If-None-Match`; `starters` uses `"0"` for an empty slot; picks have
no timestamp; DEF entries are keyed by team abbreviation. All recorded in DATA_SOURCES.md.

## Decisions
- Roster snapshot week: `state.week` when `season_type == "regular"`, else **0**
- `platform_sync` is **sync** (`Session`), adapters are **async**; boundary crossed once in
  `load_league` via `asyncio.run`. No `WindowsSelectorEventLoopPolicy` hook needed.
- Sleeper `DEF` → `DST` at the adapter boundary; a defense's ref name is its team abbrev
- `matchups`/`transactions` are adapter-complete but not persisted until Phase 2

## Deviation recorded in ARCHITECTURE.md
`ffh.ingest.platform_sync` lands a league in Postgres rather than Parquet.

## Dependencies
None added.

Spec: docs/superpowers/specs/2026-08-15-phase0-foundation-design.md §5
Plan: docs/superpowers/plans/2026-08-15-phase0-05-adapter-sleeper.md

🤖 Generated with [Claude Code](https://claude.com/claude-code)

https://claude.ai/code/session_018YN1sk8qzRpGdzgXYHBPgP
```

- [ ] **Step 8: Codex adversarial review** (Chris runs it, per `docs/WORKFLOW.md` and
`AGENTS.md`). Hunt list to hand over:
  - **Tier 1:** any hardcoded scoring/roster default; the `"0"` starter placeholder reaching
    the crosswalk; epoch-ms vs seconds on `last_picked`/`start_time`/`status_updated`;
    week 0 vs `state.week` off-by-one; a roster whose `starters` length disagrees with
    `roster_positions`; unmatched players dropped instead of reported.
  - **Tier 2:** anything that writes to a platform; `import pandas`.
  - **Tier 3:** the 429/5xx backoff actually firing; retries re-firing DB writes (they
    cannot — retries are inside `get_json`, before any persistence); undocumented endpoints
    (none are used in this PR).

Resolve BLOCKING findings or rebut them in writing; then merge.

---

## Self-Review

**1. Spec coverage** (`spec §5` and the overview's ⑤ block):

| Requirement | Task |
|---|---|
| `ffh.adapters.base`: Protocol verbatim + normalized models | 1 |
| `ScoringSettings` flat mapping, always platform-fetched | 1 (model), 5 (mapping), 7 (verbatim persist test) |
| `RosterSettings` starters/bench/IR/flex composition | 1, 5 |
| `League`, `LeagueTeam`, `Roster`, `Matchup`, `Transaction`, `PlayerRef`, `Draft`, `DraftPick` | 1 |
| `PlatformError`/`PlatformAuthError`/`PlatformNotFound` | 1 |
| `sleeper/models.py` raw models incl. `scoring_settings`, `roster_positions`, `settings`, `metadata` | 2 |
| Token bucket ≤ 300 req/min, burst 30, deterministic test | 3 |
| tenacity backoff on 429/5xx, `PlatformNotFound` on 404 | 4 |
| Every documented endpoint (`/state/nfl`, `/user/*`, `/league/*`, `/rosters`, `/users`, `/matchups`, `/transactions`, `/league/{id}/drafts`, `/draft/{id}`, `/picks`) | 4 |
| `/players/nfl` only via IngestJob | 6 (and explicitly excluded from the client in 4) |
| Adapter mapping incl. slot assignment, `DEF`→`DST`, `is_me`, `draft_changed_since` epoch ms | 5 |
| `get_free_agents` from the lake blob | 5 (`LakePlayerCatalog`) |
| `sleeper_players` job, Parquet landing, ≤1×/day | 6 |
| `platform_sync.load_league` + `LeagueLoadReport` | 7 |
| `is_superflex` derived; settings JSONB verbatim; `my_team_id` after teams | 7 |
| Same-league invariant on `draft_picks` with a test | 7 |
| Idempotency | 7 |
| CLI exits 1 on unmatched or pending_review | 8 |
| Fixtures + `record_sleeper_fixtures.py` (network-marked) | 4 (hand-written), 9 (recorder) |
| `test_crosswalk_covers_all_rostered_players` | 10 |
| Docs: ARCHITECTURE module map, DATA_SOURCES, ROADMAP, PR body | 11 |

No gaps.

**2. Placeholder scan** — no "TBD", no "add error handling", no "similar to Task N". Every
code step carries the actual code. Three places consume a contract that is merged in a
different PR, using its **exact** merged names (③'s `IngestJob`/`HttpIngestJob`,
`@register`, `name` ClassVar, `IngestValidationError`, `NotModified(etag)`, `scrape_date`,
`_session_scope`; ④'s `ResolveInput` / `ResolveManyReport(resolved keyed by (source,
external_id), unmatched, pending_review)`; ④'s `DP_REQUIRED_COLUMNS` incl. `mfl_id`);
each says where to re-verify after the rebase and names the single place to change if
`main` differs (`SleeperPlayersJob`, `_resolve_refs`, `tests/ingest/_sleeper_seed.py`).
That is a dependency boundary, not a placeholder.

**3. Type consistency** — checked across tasks:
`ScoringSettings.points` / `.format`; `RosterSettings.starters|bench|ir|taxi|flex_composition|is_superflex`;
`League.playoff_start_week` (model) → `leagues.playoff_start_wk` (column) mapped explicitly in
`_upsert_league`; `Draft.last_picked_ms` used by `draft_changed_since` and by
`RawDraft.last_picked`; `LeagueSnapshot.player_refs` produced by `SleeperAdapter.get_player_refs`
and consumed by `persist_snapshot`; `LakePlayerCatalog.REQUIRED_COLUMNS` == the first four
entries of `players_job.PLAYER_COLUMNS` == `SleeperPlayersJob.REQUIRED_COLUMNS`
(`player_id, name, position, team`) and Task 6 has an explicit test that the landed
partition is readable by the catalog; `LeagueLoadReport` fields (incl. `pending_review`)
match the CLI's output line and the tests in Tasks 7, 8 and 10; the `seeded` fixture in
Tasks 7 and 10 shares one recipe (`tests/ingest/_sleeper_seed.py`, `SEEDED_PLAYERS = 23 +
32`); `player_ref()` is defined once in `adapter.py` and reused by `players_job.py`.

One issue found and fixed inline while reviewing: `_rostered_refs` originally passed an empty
position and no team to the crosswalk, which made team defenses unresolvable by construction
(rung 3 needs `(normalized_name, position, team)`). Replaced with
`SleeperAdapter.get_player_refs` + `LeagueSnapshot.player_refs`, added to Task 7's adapter
edit, with tests in Tasks 7 and 10.

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-08-15-phase0-05-adapter-sleeper.md`.
Tasks 1–5 and 9 need only `main` at ef4b656; Task 6 requires ③ merged; Tasks 7, 8 and 10
require ③ **and** ④ merged (merge order ③ → ④ → ⑤) — rebase onto `main` before starting
each. `backend/src/ffh/cli.py` and `docs/ROADMAP.md` are touched by all three PRs: reuse
③'s `_session_scope()` (never redefine it) and keep every ROADMAP tick when resolving the
expected trivial conflict.

Two execution options:

1. **Subagent-Driven (recommended)** — a fresh Fable subagent per task, review between tasks.
2. **Inline Execution** — `superpowers:executing-plans`, batch execution with checkpoints.
