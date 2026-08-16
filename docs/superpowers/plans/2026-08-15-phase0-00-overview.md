# Phase 0 — Foundation: plan overview

> **For agentic workers:** This is the index. Each PR has (or will have) its own
> step-level plan file next to this one. Execute PR plans with
> superpowers:subagent-driven-development, one PR at a time (parallel PRs in separate
> worktrees). Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship every Phase 0 checklist item in `docs/ROADMAP.md` by 2026-08-22 so that a
Sleeper mock league loads with zero unmatched rostered players and nflverse data queries
from DuckDB.

**Spec:** `docs/superpowers/specs/2026-08-15-phase0-foundation-design.md`

**Architecture:** Two sequential foundation PRs (scaffold+CI, then schema+migration), then
three parallel tracks in isolated worktrees (nflverse/games ingest, crosswalk, Sleeper
adapter), then ADP/ECR, then the exit-criteria run.

**Tech Stack:** Python 3.13 · uv · FastAPI · Polars · DuckDB · SQLAlchemy 2 + Alembic ·
psycopg 3 · httpx · tenacity · typer · rapidfuzz · pytest/respx · Bun · Vite · React 19 ·
Tailwind v4 · Docker Compose · GitHub Actions (`ubuntu-24.04-arm`) · GHCR.

## Global Constraints (apply to every task in every PR plan)

- Never `import pandas`; never `nfl_data_py`; never `nflreadpy` (read Parquet URLs directly).
- No SQLite, no `.duckdb` file — anywhere, including tests. Postgres is the test DB.
- Python base image `python:3.13-slim-bookworm`, never Alpine. Frontend `oven/bun:*-debian`.
- CI runs on `ubuntu-24.04-arm`; no QEMU; images `linux/arm64` only.
- Every dependency version pinned only after checking its publish date is ≥ 7 days old
  (`https://pypi.org/pypi/<pkg>/json`, `npm view <pkg> time`, Docker Hub tags API,
  `gh release view` for Actions). Verified on 2026-08-15 — see the scaffold plan.
- Every Polars join asserts row counts or passes `validate=`.
- No secret in the repo. Compose dev credentials (`ffh`/`ffh`) are throwaway and env-passed.
- Docs updated in the same PR as the code (DATABASE.md, DATA_SOURCES.md, ARCHITECTURE.md).
- Branch per PR (`chore/`, `feat/`, `docs/`), conventional commits scoped to the module.
- Definition of done per `docs/WORKFLOW.md`, including the **Codex adversarial review**
  that Chris runs on the open PR. BLOCKING findings are fixed or rebutted in writing.

## Model routing

| Role | Model | How |
|---|---|---|
| Plan author | Fable (this session) | Plans written by the session that holds the docs |
| Implementer subagent (one per task) | **Fable** (`model: "fable"` on the Agent tool) | Fresh context, receives the task text + spec path |
| Spec-compliance reviewer subagent | **Fable** | Fresh context, receives task text + diff |
| Code-quality reviewer subagent | **Fable** | Fresh context, receives diff |
| Adversarial PR review | **Codex** (Chris runs it) | Per `docs/WORKFLOW.md`, `AGENTS.md` format |

## PR sequence and status

| # | Branch | Plan file | Depends on | Status |
|---|---|---|---|---|
| ① | `chore/scaffold` | `2026-08-15-phase0-01-scaffold.md` | — | merged |
| ② | `feat/db-schema` | `2026-08-15-phase0-02-db-schema.md` | ① | merged |
| ③ | `feat/ingest-nflverse-games` | `…-03-ingest-nflverse-games.md` | ② | plan written 2026-08-16 |
| ④ | `feat/crosswalk` | `…-04-crosswalk.md` | ② | plan written 2026-08-16 |
| ⑤ | `feat/adapter-sleeper` | `…-05-adapter-sleeper.md` | ② (client), ④ (sync) | plan written 2026-08-16 |
| ⑥ | `feat/ingest-adp-ecr` | `…-06-ingest-adp-ecr.md` | ③ ④ | write after ④ merges |
| ⑦ | `docs/phase0-complete` | `…-07-exit-run.md` | ③ ④ ⑤ ⑥ | write after ⑥ merges |

Plans ③–⑦ are deliberately written just-in-time: their step-level code depends on the
concrete module names, fixtures, and session helpers that ① and ② produce. Their **scope
is fixed now** (below) so the shape cannot drift.

## Locked scope for the just-in-time plans

### ③ `feat/ingest-nflverse-games`
- Create `backend/src/ffh/ingest/base.py` — `IngestJob` ABC (`source`, `asset`,
  `partition() -> dict[str,str]`, `fetch(etag: str | None) -> Fetched | NotModified`,
  `validate(df: pl.DataFrame) -> None`, `run(session, lake_root) -> IngestRunResult`),
  `Fetched(bytes, etag, mtime)`, `NotModified`, `IngestRunResult(status, rows_written,
  output_path)`; registry `JOBS: dict[str, type[IngestJob]]` and `get_job(name)`.
- Create `backend/src/ffh/ingest/http.py` — shared `httpx.Client` factory with tenacity
  retry (429/5xx, exponential, max 5) and `If-None-Match` support.
- Create `backend/src/ffh/ingest/lake.py` — `partition_path(lake_root, source, asset,
  **keys) -> Path`, `write_parquet(df, path)` (fails if path exists — never overwrite).
- Create `backend/src/ffh/ingest/nflverse.py` — jobs `nflverse_players`,
  `nflverse_stats_player_week`, `nflverse_snap_counts`, `nflverse_depth_charts`,
  `nflverse_injuries`, `nflverse_pbp` (404 → `skipped`).
- Create `backend/src/ffh/ingest/games.py` — `nfldata_games` job + `upsert_games(session,
  df, season)`; `backend/src/ffh/ingest/reference.py` — `seed_nfl_teams(session)` from
  `backend/src/ffh/data/nfl_teams.csv` (checked in), `seed_stadiums(session, df)` from
  the greerreNFL CSV job `stadiums`, `seed_generic_league(session)` (sentinel
  `leagues` row per DATABASE.md §6: `league_id = GENERIC_LEAGUE_ID`
  (`00000000-0000-0000-0000-000000000000`), `platform='ffh'`, `external_id='generic'`,
  `season=0`, `name='Generic PPR'`, `num_teams=12`, `league_type='redraft'`,
  `is_superflex=false`, `scoring_settings={"pass_yd":0.04,"pass_td":4,"pass_int":-2,
  "rush_yd":0.1,"rush_td":6,"rec":1,"rec_yd":0.1,"rec_td":6,"fum_lost":-2,"two_pt":2}`
  (canonical full-PPR reference, NOT a default for real leagues — those are always
  platform-fetched), `roster_settings={"QB":1,"RB":2,"WR":2,"TE":1,"FLEX":1,"K":1,
  "DST":1,"BN":6}`).
- Ingest upserts must `SET updated_at = now()` explicitly (ORM `onupdate` does not fire
  for `INSERT ... ON CONFLICT`).
- Create `backend/src/ffh/features/duck.py` — `connect(lake_root, season) ->
  duckdb.DuckDBPyConnection` with views `stats_player_week`, `snap_counts`, `players`,
  `depth_charts`, `injuries`, `games`.
- CLI: `ffh ingest run <job> [--season]`, `ffh ingest list`.
- Tests: framework idempotency + 304 path; per-job validate on small recorded fixtures under
  `backend/tests/fixtures/nflverse/`; DuckDB view query; games upsert idempotent;
  stadium join 100 % matched.
- Docs: DATA_SOURCES.md — confirm URLs used; DATABASE.md — lake layout unchanged.

### ④ `feat/crosswalk`
- Create `backend/src/ffh/crosswalk/normalize.py` — `normalize_name(raw: str) -> str`,
  `ALIASES: dict[str,str]`, `normalize_dst(raw: str) -> str | None`.
- Create `backend/src/ffh/crosswalk/registry.py` — `seed_players(session, players_df) ->
  int` (upsert on `gsis_id`).
- Create `backend/src/ffh/crosswalk/dynastyprocess.py` — job `dynastyprocess_playerids`
  + `apply_playerids(session, df) -> CrosswalkApplyReport`.
- Create `backend/src/ffh/crosswalk/resolve.py` — `Resolution(player_id, method,
  confidence)`, `resolve(session, source, external_id, raw_name, raw_position, raw_team)
  -> Resolution | None` (ladder, unmatched upsert on `None`), `resolve_many(...)`.
- Create `backend/src/ffh/crosswalk/report.py` + CLI `ffh crosswalk report`.
- Tests: normalization table (≥ 40 cases), ladder ordering, unmatched create/bump,
  `test_crosswalk_no_duplicate_player_ids`, `test_crosswalk_low_confidence_reviewed`.
- Docs: DATABASE.md §3 — record the alias table location and the DST canonical form.

### ⑤ `feat/adapter-sleeper`
- Create `backend/src/ffh/adapters/base.py` — Protocol + Pydantic models exactly as
  spec §5.
- Create `backend/src/ffh/adapters/sleeper/{client.py,adapter.py,models.py}` — token
  bucket ≤ 300 req/min; raw response models → normalized models; `players_nfl` blob job
  `sleeper_players` landing to lake.
- Create `backend/src/ffh/ingest/platform_sync.py` — `load_league(session, adapter,
  external_id, season) -> LeagueLoadReport(league_id, teams, rostered, unmatched)`.
- CLI: `ffh league load sleeper <league_id> [--season]` exits 1 if unmatched > 0.
- Fixtures: `backend/tests/fixtures/sleeper/*.json` recorded from Chris's mock league via
  a `network`-marked recorder script `backend/scripts/record_sleeper_fixtures.py`.
- Tests: adapter contract per method; rate limiter burst; `platform_sync` persists and is
  idempotent; `test_crosswalk_covers_all_rostered_players`.
- Docs: ARCHITECTURE.md module map gains `ingest/platform_sync.py`.

### ⑥ `feat/ingest-adp-ecr`
- Step 0: live-verify FFC `https://fantasyfootballcalculator.com/api/v1/adp/ppr?teams=12&year=2026`;
  record result in DATA_SOURCES.md.
- Create `backend/src/ffh/ingest/adp.py` — jobs `dynastyprocess_ecr`, `ffcalculator_adp`
  (if verified), `sleeper_adp`; `upsert_adp(session, df, source, format, num_teams,
  scrape_date)` rejecting NULL `adp_stdev`; `estimate_stdev_from_ecr(...)`.
- Tests: `test_crosswalk_covers_top_300_adp`; idempotency; stdev-not-null enforcement.

### ⑦ `docs/phase0-complete`
- Run every job + `ffh league load sleeper <mock_id>` against local compose; capture the
  `crosswalk report` output in the PR description; tick ROADMAP.md boxes; append progress
  log line; open PR.

## Local environment (once, before PR ①)

1. Start Docker Desktop.
2. `uv python install 3.13` (uv manages the interpreter; system Python 3.14 is not used).
3. Chris creates a Sleeper mock draft league and records the `league_id` and `draft_id`
   in `backend/.env` (`FFH_SLEEPER_MOCK_LEAGUE_ID`, gitignored) — needed by PR ⑤.
