# Phase 0 — Foundation: design spec

**Date:** 2026-08-15 · **Window:** Aug 15–22 · **Status:** approved by Chris 2026-08-15

This spec resolves the decisions `docs/ROADMAP.md` Phase 0 leaves open. The authoritative
requirements remain in `docs/` (ARCHITECTURE, DATABASE, DATA_SOURCES, DEPLOYMENT,
WORKFLOW); this document only says *how Phase 0 is built and sequenced*, and records the
deviations from those docs that Phase 0 introduces.

## Goal and exit criteria (from ROADMAP.md)

A league loads from Sleeper, every rostered player resolves through the crosswalk with
zero unmatched, and nflverse data queries from DuckDB. Every Phase 0 checklist item in
`ROADMAP.md` is checked off, and the progress log has a Phase 0 entry.

## Decisions made in brainstorming

| Decision | Choice | Why |
|---|---|---|
| Local Postgres/Redis | `docker-compose.yml` at repo root (`postgres:17`, `redis:7`) | Self-contained; CI mirrors it with GitHub service containers |
| Live Sleeper target | A Sleeper **mock draft** league Chris creates during Phase 0 | No real 2026 league yet; mocks are free and unlimited |
| Review gate | **Fable codes, Codex reviews** — `docs/WORKFLOW.md` unchanged | Chris's call; Codex remains the required adversarial gate |
| Execution shape | Foundation PRs first, then parallel tracks in worktrees | Crosswalk (highest risk) starts day 2, not day 5 |
| nflverse loader | Read release Parquet URLs directly with httpx + Polars; **no `nflreadpy` dependency** | Endorsed in DATA_SOURCES.md §1; last package release Nov 2025 |
| Test DB | Real Postgres via `DATABASE_URL`; tests marked `db`; **no SQLite ever** | CLAUDE.md rule 5 and fidelity of migration tests |
| CLI | `typer`-based `ffh` CLI: `ingest run`, `league load`, `crosswalk report` | Same entrypoints k8s CronJobs call in Phase 3 |
| Model routing | Fable for every implementation and review subagent; plan written by Fable in-session | Chris's instruction; planner already holds the docs in context |

## Components

### 1. Repo scaffold and tooling — PR ① `chore/scaffold`

```
backend/
  pyproject.toml           uv-managed, hatchling, Python 3.13 (.python-version)
  src/ffh/                 adapters/ ingest/ crosswalk/ features/ projections/
                           engine/ ai/ api/ db/ — all packages exist, most empty
  src/ffh/config.py        pydantic-settings: DATABASE_URL, REDIS_URL, LAKE_ROOT,
                           SLEEPER_BASE_URL, LOG_LEVEL
  src/ffh/cli.py           typer app; subcommands registered by later PRs
  src/ffh/api/app.py       FastAPI app with GET /health only
  tests/                   pytest, pytest-asyncio; markers: db, network (network never in CI)
  tests/test_engine_purity.py
  alembic.ini, alembic/    (env wired in PR ②)
  Dockerfile               python:3.13-slim-bookworm, multi-stage, uv sync --frozen
frontend/
  bun + vite + react 19 + typescript + tailwind v4; one /health page; bun test wired
  Dockerfile               oven/bun:debian build stage → static serve
deploy/
  base/kustomization.yaml  namespace + placeholder; filled in Phase 3
  overlays/homelab/        kustomization.yaml referencing base
  argocd/                  Application skeleton per DEPLOYMENT.md
docker-compose.yml         postgres:17 (port 5432), redis:7 (port 6379), named volumes
.github/workflows/ci.yml   ubuntu-24.04-arm; ruff check + ruff format --check → pytest
                           (postgres service) → bun test → on main: build + push GHCR
                           (linux/arm64 only, no QEMU)
```

Engine purity test (ships here, guards all later PRs): parse every module under
`ffh.engine` with `ast`, fail on any import of `httpx`, `requests`, `aiohttp`,
`anthropic`, `openai`, `sqlalchemy`, `redis`, `duckdb`, `psycopg`, or `ffh.ai`,
`ffh.adapters`, `ffh.db`, `ffh.ingest`, `ffh.api`. Also imports `ffh.engine` and asserts
none of those modules newly appear in `sys.modules`.

Every dependency version is checked against the 7-day supply-chain cooldown
(`npm view <pkg> time`, PyPI JSON) before it is pinned.

### 2. Database — PR ② `feat/db-schema`

SQLAlchemy 2 declarative models mirroring `docs/DATABASE.md` §2–7 table-for-table and
column-for-column: `players`, `player_external_ids`, `nfl_teams`, `stadiums`, `games`,
`game_weather_forecasts`, `crosswalk_unmatched`, `leagues`, `league_teams`, `roster_slots`,
`matchups`, `transactions`, `drafts`, `draft_picks`, `adp`, `projections`,
`projection_correlations`, `player_week_actuals`, `player_injury_status`,
`recommendations`, `ai_debates`, `ingest_runs`. Indexes as specified (incl. BRIN on
`games.kickoff_at`). One initial Alembic migration. Async engine on
`postgresql+psycopg://`; a sync engine factory for Alembic and CLI jobs.

Tests: fresh DB → `alembic upgrade head` succeeds; `alembic check` reports no drift;
`alembic downgrade base` succeeds.

**Deviation:** `nfl_teams.bye_week` is defined in DATABASE.md as a column on a static
table but is per-season data. Phase 0 keeps the column (schema fidelity) but leaves it
NULL and derives byes from `games` at query time. Noted in DATABASE.md in the same PR.

### 3. Ingest framework, nflverse, games — PR ③ `feat/ingest-nflverse-games`

`ffh.ingest.base`:

- `IngestJob` (abstract): `source`, `asset`, `partition_keys`; `fetch() -> Fetched | NotModified`
  using httpx with `If-None-Match` from the last successful `ingest_runs.source_etag`,
  tenacity backoff on 5xx/429; `validate(df: pl.DataFrame)` asserts required columns and
  `len(df) > 0`; `land()` writes Parquet to
  `LAKE_ROOT/raw/<source>/<asset>/<partition>/<file>.parquet` — **new partition per
  scrape, never overwrite**; wraps the run in an `ingest_runs` row
  (`running` → `success` | `failed` | `skipped_not_modified`, with `rows_written`,
  `source_etag`, `output_path`, `error`).
- Registry so `ffh ingest run <name>` dispatches by name.

nflverse jobs (direct Parquet URLs, per DATA_SOURCES.md §1): `nflverse_players`,
`nflverse_stats_player_week` (**`stats_player/stats_player_week_{YEAR}.parquet`**, not the
frozen `player_stats/` path), `nflverse_snap_counts`, `nflverse_depth_charts`,
`nflverse_injuries`, `nflverse_pbp` (404 until Week 1 → `skipped`, not `failed`).
Partitioned by `season=YYYY` and `scrape_date=` where the asset is a time series.

Games: `nfldata_games` fetches `nfldata/data/games.csv`, lands Parquet, and upserts
`games` for the configured season (spread/total/moneylines/roof/surface/rest/
`neutral_site`/post-game actuals). `stadiums` seeded from `greerreNFL/stadiums` CSV;
`nfl_teams` seeded from a checked-in static table (32 rows, nflverse abbreviations, ESPN
ids). Join `games.stadium_id → stadiums` asserted 100 % matched (DATA_SOURCES.md says
30/30).

`ffh.features.duck.connect(lake_root) -> duckdb.Connection` opens an in-memory DuckDB
(never a `.duckdb` file) with `read_parquet` views over the lake for the season.

Tests: framework idempotency (run twice → one partition, two `ingest_runs` rows, second is
`skipped_not_modified` when the fixture returns 304); each job's `validate` on a recorded
small fixture; DuckDB view query on the landed fixture; games upsert idempotent; every
Polars join asserts row counts.

### 4. Crosswalk — PR ④ `feat/crosswalk`

- `ffh.crosswalk.normalize.normalize_name(raw) -> str`: lowercase; strip suffixes
  (`Jr.`, `Sr.`, `II`, `III`, `IV`, `V`); collapse punctuation (`D.J.`/`DJ`/`D J`);
  strip apostrophes and hyphens; apply an alias table (`Robby→Robert`, `Cam→Cameron`,
  `Mitch→Mitchell`, ...); DST canonicalization (`KC`, `KC DST`, `Chiefs D/ST`,
  `Kansas City` → `kc dst`). Table-driven tests with ≥40 cases including every example in
  DATABASE.md §3.
- `ffh.crosswalk.registry.seed_players(lake)`: upsert `players` from nflverse
  `players.parquet` keyed on `gsis_id`, computing `normalized_name`.
- `ffh.crosswalk.dynastyprocess`: `IngestJob` for `db_playerids.csv`; then populate
  `player_external_ids` for `sleeper`, `espn`, `yahoo`, `pfr`, `fantasypros`,
  `sportradar`, `rotowire` at `confidence=1.0`, `match_method='dynastyprocess'`, joined
  to `players` via `gsis_id`. Rows whose gsis is absent create a `players` row (rookies).
- `ffh.crosswalk.resolve.resolve(source, external_id, raw_name, raw_pos, raw_team) -> Resolution`
  implementing the ladder **strictly in order**: dynastyprocess (lookup) → gsis (direct)
  → exact `(normalized_name, position, team)` at 0.95 → Jaro-Winkler ≥ 0.92 on
  `(normalized_name, position)` disambiguated by birth date/college at < 0.9 (persisted
  with `verified_at NULL`) → `crosswalk_unmatched` upsert (`last_seen` bumped). Every
  resolution records `match_method`. Nothing is silently dropped.
- `ffh crosswalk report`: counts by `match_method`, unverified low-confidence rows,
  unmatched rows.

Tests: `test_crosswalk_no_duplicate_player_ids`, `test_crosswalk_low_confidence_reviewed`,
ladder ordering (a name that matches at rung 3 and rung 4 resolves via rung 3), unmatched
row created and re-bumped, plus the normalization table. `test_crosswalk_covers_all_rostered_players`
and `test_crosswalk_covers_top_300_adp` land in PRs ⑤ and ⑥.

### 5. Sleeper adapter and league sync — PR ⑤ `feat/adapter-sleeper`

- `ffh.adapters.base`: `FantasyPlatformAdapter` Protocol exactly as in ARCHITECTURE.md,
  plus normalized Pydantic v2 models: `League`, `ScoringSettings` (a flat mapping of
  stat → points, always platform-fetched), `RosterSettings` (starter slots, bench, IR,
  flex composition), `LeagueTeam`, `Roster`, `Matchup`, `Transaction`, `PlayerRef`,
  `Draft`, `DraftPick`.
- `ffh.adapters.sleeper`: in-house httpx client. Token bucket at ≤ 300 req/min
  (well under the 1000 IP-based ceiling), tenacity backoff on 429/5xx. Endpoints:
  `/state/nfl`, `/user/{id}`, `/user/{id}/leagues/nfl/{season}`, `/league/{id}`,
  `/rosters`, `/users`, `/matchups/{wk}`, `/transactions/{round}`, `/league/{id}/drafts`,
  `/draft/{id}`, `/draft/{id}/picks`, `/players/nfl` (5 MB; landed to the lake via an
  `IngestJob`, ≤ 1×/day). `draft_changed_since` compares `last_picked` epoch ms.
- `ffh.ingest.platform_sync.load_league(adapter, external_id, season)`: persists
  `leagues` (scoring + roster settings JSONB), `league_teams`, `roster_slots` for the
  current week, `drafts`, `draft_picks`, resolving every Sleeper player id through the
  crosswalk. Any unresolved player is recorded in `crosswalk_unmatched` and the load
  returns a report; the CLI exits non-zero if unmatched > 0.
- Fixtures: recorded with `respx` from Chris's mock-draft league (JSON checked in under
  `tests/fixtures/sleeper/`). CI never touches the network (`network` marker excluded).
- **Deviation:** the module map in ARCHITECTURE.md has no home for "load a league into
  Postgres". It lives in `ffh.ingest.platform_sync` (fetch → validate → land, landing in
  Postgres rather than Parquet). Noted in ARCHITECTURE.md in this PR.

Tests: adapter contract tests per method against fixtures; rate limiter never exceeds its
budget under a burst; `test_crosswalk_covers_all_rostered_players` over the fixture league.

### 6. ADP and ECR — PR ⑥ `feat/ingest-adp-ecr`

Step 0 (WORKFLOW.md "adding a data source", rule 1): verify Fantasy Football Calculator
live before writing code; record the result in DATA_SOURCES.md.

Jobs: `dynastyprocess_ecr` (`db_fpecr.parquet`, has `sd`/`best`/`worst`) → lake +
`adp` rows with `source='dynastyprocess_ecr'` and `adp_stdev` = `sd`;
`ffcalculator_adp` (`/api/v1/adp/{format}?teams=12&year=2026`) if verified — carries
`stdev`; `sleeper_adp` (`api.sleeper.com/projections/nfl/{season}/{week}` `adp_dd_ppr`)
as a fallback with `adp_stdev` estimated from ECR dispersion. Application-level check:
no `adp` row is written with `adp_stdev IS NULL`. Every player id is resolved through
the crosswalk (fantasypros/sleeper ids), unmatched → `crosswalk_unmatched`.

Test: `test_crosswalk_covers_top_300_adp` on the fixture snapshot; ingest idempotency.

### 7. Exit-criteria run — PR ⑦ `docs/phase0-complete`

Against the mock league: `ffh ingest run` for every job, `ffh league load sleeper <id>`
returns unmatched = 0, DuckDB query over `stats_player_week` returns rows. Tick every
Phase 0 box in ROADMAP.md and append the progress-log line.

## Cross-cutting rules enforced in Phase 0

- Polars-native; `import pandas` fails CI (ruff banned-import rule).
- Every Polars join in ingest/crosswalk asserts row counts or uses `validate=`.
- No secrets in the repo; `.env` is gitignored; compose uses throwaway dev credentials
  passed via environment.
- No SQLite / `.duckdb` file anywhere, including tests.
- Docs updated in the same PR as the code they describe (DATABASE.md, DATA_SOURCES.md,
  ARCHITECTURE.md as noted).

## Sequencing and parallelism

```
① scaffold ──► ② db-schema ──┬─► ③ ingest-nflverse-games ─┐
                              ├─► ④ crosswalk ────────────┼─► ⑥ adp-ecr ─► ⑦ exit run
                              └─► ⑤ adapter-sleeper ──────┘
                                    (platform_sync waits on ④)
```

Each PR: branch per WORKFLOW.md → Fable implementer subagent in an isolated worktree
(TDD) → Fable spec-compliance and code-quality review → PR opened → **Codex adversarial
review (Chris runs it)** → BLOCKING findings resolved → merge.

## Out of scope for Phase 0

Projections, VORP/VONA, tiers, live draft poller, WebSocket, any UI beyond `/health`,
ESPN/Yahoo adapters, real k8s manifests, LLM providers, weather, FantasyCalc, ESPN odds.
