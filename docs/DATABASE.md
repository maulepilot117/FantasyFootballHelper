# Database Schema

Postgres 17 is the system of record. DuckDB reads Parquet on NFS for analytics and owns no
state. This document is authoritative — change it in the same PR as the migration.

**Migrations:** Alembic. One migration per PR. Never edit an applied migration.
**Naming:** snake_case, plural tables, `{table}_id` FKs, `created_at`/`updated_at` on
everything mutable.

---

## 1. Storage split — what goes where

Migrations live in `backend/alembic/versions/`; run `uv run alembic upgrade head` from `backend/`.

| Data | Store | Why |
|---|---|---|
| Leagues, rosters, drafts, transactions, recommendations, AI debates | **Postgres** | Transactional, queried by the API, must survive |
| Player registry + ID crosswalk | **Postgres** | Joined constantly, small, correctness-critical |
| Raw nflverse pbp, stats, snaps, charting | **Parquet on NFS** | Large, immutable, recomputable |
| Derived features | **Parquet on NFS**, read via DuckDB | Recomputable from the lake at any time |
| Projections | **Postgres** | Small, versioned, needs point-in-time audit |

**Rule: anything recomputable from source lives in the lake. Anything that represents a
decision we made, or state we can't re-derive, lives in Postgres.**

### Lake layout

Written by `ffh.ingest.lake.partition_path`. Every partition holds exactly one file named
`<asset>.parquet`; a re-scrape is a new `scrape_date=` directory, never an overwrite.

```
/nfs/ffh/lake/
  raw/
    nflverse/players/scrape_date=2026-08-16/players.parquet
    nflverse/stats_player_week/season=2026/scrape_date=2026-08-16/stats_player_week.parquet
    nflverse/snap_counts/season=2026/scrape_date=2026-08-16/snap_counts.parquet
    nflverse/depth_charts/season=2026/scrape_date=2026-08-16/depth_charts.parquet
    nflverse/injuries/season=2026/scrape_date=2026-08-16/injuries.parquet
    nflverse/pbp/season=2026/scrape_date=2026-09-14/pbp.parquet
    nfldata/games/scrape_date=2026-08-16/games.parquet
    greerre/stadiums/scrape_date=2026-08-16/stadiums.parquet
    odds/espn_live/date=2026-09-13/odds.parquet
    weather/forecast/game_id=.../forecast.parquet
    market/fantasycalc/scrape_date=2026-08-15/values.parquet
    ecr/dynastyprocess/scrape_date=2026-08-15/ecr.parquet
  features/
    player_week_usage/season=2026/...
    defense_vs_position/season=2026/...
    team_pace_script/season=2026/...
```

Partition by `season`, then by `week` or `scrape_date` where the data is a time series.
**Never overwrite a scrape partition** — a new scrape is a new partition. Reproducing a
past recommendation requires the inputs as they were.

*Phase 0 note:* `games.csv` lands under `raw/nfldata/games/`, named for its job
(`nfldata_games`), not under the `odds/` sketch this document previously showed.
`ffh.features.duck.connect()` reads the **lexicographic maximum** partition path per asset,
which is chronological because both partition keys are zero-padded ISO.

---

## 2. Core reference tables

```sql
-- Canonical player identity. One row per human being, ever.
CREATE TABLE players (
    player_id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    gsis_id         TEXT UNIQUE,              -- nflverse canonical; NULL for rookies pre-debut
    full_name       TEXT NOT NULL,
    first_name      TEXT,
    last_name       TEXT,
    normalized_name TEXT NOT NULL,            -- lowercase, punctuation + suffix stripped
    position        TEXT NOT NULL,            -- QB RB WR TE K DST
    birth_date      DATE,
    rookie_year     SMALLINT,
    height_in       SMALLINT,
    weight_lb       SMALLINT,
    college         TEXT,
    status          TEXT,                     -- Active, IR, PUP, Retired, ...
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX players_normalized_name_pos_idx ON players (normalized_name, position);
```

`updated_at` (here and on `games.updated_at`) is maintained by the ORM `onupdate` only; upserts via `INSERT ... ON CONFLICT` must set `updated_at = now()` explicitly in `SET`.

```sql
-- ★ THE CROSSWALK ★ — see §3. This is the highest-risk table in the system.
CREATE TABLE player_external_ids (
    player_id    UUID NOT NULL REFERENCES players(player_id) ON DELETE CASCADE,
    source       TEXT NOT NULL,   -- sleeper|espn|yahoo|pfr|fantasypros|sportradar|rotowire
    external_id  TEXT NOT NULL,
    confidence   REAL NOT NULL DEFAULT 1.0,   -- <1.0 means fuzzy-matched; review these
    match_method TEXT NOT NULL,               -- dynastyprocess|gsis|exact_name|fuzzy|manual
    verified_at  TIMESTAMPTZ,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (source, external_id)
);
CREATE INDEX player_external_ids_player_idx ON player_external_ids (player_id);

CREATE TABLE nfl_teams (
    team_abbr    TEXT PRIMARY KEY,      -- nflverse convention: KC, LA, LAC, ...
    espn_id      INTEGER UNIQUE,
    full_name    TEXT NOT NULL,
    conference   TEXT NOT NULL,
    division     TEXT NOT NULL,
    bye_week     SMALLINT               -- per season; see season_team_meta if this varies
);
```

*Phase 0 note:* `bye_week` is per-season data on a static table; it is left NULL and byes derive from `games` at query time. Revisit if a `season_team_meta` table is added.

```sql
CREATE TABLE stadiums (
    stadium_id   TEXT PRIMARY KEY,      -- joins nflverse games.stadium_id (verified 30/30)
    name         TEXT NOT NULL,
    latitude     DOUBLE PRECISION NOT NULL,
    longitude    DOUBLE PRECISION NOT NULL,
    altitude_ft  INTEGER,
    heading_deg  REAL,                  -- field orientation, for crosswind modeling
    surface_type TEXT,
    roof_type    TEXT,                  -- Outdoors|Dome (NOT retractable — see games.roof)
    tz           TEXT NOT NULL
);
```

*Phase 0 note:* greerreNFL's `altitude` column is **metres**; `altitude_ft` is populated by
`ffh.ingest.reference.seed_stadiums` as `round(altitude * 3.280839895)`. Its `name` comes
from the upstream `stadium_name` column.

```sql
CREATE TABLE games (
    game_id        TEXT PRIMARY KEY,    -- nflverse game_id, e.g. 2026_01_NE_SEA
    season         SMALLINT NOT NULL,
    week           SMALLINT NOT NULL,
    season_type    TEXT NOT NULL,       -- REG|POST
    kickoff_at     TIMESTAMPTZ NOT NULL,
    home_team      TEXT NOT NULL REFERENCES nfl_teams(team_abbr),
    away_team      TEXT NOT NULL REFERENCES nfl_teams(team_abbr),
    stadium_id     TEXT REFERENCES stadiums(stadium_id),
    -- Vegas (nflverse games.csv, refreshed every 5 min in season)
    spread_line    REAL,                -- positive favors home
    total_line     REAL,
    home_moneyline INTEGER,
    away_moneyline INTEGER,
    -- Context
    roof           TEXT,                -- ⚠️ PER-GAME ACTUAL: outdoors|dome|closed|open
    surface        TEXT,
    div_game       BOOLEAN,
    home_rest      SMALLINT,
    away_rest      SMALLINT,
    neutral_site   BOOLEAN NOT NULL DEFAULT FALSE,  -- 2026 has a Melbourne game
    -- Post-game actuals (training targets)
    home_score     SMALLINT,
    away_score     SMALLINT,
    temp_f         REAL,
    wind_mph       REAL,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX games_season_week_idx ON games (season, week);

CREATE TABLE game_weather_forecasts (
    game_id       TEXT NOT NULL REFERENCES games(game_id),
    forecast_at   TIMESTAMPTZ NOT NULL,   -- when we pulled it — keep the history
    temp_f        REAL,
    wind_mph      REAL,
    wind_gust_mph REAL,
    wind_dir_deg  REAL,                   -- with stadiums.heading_deg → crosswind component
    precip_mm     REAL,
    precip_prob   REAL,
    PRIMARY KEY (game_id, forecast_at)
);
```

---

## 3. ★ The player ID crosswalk — read this before touching ingest ★

**This is the single highest-risk component in the system.** Every source uses its own
player IDs: Sleeper, ESPN, Yahoo, GSIS (nflverse), PFR, FantasyPros, Sportradar. A wrong
or missing mapping does not raise an exception — it produces a **silently missing player**,
which looks exactly like a player who isn't rostered. Downstream, a missing WR2 quietly
changes every VORP baseline.

### Resolution order — strictly in this order, record which one won

1. **DynastyProcess `db_playerids.csv`** — pre-built mapping covering most sources.
   `match_method = 'dynastyprocess'`, confidence 1.0.
2. **GSIS ID** direct match against nflverse. `match_method = 'gsis'`, confidence 1.0.
3. **Exact match** on `(normalized_name, position, team)`.
   `match_method = 'exact_name'`, confidence 0.95.
4. **Fuzzy match** on normalized name + position, Jaro-Winkler ≥ 0.92, disambiguated by
   birth date or college where available. `match_method = 'fuzzy'`, **confidence < 0.9 —
   these require human review before use.**
5. **Unmatched** → row in `crosswalk_unmatched`, alert raised, never silently dropped.

### Name normalization must handle

Suffixes (`Jr.`, `Sr.`, `III`, `IV`), punctuation (`D.J.` / `DJ` / `D J`), apostrophes
(`Ja'Marr`), hyphens (`Amon-Ra`), and known aliases (`Robby` / `Robert` Anderson,
`Cam` / `Cameron`). Defensive units are a special case: sources vary between team
abbreviation, `KC DST`, and `Chiefs D/ST`.

### Mandatory tests — these are not optional

```
test_crosswalk_covers_all_rostered_players   every player on every roster in the league
                                             resolves. Failing this = the app is wrong.
test_crosswalk_covers_top_300_adp            top 300 by ADP all resolve pre-draft
test_crosswalk_no_duplicate_player_ids       no two external IDs from the same source map
                                             to one player_id (and vice versa)
test_crosswalk_low_confidence_reviewed       no confidence < 0.9 row is used unverified
```

```sql
CREATE TABLE crosswalk_unmatched (
    id           BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    source       TEXT NOT NULL,
    external_id  TEXT NOT NULL,
    raw_name     TEXT,
    raw_position TEXT,
    raw_team     TEXT,
    first_seen   TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen    TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved     BOOLEAN NOT NULL DEFAULT FALSE,
    UNIQUE (source, external_id)
);
```

---

## 4. Fantasy league state

```sql
CREATE TABLE leagues (
    league_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    platform         TEXT NOT NULL,      -- sleeper|espn|yahoo
    external_id      TEXT NOT NULL,
    season           SMALLINT NOT NULL,
    name             TEXT,
    num_teams        SMALLINT NOT NULL,
    -- ⚠️ ALWAYS fetched from the platform, NEVER hardcoded
    scoring_settings JSONB NOT NULL,     -- normalized; see ScoringSettings model
    roster_settings  JSONB NOT NULL,     -- starter slots, bench, IR, flex composition
    league_type      TEXT NOT NULL,      -- redraft|keeper|dynasty
    is_superflex     BOOLEAN NOT NULL DEFAULT FALSE,
    playoff_teams    SMALLINT,
    playoff_start_wk SMALLINT,
    faab_budget      INTEGER,            -- NULL if waiver priority instead of FAAB
    my_team_id       UUID,               -- composite FK to league_teams, added below
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (platform, external_id, season)
);

CREATE TABLE league_teams (
    league_team_id  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    league_id       UUID NOT NULL REFERENCES leagues(league_id) ON DELETE CASCADE,
    external_id     TEXT NOT NULL,
    display_name    TEXT,
    manager_name    TEXT,
    draft_slot      SMALLINT,
    faab_remaining  INTEGER,
    waiver_priority SMALLINT,
    is_me           BOOLEAN NOT NULL DEFAULT FALSE,
    UNIQUE (league_id, external_id),
    -- Target for the composite same-league FKs on matchups and leagues.my_team_id.
    CONSTRAINT league_teams_league_id_league_team_id_key UNIQUE (league_id, league_team_id)
);

-- Cyclic leagues <-> league_teams FK: added after both tables exist
-- (SQLAlchemy use_alter=True; migration emits op.create_foreign_key after create_table,
-- and drops it first in downgrade).
ALTER TABLE leagues ADD CONSTRAINT leagues_my_team_fkey
    FOREIGN KEY (league_id, my_team_id) REFERENCES league_teams (league_id, league_team_id);

-- Roster snapshots. One row per player per team per week — keep the history,
-- it's the input to "what did this manager need at the time".
CREATE TABLE roster_slots (
    league_team_id UUID NOT NULL REFERENCES league_teams(league_team_id) ON DELETE CASCADE,
    week           SMALLINT NOT NULL,
    player_id      UUID NOT NULL REFERENCES players(player_id),
    slot           TEXT NOT NULL,        -- QB|RB|WR|TE|FLEX|SUPER_FLEX|K|DST|BN|IR
    is_starter     BOOLEAN NOT NULL,
    captured_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (league_team_id, week, player_id)
);

CREATE TABLE matchups (
    league_id      UUID NOT NULL REFERENCES leagues(league_id) ON DELETE CASCADE,
    week           SMALLINT NOT NULL,
    matchup_no     SMALLINT NOT NULL,
    home_team_id   UUID NOT NULL,
    away_team_id   UUID,                 -- NULL = bye
    home_points    REAL,
    away_points    REAL,
    PRIMARY KEY (league_id, week, matchup_no),
    -- Composite FKs: both teams must belong to THIS league. A NULL away_team_id is
    -- simply not enforced (MATCH SIMPLE), which is what a bye needs.
    CONSTRAINT matchups_home_team_fkey FOREIGN KEY (league_id, home_team_id)
        REFERENCES league_teams (league_id, league_team_id),
    CONSTRAINT matchups_away_team_fkey FOREIGN KEY (league_id, away_team_id)
        REFERENCES league_teams (league_id, league_team_id)
);

CREATE TABLE transactions (
    transaction_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    league_id      UUID NOT NULL REFERENCES leagues(league_id) ON DELETE CASCADE,
    external_id    TEXT,
    type           TEXT NOT NULL,        -- add|drop|trade|waiver
    week           SMALLINT,
    executed_at    TIMESTAMPTZ,
    faab_spent     INTEGER,
    payload        JSONB NOT NULL,       -- normalized adds/drops/picks by player_id
    UNIQUE NULLS NOT DISTINCT (league_id, external_id)   -- external_id nullable; upserts must still conflict
);
```

**Cross-league integrity.** `league_team_id` is globally unique, so a plain
`REFERENCES league_teams(league_team_id)` would happily accept a team from a *different*
league in `matchups.home_team_id`/`away_team_id` or `leagues.my_team_id` — a bug that
would silently corrupt win-probability, playoff odds and "my roster" everywhere
downstream. The `UNIQUE (league_id, league_team_id)` on `league_teams` exists purely as
the target of composite FKs `(league_id, team_id) → league_teams(league_id, league_team_id)`,
which make the same-league invariant a database guarantee rather than an ingest
convention. `leagues ↔ league_teams` is cyclic, so `leagues_my_team_fkey` is added by a
separate `ALTER TABLE` after both tables exist (and dropped first on downgrade).
`draft_picks.league_team_id` is deliberately NOT covered — see §5.

---

## 5. Draft

```sql
CREATE TABLE drafts (
    draft_id     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    league_id    UUID NOT NULL REFERENCES leagues(league_id) ON DELETE CASCADE,
    external_id  TEXT NOT NULL,
    draft_type   TEXT NOT NULL,          -- snake|linear|auction
    rounds       SMALLINT NOT NULL,
    status       TEXT NOT NULL,          -- pre_draft|drafting|complete
    my_slot      SMALLINT,
    started_at   TIMESTAMPTZ,
    UNIQUE (league_id, external_id)
);

CREATE TABLE draft_picks (
    draft_id        UUID NOT NULL REFERENCES drafts(draft_id) ON DELETE CASCADE,
    pick_no         SMALLINT NOT NULL,   -- overall, 1-indexed
    round           SMALLINT NOT NULL,
    draft_slot      SMALLINT NOT NULL,
    league_team_id  UUID REFERENCES league_teams(league_team_id),
    player_id       UUID REFERENCES players(player_id),   -- NULL until picked
    is_keeper       BOOLEAN NOT NULL DEFAULT FALSE,
    auction_amount  INTEGER,
    picked_at       TIMESTAMPTZ,
    PRIMARY KEY (draft_id, pick_no)
);
CREATE INDEX draft_picks_player_idx ON draft_picks (player_id);
```

Same-league invariant for `draft_picks.league_team_id` (must belong to `drafts.league_id`)
is enforced by `ffh.ingest.platform_sync` with a test, not by the schema. A composite FK
here would require denormalizing `league_id` onto `draft_picks`; §4's `matchups` and
`leagues.my_team_id` already carry `league_id`, so they get the schema-level guarantee.

```sql

-- ADP by format. Multiple sources; the engine blends them.
CREATE TABLE adp (
    source      TEXT NOT NULL,           -- ffcalculator|sleeper|fantasypros_bestball
    format      TEXT NOT NULL,           -- ppr|half_ppr|standard|superflex
    num_teams   SMALLINT NOT NULL,
    scrape_date DATE NOT NULL,
    player_id   UUID NOT NULL REFERENCES players(player_id),
    adp         REAL NOT NULL,
    adp_stdev   REAL,                    -- ⚠️ REQUIRED for VONA — see ENGINE.md §2
    times_drafted INTEGER,
    PRIMARY KEY (source, format, num_teams, scrape_date, player_id)
);
```

⚠️ **`adp_stdev` is not optional.** VONA samples from the ADP *distribution*; a point ADP
makes the simulation degenerate. If a source doesn't publish dispersion, estimate it from
the observed spread of `best`/`worst` in the ECR data or from historical draft variance.

---

## 6. Projections and stats

```sql
-- ⚠️ A projection is a DISTRIBUTION. Never store or pass only the mean.
CREATE TABLE projections (
    projection_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    player_id     UUID NOT NULL REFERENCES players(player_id),
    season        SMALLINT NOT NULL,
    week          SMALLINT NOT NULL,     -- 0 = full-season projection
    league_id     UUID NOT NULL REFERENCES leagues(league_id),  -- GENERIC_LEAGUE_ID for league-agnostic (generic PPR) rows
    source        TEXT NOT NULL,         -- ffh_engine|sleeper_rotowire|ecr_derived
    model_version TEXT NOT NULL,         -- ⚠️ required for backtest comparability
    -- Gamma parameters — see ENGINE.md §4
    mean_points   REAL NOT NULL,
    gamma_shape   REAL NOT NULL,         -- k
    gamma_scale   REAL NOT NULL,         -- θ  (mean = k·θ, var = k·θ²)
    floor_p10     REAL,
    ceiling_p90   REAL,
    -- Provenance: what drove this number
    inputs        JSONB NOT NULL,        -- implied_total, usage shares, opp rank, wx, injury
    computed_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT projections_scope_key
        UNIQUE (player_id, season, week, league_id, source, model_version)
        -- league_id is NOT NULL (sentinel for generic rows), so a plain UNIQUE conflicts on upsert.
        -- Explicit name: the convention-derived name exceeds Postgres' 63-char identifier limit.
);
CREATE INDEX projections_lookup_idx ON projections (season, week, source, model_version);

-- Correlation matrix for the copula. Stored sparse: only non-zero pairs.
CREATE TABLE projection_correlations (
    season     SMALLINT NOT NULL,
    week       SMALLINT NOT NULL,
    player_a   UUID NOT NULL REFERENCES players(player_id),
    player_b   UUID NOT NULL REFERENCES players(player_id),
    rho        REAL NOT NULL,
    reason     TEXT NOT NULL,   -- same_team_qb_wr|same_game_opposing|dst_vs_opposing_offense
    PRIMARY KEY (season, week, player_a, player_b),
    CHECK (player_a < player_b)     -- canonical ordering, store each pair once
);

-- Actuals. Mirrors the lake but scored for THIS league's settings.
CREATE TABLE player_week_actuals (
    player_id      UUID NOT NULL REFERENCES players(player_id),
    season         SMALLINT NOT NULL,
    week           SMALLINT NOT NULL,
    league_id      UUID NOT NULL REFERENCES leagues(league_id),
    game_id        TEXT REFERENCES games(game_id),
    fantasy_points REAL NOT NULL,
    snap_pct       REAL,
    target_share   REAL,
    carry_share    REAL,
    rz_touches     SMALLINT,
    PRIMARY KEY (player_id, season, week, league_id)
);
```

**Sentinel generic league.** `league_id` is NOT NULL in both `projections` and `player_week_actuals` (in the latter it is part of the PK). League-agnostic ("generic PPR") rows in either table use the sentinel `00000000-0000-0000-0000-000000000000` (`ffh.db.models.GENERIC_LEAGUE_ID`) — never NULL. Backtest joins `projections ⋈ player_week_actuals` on `(player_id, season, week, league_id)` directly — both use the sentinel; never COALESCE.

Because of the FK, a sentinel `leagues` row must exist. It is seeded by
`ffh.ingest.reference.seed_generic_league(session)` (shipped in PR ③, idempotent via
`ON CONFLICT (league_id) DO NOTHING`) and run by `ffh ingest seed` alongside `nfl_teams`
and `stadiums`, with exactly this row:

| column | value |
|---|---|
| `league_id` | `00000000-0000-0000-0000-000000000000` |
| `platform` | `'ffh'` |
| `external_id` | `'generic'` |
| `season` | `0` |
| `name` | `'Generic PPR'` |
| `num_teams` | `12` |
| `league_type` | `'redraft'` |
| `is_superflex` | `false` |
| `scoring_settings` | `{"pass_yd":0.04,"pass_td":4,"pass_int":-2,"rush_yd":0.1,"rush_td":6,"rec":1,"rec_yd":0.1,"rec_td":6,"fum_lost":-2,"two_pt":2}` — canonical full-PPR reference; **NOT** a default for real leagues, whose settings are always platform-fetched |
| `roster_settings` | `{"QB":1,"RB":2,"WR":2,"TE":1,"FLEX":1,"K":1,"DST":1,"BN":6}` |

All other `leagues` columns (`playoff_teams`, `playoff_start_wk`, `faab_budget`, `my_team_id`) are NULL; `created_at` takes its default.

*`roster_settings` has two shapes.* The sentinel row stores a **count map**
(`GENERIC_ROSTER`: `{"QB": 1, "RB": 2, "WR": 2, "TE": 1, "FLEX": 1, "K": 1, "DST": 1,
"BN": …}`). Platform-loaded leagues (`ffh.ingest.platform_sync`, PR ⑤) store
`RosterSettings.model_dump()` — `{"starters": [...], "bench", "ir", "taxi",
"flex_composition", "is_superflex"}`. Consumers must branch on shape (e.g.
`"starters" in roster_settings`), never assume one.

```sql
CREATE TABLE player_injury_status (
    player_id            UUID NOT NULL REFERENCES players(player_id),
    season               SMALLINT NOT NULL,
    week                 SMALLINT NOT NULL,
    observed_at          TIMESTAMPTZ NOT NULL,   -- keep history; status moves all week
    source               TEXT NOT NULL,          -- sleeper|espn|nflverse
    report_status        TEXT,                   -- Out|Doubtful|Questionable|NULL
    practice_status      TEXT,                   -- DNP|Limited|Full
    injury_body_part     TEXT,
    notes                TEXT,
    PRIMARY KEY (player_id, season, week, observed_at, source)
);
```

---

## 7. Decisions, AI debates, and backtesting

**Everything we recommend is logged with its inputs and its outcome.** This is what makes
the LLM layer falsifiable — at season end we can answer "did the debate beat the engine
alone?" and cut it if the answer is no.

```sql
CREATE TABLE recommendations (
    recommendation_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    league_id      UUID NOT NULL REFERENCES leagues(league_id),
    module         TEXT NOT NULL,        -- draft|lineup|waiver|trade
    season         SMALLINT NOT NULL,
    week           SMALLINT,
    context        JSONB NOT NULL,       -- draft pick_no, roster state, opponent, ...
    engine_output  JSONB NOT NULL,       -- ranked candidates + all numbers, PRE-debate
    final_output   JSONB NOT NULL,       -- what we actually showed, POST-debate
    engine_version TEXT NOT NULL,
    debate_id      UUID,                 -- logical ref to ai_debates (no FK constraint); NULL if debate skipped/failed
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Outcome, backfilled later
    action_taken   JSONB,                -- what Chris actually did
    outcome        JSONB,                -- points scored, win/loss, value realized
    outcome_at     TIMESTAMPTZ
);
CREATE INDEX recommendations_module_idx ON recommendations (module, season, week);

CREATE TABLE ai_debates (
    debate_id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    module          TEXT NOT NULL,
    evidence_packet JSONB NOT NULL,      -- the EXACT input both models received
    -- Round 1: independent. provider_a/b assignment is randomized per debate.
    provider_a      TEXT NOT NULL,       -- anthropic|openai
    provider_b      TEXT NOT NULL,
    model_a         TEXT NOT NULL,
    model_b         TEXT NOT NULL,
    round1_a        JSONB NOT NULL,
    round1_b        JSONB NOT NULL,
    -- Round 2: forced refutation, anonymized
    round2_a        JSONB,
    round2_b        JSONB,
    -- Round 3: blind judge, provider alternates
    judge_provider  TEXT NOT NULL,
    judge_model     TEXT NOT NULL,
    verdict         JSONB NOT NULL,
    consensus_score REAL NOT NULL,       -- 0..1; low = flag it in the UI
    disagreement_axis TEXT,              -- what they actually differed on
    -- Ops
    latency_ms      INTEGER NOT NULL,
    cost_usd        NUMERIC(10,6),
    tokens_in       INTEGER,
    tokens_out      INTEGER,
    cache_hit       BOOLEAN,
    error           TEXT,                -- non-NULL if degraded; engine result still stands
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX ai_debates_consensus_idx ON ai_debates (consensus_score);

-- Ingest provenance and watermarks. Makes ingest idempotent and resumable.
CREATE TABLE ingest_runs (
    run_id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source       TEXT NOT NULL,
    asset        TEXT NOT NULL,
    season       SMALLINT,
    week         SMALLINT,
    started_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at  TIMESTAMPTZ,
    status       TEXT NOT NULL,          -- running|success|failed|skipped_not_modified
    rows_written INTEGER,
    source_etag  TEXT,                   -- for If-None-Match / 304 handling
    source_mtime TIMESTAMPTZ,
    output_path  TEXT,
    error        TEXT
);
CREATE INDEX ingest_runs_source_idx ON ingest_runs (source, asset, started_at);
-- DESC omitted — btree scans backward; keeps autogenerate drift-free.
```

---

## 8. Indexing and performance notes

- Time-range scans use **BRIN** indexes, not B-tree — the data is naturally time-ordered
  and BRIN is a fraction of the size, which matters on a Pi:
  `CREATE INDEX games_kickoff_brin ON games USING BRIN (kickoff_at);`
- `projections` is the hottest table. The composite
  `(season, week, source, model_version)` index is load-bearing — don't drop it.
- JSONB columns holding engine output are for audit and replay, **not** for querying in
  hot paths. If you find yourself writing a JSONB path query in a request handler, the
  field belongs in a real column.
- Postgres on NFS: `shared_buffers` 1–2GB on a 16GB node. Do not let Postgres swap.

---

## 9. Retention

| Data | Retention |
|---|---|
| `recommendations`, `ai_debates` | **Forever.** This is the backtest corpus. |
| `projections` | Forever, keyed by `model_version`, so old models stay comparable |
| `player_injury_status` | Full within-week history — status movement is itself signal |
| Raw lake partitions | Forever. Storage is cheap; reproducibility isn't. |
| `ingest_runs` | 90 days |
