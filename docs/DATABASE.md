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
    dynastyprocess/playerids/scrape_date=2026-08-16/playerids.parquet
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

*Lake grain is one snapshot per UTC day — by design (decided 2026-08-16, Codex review of
PR ③).* `IngestJob.run()` persists to Postgres **before** landing Parquet, so a second
run on the same day that fetches changed content still upserts Postgres (all `persist()`
implementations are idempotent upserts) but hits `PartitionExistsError` → status
`skipped`; the lake keeps the first-of-day snapshot and the ETag watermark stays at the
last *landed* version, so later same-day runs re-download rather than 304. Consequences:
**Postgres is the live system of record; the lake is a daily archive.** Nothing that needs
intraday freshness (lines, injury status) may read it from DuckDB. If a future job needs
intraday lake versions, add a finer partition key (`scrape_ts=`) for that job — do not
loosen the never-overwrite rule.

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
    team_abbr       TEXT,                     -- Phase 0: nflverse latest_team; crosswalk rung-3 tie-breaker ONLY, never roster truth
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX players_normalized_name_pos_idx ON players (normalized_name, position);
```

*Phase 0 note (PR ④):* `team_abbr` was added because §3 rung 3 matches on
`(normalized_name, position, team)` and the table had no team column. It is refreshed
from nflverse `latest_team` by `seed_players` (and set from the DynastyProcess `team` for
rows created there); it is used only inside `ffh.crosswalk.resolve` to break ties and is
**not** a roster field. Migration `0002_players_team_abbr`.

`updated_at` (here and on `games.updated_at`) is maintained by the ORM `onupdate` only; upserts via `INSERT ... ON CONFLICT` must set `updated_at = now()` explicitly in `SET`.

```sql
-- ★ THE CROSSWALK ★ — see §3. This is the highest-risk table in the system.
CREATE TABLE player_external_ids (
    player_id    UUID NOT NULL REFERENCES players(player_id) ON DELETE CASCADE,
    source       TEXT NOT NULL,   -- sleeper|espn|yahoo|pfr|fantasypros|sportradar|rotowire
    external_id  TEXT NOT NULL,
    confidence   REAL NOT NULL DEFAULT 1.0,   -- <1.0 means fuzzy-matched; review these
    match_method TEXT NOT NULL,               -- dynastyprocess|gsis|exact_name|fuzzy|manual|rejected (tombstone, not a mapping — see §3)
    verified_at  TIMESTAMPTZ,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (source, external_id)
);
CREATE INDEX player_external_ids_player_idx ON player_external_ids (player_id);
CREATE UNIQUE INDEX player_external_ids_source_player_uidx ON player_external_ids (source, player_id)
    WHERE match_method <> 'rejected';   -- PARTIAL: a tombstone is not a mapping (§3)
```

*Phase 0 note (PR ④, migration `0002_players_team_abbr`):* `player_external_ids_source_player_uidx`
enforces the `test_crosswalk_no_duplicate_player_ids` invariant below at the DB level — one
external id per source per player. It was added late: a preflight check found the plan's
original claim that this was "enforced by construction" was false, since `resolve._persist`
and `apply_playerids` can each attempt to insert a second id for a source against a player
that already holds one. Both writers MUST pre-check `(source, player_id)` before insert and
route the loser to `crosswalk_unmatched` / the ambiguity report — this index is a backstop,
not their conflict policy; they must not rely on catching the resulting `IntegrityError`.
The index is **partial** (`WHERE match_method <> 'rejected'`); migration `0003` is what
guarantees that predicate on databases already stamped at `0002` (deviation 20 in §3).

```sql
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
   `match_method = 'exact_name'`, confidence 0.95. (Team comes from `players.team_abbr`;
   see the rung-3 notes below.)
4. **Fuzzy match** on normalized name + position, Jaro-Winkler ≥ 0.92, disambiguated by
   birth date or college where available. `match_method = 'fuzzy'`, **confidence < 0.9 —
   these require human review before use.** (Persisted unverified; `resolve` returns
   `None` until a human verifies — see the rung-4 notes below.)
5. **Unmatched** → row in `crosswalk_unmatched`, alert raised, never silently dropped.

### Name normalization must handle

Suffixes (`Jr.`, `Sr.`, `III`, `IV`), punctuation (`D.J.` / `DJ` / `D J`), apostrophes
(`Ja'Marr`), hyphens (`Amon-Ra`), and known aliases (`Robby` / `Robert` Anderson,
`Cam` / `Cameron`). Defensive units are a special case: sources vary between team
abbreviation, `KC DST`, and `Chiefs D/ST`.

### Phase 0 implementation notes (PR ④, `ffh.crosswalk`)

Everything below describes what **shipped**, not what the plan intended. Where the shipped
behaviour is narrower or wider than the ladder as written above, it is called out as a
numbered deviation at the end of this subsection.

**Alias table and name normalization.** `ffh.crosswalk.normalize.ALIASES` (28 entries;
applied to the **first token only**, after suffix stripping — Robby/Robbie/Rob/Bob/Bobby→
Robert, Cam→Cameron, Mitch→Mitchell, Josh→Joshua, Mike→Michael, Matt→Matthew,
Chris→Christopher, Nick→Nicholas, Pat→Patrick, Will→William, Ken/Kenny→Kenneth,
Tony→Anthony, Dan/Danny→Daniel, Dave→David, Jim/Jimmy→James, Joe→Joseph, Zach/Zack→
Zachary, Ben→Benjamin, Gabe→Gabriel, Jon→Jonathan). Team spellings live in
`normalize.TEAMS` (32 rows: nflverse abbr, city, nickname, MFL/PFR/ESPN/Sleeper aliases).
`normalize_name` is: **fold accents** (NFKD, drop combining marks — `Andrés Peña` →
`andres pena`; this MUST precede the character class, which would otherwise turn every
non-ASCII letter into a space) → lowercase → drop `.` `'` `’` `-` → non-alphanumerics to
space → merge
single-letter runs (`D J`→`dj`, so `D.J. Moore` == `DJ Moore` == `D J Moore`) → strip
trailing `jr sr ii iii iv v` → alias the first token. `Amon-Ra`→`amonra` but `Amon Ra`→
`amon ra` (the hyphen is removed, a real space is kept) — sources spell it hyphenated.
Both sides of every comparison go through the same function, so determinism matters more
than the output looking like a real name. Covered by 59 name / 59 DST+team / 20 position
parametrized cases in `backend/tests/crosswalk/test_normalize.py` (spec bar: ≥ 40). The
team table carries every PFR spelling, including the five that look nothing like the team
(`CRD`=ARI, `RAV`=BAL, `HTX`=HOU, `CLT`=IND, `RAI`=LV) — `pfr` is one of the seven sources.

**DST canonical form.** `players.normalized_name = "<abbr lowercase> dst"` (e.g. `kc dst`),
`position = 'DST'`, `full_name = "<City> <Nickname> DST"`, `gsis_id NULL`, `team_abbr` set.
32 rows, created by `registry.seed_dst_players` (insert-only, idempotent). Any spelling
(`KC`, `KC DST`, `Chiefs D/ST`, `Kansas City`, `KCC`, `KAN`, `LAR`, `WSH`, `Bucs`, …)
canonicalizes through `normalize_dst`, including a **bare** team abbreviation. Position
aliases: `DEF`/`D/ST`→`DST`, `PK`→`K`, `FB`/`HB`→`RB`; anything non-fantasy → `None`.

**DST canonicalization precedence — name, then team, then external id.** A source row
carries several fields that could name a defense and they can *disagree* (a row named
`Kansas City Chiefs` listed at `DEN`). The precedence is a single explicit argument list,
`normalize.canonical_dst_key(*candidates)` — first candidate that resolves to a team wins —
and **both writers call it with the name first**: `resolve._canonical_name` passes
`(raw_name, raw_team, external_id)`, `dynastyprocess.apply_playerids` passes `(name, team)`.
The name is what the row is *about*; the team column is a mutable weekly attribute. Before
this the two writers inlined their own `or` chains in opposite orders, so the same row
canonicalized to a different defense depending on which writer saw it first — a silently
wrong mapping, the one failure mode the crosswalk exists to prevent. (Rung 3 additionally
weighs `players.team_abbr`, so a *ladder* lookup whose `raw_team` contradicts the seeded
defense still falls through to rung 4 — that is the team rule, not the canonical key.)

**Rungs 1–2 and what persists.** Rung 1 is a lookup of the existing
`(source, external_id)` row and writes nothing new. **Rungs 2–4 persist** their result, so
the next call for the same key is a cheap rung-1 hit — with one exception: **rung 2 skips
persisting when `source == "gsis"`**, because a gsis id filed under source `gsis` would
duplicate `players.gsis_id`. Consequence worth knowing before you write a coverage query:
a gsis-sourced resolution returns a 1.0 `Resolution` but leaves **no** `player_external_ids`
row, so any coverage measured off that table will read short for those ids by design. It
does still close an open `crosswalk_unmatched` entry for that key (`resolve.close_unmatched`
is called directly on this branch, since `_persist` — where the call normally lives — is
skipped): the id resolved at 1.0, and leaving the queue row open would latch the gate red
on an id nothing is wrong with.

**The authority rule — a 1.0 fact beats an unverified guess.** Both writers hit the same
state: the incoming id belongs to a player who *already* holds an id for that source
(`player_external_ids_source_player_uidx` allows exactly one). The ruling is by
**authority, not arrival order**:

* Incoming row is a 1.0 fact (`gsis` / `dynastyprocess` / `manual`) **and** the incumbent
  is an unverified guess (`match_method` not in `{dynastyprocess, manual, rejected}` **and**
  `verified_at IS NULL`) → **the fact wins.** The guess is deleted and *its* external id is
  upserted into `crosswalk_unmatched` (`crosswalk.resolve.incumbent_displaced` /
  `crosswalk.dynastyprocess.incumbent_displaced`, `CrosswalkApplyReport.displaced`).
* Otherwise → **the holder wins** and the *incoming* id goes to `crosswalk_unmatched`
  (`crosswalk.resolve.duplicate_for_source` / `CrosswalkApplyReport.blocked_by_existing`).

Either way exactly one id ends up mapped and the other ends up on the gate — never
dropped. Without this, an unverified `exact_name` 0.95 or `fuzzy` 0.89 guess outranked a
1.0 gsis/DynastyProcess fact simply by having got there first — the opposite of the ruling
rung 1's upgrade path already makes.

**Rejection tombstones.** `ffh crosswalk verify <source> <id> --reject` does **not** delete
the row. It rewrites it as `match_method = 'rejected'`, `confidence = 0.0`,
`verified_at = NULL`, keeping the rejected `player_id`. A deletion is forgotten by the next
sync, which re-mints the identical wrong mapping — and `close_unmatched` then turns the
gate **green on a mapping a human explicitly rejected** (for a rung-4 row the loop never
ends: re-mint, red, reject, re-mint). The tombstone is what makes a rejection durable:

* It is **not a mapping**: rung 1 records the key unmatched and returns `None`; `is_usable`
  returns false regardless of `verified_at`; `verify_mapping` refuses to stamp one; the
  report excludes it from `unverified_low_confidence` (its gate signal is the open
  `crosswalk_unmatched` row); it is excluded from the rung-3/4 "already mapped for this
  source" candidate filter.
* It does **not occupy** the player's slot for that source — the unique index is PARTIAL
  (`WHERE match_method <> 'rejected'`, migration 0002), so the *correct* id can still map
  to that player.
* Re-minting the **same** `(source, external_id) → player_id` pairing is refused by
  `_persist` and by `apply_playerids` (`CrosswalkApplyReport.blocked_by_rejection`). A
  **different** player is allowed — that is the correction the rejection asked for.
* The escape is `ffh crosswalk map` (below), which replaces the tombstone with a `manual`
  mapping. That is the only path from "rejected" back to exit 0 with the id *mapped*
  (`resolve-unmatched` only silences the queue entry).

**Rung 3 (`exact_name`) and team.** Candidates are `players` rows with the same
`(normalized_name, position)` that **do not already hold an id for this source**. Then:
no team supplied → match iff exactly one candidate remains; team supplied → keep the
candidates whose `team_abbr` equals it **or is NULL**, match iff exactly one remains.
The "already mapped" exclusion is a no-duplicate mechanism, **not evidence about
identity**, so the pick is made twice: once on the post-exclusion set and once on the
pre-exclusion set. They must agree. When the exclusion is the only reason the answer looks
unique — two same-name/same-position players and one of them already holds an id for this
source — rung 3 refuses rather than minting a *usable* 0.95 on the homonym, and the id
falls straight to rung 5 with `reason="homonym_blocked_by_existing_mapping"`
(`crosswalk.resolve.homonym_blocked_by_existing_mapping`). A team that disambiguates is
real evidence and still matches.
A team disagreement never matches at rung 3 — it falls through to rung 4.
`players.team_abbr` is refreshed from nflverse `latest_team` by `seed_players` (and set
from the DynastyProcess `team`, MFL-style, for rows created there); it is a tie-breaker
only, never roster truth.

**Rung 4 (`fuzzy`) semantics.** rapidfuzz `JaroWinkler.normalized_similarity ≥ 0.92`
against same-position players not already mapped for the source, then two disambiguation
legs on birth date and college: **negative** first (a candidate whose stored value is
non-NULL and differs from the supplied value is eliminated), then **positive**. The
positive leg differs by column: on **birth date** (exact equality) confirmation keeps only
the confirmed survivors; on **college** it keeps the confirmed survivors **plus every
candidate whose stored college is NULL** — an unknown college is no evidence *against* a
candidate, and evicting it hands the id to a lower-similarity homonym that merely happens
to have a college on file. Whatever survives is then ruled on by the tie margin, which can
legitimately end in "no match". Two survivors within
`0.01` = tie = no match (falls to rung 5). A hit is **persisted** with
`match_method='fuzzy'`, `confidence = min(similarity, 0.89)`, `verified_at NULL`, and
`resolve` returns `None` — the outcome is "pending review", not "unmatched", and the row
is *not* written to `crosswalk_unmatched`. `ffh crosswalk verify <source> <id>` sets
`verified_at`; `--reject` tombstones the row (above) and parks the id in
`crosswalk_unmatched`. The two disambiguation legs are **symmetric**: college agreement is
a shared meaningful token or either string containing the other (`"Ohio St."` agrees with
stored `"Ohio State"`; `"Michigan State"` does not agree with `"Ohio State"`) — an
asymmetric `needle in stored` test eliminated correct candidates. When candidates existed
but the evidence ruled them all out, rung 5 records `reason="fuzzy_eliminated"` (with
`crosswalk.resolve.fuzzy_eliminated` naming the leg) or `"fuzzy_tie"`, never the
`"no_candidate"` that means "no name matched at all".

**Rung 5.** `crosswalk_unmatched` upsert (`resolve.upsert_unmatched`, the single writer):
`first_seen` defaults on insert; on conflict the raw fields refresh, `last_seen` advances
with **`clock_timestamp()`**, and `resolved` flips back to `false`.

**Consumer filter rule.** A `player_external_ids` row is usable iff
`match_method <> 'rejected' AND (confidence >= 0.9 - epsilon OR verified_at IS NOT NULL)` —
`ffh.crosswalk.resolve.is_usable`.
`resolve` / `resolve_many` already apply it; **any direct SQL over `player_external_ids`
must too.** The epsilon is not cosmetic: `confidence` is Postgres `REAL` (float4), so a
stored `0.9` reads back as `0.899999976…` and a naive `>= 0.9` would reject rows that are
supposed to pass. Use the exported `resolve.USABLE_CONFIDENCE` and
`resolve.CONFIDENCE_EPSILON` (`1e-6`) — `report.py`'s SQL predicate uses the identical pair.

**Review-queue lifecycle.** `crosswalk_unmatched.resolved` is set `true` at every point a
mapping row is **created** for the key — `resolve._persist`,
`dynastyprocess.apply_playerids` and `review.map_mapping`, and no other mapping-creation
path — plus the one resolution that creates no row: a rung-2 hit with `source == "gsis"`
(above). The human review
commands close it too: `ffh crosswalk verify` closes the entry;
`ffh crosswalk map <source> <id> <player_id>` creates a `manual` 1.0 verified mapping and
closes it; `--reject` deliberately leaves it **open** (the id is now unmapped and must stay
on the gate); `ffh crosswalk resolve-unmatched <source> <id>` closes an entry that will
never map (retired, practice squad, non-NFL).

`resolve-unmatched` **refuses while the key still has a live (non-tombstone) mapping row**
and prints the operator's real options, because that state is not "an id that will never
map" — it is a mapping under dispute, and closing the queue row would green the gate while
`resolve` keeps handing consumers the contradicted player. Rule on it with
`verify --reject` or `map`; `--force` restores the unconditional close for the rare case
where the mapping is accepted as-is. (`ffh crosswalk verify` closes the entry through the
same `force` path: accepting the mapping *is* the decision.)

**A queue entry re-opens only when the id is described differently.** `upsert_unmatched`
sets `resolved = false` on conflict *only* when a `raw_*` field actually changed
(`IS DISTINCT FROM`, NULL-safe). An unconditional re-open undid the operator's ruling on
every seed: the permanently-glitched DynastyProcess ids (DATA_SOURCES.md §5) are re-asserted
verbatim by every weekly snapshot, so the gate could never reach green. Three states deliberately leave one key in
*both* tables so `ffh crosswalk report` keeps exiting 1 on it: `upgrade_conflict`,
`human_decision_conflict` (a `manual`/verified row contradicted by a 1.0 gsis fact — the
human decision stands and is still returned, but the dispute is on the gate, not only in a
log line), and a rejection tombstone.

**Every id a writer refuses to map is queued.** Not just rung 5: the ids
`apply_playerids` drops for ambiguity, the losers of the authority rule, the ids blocked
by a tombstone, and the incumbents displaced by a 1.0 fact all land in
`crosswalk_unmatched`. A report field and a log line are not the gate — a known
fantasy-relevant id that is unmapped must make `ffh crosswalk report` exit 1.

**Exit codes (both crosswalk commands).** `0` = green. `1` = **gate red**, a data state a
human resolves. `2` = `CrosswalkConflictError`, a crosswalk conflict a human must rule on
(nothing is committed). `3` = **operational failure** — a malformed CSV, a truncated or
missing lake partition, a database outage. 3 exists because an operational failure used to
exit 1 as well, leaving a cron wrapper unable to tell "the crosswalk has a gap" from "the
run never happened"; `ffh crosswalk seed` and `ffh crosswalk report` both wrap their whole
guarded region (frame reads, `seed_players`, `apply_playerids`, the report query).

**`ffh crosswalk report`** exits 1 if any `crosswalk_unmatched` row is open, any
unverified `confidence < 0.9` non-tombstone row exists, **or the crosswalk is empty**
(`players_total == 0` or no `player_external_ids` rows at all). Emptiness is the state
where every downstream lookup silently finds no player, and it produces zero unmatched and
zero unverified rows — so without that floor the gate reads green on a database where
nothing was ever seeded. `--allow-empty` opts out for a deliberate pre-seed invocation. **`ffh crosswalk map SOURCE
EXTERNAL_ID PLAYER_ID`** writes the human decision (`manual`, confidence 1.0,
`verified_at` stamped) and closes the queue entry; it refuses an unknown `player_id` and
pre-checks the `(source, player_id)` unique index, reporting the clash by name instead of
raising an `IntegrityError`. It is the only way an id in `crosswalk_unmatched` becomes
*mapped* rather than merely silenced, and the only escape from a tombstone.

**DynastyProcess apply policy** (`dynastyprocess.apply_playerids`, rung 1 bulk load):
positions normalized; non-fantasy rows and rows with no id are skipped and **counted**;
player assignment is `gsis_id` → registry player, else any already-mapped id → its player,
else a placeholder (`gsis:<id>` / `mfl:<mfl_id>`) that becomes a new `players` row
(rookies/UDFAs) — except for DST rows, which resolve against the seeded `<abbr> dst`
players (see deviation 11) and never take the placeholder path. Rows with **neither a `gsis_id` nor an `mfl_id`** have no person key
at all — their placeholder would be the literal `"mfl:None"`, shared by every such row, so
one invented `players` row would accumulate ids belonging to several different people (the
ambiguity pass catches only the overlapping case, never the disjoint one). They are counted
in `skipped_no_person_key` and dropped. Ids appearing on more than one player, or a player
holding more than one id for a source, are reported **and queued in `crosswalk_unmatched`**,
split by cause: `ambiguous_in_file` (DynastyProcess contradicts itself) vs
`blocked_by_existing` (a DB row won the authority rule) vs `blocked_by_rejection` (a
tombstone), with `displaced` naming the guesses DP evicted. An ambiguous key that already
has a **live mapping** is reported but NOT queued — it *is* mapped, and queueing it
re-opened the entry on every seed. An existing
row pointing at a *different* player raises `CrosswalkConflictError` **before any write**
— the conflict scan runs before placeholder players are created *and* before the ambiguity
queueing, so an aborted seed leaves nothing behind in either table. The raise is reserved
for an incumbent that **outranks** DP's 1.0 fact (`manual`, `dynastyprocess`, verified, or a
tombstone). An unverified `exact_name`/`fuzzy` guess pointing elsewhere is not a conflict —
it is exactly what rung 1's upgrade path re-points without complaint — so it is re-pointed
in place (`crosswalk.dynastyprocess.stale_guess_repointed`, counted in `updated`) rather
than aborting the entire seed, which discards `seed_players` along with it. Ids are stored as TEXT
exactly as in the CSV, and a Parquet round-trip that typed them as floats is rejected
rather than silently mangled (`4046.0` must never become `"4046.0"`).

**Deviations from §3 as written above — all shipped deliberately:**

1. **`players.team_abbr` added** (migration `0002_players_team_abbr`) — §3 rung 3 matches
   on team and the table had no team column. Tie-breaker only, never roster truth.
2. **Rung-4 hits are persisted unverified and `resolve` returns `None`** for them. This is
   *how* §3's "require human review before use" is implemented: the candidate mapping is
   not lost, but no consumer can use it until `verify` stamps `verified_at`.
3. **`crosswalk_unmatched.last_seen` bumps with `clock_timestamp()`, not `now()`** —
   `now()` is transaction-constant, so within a single sync it would never advance.
4. **One id per source per player is enforced by a PARTIAL UNIQUE index**
   (`player_external_ids_source_player_uidx`, added in 0002,
   `WHERE match_method <> 'rejected'` — tombstones are not mappings and must not squat a
   player's slot) **plus an in-code pre-check in both writers** —
   stronger than §3, which asked only for a test. The plan's original "enforced by
   construction" claim was disproved during verification. Writers pre-check
   `(source, player_id)` and apply the **authority rule** (above): a 1.0 fact displaces an
   unverified guess and the *guess's* id goes to `crosswalk_unmatched`; otherwise the
   holder wins and the *incoming* id goes there. Either way the loser is queued — in
   `apply_playerids` it is queued *and* reported. The index is a backstop, not the policy.
5. **Rung 1 is generalized** from "the DynastyProcess lookup" to "**any** existing
   `player_external_ids` row" — it is the cache that makes repeat syncs cheap. It gains an
   **upgrade path**: *every* rung-1 row is re-checked against a supplied `gsis_id`. A
   **1.0** row is never rewritten (it is a fact, not a guess), but a gsis id naming a
   *different* player is two 1.0 facts in contradiction: it logs
   `crosswalk.resolve.human_decision_conflict` and queues the key, while the stored row is
   still returned. Gating this check on `confidence < 1.0` made it unreachable for every
   row `ffh crosswalk map` can actually write (`manual`, confidence 1.0, verified), i.e.
   for every real human decision. For a row **below** 1.0: a *different* player means the
   gsis fact wins and the stored row is corrected (`verified_at` cleared); the *same*
   player upgrades the stored method/confidence to `gsis`/1.0. Rows that are `verified_at IS NOT NULL` or `match_method = 'manual'` are
   locked against this entirely: a *disagreeing* gsis id logs
   `crosswalk.resolve.human_decision_conflict`, changes nothing **and queues the key in
   `crosswalk_unmatched`** so the dispute is on the gate rather than only in a log; a
   *confirming* one is dropped without a log (the lock is wider than the case it exists
   for — a known follow-up, not a behaviour to rely on). If a correction would give the
   target player a second id for the source, the authority rule decides: an unverified
   incumbent is displaced, otherwise the incoming id is routed to `crosswalk_unmatched`
   (`crosswalk.resolve.upgrade_conflict`) rather than returning a mapping the gsis fact
   just contradicted.
6. **Rung 3 excludes candidates that already hold an id for that source** — and so does
   rung 4 (tombstones excluded from both). This is part of the no-duplicate mechanism, and
   rung 3 checks that the exclusion is not what *created* the match (above): a homonym
   pair with one member already mapped goes red rather than minting a usable 0.95 guess.
   Consequence: a *wrong* id occupying a player's slot keeps the *correct* id unmatched
   until a human runs `ffh crosswalk verify <source> <id> --reject` (or the authority rule
   displaces it automatically, when the correct id arrives as a 1.0 fact). Visibility was
   originally claimed unconditionally; it is true **only where the losing id is queued**.
   That is now every path — `resolve`'s rungs *and* `apply_playerids`' ambiguity /
   blocked / displaced buckets — but before this wave the DynastyProcess buckets were
   report-and-log only, so `ffh crosswalk report` could exit 0 with known ids unmapped.
7. **Rung 3's team rule is relaxed:** `team is None` matches iff exactly one candidate
   remains, and a NULL `players.team_abbr` counts as compatible with any supplied team.
8. **Rung 4 disambiguates in both directions** (negative elimination then positive
   confirmation, above) rather than merely tolerating NULLs. A candidate with a NULL
   **birth date** loses to one whose birth date matches the input. A candidate with a NULL
   **college** does not: confirmation keeps it alongside the confirmed ones and the tie
   margin rules. NULL is the absence of evidence, and evicting on it handed ids to
   lower-similarity homonyms; the price is that a college can no longer break a tie
   against an unknown-college candidate, which falls to rung 5 as it should.
9. **`seed_players(session, df)`** takes a Polars frame, not a lake path: the lake read
   lives in the CLI (`ffh crosswalk seed`). This is what keeps `ffh.crosswalk` free of
   `ffh.ingest` (the sole exception is `DynastyProcessPlayerIdsJob`) and testable with no
   network.
10. **Review-queue lifecycle** (above) is a mechanism §3 does not describe: `resolved` is
    maintained at the mapping-creation sites (plus the row-less `source == "gsis"` rung-2
    hit), `--reject` deliberately re-opens/keeps open, `ffh crosswalk map` is the
    operator's path to *mapped*, and `resolve-unmatched` is the path back to exit 0 for an
    id that will never map — refused while a live mapping still contradicts it, and
    re-opened by a later `upsert_unmatched` only when a `raw_*` field changed.
11. **`CrosswalkApplyReport.skipped_dst`** — DynastyProcess rows whose position normalizes
    to DST/DEF **do** get mapped: the row resolves to the seeded `<abbr> dst` player via
    `canonical_dst_key(name, team)` (name-first — see the DST canonicalization precedence
    above), or failing that to whatever player one of its ids is already crosswalked to. Only a row matching *neither* is counted in
    `skipped_dst` and skipped — counted and logged, never silently dropped. What a DST row
    can never do is fall through to the placeholder path and mint a `players` row: defenses
    exist only as the 32 rows `seed_dst_players` creates. (The live file has no DST rows
    today; the counter exists so a future one cannot invent a 33rd defense.)

12. **`match_method = 'rejected'` is a fourth kind of row** (tombstone), alongside the
    ladder's mapping methods. §3 lists only the five rungs; a rejection had nowhere to
    live, so it was a deletion and therefore not durable.
13. **`ffh crosswalk map SOURCE EXTERNAL_ID PLAYER_ID`** — `manual` was already
    first-class in the ladder, the human-decision lock and these docs, but nothing could
    *create* one. Without it an id in `crosswalk_unmatched` had no operator path to
    becoming mapped, only to being silenced.
14. **`resolve_many` runs in priority passes, not one ordered walk.** Rungs 2-4 persist
    and rungs 3-4 exclude already-mapped players, so a rung-4 guess early in a batch could
    claim the player a later rung-3 exact match wanted. Pass 1 walks the whole batch with
    rung 4 deferred (persisting nothing for those inputs), pass 2 re-runs only the deferred
    ones. The gsis-first ordering inside pass 1 is unchanged; single-id `resolve()` is
    unaffected. Both passes share one rung-4 **candidate-pool cache** keyed on
    `(source, position)`, so "every unmapped same-position player" is selected once per
    key for the whole batch instead of once per input; `_persist` drops a player from the
    cached pools the moment it claims his slot (what the reload's
    `NOT IN (mapped for source)` filter would have excluded), and the rarer paths that
    *free* a player invalidate the pools for that source. `resolve()` passes no cache.
15. **`ffh crosswalk seed` / `report` exit 3 on operational failure** and the report has an
    **emptiness floor** plus `--allow-empty` (both above). §3 defined only the green/red
    gate, which silently conflated "the crosswalk has a gap" with "the run never happened",
    and read green on a database where nothing had been seeded at all.
16. **DynastyProcess re-points a stale unverified guess instead of raising** (above). §3
    and the original policy treated any mismatching existing row as a conflict, so one
    stale ladder guess aborted the whole seed — including `seed_players`, which is
    committed in the same transaction.
17. **Name normalization folds accents** (NFKD + combining-mark strip) rather than
    deleting non-ASCII letters, and rung 4's college leg is symmetric. Both were silent
    wrong-answer bugs: `Andrés Peña` could never match `Andres Pena`, and `"Ohio St."`
    eliminated the candidate stored as `"Ohio State"`.
18. **`apply_playerids` reconciles placeholder duplicates before anything else**
    (`CrosswalkApplyReport.merged_placeholders`, `crosswalk.dynastyprocess.placeholder_merged`).
    A DP row with no `gsis_id` mints a placeholder `players` row carrying `gsis_id NULL`;
    when nflverse later publishes that person, `seed_players`' ON CONFLICT is on `gsis_id`
    and **NULL conflicts with nothing**, so it inserts a *second* `players` row for the same
    human. Every later seed then failed permanently: the DP row resolves by gsis to the new
    row while its ids still point at the placeholder, and the incumbent is `dynastyprocess`
    — protected, never re-pointed — so the conflict scan raised on every run; rung 3 saw two
    `(normalized_name, position)` candidates and returned `None`. The pre-pass repoints the
    placeholder's `player_external_ids` rows (tombstones included) at the gsis player and
    deletes the placeholder. It is the **one write that precedes the conflict scan**,
    because it is the repair that unblocks that scan; an id it cannot carry over (the
    keeper already holds a live id for that source) is deleted and queued, never dropped.
    Seeded DST rows are excluded explicitly — they also carry a NULL `gsis_id`.
19. **The crosswalk write commands take a Postgres advisory lock** —
    `ffh crosswalk seed` / `map` / `verify` wrap their session in
    `ffh.db.lock.advisory_lock(session, "ffh.crosswalk/apply")` (the same helper
    `ffh.ingest.base` uses, lifted into `ffh.db` so there is exactly one implementation).
    All three read `player_external_ids`, decide `(source, player_id)` slot ownership, then
    write — `apply_playerids` for ~12.5k rows at a time — and that plan is TOCTOU: two
    concurrent runs (a cron overlapping a manual re-run) can both pass the same pre-check.
20. **Migration `0003` re-creates `player_external_ids_source_player_uidx`.** The partial
    predicate was added to `0002` *after* that revision had already been applied somewhere,
    and Alembic records revisions rather than their content — so a database stamped at 0002
    silently kept the non-partial index while a freshly created one got the partial form.
    `0003` drops and recreates it unconditionally. Both migrations' docstrings carry the
    duplicate pre-flight scan and the `pg_indexes` post-deploy verification query, because
    `CREATE UNIQUE INDEX` fails the deploy outright on pre-existing duplicates and
    `alembic check` is blind to a missing predicate.
21. **`DynastyProcessPlayerIdsJob` enforces a row-count floor** (`DP_MIN_ROWS = 8000`
    against a live file of ~12,472). `validate` previously rejected only an *empty* frame,
    so a truncated upstream snapshot landed as a successful partition — and since the lake
    never overwrites a partition, that day could not then be cleanly re-landed.

### Mandatory tests — these are not optional

```
test_crosswalk_covers_all_rostered_players   every player on every roster in the league
                                             resolves. Failing this = the app is wrong.
test_crosswalk_covers_top_300_adp            top 300 by ADP all resolve pre-draft
test_crosswalk_no_duplicate_player_ids       no two external IDs from the same source map
                                             to one player_id (and vice versa)
test_crosswalk_low_confidence_reviewed       no confidence < 0.9 row is used unverified
```

*Phase 0 status (PR ④):* `test_crosswalk_no_duplicate_player_ids` and
`test_crosswalk_low_confidence_reviewed` ship in
`backend/tests/crosswalk/test_crosswalk_invariants.py` under exactly those names.
`test_crosswalk_covers_all_rostered_players` lands with the Sleeper adapter (PR ⑤ — it
needs a fixture league) and `test_crosswalk_covers_top_300_adp` with ADP ingest (PR ⑥ — it
needs an ADP snapshot). **Until both land, §3 is only half-tested**; Phase 0's exit
criteria are not met by PR ④ alone.

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
    status       TEXT NOT NULL,          -- running|success|failed|skipped_not_modified|skipped
    rows_written INTEGER,
    source_etag  TEXT,                   -- for If-None-Match / 304 handling
    source_mtime TIMESTAMPTZ,
    output_path  TEXT,
    error        TEXT
);
CREATE INDEX ingest_runs_source_idx ON ingest_runs (source, asset, started_at);
-- DESC omitted — btree scans backward; keeps autogenerate drift-free.
```

`status` vocabulary: `running` (row created, lifecycle in flight) · `success` (landed a new partition; `source_etag` becomes the watermark) · `skipped_not_modified` (304 against the watermark) · `skipped` (either a 404 on a `skip_on_404` seasonal asset — not published yet — or the day's partition already existed; `error` says which) · `failed` (any exception; `error` holds the repr). Only `success` rows advance the ETag watermark.


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
