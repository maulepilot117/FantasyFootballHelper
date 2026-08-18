# Data Sources

**Every URL and claim in this document was verified live on 2026-08-15.** Several
contradict what a model trained before mid-2025 believes. Trust this document over your
priors, and re-verify before assuming a source is broken.

---

## ⚠️ Four things that will break code written from memory

1. **`nfl_data_py` is ARCHIVED** (read-only since 2025-09-25). Its README: *"nfl_data_py
   has been deprecated in favour of nflreadpy. All future development will occur in
   nflreadpy and users are encouraged to switch immediately."* Nearly every tutorial and
   blog post still references the dead package. **This project uses neither:** the nflverse
   release Parquet/CSV URLs are read directly with httpx + Polars (`ffh.ingest.nflverse`);
   `nflreadpy` is a thin loader we chose not to depend on. Both imports are ruff-banned.

2. **The nflverse player-stats asset was renamed.** `player_stats/player_stats.parquet` is
   frozen at 2025-05-07 and silently serves year-old data. The live path is
   **`stats_player/stats_player_week_{YEAR}.parquet`**.

3. **The Odds API free tier no longer includes NFL.** Free is 25 requests/**day**, NBA+MLB
   moneyline only. The old "500/month, all sports" tier is gone. NFL player props require
   the **$99/mo Business** plan. We are deferring this — see "Deferred" below.

4. **ESPN's fantasy read host changed.** Use **`lm-api-reads.fantasy.espn.com`**, not
   `fantasy.espn.com`.

Also worth knowing: **NFL.com fantasy is dead.** The NFL exited season-long fantasy as of
July 2026 and ESPN is now the official game. Do not build an NFL.com adapter.

---

## 1. nflverse — the backbone

Plain HTTPS Parquet on GitHub Releases. **No API key, no rate limit, no auth.**

```
https://github.com/nflverse/nflverse-data/releases/download/{asset}/{file}.parquet
```

| Asset path | Contents | Refresh cadence (in season) |
|---|---|---|
| `pbp/play_by_play_{YEAR}.parquet` | Full play-by-play with EPA, WP, situational context | Nightly + intra-gameday |
| `stats_player/stats_player_week_{YEAR}.parquet` | **150 columns.** Includes `target_share`, `air_yards_share`, `wopr`, `racr`, `receiving_epa`, `rushing_epa`, `passing_epa`, `fantasy_points_ppr` — all precomputed | Nightly + intra-gameday |
| `snap_counts/snap_counts_{YEAR}.parquet` | `offense_snaps`, `offense_pct`, `defense_pct`, `st_pct` (source: PFR) | 0/6/12/18 UTC daily |
| `depth_charts/depth_charts_{YEAR}.parquet` | **Time series with a `dt` column** — daily snapshots, not current state | Daily 07:00 UTC |
| `injuries/injuries_{YEAR}.parquet` | `report_status`, **`practice_status` (DNP / Limited / Full)**, primary + secondary injury | See caveat below |
| `ftn_charting/{YEAR}.parquet` | `is_play_action`, `is_screen_pass`, `n_blitzers`, `n_defense_box`, `is_qb_out_of_pocket`, coverage | 0/6/12/18 UTC daily |
| `nextgen_stats/ngs_{receiving,rushing,passing}.parquet` | NGS separation, cushion, time-to-throw | Nightly 03:00–05:00 ET |
| `players/players.parquet` | Canonical player registry | Daily, year-round |
| `ff_opportunity/` | **Expected fantasy points, already computed** — don't rebuild this | Nightly |
| `pbp_participation/{YEAR}.parquet` | Routes run, coverage type, personnel groupings | ⚠️ **POST-SEASON ONLY** |

**Caveats:**
- **Every per-season asset 404s until Week 1 — not just play-by-play.** Verified live
  2026-08-16 (24 days before kickoff): `players.parquet` **200** and
  `depth_charts_2026.parquet` **200**, but `stats_player_week_2026`, `snap_counts_2026`,
  `injuries_2026` and `play_by_play_2026` all **404**. Every seasonal ingest job therefore
  maps 404 → `skipped`, never `failed`.
- `injuries_{YEAR}.parquet` exists and is populated for completed seasons (2025: 6,068
  rows, 16 columns) even though the official schedule page claims it doesn't. **In-season
  cadence for 2026 is unproven.** Use Sleeper for live injury status and treat nflverse
  injuries as historical training data. The 2025 file dropped the `date_modified` column
  present in 2024 — confirmed 2026-08-16.
- `pbp_participation` (routes run) is delivered by FTN only **after the season ends**.
  Useless in-season. **Use snap % as the route-participation proxy** — it refreshes 4×/day.
- **ETag / `If-None-Match` works on both hosts** (verified 2026-08-16: conditional GET →
  304 on `players.parquet`, `games.csv` and `stadiums.csv`). GitHub Releases serves a
  strong ETag plus `Last-Modified`; `raw.githubusercontent.com` serves a weak ETag and no
  `Last-Modified`. ⚠️ **The ETag value depends on the negotiated `Accept-Encoding`**, so a
  stored ETag is only valid for a request made by the same client configuration — which is
  why every job goes through `ffh.ingest.http.make_client()`.

**Verified schemas (2026-08-16) — these field names contradict the table names in
`DATABASE.md`, which is deliberate; the lake stores upstream names verbatim:**

| Asset | Rows × cols | Player key | Gotchas |
|---|---|---|---|
| `players/players.parquet` | 25,033 × 39 | `gsis_id` | `display_name` (no `full_name`), `college_name` (no `college`), `rookie_season` (no `rookie_year`), `height`/`weight` (no `height_in`/`weight_lb`), **`birth_date` is a String** |
| `stats_player/stats_player_week_2025.parquet` | 19,422 × **150** | **`player_id`** (this IS the GSIS id) | there is **no** `gsis_id` column |
| `snap_counts/snap_counts_2025.parquet` | 26,612 × 16 | **`pfr_player_id`** | **carries no GSIS id at all** — the crosswalk must hold `pfr` ids |
| `depth_charts/depth_charts_2026.parquet` | 439,615 × 12 | `gsis_id`, `espn_id` | **`dt` is a String** ISO-8601 UTC stamp, not a datetime; positions in `pos_abb`/`pos_rank` |
| `injuries/injuries_2025.parquet` | 6,068 × 16 | `gsis_id` | no `date_modified` |
| `pbp/play_by_play_2025.parquet` | — × 372 | `passer/rusher/receiver_player_id` | 20 MB compressed |

**License:** package code MIT. Data CC-BY 4.0 — **except FTN charting, which is
CC-BY-SA 4.0 (share-alike).** If we build on FTN charting that obligation is real.

**Library:** none. `nflreadpy` (PyPI, MIT, Polars-native) exists but v0.1.5 dates to
2025-11-19 with no 2026 release; the loader is thin, so PR ③ reads the release Parquet URLs
directly (`ffh.ingest.nflverse`) and `nflreadpy` is **not** a dependency — decided 2026-08-16.

---

## 2. Vegas lines — free for what we need

### Game totals and spreads: **free, solved**

`https://raw.githubusercontent.com/nflverse/nfldata/master/data/games.csv`

7,548 games, **1999–2026**. Columns: `spread_line`, `total_line`, `away_moneyline`,
`home_moneyline`, `over_odds`, `under_odds`, plus `roof`, `surface`, `temp`, `wind`,
`stadium_id`, `div_game`, `away_rest`, `home_rest`, `referee`. The 2026 season is already
published (272 games). **Refreshes every 5 minutes in season.**

This single file is both our live line feed and our backtest dataset. `temp`/`wind` are
post-game actuals, which makes it the training target for weather adjustment.

**Verified live 2026-08-16 — 46 columns, 7,548 rows, 272 for season 2026:**

- `gameday` (`YYYY-MM-DD`) and `gametime` (`HH:MM`) are **Eastern wall-clock**, not UTC.
  Convert with `America/New_York` → UTC (`2026_01_NE_SEA` is `2026-09-09 20:20` ET, the
  8:20 pm Wednesday opener). Both are Strings; there is no timezone column.
- ⚠️ **`roof` is the literal quoted empty string `""`, not NULL, for retractable-roof
  stadiums whose game has not been played** — 43 of 272 rows in 2026 (HOU00, IND00, ATL97,
  DAL00, PHO00, VEG00 …). Polars reports `null_count = 0`. Map `""` → NULL explicitly or
  you will store an empty roof state. All-time values: `outdoors` 5,510 · `dome` 1,246 ·
  `closed` 621 · `open` 128 · `""` 43.
- `game_type` ∈ {`REG`, `WC`, `DIV`, `CON`, `SB`}; `games.season_type` in `DATABASE.md` is
  `REG|POST`, so map every non-`REG` value to `POST`.
- `location` ∈ {`Home`, `Neutral`} → `games.neutral_site`. 2026 has **8** neutral-site games.
- ⚠️ **Neutral-site games carry the nominal HOME team's `stadium_id` while `stadium` names
  the real venue** (2026: SoFi/Melbourne, DAL00/Maracana, WAS00/Tottenham, JAX00/Wembley,
  NOR00/Stade de France, ATL97/Bernabeu, DET00/Munich, SFO01/Estadio Banorte). The 30/30
  stadium join succeeds *because* it degrades to the home stadium — **never read stadium
  coordinates, altitude or tz for a row with `neutral_site = true`.**
- Lines are sparse pre-season: on 2026-08-16, 68 of 272 rows had `spread_line`/`total_line`.
  `temp`/`wind`/scores are NULL until a game is played.

### Live odds: ESPN, undocumented, no auth

```
https://site.web.api.espn.com/apis/v3/sports/football/nfl/odds
https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/{ID}/competitions/{ID}/odds
```

Returns DraftKings spreads/totals/moneylines with **both opening and closing lines**.
Line movement from open to close is signal — capture it.

### Player props: **deferred, not free**

No free source exists. The Odds API Business tier is $99/mo and is the only realistic
option. **Decision: build our own projections first, backtest against 2025 walk-forward,
and only buy props if we lose to them.** If we do buy: requests returning **304 Not
Modified via ETag consume zero credits**, so poll with `If-None-Match` and 20k/mo goes very
far. Run seasonally (Sept–Feb) to halve annual cost.

---

## 3. Platform adapters

All adapters implement one interface (see `docs/ARCHITECTURE.md`). **Build Sleeper
first** — it is the only one that cannot block on an external approval.

### Sleeper — primary

Base `https://api.sleeper.app/v1`. **No auth, no key.** Rate limit: stay under
**1000 req/min** or risk an IP block (IP-based; no key identifies you, so back off properly).
Read-only by design — the API cannot write, so lineup sets and waiver claims must be done
by hand in the app.

```
/state/nfl                                   season, week, season_type
/user/{username|id}
/user/{user_id}/leagues/nfl/{season}
/league/{id}  /rosters  /users  /matchups/{wk}  /transactions/{round}  /traded_picks
/league/{id}/drafts
/draft/{id}  /picks  /traded_picks
/players/nfl                                 ~5MB — cache to disk, call ≤1×/day
/players/nfl/trending/{add|drop}?lookback_hours=24&limit=25
```

**Live draft:** poll `/draft/{id}` and watch `last_picked` (epoch ms) as a cheap change
detector; fetch `/draft/{id}/picks` only when it advances. 1–2s polling is ~0.1% of the
rate budget. No websocket exists.

**Undocumented endpoints on `api.sleeper.com` (verified live, higher break risk, high value):**

```
/players/nfl/research/regular/{season}/{week}    ← league-wide {"owned":97.0,"started":94.9}
/players/nfl/{TEAM}/depth_chart                  ← {"QB":[...],"WR1":[...]}
/projections/nfl/{season}/{week}?season_type=regular&position[]=RB   ← Rotowire, has adp_dd_ppr
/schedule/nfl/regular/{year}
/stats/nfl/player/{player_id}?season_type=regular&season={year}
```

The **ownership + start-rate** endpoint is the single best signal on the platform and no
other provider exposes it. Wrap all undocumented endpoints with cached last-known-good.

**License:** non-commercial use only. Self-hosted personal use is fine.
**Library:** `sleeper-api-wrapper` 1.2.1 (dtsong fork, MIT). ⚠️ The original
`SwapnikKatkoori/sleeper-api-wrapper` is abandoned and outranks the fork in search.
The API is simple enough that a thin in-house client is defensible.

### ESPN — secondary

Base `https://lm-api-reads.fantasy.espn.com/apis/v3/games/ffl`. **Public leagues work
unauthenticated.** Private leagues need `espn_s2` + `SWID` cookies, extracted manually from
browser devtools (username/password auth has been dead since 2020). Store them in Vault.

```
/seasons/{YEAR}?view=proTeamSchedules_wl
/seasons/{YEAR}/segments/0/leagues/{ID}?view=mDraftDetail
/seasons/{YEAR}/segments/0/leagues/{ID}?view=mMatchup&view=mMatchupScore&scoringPeriodId={WK}
```

**Live draft — cleanest structure of the three.** `mDraftDetail` returns `inProgress`
plus a **pre-allocated pick array** (rounds × teams) where `playerId` is `-1` until filled.
You know slot ordering before the draft starts.

Player queries need a custom `x-fantasy-filter` JSON header. `statSourceId=1` is projected,
`0` is actual.

**Risk:** undocumented, no terms grant, no stability guarantee. ESPN absorbed the entire
NFL.com fantasy userbase this year — more traffic and more scrutiny on these endpoints.
Always have a fallback.

**Library:** `espn-api` 0.46.0 (MIT, actively maintained — 2026-03-23).

### Yahoo — stretch goal only

Official OAuth2 API, but: registration now requires **human review with no SLA**, and
**write access was removed entirely in 2026** (*"The Yahoo Fantasy Sports API currently
provides read access only"*). The detailed developer docs were taken down and now redirect
to `sports.yahoo.com/developer/`. No published rate limit anymore. XML by default; append
`?format=json` for an awkward XML-transliterated JSON.

Design it read-only. Submit the application early if we're going to need it at all —
it's the one item with a dependency we can't control.

**Library:** `spilchen/yahoo_fantasy_api` 2.12.3 (**MIT** — prefer this). `yfpy` is more
complete but **GPL-3.0**, which is viral; do not link it in.

---

## 4. Weather

**Open-Meteo** — `https://api.open-meteo.com/v1/forecast`

No API key. Limits: 600/min, 5,000/hr, **10,000/day**. We need ~272 forecasts per season,
so this is ~37× more headroom than required. Data CC-BY 4.0 (attribution required).
Free tier is **non-commercial**; historical/archive endpoints require a paid plan.

```
?latitude=&longitude=&hourly=temperature_2m,precipitation,wind_speed_10m,wind_gusts_10m
```

**Stadium coordinates** — `greerreNFL/stadiums`, raw CSV:
`https://raw.githubusercontent.com/greerreNFL/stadiums/main/data/stadiums.csv`

62 stadiums with `stadium_id`, `lat`, `lon`, `altitude`, **`heading`** (field orientation —
enables crosswind modeling), `surface_type`, `roof_type`, `tz`. **Verified: 30/30
`stadium_id` values for 2025+ nflverse games join exactly, zero unmatched.** Clean key.

⚠️ Its `roof_type` is only `Outdoors`/`Dome` — **no retractable category.** For retractables
use the nflverse `games.csv` **`roof`** column, which carries the *per-game actual state*
(`closed` vs `open`). A retractable roof that was open in a downpour is a very different
game than one that was closed, and only `games.csv` knows which happened.

`ThompsonJamesBliss/WeatherData` is stale (2000–2020) but its `stadium_coordinates.csv` has
a true Indoor/Outdoor/**Retractable** classification and stadium azimuth, useful as
reference.

**Verified live 2026-08-16:** 62 rows × 35 columns, `stadium_id` unique. The columns this
project uses are `stadium_id, stadium_name, lat, lon, altitude, heading, surface_type,
roof_type, tz` — all non-null for the 30 stadiums referenced by 2026 games, and all 30 join
(0 unmatched, confirming the 30/30 claim).

⚠️ **`altitude` is in METRES, not feet.** `DEN00` = 1583.586 (Mile High is 1,609 m /
5,280 ft); `SEA00` = 5.214 (sea level). `DATABASE.md` §2 declares `stadiums.altitude_ft
INTEGER`, so `ffh.ingest.reference.seed_stadiums` multiplies by 3.280839895 and rounds.
`surface_type` ∈ {`Grass`, `Turf`}; `roof_type` ∈ {`Dome`, `Outdoors`}; `tz` is a valid
IANA name. The column is `stadium_name`, not `name`.

---

## 5. Rankings, ADP, and market values

| Source | URL | Key | Cadence | Use for |
|---|---|---|---|---|
| **DynastyProcess player IDs** | `raw.githubusercontent.com/dynastyprocess/data/master/files/db_playerids.csv` | No | Weekly | ⭐ **The crosswalk.** Job `dynastyprocess_playerids`; see below and `docs/DATABASE.md` §3 |
| **DynastyProcess ECR history** | `.../files/db_fpecr.parquet` | No | **Weekly, Fridays** | FantasyPros expert consensus w/ `sd`, `best`, `worst` — the dispersion feeds tier clustering |
| **DynastyProcess values** | `.../files/values-players.csv` | No | Weekly | `ecr_1qb`, `ecr_2qb`, `value_1qb`, `value_2qb` |
| **FantasyCalc** | `api.fantasycalc.com/values/current?isDynasty=false&numQbs=1&numTeams=12&ppr=1` | No | Continuous | ⭐ **Market trade values** from ~1M real trades. `trend30Day` included. The market half of trade arbitrage. |
| **Fantasy Football Calculator** | `fantasyfootballcalculator.com/api/v1/adp/{ppr\|half-ppr\|standard\|2qb}?teams=12&year=2026` | No | Continuous | True ADP from real mock drafts. ⚠️ Unverified — robots.txt blocked automated checking. **Test before depending on it.** |
| **Sleeper projections** | `api.sleeper.com/projections/nfl/{season}/{week}` | No | ~Daily | Rotowire projections + `adp_dd_ppr` |

### DynastyProcess `db_playerids.csv` — verified live 2026-08-16 (PR ④)

**12,472 rows × 35 columns**, `NA` is the null sentinel. The URL above is exactly what
`ffh.crosswalk.dynastyprocess.DP_URL` requests
(`https://raw.githubusercontent.com/dynastyprocess/data/master/files/db_playerids.csv`).

Columns — ids: `mfl_id sportradar_id fantasypros_id gsis_id pff_id sleeper_id nfl_id
espn_id yahoo_id fleaflicker_id cbs_id pfr_id cfbref_id rotowire_id rotoworld_id ktc_id
stats_id stats_global_id fantasy_data_id swish_id`; attributes: `name merge_name position
team birthdate age draft_year draft_round draft_pick draft_ovr twitter_username height
weight college db_season`. Of these, seven map to `player_external_ids.source`:
`sleeper_id espn_id yahoo_id pfr_id fantasypros_id sportradar_id rotowire_id`.

Gotchas, all confirmed against the live file — these contradict what you would guess:

- **Kickers are `PK`, not `K`** (`normalize_position` maps `PK`→`K`, `FB`/`HB`→`RB`).
- **There are no DST/DEF rows at all.** Defenses come only from
  `ffh.crosswalk.registry.seed_dst_players`; a DST-positioned DP row would be counted in
  `CrosswalkApplyReport.skipped_dst` and never mint a `players` row.
- **`team` uses MFL codes**, not nflverse: `KCC TBB GBP NEP NOS SFO LVR LAR JAC` plus
  historic `OAK SDC STL RAM` and `FA` / `FA*`. Everything goes through `normalize_team`.
- **Every id column must be read as text.** `sportradar_id` is a UUID, `pfr_id` is
  alphanumeric (`CartKy01`), and a numeric id that passes through a float becomes
  `"4046.0"` — silent corruption of the highest-risk table in the system.
  `read_playerids_csv` forces `pl.Utf8` on the id columns and the ingest job re-asserts it
  before landing Parquet.
- **~144 QB/RB/WR/TE/PK rows have a `sleeper_id` but no `gsis_id`** (2026 rookies and
  UDFAs). These are keyed on an `mfl:<mfl_id>` placeholder and create a new `players` row.
- **The file contains duplicate-id glitch rows.** Two rows (`Fred Williams` / `Kevin Smith`,
  both WR) share every id including `gsis_id 00-0031320` but differ on `rotowire_id`;
  `espn_id 2582138` and `pfr_id CartKy01` each appear on two different TEs. This is why
  `apply_playerids` has an ambiguity policy — such ids are dropped into
  `CrosswalkApplyReport.ambiguous` and reported, never applied.

**Ingest:** job `dynastyprocess_playerids` (`ffh ingest run dynastyprocess_playerids`) —
weekly full snapshot, ETag-conditional, no `persist()`. It lands
`raw/dynastyprocess/playerids/scrape_date=YYYY-MM-DD/playerids.parquet` in the lake;
Postgres is populated separately by `ffh crosswalk seed --playerids <parquet>`, which
re-reads that partition and calls `apply_playerids`.

⚠️ **ECR is an ordinal ranking, not projected points.** Use it for tier clustering and as a
sanity check on our own projections — never as a projection itself.

**FantasyPros' own API free tier explicitly bars production use.** Don't sign up; the
DynastyProcess mirror gives us the same ECR data under GPL-3.0 without the restriction.

---

## 6. News and injury status — freshest first

1. **Sleeper `/players/nfl`** — `injury_status`, `injury_body_part`, `injury_notes`,
   `practice_participation`. Fastest-moving free status field. 5MB payload, ≤1×/day.
2. **ESPN injuries** —
   `https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/teams/{ID}/injuries?limit=100`
   Free, no auth, no documented limit.
3. **ESPN news** — `https://site.api.espn.com/apis/site/v2/sports/football/nfl/news?limit=50`
   (also `?team={ID}`). **The only genuinely free structured news feed.** This is the
   unstructured input the LLM layer parses.
4. **nflverse injuries** — historical practice participation for model training.

RotoWire and RotoBaller are sales-contact-only with no published pricing. Not viable.

---

## 7. Defensive matchup metrics

**DVOA is fully paywalled** at FTN (Football Outsiders was absorbed). Do not budget for it.

**Compute our own from play-by-play instead** — competitive, transparent, and free:

- Defensive EPA/play allowed, split by pass/run, personnel grouping, down-distance
- Success rate allowed
- Fantasy points allowed by position: group `stats_player_week` by `opponent_team` ×
  `position`, sum `fantasy_points_ppr`. Trivial, and strictly more flexible than any
  scraped table since we can slice it by any window.
- Pressure rate and blitz rate from `ftn_charting` (updates 4×/day in season)

---

## Deferred / rejected

| Source | Status |
|---|---|
| The Odds API player props | **Deferred.** $99/mo. Revisit only after 2025 backtest shows our projections lose. |
| DVOA / FTN | Rejected — paywalled, replaceable with EPA metrics |
| PFF | Rejected — paywalled, no free tier |
| Fantasy Nerds | Rejected — $499/yr |
| RotoWire / RotoBaller feeds | Rejected — no published pricing or terms |
| NFL.com | **Dead.** NFL exited season-long fantasy July 2026 |
| CBS Sports API | Dead — `developer.cbssports.com` no longer resolves |

---

## Licensing summary — respect these

| Source | Restriction |
|---|---|
| Sleeper | **Non-commercial only** |
| Open-Meteo free tier | **Non-commercial only**; attribution required (CC-BY 4.0) |
| nflverse FTN charting | **CC-BY-SA 4.0 — share-alike** |
| nflverse (everything else) | CC-BY 4.0, attribution |
| DynastyProcess | GPL-3.0 (data repo) |
| ESPN endpoints | Undocumented, no terms grant, no stability guarantee |
| `yfpy` | GPL-3.0 — **do not link into this codebase** |

This project is personal, self-hosted, and non-commercial, which keeps us inside all of
the above. That constraint is real — do not build a hosted multi-user version without
revisiting this table.
