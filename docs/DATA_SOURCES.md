# Data Sources

**Every URL and claim in this document was verified live on 2026-08-15.** Several
contradict what a model trained before mid-2025 believes. Trust this document over your
priors, and re-verify before assuming a source is broken.

---

## ⚠️ Four things that will break code written from memory

1. **`nfl_data_py` is ARCHIVED** (read-only since 2025-09-25). Its README: *"nfl_data_py
   has been deprecated in favour of nflreadpy. All future development will occur in
   nflreadpy and users are encouraged to switch immediately."* Nearly every tutorial and
   blog post still references the dead package. **Use `nflreadpy` — it is Polars-native,
   not pandas.**

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
- `injuries_2025.parquet` exists and is populated (6,068 rows, weeks 1–22) even though the
  official schedule page still claims it doesn't. But it was last written 2026-03-18 — a
  post-season backfill. **In-season cadence for 2026 is unproven.** Use Sleeper for live
  injury status and treat nflverse injuries as historical training data. Also note the 2025
  file dropped the `date_modified` column present in 2024.
- `pbp_participation` (routes run) is delivered by FTN only **after the season ends**.
  Useless in-season. **Use snap % as the route-participation proxy** — it refreshes 4×/day.
- `play_by_play_2026.parquet` returns 404 until Week 1 (Sept 9–13). Handle this.

**License:** package code MIT. Data CC-BY 4.0 — **except FTN charting, which is
CC-BY-SA 4.0 (share-alike).** If we build on FTN charting that obligation is real.

**Library:** `nflreadpy` (PyPI, MIT, Polars-native). Note v0.1.5 dates to 2025-11-19 with
no 2026 release; the loader is thin, so falling back to reading the Parquet URLs directly
is entirely reasonable and removes a dependency.

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

---

## 5. Rankings, ADP, and market values

| Source | URL | Key | Cadence | Use for |
|---|---|---|---|---|
| **DynastyProcess player IDs** | `raw.githubusercontent.com/dynastyprocess/data/master/files/db_playerids.csv` | No | Weekly | ⭐ **The crosswalk.** See `docs/DATABASE.md` |
| **DynastyProcess ECR history** | `.../files/db_fpecr.parquet` | No | **Weekly, Fridays** | FantasyPros expert consensus w/ `sd`, `best`, `worst` — the dispersion feeds tier clustering |
| **DynastyProcess values** | `.../files/values-players.csv` | No | Weekly | `ecr_1qb`, `ecr_2qb`, `value_1qb`, `value_2qb` |
| **FantasyCalc** | `api.fantasycalc.com/values/current?isDynasty=false&numQbs=1&numTeams=12&ppr=1` | No | Continuous | ⭐ **Market trade values** from ~1M real trades. `trend30Day` included. The market half of trade arbitrage. |
| **Fantasy Football Calculator** | `fantasyfootballcalculator.com/api/v1/adp/{ppr\|half-ppr\|standard\|2qb}?teams=12&year=2026` | No | Continuous | True ADP from real mock drafts. ⚠️ Unverified — robots.txt blocked automated checking. **Test before depending on it.** |
| **Sleeper projections** | `api.sleeper.com/projections/nfl/{season}/{week}` | No | ~Daily | Rotowire projections + `adp_dd_ppr` |

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
