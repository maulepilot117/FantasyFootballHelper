# PR ③ `feat/ingest-nflverse-games` — Ingest Framework, nflverse Lake, Games + Reference Seeds Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A reusable, idempotent, watermarked ingest framework that lands nflverse Parquet and `nfldata/games.csv` in the NFS lake, upserts `games`/`nfl_teams`/`stadiums`/the sentinel `leagues` row into Postgres, and exposes the lake to DuckDB — driven by `ffh ingest run <job>`.

**Architecture:** One `IngestJob` ABC owns the whole lifecycle (`fetch → parse → validate → land → persist`) and writes exactly one `ingest_runs` row per invocation. HTTP conditional GETs (`If-None-Match` against the last successful `ingest_runs.source_etag`) make re-runs free; a partition is never overwritten, so a new scrape is always a new directory. Jobs are registered by name in a module-level registry the CLI dispatches through. `ffh.features.duck` opens an **in-memory** DuckDB with `read_parquet` views over the latest partition per asset — the lake is the only source of truth for analytics, and no `.duckdb` file is ever created.

**Tech Stack:** Polars 1.43.2 · httpx 0.28.1 · tenacity 9.1.4 · DuckDB 1.5.5 · SQLAlchemy 2.0.51 (`postgresql+psycopg`) · typer 0.27.1 · structlog 26.1.0 · pytest 9.1.1 + respx 0.23.1.

**Spec:** `docs/superpowers/specs/2026-08-15-phase0-foundation-design.md` §3 · scope locked in `docs/superpowers/plans/2026-08-15-phase0-00-overview.md` §③

---

## Live verification log — performed 2026-08-16 by the plan author

Every URL, HTTP status, column name, dtype and unit below was fetched live. **Where this
section and `docs/DATA_SOURCES.md` disagree, this section is newer and wins**; Task 10
writes these findings back into `DATA_SOURCES.md` in the same PR.

### Availability (2026-08-16, 24 days before Week 1)

| URL | Status |
|---|---|
| `…/releases/download/players/players.parquet` | **200** (25,033 rows × 39 cols) |
| `…/releases/download/depth_charts/depth_charts_2026.parquet` | **200** (439,615 rows × 12 cols) |
| `…/releases/download/stats_player/stats_player_week_2026.parquet` | **404** |
| `…/releases/download/snap_counts/snap_counts_2026.parquet` | **404** |
| `…/releases/download/injuries/injuries_2026.parquet` | **404** |
| `…/releases/download/pbp/play_by_play_2026.parquet` | **404** |
| `…/stats_player/stats_player_week_2025.parquet` | 200 (19,422 × 150) |
| `…/snap_counts/snap_counts_2025.parquet` | 200 (26,612 × 16) |
| `…/injuries/injuries_2025.parquet` | 200 (6,068 × 16) |
| `…/pbp/play_by_play_2025.parquet` | 200 (372 cols, 20.3 MB) |
| `raw.githubusercontent.com/nflverse/nfldata/master/data/games.csv` | 200 (7,548 × 46) |
| `raw.githubusercontent.com/greerreNFL/stadiums/main/data/stadiums.csv` | 200 (62 × 35) |
| `site.api.espn.com/apis/site/v2/sports/football/nfl/teams` | 200 (32 teams) |

> ⚠️ **`DATA_SOURCES.md` §1 says only `play_by_play_{YEAR}` 404s until Week 1. That is
> wrong.** *Every* per-season weekly asset 404s before Week 1. `depth_charts` is the lone
> seasonal exception (preseason depth charts publish in March). Therefore **every seasonal
> job maps 404 → `skipped`**, not just pbp.

### Conditional-request support

`HEAD` + `If-None-Match` → `304` verified on `games.csv`, `players.parquet` and
`stadiums.csv`. GitHub Releases serves a strong ETag (`"0x8DEFB6F9F2A452A"`) plus
`Last-Modified`; `raw.githubusercontent.com` serves a weak ETag (`W/"…"`) and no
`Last-Modified`. **The ETag value differs by `Accept-Encoding`** (a gzip response and an
identity response carry different ETags for the same bytes). Consequence: always issue the
conditional GET from the *same* client configuration that produced the stored ETag — which
is why every job goes through the single `make_client()` factory.

### `games.csv` (46 columns, 7,548 rows; 272 rows for season 2026)

Header, in order:
`game_id, season, game_type, week, gameday, weekday, gametime, away_team, away_score,
home_team, home_score, location, result, total, overtime, old_game_id, gsis,
nfl_detail_id, pfr, pff, espn, ftn, away_rest, home_rest, away_moneyline, home_moneyline,
spread_line, away_spread_odds, home_spread_odds, total_line, under_odds, over_odds,
div_game, roof, surface, temp, wind, away_qb_id, home_qb_id, away_qb_name, home_qb_name,
away_coach, home_coach, referee, stadium_id, stadium`

- `gameday` is `YYYY-MM-DD` (String) and `gametime` is `HH:MM` (String), both **Eastern**
  local wall-clock. Verified: `2026_01_NE_SEA` is `2026-09-09 20:20` — the 8:20 pm ET
  Wednesday opener at Lumen Field.
- **`roof` is the literal quoted empty string `""`, not NULL, for retractable-roof
  stadiums whose game has not been played.** 43 of 272 rows in 2026 (HOU00, IND00, ATL97,
  DAL00, PHO00, VEG00 …). Polars reports `null_count = 0` and `empty_str = 43`. Any code
  that assumes NULL here silently stores `""` as the roof state. All-time values:
  `outdoors` 5,510 · `dome` 1,246 · `closed` 621 · `open` 128 · `""` 43.
- `game_type` ∈ {`REG` 7,239, `WC` 120, `DIV` 108, `CON` 54, `SB` 27}. `DATABASE.md` §2
  declares `games.season_type` as `REG|POST`, so map `REG → 'REG'`, everything else
  `→ 'POST'`.
- `location` ∈ {`Home` 264, `Neutral` 8} for 2026 → `neutral_site = (location == 'Neutral')`.
- `div_game` is Int64 0/1. `away_rest`/`home_rest` Int64, non-null. `spread_line`/
  `total_line` Float64 (204 of 272 null on 2026-08-16 — lines exist only for the first
  weeks). `home_score`/`away_score`/`temp`/`wind` all null for unplayed games.
- `stadium_id` non-null for all 272 rows of 2026; 30 distinct values.
- ⚠️ **Neutral-site games carry the nominal home team's `stadium_id` while `stadium` names
  the real venue.** Verified for all 8 of 2026:

  | game_id | stadium_id | stadium |
  |---|---|---|
  | `2026_01_SF_LA` | `LAX01` (SoFi Stadium) | Melbourne Cricket Ground |
  | `2026_03_BAL_DAL` | `DAL00` | Maracana Stadium |
  | `2026_04_IND_WAS` | `WAS00` | Tottenham Hotspur Stadium |
  | `2026_06_HOU_JAX` | `JAX00` | Wembley Stadium |
  | `2026_07_PIT_NO` | `NOR00` | Stade de France |
  | `2026_09_CIN_ATL` | `ATL97` | Bernabeu |
  | `2026_10_NE_DET` | `DET00` | FC Bayern Munich Stadium |
  | `2026_11_MIN_SF` | `SFO01` | Estadio Banorte |

  The `stadium_id → stadiums` join is therefore 100 % matched **because** it degrades to
  the home stadium. Never read `stadiums.latitude/longitude/altitude_ft/tz` for a row
  where `neutral_site = true` (matters for the Phase-1 weather module, not for this PR).

### `stadiums.csv` (35 columns, 62 rows, `stadium_id` unique)

Columns used: `stadium_id, stadium_name, lat, lon, altitude, heading, surface_type,
roof_type, tz`. All non-null for the 30 stadiums referenced by 2026 games; all 30 join
(**0 unmatched**, confirming DATA_SOURCES.md §4's 30/30 claim).

- ⚠️ **`altitude` is in METRES, not feet.** `DEN00` = 1583.586 (Mile High is 1,609 m /
  5,280 ft); `SEA00` = 5.214 (sea level). `DATABASE.md` §2 declares `stadiums.altitude_ft
  INTEGER`, so multiply by `3.280839895` and round.
- `heading` is Int64 degrees, non-null. `surface_type` ∈ {`Grass`, `Turf`}. `roof_type` ∈
  {`Dome`, `Outdoors`} — no retractable category, exactly as DATA_SOURCES.md §4 warns.
- `tz` is a valid IANA name (`America/New_York`, `America/Phoenix`, `Europe/London`, …).

### nflverse schemas (read with `polars.read_parquet_schema`)

- **`players.parquet`** — 39 cols. Field names are **not** the `players` table's names:
  `gsis_id, display_name, common_first_name, first_name, last_name, short_name,
  football_name, suffix, esb_id, nfl_id, pfr_id, pff_id, otc_id, espn_id, smart_id,
  birth_date (String!), position_group, position, ngs_position_group, ngs_position,
  height (Int32), weight (Int32), headshot, college_name, college_conference,
  jersey_number, rookie_season (Int32), last_season, latest_team, status, ngs_status,
  ngs_status_short_description, years_of_experience, pff_position, pff_status, draft_year,
  draft_round, draft_pick, draft_team`. There is **no** `full_name`, no `college`, no
  `rookie_year`, no `height_in`/`weight_lb`; `birth_date` is a String, not a Date. PR ④
  does the renaming — this PR only lands the file verbatim.
- **`stats_player_week_2025.parquet`** — 150 cols (matches DATA_SOURCES.md). ⚠️ The player
  key is **`player_id`**, which holds the GSIS id — there is no `gsis_id` column.
- **`snap_counts_2025.parquet`** — 16 cols: `game_id, pfr_game_id, season, game_type, week,
  player, pfr_player_id, position, team, opponent, offense_snaps, offense_pct,
  defense_snaps, defense_pct, st_snaps, st_pct`. ⚠️ **No GSIS id at all** — snap counts key
  on `pfr_player_id`, so PR ④'s crosswalk must carry `pfr` ids.
- **`depth_charts_2026.parquet`** — 12 cols: `dt, team, player_name, espn_id, gsis_id,
  pos_grp_id, pos_grp, pos_id, pos_name, pos_abb, pos_slot (Int32), pos_rank (Int32)`.
  ⚠️ `dt` is a **String** ISO-8601 UTC stamp (`2026-08-16T07:26:28Z`), not a datetime;
  values span `2026-03-22T06:38:42Z` … `2026-08-16T07:26:28Z`.
- **`injuries_2025.parquet`** — 16 cols: `season, season_type, game_type, team, week,
  gsis_id, position, full_name, first_name, last_name, report_primary_injury,
  report_secondary_injury, report_status, practice_primary_injury,
  practice_secondary_injury, practice_status`. Confirms DATA_SOURCES.md: **no
  `date_modified`** in 2025+.
- **`play_by_play_2025.parquet`** — 372 cols; all 26 columns this plan requires are
  present.

### ESPN team ids (`site.api.espn.com/.../nfl/teams`, 32 teams)

ESPN abbreviations differ from nflverse for two teams: ESPN `LAR`/`WSH` vs nflverse
`LA`/`WAS`. The checked-in `nfl_teams.csv` in Task 6 uses **nflverse** abbreviations with
ESPN's numeric ids.

---

## Global Constraints

Every task's requirements implicitly include this section.

- **Polars-native.** Never `import pandas`, `nfl_data_py`, or `nflreadpy` — all three are
  banned by `ruff` (`flake8-tidy-imports`) in `backend/pyproject.toml` and CI fails on
  them. Remote Parquet is read as `pl.read_parquet(io.BytesIO(content))` after an httpx
  GET; remote CSV as `pl.read_csv(io.BytesIO(content))`.
- **No `.duckdb` file, ever — including tests.** `duckdb.connect()` with no argument (or
  `":memory:"`) only. No SQLite anywhere. Postgres is the test DB (`db` marker).
- **Never overwrite a lake partition.** `write_parquet` creates the target with an
  exclusive link and raises `PartitionExistsError` if it already exists. A new scrape is a
  new `scrape_date=` directory (`DATABASE.md` §1).
- **Every Polars join asserts row counts or passes `validate=`.** This PR has exactly one
  join (the stadium-coverage anti-join in Task 5) and it asserts
  `matched.height + unmatched.height == df.height`.
- **Every `INSERT … ON CONFLICT` sets `updated_at = now()` explicitly.** SQLAlchemy's ORM
  `onupdate=func.now()` does **not** fire for Core/`pg_insert` upserts (`DATABASE.md` §2).
- **No new dependencies.** Everything used here is already pinned in
  `backend/pyproject.toml` (polars, duckdb, httpx, tenacity, sqlalchemy, psycopg, typer,
  structlog, pytest, respx). `uv`'s `exclude-newer = "2026-08-09T00:00:00Z"` already
  enforces the 7-day supply-chain cooldown; do not raise it in this PR. If you believe you
  need a new package, stop and escalate instead of adding it.
- **No secrets.** Every URL in this PR is public and unauthenticated. No key, cookie or
  token appears in code, tests, fixtures, or the lake.
- **Docs in the same PR as the code** — `docs/DATA_SOURCES.md`, `docs/DATABASE.md`,
  `docs/ROADMAP.md` (WORKFLOW.md "definition of done").
- **Branch `feat/ingest-nflverse-games`** off `main` at `ef4b656`. Conventional commits
  scoped to the module (`feat(ingest): …`, `test(ingest): …`, `docs(data): …`). Never
  commit to `main`; never force-push.
- **Implementers do not push and do not open the PR.** Commit locally on the branch and
  stop. The controlling session pushes, opens the PR and runs the Codex gate.
- Run `uv run ruff check . && uv run ruff format .` from `backend/` before every commit.
- Tests that need Postgres carry `pytestmark = pytest.mark.db`. Tests that hit the real
  network carry `pytest.mark.network` and never run in CI (`addopts = -m 'not network'`).

---

## File structure

| File | Responsibility |
|---|---|
| `backend/src/ffh/ingest/lake.py` | Partition paths, the never-overwrite Parquet writer, `scrape_date` |
| `backend/src/ffh/ingest/http.py` | The one shared `httpx.Client` factory; retrying conditional GET returning `Fetched`/`NotModified`/`NotFound` |
| `backend/src/ffh/ingest/base.py` | `IngestJob` ABC + `HttpIngestJob`, result dataclasses, statuses, the `JOBS` registry, and the `run()` lifecycle that writes `ingest_runs` |
| `backend/src/ffh/ingest/nflverse.py` | The six nflverse Parquet jobs and their `REQUIRED_COLUMNS` |
| `backend/src/ffh/ingest/games.py` | `nfldata_games` job + `upsert_games` (games.csv → `games`) |
| `backend/src/ffh/ingest/reference.py` | `nfl_teams` seed, `stadiums` job + seed, sentinel `leagues` seed, coverage assertions |
| `backend/src/ffh/data/nfl_teams.csv` | Checked-in 32-row static team table |
| `backend/src/ffh/features/duck.py` | In-memory DuckDB connection with `read_parquet` views over the latest lake partitions |
| `backend/src/ffh/cli.py` *(modify)* | `ffh ingest run|list|seed` |
| `backend/scripts/record_nflverse_fixtures.py` | One-shot recorder for the small committed test fixtures |
| `backend/tests/ingest/*`, `backend/tests/features/*`, `backend/tests/test_cli_ingest.py` | Tests |
| `backend/tests/fixtures/{nflverse,nfldata,stadiums}/*` | Recorded fixtures |

---

### Task 1: `ffh.ingest.lake` — partition paths and the never-overwrite writer

**Files:**
- Create: `backend/src/ffh/ingest/lake.py`
- Create: `backend/tests/ingest/__init__.py` (empty)
- Test: `backend/tests/ingest/test_lake.py`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `ffh.ingest.lake.scrape_date(now: datetime | None = None) -> str` — today's UTC date as `YYYY-MM-DD`.
  - `ffh.ingest.lake.partition_path(lake_root: Path, source: str, asset: str, **keys: str | int) -> Path` — `<lake_root>/raw/<source>/<asset>/<k>=<v>/…` in the insertion order of `keys`.
  - `ffh.ingest.lake.parquet_file(lake_root: Path, source: str, asset: str, **keys: str | int) -> Path` — the partition dir plus `<asset>.parquet`.
  - `ffh.ingest.lake.write_parquet(df: pl.DataFrame, path: Path) -> int` — returns rows written; raises `PartitionExistsError` if `path` exists.
  - `ffh.ingest.lake.PartitionExistsError(FileExistsError)`.

- [ ] **Step 1: Write the failing test `backend/tests/ingest/test_lake.py`**

```python
from datetime import UTC, datetime
from pathlib import Path

import polars as pl
import pytest

from ffh.ingest.lake import (
    PartitionExistsError,
    parquet_file,
    partition_path,
    scrape_date,
    write_parquet,
)


def test_scrape_date_is_utc_iso_day():
    assert scrape_date(datetime(2026, 8, 16, 23, 59, tzinfo=UTC)) == "2026-08-16"


def test_scrape_date_defaults_to_now_utc():
    assert len(scrape_date()) == 10 and scrape_date()[4] == "-"


def test_partition_path_uses_hive_keys_in_insertion_order(tmp_path: Path):
    p = partition_path(tmp_path, "nflverse", "injuries", season=2026, scrape_date="2026-08-16")
    assert p == tmp_path / "raw" / "nflverse" / "injuries" / "season=2026" / "scrape_date=2026-08-16"


def test_partition_path_with_no_keys_is_the_asset_dir(tmp_path: Path):
    assert partition_path(tmp_path, "nfldata", "games") == tmp_path / "raw" / "nfldata" / "games"


def test_parquet_file_appends_asset_filename(tmp_path: Path):
    p = parquet_file(tmp_path, "nfldata", "games", scrape_date="2026-08-16")
    assert p.name == "games.parquet"
    assert p.parent.name == "scrape_date=2026-08-16"


def test_write_parquet_creates_parents_and_returns_row_count(tmp_path: Path):
    df = pl.DataFrame({"a": [1, 2, 3]})
    path = parquet_file(tmp_path, "nflverse", "players", scrape_date="2026-08-16")
    assert write_parquet(df, path) == 3
    assert path.exists()
    assert pl.read_parquet(path).height == 3


def test_write_parquet_refuses_to_overwrite(tmp_path: Path):
    df = pl.DataFrame({"a": [1]})
    path = parquet_file(tmp_path, "nflverse", "players", scrape_date="2026-08-16")
    write_parquet(df, path)
    with pytest.raises(PartitionExistsError):
        write_parquet(pl.DataFrame({"a": [9]}), path)
    # the original content survives — DATABASE.md §1 "never overwrite a scrape partition"
    assert pl.read_parquet(path)["a"].to_list() == [1]


def test_write_parquet_leaves_no_temp_file_behind(tmp_path: Path):
    path = parquet_file(tmp_path, "nflverse", "players", scrape_date="2026-08-16")
    write_parquet(pl.DataFrame({"a": [1]}), path)
    assert sorted(p.name for p in path.parent.iterdir()) == ["players.parquet"]
```

- [ ] **Step 2: Run test to verify it fails**

Run from `backend/`: `uv run pytest tests/ingest/test_lake.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'ffh.ingest.lake'`.

- [ ] **Step 3: Write `backend/src/ffh/ingest/lake.py`**

```python
"""Lake layout: partition paths and the never-overwrite Parquet writer (DATABASE.md §1)."""

import os
from datetime import UTC, datetime
from pathlib import Path

import polars as pl


class PartitionExistsError(FileExistsError):
    """Raised when a lake partition file already exists. A new scrape is a NEW partition."""


def scrape_date(now: datetime | None = None) -> str:
    """Today's UTC date as ``YYYY-MM-DD`` — the ``scrape_date=`` partition key."""
    return (now or datetime.now(UTC)).strftime("%Y-%m-%d")


def partition_path(lake_root: Path, source: str, asset: str, **keys: str | int) -> Path:
    """``<lake_root>/raw/<source>/<asset>/<k>=<v>/...`` in the insertion order of ``keys``.

    Hive-style so DuckDB can read the tree with ``hive_partitioning=1`` later.
    """
    path = Path(lake_root) / "raw" / source / asset
    for key, value in keys.items():
        path = path / f"{key}={value}"
    return path


def parquet_file(lake_root: Path, source: str, asset: str, **keys: str | int) -> Path:
    """The single Parquet file inside a partition directory."""
    return partition_path(lake_root, source, asset, **keys) / f"{asset}.parquet"


def write_parquet(df: pl.DataFrame, path: Path) -> int:
    """Write ``df`` to ``path``, refusing to overwrite. Returns the row count.

    Writes to a sibling ``.tmp`` first and then hard-links it into place: ``os.link``
    fails atomically if the target already exists, so a crash mid-write can never leave a
    partial file at the real partition path.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    df.write_parquet(tmp, compression="zstd")
    try:
        os.link(tmp, path)
    except FileExistsError as exc:
        raise PartitionExistsError(f"lake partition already exists: {path}") from exc
    finally:
        tmp.unlink(missing_ok=True)
    return df.height
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/ingest/test_lake.py -v`
Expected: 8 passed.

- [ ] **Step 5: Lint and commit**

```bash
cd backend && uv run ruff check . && uv run ruff format .
cd .. && git add backend/src/ffh/ingest/lake.py backend/tests/ingest/__init__.py backend/tests/ingest/test_lake.py
git commit -m "feat(ingest): lake partition paths and never-overwrite parquet writer"
```

---

### Task 2: `ffh.ingest.http` — shared client and retrying conditional GET

**Files:**
- Create: `backend/src/ffh/ingest/http.py`
- Test: `backend/tests/ingest/test_http.py`

**Interfaces:**
- Consumes: `ffh.__version__`.
- Produces:
  - `ffh.ingest.http.Fetched(content: bytes, etag: str | None, mtime: datetime | None)` — frozen dataclass.
  - `ffh.ingest.http.NotModified(etag: str | None)` — frozen dataclass.
  - `ffh.ingest.http.NotFound(url: str)` — frozen dataclass.
  - `ffh.ingest.http.FetchResult = Fetched | NotModified | NotFound` (type alias).
  - `ffh.ingest.http.RetryableStatus(Exception)` with `.status_code: int` and `.retry_after: float | None`.
  - `ffh.ingest.http.make_client(timeout: float = 60.0) -> httpx.Client`.
  - `ffh.ingest.http.get_bytes(client: httpx.Client, url: str, etag: str | None = None) -> FetchResult`.
  - `ffh.ingest.http.RETRYABLE_STATUSES: frozenset[int]`.
- Note for later tasks: the result dataclasses live **here**; `ffh.ingest.base` re-exports
  them so `from ffh.ingest.base import Fetched, NotModified, NotFound` also works.

- [ ] **Step 1: Write the failing test `backend/tests/ingest/test_http.py`**

```python
from datetime import UTC, datetime

import httpx
import pytest
import respx

from ffh.ingest.http import (
    Fetched,
    NotFound,
    NotModified,
    RetryableStatus,
    get_bytes,
    make_client,
)

URL = "https://example.invalid/asset.parquet"


def test_make_client_follows_redirects_and_sets_user_agent():
    with make_client() as client:
        assert client.follow_redirects is True
        assert client.headers["user-agent"].startswith("ffh/")


@respx.mock
def test_get_bytes_returns_fetched_with_etag_and_mtime():
    respx.get(URL).mock(
        return_value=httpx.Response(
            200,
            content=b"PAR1",
            headers={
                "ETag": '"v1"',
                "Last-Modified": "Sun, 16 Aug 2026 08:23:16 GMT",
            },
        )
    )
    with make_client() as client:
        result = get_bytes(client, URL)
    assert isinstance(result, Fetched)
    assert result.content == b"PAR1"
    assert result.etag == '"v1"'
    assert result.mtime == datetime(2026, 8, 16, 8, 23, 16, tzinfo=UTC)


@respx.mock
def test_get_bytes_sends_if_none_match_and_maps_304():
    route = respx.get(URL).mock(return_value=httpx.Response(304))
    with make_client() as client:
        result = get_bytes(client, URL, etag='"v1"')
    assert isinstance(result, NotModified)
    assert result.etag == '"v1"'
    assert route.calls.last.request.headers["if-none-match"] == '"v1"'


@respx.mock
def test_get_bytes_does_not_send_if_none_match_without_an_etag():
    route = respx.get(URL).mock(return_value=httpx.Response(200, content=b"x"))
    with make_client() as client:
        get_bytes(client, URL)
    assert "if-none-match" not in route.calls.last.request.headers


@respx.mock
def test_get_bytes_maps_404_to_notfound_without_retrying():
    route = respx.get(URL).mock(return_value=httpx.Response(404))
    with make_client() as client:
        result = get_bytes(client, URL)
    assert isinstance(result, NotFound)
    assert result.url == URL
    assert route.call_count == 1


@respx.mock
def test_get_bytes_retries_5xx_then_succeeds(monkeypatch):
    monkeypatch.setattr("ffh.ingest.http._RETRY_WAIT_CAP", 0.0)
    route = respx.get(URL).mock(
        side_effect=[
            httpx.Response(503),
            httpx.Response(502),
            httpx.Response(200, content=b"ok", headers={"ETag": '"v2"'}),
        ]
    )
    with make_client() as client:
        result = get_bytes(client, URL)
    assert isinstance(result, Fetched)
    assert result.content == b"ok"
    assert route.call_count == 3


@respx.mock
def test_get_bytes_gives_up_after_five_attempts(monkeypatch):
    monkeypatch.setattr("ffh.ingest.http._RETRY_WAIT_CAP", 0.0)
    route = respx.get(URL).mock(return_value=httpx.Response(429))
    with make_client() as client:
        with pytest.raises(RetryableStatus) as excinfo:
            get_bytes(client, URL)
    assert excinfo.value.status_code == 429
    assert route.call_count == 5


@respx.mock
def test_get_bytes_raises_on_unexpected_4xx():
    respx.get(URL).mock(return_value=httpx.Response(403))
    with make_client() as client:
        with pytest.raises(httpx.HTTPStatusError):
            get_bytes(client, URL)


@respx.mock
def test_retry_after_header_is_captured(monkeypatch):
    monkeypatch.setattr("ffh.ingest.http._RETRY_WAIT_CAP", 0.0)
    respx.get(URL).mock(
        side_effect=[
            httpx.Response(429, headers={"Retry-After": "2"}),
            httpx.Response(200, content=b"ok"),
        ]
    )
    with make_client() as client:
        assert isinstance(get_bytes(client, URL), Fetched)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/ingest/test_http.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'ffh.ingest.http'`.

- [ ] **Step 3: Write `backend/src/ffh/ingest/http.py`**

```python
"""The one HTTP client every ingest job uses. Conditional GET + tenacity backoff.

The ETag a server returns depends on the negotiated Content-Encoding, so a stored ETag is
only valid for a request made with the same client configuration. That is why there is
exactly one client factory and every job goes through it.
"""

from dataclasses import dataclass
from datetime import datetime
from email.utils import parsedate_to_datetime

import httpx
import structlog
from tenacity import RetryCallState, retry, retry_if_exception_type, stop_after_attempt
from tenacity.wait import wait_exponential

from ffh import __version__

log = structlog.get_logger(__name__)

RETRYABLE_STATUSES = frozenset({408, 425, 429, 500, 502, 503, 504})
MAX_ATTEMPTS = 5

# Multiplied into every computed wait; tests set it to 0.0 to keep the suite fast.
_RETRY_WAIT_CAP = 30.0


@dataclass(frozen=True, slots=True)
class Fetched:
    """The asset was downloaded."""

    content: bytes
    etag: str | None
    mtime: datetime | None


@dataclass(frozen=True, slots=True)
class NotModified:
    """The server answered 304 — the stored ETag is still current."""

    etag: str | None


@dataclass(frozen=True, slots=True)
class NotFound:
    """The asset does not exist yet (nflverse seasonal assets 404 before Week 1)."""

    url: str


type FetchResult = Fetched | NotModified | NotFound


class RetryableStatus(Exception):
    """A transient HTTP status worth retrying."""

    def __init__(self, status_code: int, url: str, retry_after: float | None = None) -> None:
        super().__init__(f"{status_code} from {url}")
        self.status_code = status_code
        self.url = url
        self.retry_after = retry_after


def make_client(timeout: float = 60.0) -> httpx.Client:
    """The shared client. GitHub Releases redirect to objects.githubusercontent.com."""
    return httpx.Client(
        follow_redirects=True,
        timeout=httpx.Timeout(timeout, connect=15.0),
        headers={"User-Agent": f"ffh/{__version__} (+https://github.com/nflverse)"},
    )


def _retry_after_seconds(response: httpx.Response) -> float | None:
    raw = response.headers.get("retry-after")
    if raw is None:
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def _wait(state: RetryCallState) -> float:
    exc = state.outcome.exception() if state.outcome is not None else None
    if isinstance(exc, RetryableStatus) and exc.retry_after is not None:
        return min(exc.retry_after, _RETRY_WAIT_CAP)
    base = wait_exponential(multiplier=1.0, min=1.0, max=30.0)(state)
    return min(base, _RETRY_WAIT_CAP)


@retry(
    retry=retry_if_exception_type((RetryableStatus, httpx.TransportError)),
    wait=_wait,
    stop=stop_after_attempt(MAX_ATTEMPTS),
    reraise=True,
)
def _request(client: httpx.Client, url: str, headers: dict[str, str]) -> httpx.Response:
    response = client.get(url, headers=headers)
    if response.status_code in RETRYABLE_STATUSES:
        raise RetryableStatus(response.status_code, url, _retry_after_seconds(response))
    return response


def get_bytes(client: httpx.Client, url: str, etag: str | None = None) -> FetchResult:
    """Conditional GET. 304 -> NotModified, 404 -> NotFound, 2xx -> Fetched.

    Any other 4xx/5xx raises. Retryable statuses are retried up to MAX_ATTEMPTS with
    exponential backoff, honouring a numeric ``Retry-After`` when the server sends one.
    """
    headers = {"If-None-Match": etag} if etag else {}
    response = _request(client, url, headers)

    if response.status_code == 304:
        log.info("ingest.http.not_modified", url=url)
        return NotModified(etag=response.headers.get("etag") or etag)
    if response.status_code == 404:
        log.info("ingest.http.not_found", url=url)
        return NotFound(url=url)
    response.raise_for_status()

    last_modified = response.headers.get("last-modified")
    mtime = parsedate_to_datetime(last_modified) if last_modified else None
    log.info("ingest.http.fetched", url=url, bytes=len(response.content))
    return Fetched(content=response.content, etag=response.headers.get("etag"), mtime=mtime)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/ingest/test_http.py -v`
Expected: 9 passed.

- [ ] **Step 5: Lint and commit**

```bash
cd backend && uv run ruff check . && uv run ruff format .
cd .. && git add backend/src/ffh/ingest/http.py backend/tests/ingest/test_http.py
git commit -m "feat(ingest): shared httpx client with tenacity retry and conditional GET"
```

---

### Task 3: `ffh.ingest.base` — `IngestJob`, the registry, and the `ingest_runs` lifecycle

**Files:**
- Create: `backend/src/ffh/ingest/base.py`
- Test: `backend/tests/ingest/test_base.py`

**Interfaces:**
- Consumes: `ffh.ingest.http.{Fetched, NotModified, NotFound, FetchResult, make_client, get_bytes}`; `ffh.ingest.lake.{parquet_file, write_parquet, PartitionExistsError, scrape_date}`; `ffh.db.models.IngestRun`.
- Produces:
  - `ffh.ingest.base.IngestValidationError(ValueError)`.
  - `ffh.ingest.base.IngestRunResult(status: str, rows_written: int | None = None, output_path: str | None = None, error: str | None = None, run_id: uuid.UUID | None = None)` — frozen dataclass.
  - Status constants `STATUS_RUNNING = "running"`, `STATUS_SUCCESS = "success"`, `STATUS_FAILED = "failed"`, `STATUS_NOT_MODIFIED = "skipped_not_modified"`, `STATUS_SKIPPED = "skipped"`.
  - `ffh.ingest.base.IngestJob` (ABC) with ClassVars `name: str`, `source: str`, `asset: str`, `seasonal: bool = False`, `season_scoped: bool = False`, `skip_on_404: bool = False`, `REQUIRED_COLUMNS: frozenset[str] = frozenset()`; `__init__(self, season: int | None = None)`; abstract `partition() -> dict[str, str]`, `fetch(etag: str | None) -> FetchResult`, `parse(content: bytes) -> pl.DataFrame`; concrete `validate(df: pl.DataFrame) -> None`, `persist(session: Session, df: pl.DataFrame) -> None`, `run(session: Session, lake_root: Path) -> IngestRunResult`.
  - `ffh.ingest.base.HttpIngestJob(IngestJob)` — adds abstract `url() -> str` and a concrete `fetch()` built on `make_client`/`get_bytes`.
  - `ffh.ingest.base.JOBS: dict[str, type[IngestJob]]`, `register(cls: type[IngestJob]) -> type[IngestJob]` (decorator), `get_job(name: str) -> type[IngestJob]` (raises `KeyError`).
  - `ffh.ingest.base.last_successful_etag(session, source, asset, season) -> str | None`.
  - Re-exports `Fetched`, `NotModified`, `NotFound` from `ffh.ingest.http`.

**`run()` lifecycle — the exact order, because two tests pin it:**

1. Reject a seasonal job with `season is None`.
2. Compute `path = parquet_file(lake_root, source, asset, **self.partition())`.
3. Insert an `ingest_runs` row with `status='running'`, `source`, `asset`, `season` and **commit** (so a crash still leaves the attempt on record).
4. `etag = last_successful_etag(...)`; `result = self.fetch(etag)`.
5. `NotModified` → finish `skipped_not_modified` (store the etag; no file written).
6. `NotFound` → `skipped` if `skip_on_404` else finish `failed`.
7. `df = self.parse(result.content)`; `self.validate(df)`.
8. `write_parquet(df, path)`; a `PartitionExistsError` finishes `skipped` (today's partition is already on disk — never overwrite), **not** `failed`.
9. `self.persist(session, df)`.
10. Finish `success` with `rows_written`, `output_path`, `source_etag`, `source_mtime`.
11. Any other exception finishes `failed` with `error = repr(exc)` (truncated to 2000 chars) after `session.rollback()`. `run()` never re-raises — the CLI decides the exit code from `result.status`.

The 304 check precedes the partition-exists check deliberately: the required idempotency
test asserts the second run reports `skipped_not_modified`.

- [ ] **Step 1: Write the failing test `backend/tests/ingest/test_base.py`**

```python
from pathlib import Path

import polars as pl
import pytest
from sqlalchemy import select

from ffh.db.models import IngestRun
from ffh.ingest.base import (
    JOBS,
    Fetched,
    IngestJob,
    IngestValidationError,
    NotFound,
    NotModified,
    get_job,
    last_successful_etag,
    register,
)

pytestmark = pytest.mark.db

FRAME = pl.DataFrame({"gsis_id": ["00-0034796"], "position": ["WR"]})


class FakeJob(IngestJob):
    """A job whose fetch() is scripted by the test — no network, no respx."""

    name = "fake_job"
    source = "fake"
    asset = "thing"
    REQUIRED_COLUMNS = frozenset({"gsis_id", "position"})

    def __init__(self, season=None, script=None, frame=FRAME):
        super().__init__(season=season)
        self.script = list(script or [])
        self.frame = frame
        self.seen_etags: list[str | None] = []

    def partition(self):
        return {"scrape_date": "2026-08-16"}

    def fetch(self, etag):
        self.seen_etags.append(etag)
        return self.script.pop(0)

    def parse(self, content):
        return self.frame


def _runs(session, source="fake"):
    return list(
        session.scalars(
            select(IngestRun).where(IngestRun.source == source).order_by(IngestRun.started_at)
        )
    )


def test_register_and_get_job_round_trip():
    @register
    class Registered(FakeJob):
        name = "registered_job"

    assert JOBS["registered_job"] is Registered
    assert get_job("registered_job") is Registered
    with pytest.raises(KeyError):
        get_job("no_such_job")


def test_run_success_writes_partition_and_ingest_run(db_session, tmp_path: Path):
    job = FakeJob(script=[Fetched(content=b"x", etag='"v1"', mtime=None)])
    result = job.run(db_session, tmp_path)

    assert result.status == "success"
    assert result.rows_written == 1
    path = Path(result.output_path)
    assert path.exists() and pl.read_parquet(path).height == 1

    (run,) = _runs(db_session)
    assert (run.status, run.rows_written, run.source_etag) == ("success", 1, '"v1"')
    assert run.output_path == str(path)
    assert run.finished_at is not None and run.error is None


def test_second_run_sends_if_none_match_and_is_skipped_not_modified(db_session, tmp_path: Path):
    first = FakeJob(script=[Fetched(content=b"x", etag='"v1"', mtime=None)])
    first.run(db_session, tmp_path)

    second = FakeJob(script=[NotModified(etag='"v1"')])
    result = second.run(db_session, tmp_path)

    assert second.seen_etags == ['"v1"']
    assert result.status == "skipped_not_modified"
    assert result.rows_written is None and result.output_path is None

    runs = _runs(db_session)
    assert [r.status for r in runs] == ["success", "skipped_not_modified"]
    files = list((tmp_path / "raw" / "fake" / "thing").rglob("*.parquet"))
    assert len(files) == 1


def test_404_is_skipped_when_skip_on_404(db_session, tmp_path: Path):
    class Seasonal404(FakeJob):
        name = "seasonal_404"
        skip_on_404 = True

    job = Seasonal404(script=[NotFound(url="https://example.invalid/x.parquet")])
    result = job.run(db_session, tmp_path)
    assert result.status == "skipped"
    (run,) = _runs(db_session)
    assert run.status == "skipped"
    assert "404" in run.error


def test_404_is_failed_when_not_skip_on_404(db_session, tmp_path: Path):
    job = FakeJob(script=[NotFound(url="https://example.invalid/x.parquet")])
    assert job.run(db_session, tmp_path).status == "failed"


def test_validate_failure_records_failed_with_error_text(db_session, tmp_path: Path):
    bad = pl.DataFrame({"gsis_id": ["00-0034796"]})  # missing `position`
    job = FakeJob(script=[Fetched(content=b"x", etag=None, mtime=None)], frame=bad)
    result = job.run(db_session, tmp_path)

    assert result.status == "failed"
    assert "position" in result.error
    (run,) = _runs(db_session)
    assert run.status == "failed" and "position" in run.error
    assert not list(tmp_path.rglob("*.parquet"))


def test_validate_rejects_empty_frame(db_session, tmp_path: Path):
    empty = pl.DataFrame({"gsis_id": [], "position": []})
    job = FakeJob(script=[Fetched(content=b"x", etag=None, mtime=None)], frame=empty)
    result = job.run(db_session, tmp_path)
    assert result.status == "failed"
    assert "0 rows" in result.error


def test_existing_partition_without_304_is_skipped_not_failed(db_session, tmp_path: Path):
    FakeJob(script=[Fetched(content=b"x", etag=None, mtime=None)]).run(db_session, tmp_path)
    again = FakeJob(script=[Fetched(content=b"x", etag=None, mtime=None)])
    result = again.run(db_session, tmp_path)
    assert result.status == "skipped"
    assert "already exists" in result.error
    assert len(list(tmp_path.rglob("*.parquet"))) == 1


def test_seasonal_job_without_season_fails_fast(db_session, tmp_path: Path):
    class Seasonal(FakeJob):
        name = "seasonal_job"
        seasonal = True
        season_scoped = True

    with pytest.raises(ValueError, match="requires --season"):
        Seasonal(season=None).run(db_session, tmp_path)


def test_last_successful_etag_is_scoped_to_source_asset_season(db_session, tmp_path: Path):
    FakeJob(script=[Fetched(content=b"x", etag='"v1"', mtime=None)]).run(db_session, tmp_path)
    assert last_successful_etag(db_session, "fake", "thing", None) == '"v1"'
    assert last_successful_etag(db_session, "fake", "other", None) is None
    assert last_successful_etag(db_session, "fake", "thing", 2026) is None


def test_validate_is_called_and_persist_receives_the_frame(db_session, tmp_path: Path):
    seen: list[pl.DataFrame] = []

    class Persisting(FakeJob):
        name = "persisting_job"

        def persist(self, session, df):
            seen.append(df)

    Persisting(script=[Fetched(content=b"x", etag=None, mtime=None)]).run(db_session, tmp_path)
    assert len(seen) == 1 and seen[0].height == 1


def test_ingest_validation_error_is_a_value_error():
    assert issubclass(IngestValidationError, ValueError)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/ingest/test_base.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'ffh.ingest.base'`.
(If Postgres is not running, start it first: `docker compose up -d postgres`.)

- [ ] **Step 3: Write `backend/src/ffh/ingest/base.py`**

```python
"""The ingest contract: fetch -> validate -> land, wrapped in an ``ingest_runs`` row.

ARCHITECTURE.md: ingest is idempotent and watermarked, and holds no business logic.
DATABASE.md §7: every invocation writes exactly one ``ingest_runs`` row.
"""

import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import ClassVar

import polars as pl
import structlog
from sqlalchemy import select
from sqlalchemy.orm import Session

from ffh.db.models import IngestRun
from ffh.ingest.http import (
    Fetched,
    FetchResult,
    NotFound,
    NotModified,
    get_bytes,
    make_client,
)
from ffh.ingest.lake import PartitionExistsError, parquet_file, write_parquet

__all__ = [
    "JOBS",
    "STATUS_FAILED",
    "STATUS_NOT_MODIFIED",
    "STATUS_RUNNING",
    "STATUS_SKIPPED",
    "STATUS_SUCCESS",
    "Fetched",
    "HttpIngestJob",
    "IngestJob",
    "IngestRunResult",
    "IngestValidationError",
    "NotFound",
    "NotModified",
    "get_job",
    "last_successful_etag",
    "register",
]

log = structlog.get_logger(__name__)

STATUS_RUNNING = "running"
STATUS_SUCCESS = "success"
STATUS_FAILED = "failed"
STATUS_NOT_MODIFIED = "skipped_not_modified"
STATUS_SKIPPED = "skipped"

_MAX_ERROR_CHARS = 2000


class IngestValidationError(ValueError):
    """A fetched frame failed its job's ``validate()`` contract."""


@dataclass(frozen=True, slots=True)
class IngestRunResult:
    status: str
    rows_written: int | None = None
    output_path: str | None = None
    error: str | None = None
    run_id: uuid.UUID | None = None


class IngestJob(ABC):
    """One asset, one lifecycle. Subclasses supply the URL, the parser and the columns."""

    name: ClassVar[str]
    source: ClassVar[str]
    asset: ClassVar[str]
    #: season appears in the URL and in the lake partition
    seasonal: ClassVar[bool] = False
    #: season is recorded on ingest_runs and available to persist()
    season_scoped: ClassVar[bool] = False
    #: a 404 means "not published yet", not "broken"
    skip_on_404: ClassVar[bool] = False
    REQUIRED_COLUMNS: ClassVar[frozenset[str]] = frozenset()

    def __init__(self, season: int | None = None) -> None:
        uses_season = type(self).seasonal or type(self).season_scoped
        self.season: int | None = season if uses_season else None

    # --- contract -------------------------------------------------------------------

    @abstractmethod
    def partition(self) -> dict[str, str]:
        """Hive partition keys for this run, in path order."""

    @abstractmethod
    def fetch(self, etag: str | None) -> FetchResult:
        """Conditional GET of the asset."""

    @abstractmethod
    def parse(self, content: bytes) -> pl.DataFrame:
        """Raw bytes -> Polars frame. No renaming, no business logic."""

    def validate(self, df: pl.DataFrame) -> None:
        """Assert required columns and a non-empty frame. Override to add checks."""
        missing = sorted(type(self).REQUIRED_COLUMNS - set(df.columns))
        if missing:
            raise IngestValidationError(
                f"{type(self).name}: missing required columns {missing}"
            )
        if df.height == 0:
            raise IngestValidationError(f"{type(self).name}: fetched 0 rows")

    def persist(self, session: Session, df: pl.DataFrame) -> None:
        """Optional Postgres side effect. Default: the lake is the only landing zone."""
        return None

    # --- lifecycle ------------------------------------------------------------------

    def run(self, session: Session, lake_root: Path) -> IngestRunResult:
        cls = type(self)
        if cls.seasonal and self.season is None:
            raise ValueError(f"{cls.name} is seasonal and requires --season")

        path = parquet_file(lake_root, cls.source, cls.asset, **self.partition())
        run = IngestRun(
            source=cls.source, asset=cls.asset, season=self.season, status=STATUS_RUNNING
        )
        session.add(run)
        session.commit()
        log.info("ingest.run.started", job=cls.name, season=self.season, path=str(path))

        try:
            etag = last_successful_etag(session, cls.source, cls.asset, self.season)
            result = self.fetch(etag)

            if isinstance(result, NotModified):
                return self._finish(session, run, STATUS_NOT_MODIFIED, source_etag=result.etag)

            if isinstance(result, NotFound):
                message = f"404 Not Found: {result.url}"
                status = STATUS_SKIPPED if cls.skip_on_404 else STATUS_FAILED
                return self._finish(session, run, status, error=message)

            df = self.parse(result.content)
            self.validate(df)

            try:
                rows = write_parquet(df, path)
            except PartitionExistsError as exc:
                return self._finish(session, run, STATUS_SKIPPED, error=str(exc))

            self.persist(session, df)
            return self._finish(
                session,
                run,
                STATUS_SUCCESS,
                rows_written=rows,
                output_path=str(path),
                source_etag=result.etag,
                source_mtime=result.mtime,
            )
        except Exception as exc:  # noqa: BLE001 - the status field is the error channel
            session.rollback()
            session.add(run)
            log.exception("ingest.run.failed", job=cls.name)
            return self._finish(session, run, STATUS_FAILED, error=repr(exc))

    def _finish(
        self,
        session: Session,
        run: IngestRun,
        status: str,
        *,
        rows_written: int | None = None,
        output_path: str | None = None,
        error: str | None = None,
        source_etag: str | None = None,
        source_mtime: datetime | None = None,
    ) -> IngestRunResult:
        run.status = status
        run.finished_at = datetime.now(UTC)
        run.rows_written = rows_written
        run.output_path = output_path
        run.error = error[:_MAX_ERROR_CHARS] if error else None
        run.source_etag = source_etag
        run.source_mtime = source_mtime
        session.commit()
        log.info(
            "ingest.run.finished",
            job=type(self).name,
            status=status,
            rows=rows_written,
            path=output_path,
        )
        return IngestRunResult(
            status=status,
            rows_written=rows_written,
            output_path=output_path,
            error=run.error,
            run_id=run.run_id,
        )


class HttpIngestJob(IngestJob):
    """An ``IngestJob`` whose asset is one public HTTPS URL."""

    @abstractmethod
    def url(self) -> str:
        """The absolute URL for this run (may embed ``self.season``)."""

    def fetch(self, etag: str | None) -> FetchResult:
        with make_client() as client:
            return get_bytes(client, self.url(), etag)


# --- registry -----------------------------------------------------------------------

JOBS: dict[str, type[IngestJob]] = {}


def register(cls: type[IngestJob]) -> type[IngestJob]:
    """Class decorator: make ``cls`` dispatchable as ``ffh ingest run <cls.name>``."""
    if cls.name in JOBS and JOBS[cls.name] is not cls:
        raise ValueError(f"duplicate ingest job name: {cls.name}")
    JOBS[cls.name] = cls
    return cls


def get_job(name: str) -> type[IngestJob]:
    """Look up a registered job. Raises ``KeyError`` with the known names."""
    try:
        return JOBS[name]
    except KeyError as exc:
        raise KeyError(f"unknown ingest job {name!r}; known: {sorted(JOBS)}") from exc


def last_successful_etag(
    session: Session, source: str, asset: str, season: int | None
) -> str | None:
    """The ETag of the newest successful run for this (source, asset, season)."""
    stmt = (
        select(IngestRun.source_etag)
        .where(
            IngestRun.source == source,
            IngestRun.asset == asset,
            IngestRun.season.is_not_distinct_from(season),
            IngestRun.status == STATUS_SUCCESS,
            IngestRun.source_etag.is_not(None),
        )
        .order_by(IngestRun.started_at.desc())
        .limit(1)
    )
    return session.scalars(stmt).first()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/ingest/test_base.py -v`
Expected: 12 passed.
If `IngestRun.season.is_not_distinct_from` is unavailable in this SQLAlchemy version,
replace that clause with
`(IngestRun.season == season) if season is not None else IngestRun.season.is_(None)`
and re-run.

- [ ] **Step 5: Lint and commit**

```bash
cd backend && uv run ruff check . && uv run ruff format .
cd .. && git add backend/src/ffh/ingest/base.py backend/tests/ingest/test_base.py
git commit -m "feat(ingest): IngestJob ABC, job registry, and ingest_runs lifecycle"
```

---

### Task 4: `ffh.ingest.nflverse` — the six nflverse Parquet jobs

**Files:**
- Create: `backend/src/ffh/ingest/nflverse.py`
- Test: `backend/tests/ingest/test_nflverse.py`

**Interfaces:**
- Consumes: `ffh.ingest.base.{HttpIngestJob, register, IngestValidationError}`; `ffh.ingest.lake.scrape_date`.
- Produces:
  - `ffh.ingest.nflverse.NFLVERSE_RELEASE_BASE: str`.
  - `ffh.ingest.nflverse.NflverseParquetJob(HttpIngestJob)` with ClassVars `release: str`, `filename: str` (a `str.format` template taking `season`), and concrete `url()`, `partition()`, `parse()`.
  - Registered job classes: `NflversePlayersJob` (`nflverse_players`), `NflverseStatsPlayerWeekJob` (`nflverse_stats_player_week`), `NflverseSnapCountsJob` (`nflverse_snap_counts`), `NflverseDepthChartsJob` (`nflverse_depth_charts`), `NflverseInjuriesJob` (`nflverse_injuries`), `NflversePbpJob` (`nflverse_pbp`).
  - Each exposes `REQUIRED_COLUMNS: frozenset[str]` (verified live 2026-08-16 — see the verification log).

- [ ] **Step 1: Write the failing test `backend/tests/ingest/test_nflverse.py`**

```python
import io

import polars as pl
import pytest

from ffh.ingest.base import JOBS, IngestValidationError
from ffh.ingest.lake import scrape_date
from ffh.ingest.nflverse import (
    NflverseDepthChartsJob,
    NflverseInjuriesJob,
    NflversePbpJob,
    NflversePlayersJob,
    NflverseSnapCountsJob,
    NflverseStatsPlayerWeekJob,
)

SEASONAL = [
    NflverseStatsPlayerWeekJob,
    NflverseSnapCountsJob,
    NflverseDepthChartsJob,
    NflverseInjuriesJob,
    NflversePbpJob,
]


def test_all_six_jobs_are_registered():
    assert {
        "nflverse_players",
        "nflverse_stats_player_week",
        "nflverse_snap_counts",
        "nflverse_depth_charts",
        "nflverse_injuries",
        "nflverse_pbp",
    } <= set(JOBS)


def test_urls_match_the_verified_nflverse_release_paths():
    assert NflversePlayersJob().url() == (
        "https://github.com/nflverse/nflverse-data/releases/download/players/players.parquet"
    )
    assert NflverseStatsPlayerWeekJob(season=2026).url() == (
        "https://github.com/nflverse/nflverse-data/releases/download/"
        "stats_player/stats_player_week_2026.parquet"
    )
    assert NflverseSnapCountsJob(season=2026).url().endswith("snap_counts/snap_counts_2026.parquet")
    assert NflverseDepthChartsJob(season=2026).url().endswith(
        "depth_charts/depth_charts_2026.parquet"
    )
    assert NflverseInjuriesJob(season=2026).url().endswith("injuries/injuries_2026.parquet")
    assert NflversePbpJob(season=2026).url().endswith("pbp/play_by_play_2026.parquet")


def test_stats_player_week_uses_the_live_asset_not_the_frozen_one():
    url = NflverseStatsPlayerWeekJob(season=2026).url()
    assert "stats_player/stats_player_week_" in url
    assert "player_stats" not in url  # frozen at 2025-05-07 (DATA_SOURCES.md warning 2)


@pytest.mark.parametrize("cls", SEASONAL)
def test_every_seasonal_job_skips_on_404(cls):
    assert cls.seasonal is True
    assert cls.season_scoped is True
    assert cls.skip_on_404 is True, "seasonal assets 404 before Week 1 (verified 2026-08-16)"


def test_players_job_is_not_seasonal_and_does_not_skip_on_404():
    assert NflversePlayersJob.seasonal is False
    assert NflversePlayersJob.skip_on_404 is False


def test_partitions_are_hive_keys_in_path_order():
    assert NflversePlayersJob().partition() == {"scrape_date": scrape_date()}
    assert NflverseInjuriesJob(season=2026).partition() == {
        "season": "2026",
        "scrape_date": scrape_date(),
    }


def test_parse_reads_parquet_bytes():
    buf = io.BytesIO()
    pl.DataFrame({"gsis_id": ["00-0034796"]}).write_parquet(buf)
    df = NflversePlayersJob().parse(buf.getvalue())
    assert df["gsis_id"].to_list() == ["00-0034796"]


def _frame(cls) -> pl.DataFrame:
    return pl.DataFrame({c: ["x"] for c in sorted(cls.REQUIRED_COLUMNS)})


@pytest.mark.parametrize(
    "cls",
    [
        NflversePlayersJob,
        NflverseStatsPlayerWeekJob,
        NflverseSnapCountsJob,
        NflverseDepthChartsJob,
        NflverseInjuriesJob,
        NflversePbpJob,
    ],
)
def test_validate_accepts_a_frame_with_exactly_the_required_columns(cls):
    cls(season=2026).validate(_frame(cls))


@pytest.mark.parametrize(
    "cls",
    [
        NflversePlayersJob,
        NflverseStatsPlayerWeekJob,
        NflverseSnapCountsJob,
        NflverseDepthChartsJob,
        NflverseInjuriesJob,
        NflversePbpJob,
    ],
)
def test_validate_rejects_a_frame_missing_one_required_column(cls):
    df = _frame(cls)
    dropped = sorted(cls.REQUIRED_COLUMNS)[0]
    with pytest.raises(IngestValidationError, match=dropped):
        cls(season=2026).validate(df.drop(dropped))


def test_stats_player_week_keys_on_player_id_not_gsis_id():
    # verified 2026-08-16: the 150-column asset has `player_id` (a GSIS id), no `gsis_id`
    assert "player_id" in NflverseStatsPlayerWeekJob.REQUIRED_COLUMNS
    assert "gsis_id" not in NflverseStatsPlayerWeekJob.REQUIRED_COLUMNS


def test_snap_counts_keys_on_pfr_player_id():
    # verified 2026-08-16: snap_counts carries no GSIS id at all
    assert "pfr_player_id" in NflverseSnapCountsJob.REQUIRED_COLUMNS
    assert "gsis_id" not in NflverseSnapCountsJob.REQUIRED_COLUMNS


def test_players_job_uses_nflverse_field_names_not_table_names():
    required = NflversePlayersJob.REQUIRED_COLUMNS
    assert {"display_name", "college_name", "rookie_season", "height", "weight"} <= required
    assert not ({"full_name", "college", "rookie_year", "height_in", "weight_lb"} & required)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/ingest/test_nflverse.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'ffh.ingest.nflverse'`.

- [ ] **Step 3: Write `backend/src/ffh/ingest/nflverse.py`**

```python
"""nflverse release Parquet jobs (DATA_SOURCES.md §1).

No API key, no rate limit. Read the release Parquet URLs directly with httpx + Polars —
`nflreadpy` is deliberately not a dependency and `nfl_data_py` is archived.

REQUIRED_COLUMNS were verified live on 2026-08-16 against:
  players.parquet (39 cols) · stats_player_week_2025.parquet (150) ·
  snap_counts_2025.parquet (16) · depth_charts_2026.parquet (12) ·
  injuries_2025.parquet (16) · play_by_play_2025.parquet (372)
Re-verify with `uv run python scripts/record_nflverse_fixtures.py` before changing them.
"""

import io
from typing import ClassVar

import polars as pl

from ffh.ingest.base import HttpIngestJob, register
from ffh.ingest.lake import scrape_date

NFLVERSE_RELEASE_BASE = "https://github.com/nflverse/nflverse-data/releases/download"


class NflverseParquetJob(HttpIngestJob):
    """One nflverse release asset. Lands the file verbatim — renaming belongs to PR ④."""

    source: ClassVar[str] = "nflverse"
    release: ClassVar[str]
    #: str.format template; seasonal assets use {season}
    filename: ClassVar[str]

    def url(self) -> str:
        return f"{NFLVERSE_RELEASE_BASE}/{self.release}/{self.filename.format(season=self.season)}"

    def partition(self) -> dict[str, str]:
        if type(self).seasonal:
            return {"season": str(self.season), "scrape_date": scrape_date()}
        return {"scrape_date": scrape_date()}

    def parse(self, content: bytes) -> pl.DataFrame:
        return pl.read_parquet(io.BytesIO(content))


class SeasonalNflverseJob(NflverseParquetJob):
    """Per-season assets. Verified 2026-08-16: these 404 before Week 1, so 404 -> skipped."""

    seasonal: ClassVar[bool] = True
    season_scoped: ClassVar[bool] = True
    skip_on_404: ClassVar[bool] = True


@register
class NflversePlayersJob(NflverseParquetJob):
    """Canonical player registry. Published daily, year-round — a 404 here is a real fault."""

    name = "nflverse_players"
    asset = "players"
    release = "players"
    filename = "players.parquet"
    REQUIRED_COLUMNS = frozenset(
        {
            "gsis_id",
            "display_name",
            "first_name",
            "last_name",
            "position",
            "position_group",
            "birth_date",
            "college_name",
            "height",
            "weight",
            "status",
            "rookie_season",
            "latest_team",
            "espn_id",
            "pfr_id",
        }
    )


@register
class NflverseStatsPlayerWeekJob(SeasonalNflverseJob):
    """150-column weekly stats. NOTE the asset path: `player_stats/` is frozen since 2025."""

    name = "nflverse_stats_player_week"
    asset = "stats_player_week"
    release = "stats_player"
    filename = "stats_player_week_{season}.parquet"
    REQUIRED_COLUMNS = frozenset(
        {
            "player_id",  # this IS the GSIS id; there is no `gsis_id` column
            "player_display_name",
            "position",
            "season",
            "week",
            "season_type",
            "game_id",
            "team",
            "opponent_team",
            "completions",
            "attempts",
            "passing_yards",
            "passing_tds",
            "passing_interceptions",
            "passing_epa",
            "carries",
            "rushing_yards",
            "rushing_tds",
            "rushing_epa",
            "receptions",
            "targets",
            "receiving_yards",
            "receiving_tds",
            "receiving_air_yards",
            "receiving_epa",
            "target_share",
            "air_yards_share",
            "wopr",
            "racr",
            "fantasy_points",
            "fantasy_points_ppr",
        }
    )


@register
class NflverseSnapCountsJob(SeasonalNflverseJob):
    """PFR snap counts — the in-season route-participation proxy (DATA_SOURCES.md §1)."""

    name = "nflverse_snap_counts"
    asset = "snap_counts"
    release = "snap_counts"
    filename = "snap_counts_{season}.parquet"
    REQUIRED_COLUMNS = frozenset(
        {
            "game_id",
            "pfr_game_id",
            "season",
            "game_type",
            "week",
            "player",
            "pfr_player_id",  # the only player key here — no GSIS id in this asset
            "position",
            "team",
            "opponent",
            "offense_snaps",
            "offense_pct",
            "defense_snaps",
            "defense_pct",
            "st_snaps",
            "st_pct",
        }
    )


@register
class NflverseDepthChartsJob(SeasonalNflverseJob):
    """Daily snapshots, not current state. `dt` is a String ISO-8601 UTC stamp."""

    name = "nflverse_depth_charts"
    asset = "depth_charts"
    release = "depth_charts"
    filename = "depth_charts_{season}.parquet"
    REQUIRED_COLUMNS = frozenset(
        {
            "dt",
            "team",
            "player_name",
            "espn_id",
            "gsis_id",
            "pos_grp",
            "pos_abb",
            "pos_name",
            "pos_slot",
            "pos_rank",
        }
    )


@register
class NflverseInjuriesJob(SeasonalNflverseJob):
    """Historical practice participation. 2025+ dropped `date_modified` — do not require it."""

    name = "nflverse_injuries"
    asset = "injuries"
    release = "injuries"
    filename = "injuries_{season}.parquet"
    REQUIRED_COLUMNS = frozenset(
        {
            "season",
            "season_type",
            "game_type",
            "team",
            "week",
            "gsis_id",
            "position",
            "full_name",
            "first_name",
            "last_name",
            "report_primary_injury",
            "report_secondary_injury",
            "report_status",
            "practice_primary_injury",
            "practice_secondary_injury",
            "practice_status",
        }
    )


@register
class NflversePbpJob(SeasonalNflverseJob):
    """Full play-by-play. 404s until Week 1 (verified 2026-08-16) -> `skipped`."""

    name = "nflverse_pbp"
    asset = "pbp"
    release = "pbp"
    filename = "play_by_play_{season}.parquet"
    REQUIRED_COLUMNS = frozenset(
        {
            "play_id",
            "game_id",
            "season",
            "week",
            "season_type",
            "posteam",
            "defteam",
            "play_type",
            "desc",
            "down",
            "ydstogo",
            "qtr",
            "yardline_100",
            "game_seconds_remaining",
            "yards_gained",
            "air_yards",
            "epa",
            "wp",
            "success",
            "pass_attempt",
            "rush_attempt",
            "complete_pass",
            "touchdown",
            "passer_player_id",
            "rusher_player_id",
            "receiver_player_id",
        }
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/ingest/test_nflverse.py -v`
Expected: 22 passed.

- [ ] **Step 5: Lint and commit**

```bash
cd backend && uv run ruff check . && uv run ruff format .
cd .. && git add backend/src/ffh/ingest/nflverse.py backend/tests/ingest/test_nflverse.py
git commit -m "feat(ingest): six nflverse parquet jobs with live-verified required columns"
```

---

### Task 5: `ffh.ingest.games` — `nfldata_games` job and `upsert_games`

**Files:**
- Create: `backend/src/ffh/ingest/games.py`
- Test: `backend/tests/ingest/test_games.py`

**Interfaces:**
- Consumes: `ffh.ingest.base.{HttpIngestJob, register}`; `ffh.ingest.lake.scrape_date`; `ffh.ingest.reference.{assert_team_coverage, assert_stadium_coverage}` (Task 6 — implement Task 6 first if you are executing out of order); `ffh.db.models.Game`; `ffh.config.get_settings`.
- Produces:
  - `ffh.ingest.games.GAMES_CSV_URL: str`.
  - `ffh.ingest.games.NfldataGamesJob(HttpIngestJob)` — `name = "nfldata_games"`, `source = "nfldata"`, `asset = "games"`, `seasonal = False`, `season_scoped = True`, `skip_on_404 = False`.
  - `ffh.ingest.games.to_game_rows(df: pl.DataFrame, season: int) -> pl.DataFrame` — games.csv columns mapped to `games` columns for one season.
  - `ffh.ingest.games.upsert_games(session: Session, df: pl.DataFrame, season: int) -> int` — takes the **raw** games.csv frame, returns rows upserted.

**Column mapping (`games.csv` → `games`), every rule verified live 2026-08-16:**

| `games` column | expression |
|---|---|
| `game_id` | `game_id` |
| `season`, `week` | `season`, `week` cast to `Int16` |
| `season_type` | `'REG'` when `game_type == 'REG'` else `'POST'` |
| `kickoff_at` | `gameday + ' ' + gametime` parsed `%Y-%m-%d %H:%M`, `.dt.replace_time_zone("America/New_York", ambiguous="earliest", non_existent="raise")`, then `.dt.convert_time_zone("UTC")` |
| `home_team`, `away_team` | verbatim (nflverse abbreviations) |
| `stadium_id` | verbatim — **nominal home stadium for neutral-site games** |
| `spread_line`, `total_line` | verbatim `Float64` |
| `home_moneyline`, `away_moneyline` | verbatim `Int64` |
| `roof` | `roof` with the literal `""` mapped to NULL |
| `surface` | `surface` with `""` mapped to NULL |
| `div_game` | `div_game == 1` |
| `home_rest`, `away_rest` | `Int16` |
| `neutral_site` | `location == 'Neutral'` |
| `home_score`, `away_score` | `Int16`, NULL until played |
| `temp_f`, `wind_mph` | `temp`, `wind` cast `Float64`, NULL until played |
| `updated_at` | **`func.now()` in the `ON CONFLICT … SET`** — ORM `onupdate` does not fire |

- [ ] **Step 1: Write the failing test `backend/tests/ingest/test_games.py`**

```python
from datetime import UTC, datetime

import polars as pl
import pytest
from sqlalchemy import select

from ffh.db.models import Game
from ffh.ingest.games import GAMES_CSV_URL, NfldataGamesJob, to_game_rows, upsert_games
from ffh.ingest.reference import seed_nfl_teams, seed_stadiums

pytestmark = pytest.mark.db

# Real rows copied verbatim from games.csv on 2026-08-16, including the quoted-empty roof.
GAMES_CSV = (
    "game_id,season,game_type,week,gameday,weekday,gametime,away_team,away_score,home_team,"
    "home_score,location,result,total,overtime,old_game_id,gsis,nfl_detail_id,pfr,pff,espn,"
    "ftn,away_rest,home_rest,away_moneyline,home_moneyline,spread_line,away_spread_odds,"
    "home_spread_odds,total_line,under_odds,over_odds,div_game,roof,surface,temp,wind,"
    "away_qb_id,home_qb_id,away_qb_name,home_qb_name,away_coach,home_coach,referee,stadium_id,"
    "stadium\n"
    "2026_01_NE_SEA,2026,REG,1,2026-09-09,Wednesday,20:20,NE,,SEA,,Home,,,,2026090900,,,"
    "202609090sea,,401872656,,7,7,154,-185,3.5,-110,-110,44.5,-110,-110,0,outdoors,fieldturf,"
    ",,,,,,Mike Vrabel,Mike Macdonald,,SEA00,Lumen Field\n"
    "2026_01_SF_LA,2026,REG,1,2026-09-10,Thursday,20:35,SF,,LA,,Neutral,,,,2026091000,,,"
    "202609100ram,,401872657,,7,7,160,-192,3.5,-110,-110,48.5,-112,-108,1,dome,matrixturf,"
    ",,,,,,Kyle Shanahan,Sean McVay,,LAX01,Melbourne Cricket Ground\n"
    '2026_01_BAL_IND,2026,REG,1,2026-09-13,Sunday,13:00,BAL,,IND,,Home,,,,2026091304,,,'
    '202609130clt,,401872659,,7,7,-175,145,-3.5,-108,-112,48.5,-110,-110,0,"",fieldturf,'
    ",,,,,,Jesse Minter,Shane Steichen,,IND00,Lucas Oil Stadium\n"
    "2025_01_DAL_PHI,2025,REG,1,2025-09-04,Thursday,20:20,DAL,20,PHI,24,Home,4,44,0,"
    "2025090400,,,202509040phi,,401772510,,7,7,330,-400,-8.5,-110,-110,47.5,-110,-110,1,"
    "outdoors,grass,72,6,,,,,Brian Schottenheimer,Nick Sirianni,,PHI00,Lincoln Financial Field\n"
)

STADIUMS_CSV = (
    "stadium_id,stadium_name,lat,lon,altitude,heading,surface_type,roof_type,tz\n"
    "SEA00,Lumen Field,47.5951513,-122.3316259,5.213504872,0,Turf,Outdoors,America/Los_Angeles\n"
    "LAX01,SoFi Stadium,33.9534635,-118.3392382,32.0,120,Turf,Dome,America/Los_Angeles\n"
    "IND00,Lucas Oil Stadium,39.7601008,-86.1638573,220.0,90,Turf,Dome,America/Indianapolis\n"
    "PHI00,Lincoln Financial Field,39.9008358,-75.1674627,4.0,20,Grass,Outdoors,America/New_York\n"
)


def _raw() -> pl.DataFrame:
    return NfldataGamesJob(season=2026).parse(GAMES_CSV.encode())


@pytest.fixture
def seeded(db_session):
    seed_nfl_teams(db_session)
    seed_stadiums(db_session, pl.read_csv(STADIUMS_CSV.encode()))
    db_session.flush()
    return db_session


def test_job_url_and_registration():
    assert GAMES_CSV_URL == (
        "https://raw.githubusercontent.com/nflverse/nfldata/master/data/games.csv"
    )
    assert NfldataGamesJob(season=2026).url() == GAMES_CSV_URL
    assert NfldataGamesJob.source == "nfldata" and NfldataGamesJob.asset == "games"
    assert NfldataGamesJob.seasonal is False and NfldataGamesJob.season_scoped is True


def test_parse_keeps_the_quoted_empty_roof_as_an_empty_string():
    raw = _raw()
    assert raw.filter(pl.col("game_id") == "2026_01_BAL_IND")["roof"].item() == ""
    assert raw["roof"].null_count() == 0


def test_kickoff_at_converts_eastern_wall_clock_to_utc():
    rows = to_game_rows(_raw(), 2026)
    opener = rows.filter(pl.col("game_id") == "2026_01_NE_SEA")["kickoff_at"].item()
    # 2026-09-09 20:20 ET (EDT, UTC-4) -> 2026-09-10 00:20 UTC
    assert opener == datetime(2026, 9, 10, 0, 20, tzinfo=UTC)


def test_empty_roof_becomes_null_not_empty_string():
    rows = to_game_rows(_raw(), 2026)
    assert rows.filter(pl.col("game_id") == "2026_01_BAL_IND")["roof"].item() is None
    assert rows.filter(pl.col("game_id") == "2026_01_NE_SEA")["roof"].item() == "outdoors"


def test_neutral_site_comes_from_location():
    rows = to_game_rows(_raw(), 2026)
    neutral = dict(zip(rows["game_id"], rows["neutral_site"], strict=True))
    assert neutral["2026_01_SF_LA"] is True
    assert neutral["2026_01_NE_SEA"] is False


def test_season_filter_and_season_type_mapping():
    rows_2026 = to_game_rows(_raw(), 2026)
    assert rows_2026.height == 3
    assert set(rows_2026["season_type"]) == {"REG"}
    assert to_game_rows(_raw(), 2025).height == 1


def test_div_game_and_rest_and_lines_map_through():
    row = to_game_rows(_raw(), 2026).filter(pl.col("game_id") == "2026_01_SF_LA").to_dicts()[0]
    assert row["div_game"] is True
    assert row["home_rest"] == 7 and row["away_rest"] == 7
    assert row["spread_line"] == pytest.approx(3.5)
    assert row["total_line"] == pytest.approx(48.5)
    assert row["home_moneyline"] == -192 and row["away_moneyline"] == 160


def test_post_game_actuals_map_through_for_a_played_game():
    row = to_game_rows(_raw(), 2025).to_dicts()[0]
    assert (row["home_score"], row["away_score"]) == (24, 20)
    assert row["temp_f"] == pytest.approx(72.0) and row["wind_mph"] == pytest.approx(6.0)


def test_upsert_games_inserts_and_is_idempotent(seeded):
    assert upsert_games(seeded, _raw(), 2026) == 3
    seeded.flush()
    assert seeded.scalar(select(Game.game_id).where(Game.game_id == "2026_01_NE_SEA"))
    first = seeded.get(Game, "2026_01_NE_SEA").updated_at

    assert upsert_games(seeded, _raw(), 2026) == 3
    seeded.flush()
    assert len(list(seeded.scalars(select(Game)))) == 3
    seeded.expire_all()
    assert seeded.get(Game, "2026_01_NE_SEA").updated_at >= first


def test_upsert_games_bumps_updated_at_on_conflict(seeded):
    upsert_games(seeded, _raw(), 2026)
    seeded.flush()
    before = seeded.get(Game, "2026_01_BAL_IND").updated_at

    changed = _raw().with_columns(
        pl.when(pl.col("game_id") == "2026_01_BAL_IND")
        .then(pl.lit(51.5))
        .otherwise(pl.col("total_line"))
        .alias("total_line")
    )
    upsert_games(seeded, changed, 2026)
    seeded.flush()
    seeded.expire_all()
    game = seeded.get(Game, "2026_01_BAL_IND")
    assert game.total_line == pytest.approx(51.5)
    assert game.updated_at > before


def test_upsert_games_raises_with_the_unmatched_stadium_list(db_session):
    seed_nfl_teams(db_session)
    seed_stadiums(
        db_session,
        pl.read_csv(
            (
                "stadium_id,stadium_name,lat,lon,altitude,heading,surface_type,roof_type,tz\n"
                "SEA00,Lumen Field,47.6,-122.3,5.2,0,Turf,Outdoors,America/Los_Angeles\n"
            ).encode()
        ),
    )
    db_session.flush()
    with pytest.raises(ValueError, match="IND00"):
        upsert_games(db_session, _raw(), 2026)


def test_upsert_games_rejects_a_season_with_no_rows(seeded):
    with pytest.raises(ValueError, match="no rows for season 1999"):
        upsert_games(seeded, _raw(), 1999)


def test_persist_upserts_for_the_jobs_season(seeded, monkeypatch):
    job = NfldataGamesJob(season=2026)
    job.persist(seeded, _raw())
    seeded.flush()
    assert len(list(seeded.scalars(select(Game)))) == 3
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/ingest/test_games.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'ffh.ingest.games'`.

- [ ] **Step 3: Write `backend/src/ffh/ingest/games.py`**

```python
"""nfldata games.csv -> lake Parquet + the `games` table (DATA_SOURCES.md §2).

Live gotchas verified 2026-08-16 and encoded below:
  * `gameday`/`gametime` are Eastern wall-clock, not UTC.
  * `roof` is the literal quoted empty string "" (not NULL) for retractable-roof stadiums
    whose game has not been played — 43 of 272 rows in 2026.
  * `game_type` is REG|WC|DIV|CON|SB; `games.season_type` is REG|POST.
  * neutral-site games carry the nominal HOME team's stadium_id.
"""

import io
from typing import ClassVar

import polars as pl
import structlog
from sqlalchemy import func
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from ffh.config import get_settings
from ffh.db.models import Game
from ffh.ingest.base import HttpIngestJob, register
from ffh.ingest.lake import scrape_date
from ffh.ingest.reference import assert_stadium_coverage, assert_team_coverage

log = structlog.get_logger(__name__)

GAMES_CSV_URL = "https://raw.githubusercontent.com/nflverse/nfldata/master/data/games.csv"

EASTERN = "America/New_York"

GAME_COLUMNS = (
    "game_id",
    "season",
    "week",
    "season_type",
    "kickoff_at",
    "home_team",
    "away_team",
    "stadium_id",
    "spread_line",
    "total_line",
    "home_moneyline",
    "away_moneyline",
    "roof",
    "surface",
    "div_game",
    "home_rest",
    "away_rest",
    "neutral_site",
    "home_score",
    "away_score",
    "temp_f",
    "wind_mph",
)


def _blank_to_null(name: str) -> pl.Expr:
    """games.csv writes an empty *quoted* string where R had `""` — that is not NULL."""
    return (
        pl.when(pl.col(name).str.strip_chars() == "")
        .then(None)
        .otherwise(pl.col(name))
        .alias(name)
    )


def to_game_rows(df: pl.DataFrame, season: int) -> pl.DataFrame:
    """Map raw games.csv rows for one season onto the `games` table's columns."""
    season_df = df.filter(pl.col("season") == season)
    if season_df.height == 0:
        raise ValueError(f"games.csv has no rows for season {season}")

    kickoff = (
        pl.concat_str([pl.col("gameday"), pl.lit(" "), pl.col("gametime")])
        .str.to_datetime("%Y-%m-%d %H:%M", time_unit="us")
        .dt.replace_time_zone(EASTERN, ambiguous="earliest", non_existent="raise")
        .dt.convert_time_zone("UTC")
        .alias("kickoff_at")
    )

    rows = season_df.select(
        pl.col("game_id"),
        pl.col("season").cast(pl.Int16),
        pl.col("week").cast(pl.Int16),
        pl.when(pl.col("game_type") == "REG")
        .then(pl.lit("REG"))
        .otherwise(pl.lit("POST"))
        .alias("season_type"),
        kickoff,
        pl.col("home_team"),
        pl.col("away_team"),
        _blank_to_null("stadium_id"),
        pl.col("spread_line").cast(pl.Float64),
        pl.col("total_line").cast(pl.Float64),
        pl.col("home_moneyline").cast(pl.Int32),
        pl.col("away_moneyline").cast(pl.Int32),
        _blank_to_null("roof"),
        _blank_to_null("surface"),
        (pl.col("div_game") == 1).alias("div_game"),
        pl.col("home_rest").cast(pl.Int16),
        pl.col("away_rest").cast(pl.Int16),
        (pl.col("location") == "Neutral").alias("neutral_site"),
        pl.col("home_score").cast(pl.Int16),
        pl.col("away_score").cast(pl.Int16),
        pl.col("temp").cast(pl.Float64).alias("temp_f"),
        pl.col("wind").cast(pl.Float64).alias("wind_mph"),
    )
    assert rows.height == season_df.height, (
        f"row loss mapping games.csv: {season_df.height} in, {rows.height} out"
    )
    assert rows["kickoff_at"].null_count() == 0, "every scheduled game must have a kickoff time"
    return rows.select(GAME_COLUMNS)


def upsert_games(session: Session, df: pl.DataFrame, season: int) -> int:
    """Upsert one season of games.csv into `games`. Returns the number of rows upserted."""
    rows = to_game_rows(df, season)

    assert_team_coverage(session, set(rows["home_team"]) | set(rows["away_team"]))
    assert_stadium_coverage(session, rows)

    records = rows.to_dicts()
    assert len(records) == rows.height, "row loss converting the frame to records"

    stmt = pg_insert(Game).values(records)
    updatable = {c: stmt.excluded[c] for c in GAME_COLUMNS if c != "game_id"}
    stmt = stmt.on_conflict_do_update(
        index_elements=[Game.game_id],
        # ORM onupdate does not fire for INSERT ... ON CONFLICT (DATABASE.md §2).
        set_={**updatable, "updated_at": func.now()},
    )
    session.execute(stmt)
    log.info("ingest.games.upserted", season=season, rows=len(records))
    return len(records)


@register
class NfldataGamesJob(HttpIngestJob):
    """Schedule + Vegas lines + per-game roof state. Refreshes every 5 minutes in season."""

    name = "nfldata_games"
    source = "nfldata"
    asset = "games"
    seasonal: ClassVar[bool] = False
    season_scoped: ClassVar[bool] = True
    REQUIRED_COLUMNS = frozenset(
        {
            "game_id",
            "season",
            "game_type",
            "week",
            "gameday",
            "gametime",
            "away_team",
            "home_team",
            "away_score",
            "home_score",
            "location",
            "away_rest",
            "home_rest",
            "away_moneyline",
            "home_moneyline",
            "spread_line",
            "total_line",
            "div_game",
            "roof",
            "surface",
            "temp",
            "wind",
            "stadium_id",
        }
    )

    def url(self) -> str:
        return GAMES_CSV_URL

    def partition(self) -> dict[str, str]:
        return {"scrape_date": scrape_date()}

    def parse(self, content: bytes) -> pl.DataFrame:
        # infer_schema_length=None scans every row: the 2026 rows are almost all-null and a
        # short inference window types spread_line/total_line as String.
        return pl.read_csv(io.BytesIO(content), infer_schema_length=None)

    def persist(self, session: Session, df: pl.DataFrame) -> None:
        upsert_games(session, df, self.season or get_settings().season)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/ingest/test_games.py -v`
Expected: 13 passed. (Task 6 must be implemented for the imports to resolve — if you are
executing tasks strictly in order, do Task 6 now and re-run both test files.)

- [ ] **Step 5: Lint and commit**

```bash
cd backend && uv run ruff check . && uv run ruff format .
cd .. && git add backend/src/ffh/ingest/games.py backend/tests/ingest/test_games.py
git commit -m "feat(ingest): nfldata games.csv job and games upsert with ET->UTC kickoff"
```

---

### Task 6: `ffh.ingest.reference` — teams, stadiums, the sentinel league, coverage assertions

**Files:**
- Create: `backend/src/ffh/data/__init__.py` (empty — makes `ffh.data` an importable package for `importlib.resources`)
- Create: `backend/src/ffh/data/nfl_teams.csv`
- Create: `backend/src/ffh/ingest/reference.py`
- Modify: `.gitignore` (append the negations — `data/` and `*.parquet` would otherwise swallow the new files)
- Test: `backend/tests/ingest/test_reference.py`

**Interfaces:**
- Consumes: `ffh.ingest.base.{HttpIngestJob, register}`; `ffh.ingest.lake.scrape_date`; `ffh.db.models.{NflTeam, Stadium, League, GENERIC_LEAGUE_ID}`.
- Produces:
  - `ffh.ingest.reference.STADIUMS_CSV_URL: str`.
  - `ffh.ingest.reference.NFL_TEAMS_CSV: Traversable` — the packaged CSV.
  - `ffh.ingest.reference.load_nfl_teams() -> pl.DataFrame`.
  - `ffh.ingest.reference.seed_nfl_teams(session: Session) -> int`.
  - `ffh.ingest.reference.StadiumsJob(HttpIngestJob)` — `name = "stadiums"`, `source = "greerre"`, `asset = "stadiums"`.
  - `ffh.ingest.reference.seed_stadiums(session: Session, df: pl.DataFrame) -> int`.
  - `ffh.ingest.reference.seed_generic_league(session: Session) -> uuid.UUID`.
  - `ffh.ingest.reference.assert_team_coverage(session: Session, abbrs: set[str]) -> None`.
  - `ffh.ingest.reference.assert_stadium_coverage(session: Session, rows: pl.DataFrame) -> None` — takes the mapped `games` frame (needs a `stadium_id` column), does a Polars **anti-join** against the DB's stadium ids and asserts `matched.height + unmatched.height == rows.height`.
  - `ffh.ingest.reference.GENERIC_SCORING: dict[str, float]`, `GENERIC_ROSTER: dict[str, int]`.

- [ ] **Step 1: Write `backend/src/ffh/data/nfl_teams.csv`**

nflverse abbreviations (note `LA` for the Rams and `WAS` for Washington — **not** ESPN's
`LAR`/`WSH`), with ESPN numeric ids fetched live from
`https://site.api.espn.com/apis/site/v2/sports/football/nfl/teams` on 2026-08-16. The 32
abbreviations match the distinct `home_team` values in `games.csv` for 2026 exactly.

```csv
team_abbr,espn_id,full_name,conference,division
ARI,22,Arizona Cardinals,NFC,West
ATL,1,Atlanta Falcons,NFC,South
BAL,33,Baltimore Ravens,AFC,North
BUF,2,Buffalo Bills,AFC,East
CAR,29,Carolina Panthers,NFC,South
CHI,3,Chicago Bears,NFC,North
CIN,4,Cincinnati Bengals,AFC,North
CLE,5,Cleveland Browns,AFC,North
DAL,6,Dallas Cowboys,NFC,East
DEN,7,Denver Broncos,AFC,West
DET,8,Detroit Lions,NFC,North
GB,9,Green Bay Packers,NFC,North
HOU,34,Houston Texans,AFC,South
IND,11,Indianapolis Colts,AFC,South
JAX,30,Jacksonville Jaguars,AFC,South
KC,12,Kansas City Chiefs,AFC,West
LA,14,Los Angeles Rams,NFC,West
LAC,24,Los Angeles Chargers,AFC,West
LV,13,Las Vegas Raiders,AFC,West
MIA,15,Miami Dolphins,AFC,East
MIN,16,Minnesota Vikings,NFC,North
NE,17,New England Patriots,AFC,East
NO,18,New Orleans Saints,NFC,South
NYG,19,New York Giants,NFC,East
NYJ,20,New York Jets,AFC,East
PHI,21,Philadelphia Eagles,NFC,East
PIT,23,Pittsburgh Steelers,AFC,North
SEA,26,Seattle Seahawks,NFC,West
SF,25,San Francisco 49ers,NFC,West
TB,27,Tampa Bay Buccaneers,NFC,South
TEN,10,Tennessee Titans,AFC,South
WAS,28,Washington Commanders,NFC,East
```

Also create an empty `backend/src/ffh/data/__init__.py`.

- [ ] **Step 2: Fix `.gitignore` — without this the new files are invisible to git**

`.gitignore` currently contains `data/` and `*.parquet`. `data/` matches at any depth, so
`backend/src/ffh/data/` is excluded and git will not even descend into it; `*.parquet`
excludes the recorded fixtures Task 9 commits. Append at the end of `.gitignore`:

```gitignore

# Checked-in reference data and recorded test fixtures must survive the broad rules above.
# `data/` above matches at any depth, so the directory itself has to be re-included.
!backend/src/ffh/data/
!backend/tests/fixtures/**/*.parquet
```

Verify: `git check-ignore -v backend/src/ffh/data/nfl_teams.csv` must print nothing.

- [ ] **Step 3: Write the failing test `backend/tests/ingest/test_reference.py`**

```python
import uuid

import polars as pl
import pytest
from sqlalchemy import select

from ffh.db.models import GENERIC_LEAGUE_ID, League, NflTeam, Stadium
from ffh.ingest.reference import (
    GENERIC_ROSTER,
    GENERIC_SCORING,
    STADIUMS_CSV_URL,
    StadiumsJob,
    assert_stadium_coverage,
    assert_team_coverage,
    load_nfl_teams,
    seed_generic_league,
    seed_nfl_teams,
    seed_stadiums,
)

pytestmark = pytest.mark.db

# Verified rows from greerreNFL/stadiums on 2026-08-16 (altitude is in METRES).
STADIUMS_CSV = (
    "stadium_id,stadium_name,lat,lon,altitude,heading,surface_type,roof_type,tz\n"
    "DEN00,Empower Field at Mile High,39.7439402,-105.0201065,1583.586238,0,Grass,Outdoors,"
    "America/Denver\n"
    "SEA00,Lumen Field,47.5951513,-122.3316259,5.213504872,0,Turf,Outdoors,"
    "America/Los_Angeles\n"
    "PHO00,State Farm Stadium,33.5277555,-112.2625948,325.7411644,328,Grass,Dome,"
    "America/Phoenix\n"
)


def _stadiums() -> pl.DataFrame:
    return pl.read_csv(STADIUMS_CSV.encode())


def test_packaged_nfl_teams_csv_has_32_unique_rows():
    df = load_nfl_teams()
    assert df.height == 32
    assert df["team_abbr"].n_unique() == 32
    assert df["espn_id"].n_unique() == 32


def test_nfl_teams_uses_nflverse_abbreviations():
    abbrs = set(load_nfl_teams()["team_abbr"])
    assert {"LA", "LAC", "LV", "WAS", "JAX", "GB", "NO", "SF", "TB", "KC", "NE"} <= abbrs
    assert "LAR" not in abbrs and "WSH" not in abbrs and "OAK" not in abbrs and "SD" not in abbrs


def test_nfl_teams_has_four_teams_in_every_division():
    df = load_nfl_teams()
    counts = df.group_by(["conference", "division"]).len().sort(["conference", "division"])
    assert counts.height == 8
    assert set(counts["len"].to_list()) == {4}


def test_seed_nfl_teams_is_idempotent(db_session):
    assert seed_nfl_teams(db_session) == 32
    db_session.flush()
    assert seed_nfl_teams(db_session) == 32
    db_session.flush()
    assert len(list(db_session.scalars(select(NflTeam)))) == 32
    rams = db_session.get(NflTeam, "LA")
    assert (rams.espn_id, rams.full_name, rams.conference, rams.division) == (
        14,
        "Los Angeles Rams",
        "NFC",
        "West",
    )
    assert rams.bye_week is None  # Phase 0 deviation: derived from `games` at query time


def test_stadiums_job_url_and_partition():
    assert STADIUMS_CSV_URL == (
        "https://raw.githubusercontent.com/greerreNFL/stadiums/main/data/stadiums.csv"
    )
    assert StadiumsJob().url() == STADIUMS_CSV_URL
    assert set(StadiumsJob().partition()) == {"scrape_date"}
    assert StadiumsJob.seasonal is False


def test_seed_stadiums_converts_altitude_metres_to_feet(db_session):
    assert seed_stadiums(db_session, _stadiums()) == 3
    db_session.flush()
    denver = db_session.get(Stadium, "DEN00")
    # 1583.586238 m * 3.280839895 = 5195.5 ft -> 5195 (Mile High really is ~5,280 ft)
    assert denver.altitude_ft == 5195
    assert db_session.get(Stadium, "SEA00").altitude_ft == 17


def test_seed_stadiums_maps_names_and_is_idempotent(db_session):
    seed_stadiums(db_session, _stadiums())
    db_session.flush()
    assert seed_stadiums(db_session, _stadiums()) == 3
    db_session.flush()
    assert len(list(db_session.scalars(select(Stadium)))) == 3
    sea = db_session.get(Stadium, "SEA00")
    assert sea.name == "Lumen Field"
    assert sea.tz == "America/Los_Angeles"
    assert sea.roof_type == "Outdoors" and sea.surface_type == "Turf"
    assert sea.heading_deg == pytest.approx(0.0)
    assert sea.latitude == pytest.approx(47.5951513)


def test_seed_generic_league_matches_database_md_section_6(db_session):
    league_id = seed_generic_league(db_session)
    db_session.flush()
    assert league_id == GENERIC_LEAGUE_ID == uuid.UUID("00000000-0000-0000-0000-000000000000")
    league = db_session.get(League, GENERIC_LEAGUE_ID)
    assert league.platform == "ffh"
    assert league.external_id == "generic"
    assert league.season == 0
    assert league.name == "Generic PPR"
    assert league.num_teams == 12
    assert league.league_type == "redraft"
    assert league.is_superflex is False
    assert league.scoring_settings == GENERIC_SCORING
    assert league.roster_settings == GENERIC_ROSTER
    assert league.playoff_teams is None
    assert league.playoff_start_wk is None
    assert league.faab_budget is None
    assert league.my_team_id is None


def test_generic_scoring_is_canonical_full_ppr():
    assert GENERIC_SCORING == {
        "pass_yd": 0.04,
        "pass_td": 4,
        "pass_int": -2,
        "rush_yd": 0.1,
        "rush_td": 6,
        "rec": 1,
        "rec_yd": 0.1,
        "rec_td": 6,
        "fum_lost": -2,
        "two_pt": 2,
    }
    assert GENERIC_ROSTER == {
        "QB": 1,
        "RB": 2,
        "WR": 2,
        "TE": 1,
        "FLEX": 1,
        "K": 1,
        "DST": 1,
        "BN": 6,
    }


def test_seed_generic_league_is_idempotent(db_session):
    seed_generic_league(db_session)
    db_session.flush()
    seed_generic_league(db_session)
    db_session.flush()
    assert len(list(db_session.scalars(select(League)))) == 1


def test_assert_team_coverage_names_the_missing_abbreviations(db_session):
    seed_nfl_teams(db_session)
    db_session.flush()
    assert_team_coverage(db_session, {"KC", "LA", "WAS"})
    with pytest.raises(ValueError, match="LAR"):
        assert_team_coverage(db_session, {"KC", "LAR"})


def test_assert_stadium_coverage_passes_when_every_id_matches(db_session):
    seed_stadiums(db_session, _stadiums())
    db_session.flush()
    rows = pl.DataFrame({"game_id": ["a", "b"], "stadium_id": ["DEN00", "SEA00"]})
    assert_stadium_coverage(db_session, rows)


def test_assert_stadium_coverage_raises_with_the_unmatched_list(db_session):
    seed_stadiums(db_session, _stadiums())
    db_session.flush()
    rows = pl.DataFrame({"game_id": ["a", "b"], "stadium_id": ["DEN00", "NOPE1"]})
    with pytest.raises(ValueError) as excinfo:
        assert_stadium_coverage(db_session, rows)
    assert "NOPE1" in str(excinfo.value)
    assert "DEN00" not in str(excinfo.value)
```

- [ ] **Step 4: Run test to verify it fails**

Run: `uv run pytest tests/ingest/test_reference.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'ffh.ingest.reference'`.

- [ ] **Step 5: Write `backend/src/ffh/ingest/reference.py`**

```python
"""Static reference seeds: nfl_teams, stadiums, and the sentinel generic league.

`nfl_teams` ships as a checked-in CSV (32 rows never change mid-season). `stadiums` is a
real ingest job over greerreNFL's CSV. The sentinel `leagues` row exists so the NOT NULL
FKs on `projections.league_id` and `player_week_actuals.league_id` can hold league-agnostic
rows (DATABASE.md §6).
"""

import io
import uuid
from importlib.resources import files
from typing import Any, ClassVar

import polars as pl
import structlog
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from ffh.db.models import GENERIC_LEAGUE_ID, League, NflTeam, Stadium
from ffh.ingest.base import HttpIngestJob, register
from ffh.ingest.lake import scrape_date

log = structlog.get_logger(__name__)

STADIUMS_CSV_URL = "https://raw.githubusercontent.com/greerreNFL/stadiums/main/data/stadiums.csv"

NFL_TEAMS_CSV = files("ffh.data") / "nfl_teams.csv"

METRES_TO_FEET = 3.280839895

#: Canonical full-PPR reference scoring. NOT a default for real leagues — those are always
#: platform-fetched (DATABASE.md §6, ARCHITECTURE.md adapter contract).
GENERIC_SCORING: dict[str, Any] = {
    "pass_yd": 0.04,
    "pass_td": 4,
    "pass_int": -2,
    "rush_yd": 0.1,
    "rush_td": 6,
    "rec": 1,
    "rec_yd": 0.1,
    "rec_td": 6,
    "fum_lost": -2,
    "two_pt": 2,
}

GENERIC_ROSTER: dict[str, Any] = {
    "QB": 1,
    "RB": 2,
    "WR": 2,
    "TE": 1,
    "FLEX": 1,
    "K": 1,
    "DST": 1,
    "BN": 6,
}


# --- nfl_teams -----------------------------------------------------------------------


def load_nfl_teams() -> pl.DataFrame:
    """The packaged 32-row team table (nflverse abbreviations, ESPN numeric ids)."""
    return pl.read_csv(io.BytesIO(NFL_TEAMS_CSV.read_bytes()))


def seed_nfl_teams(session: Session) -> int:
    """Upsert the 32 NFL teams. `bye_week` stays NULL (Phase 0 deviation, DATABASE.md §2)."""
    df = load_nfl_teams()
    if df.height != 32:
        raise ValueError(f"nfl_teams.csv must have exactly 32 rows, found {df.height}")

    records = df.select("team_abbr", "espn_id", "full_name", "conference", "division").to_dicts()
    stmt = pg_insert(NflTeam).values(records)
    stmt = stmt.on_conflict_do_update(
        index_elements=[NflTeam.team_abbr],
        set_={
            "espn_id": stmt.excluded.espn_id,
            "full_name": stmt.excluded.full_name,
            "conference": stmt.excluded.conference,
            "division": stmt.excluded.division,
        },
    )
    session.execute(stmt)
    log.info("ingest.reference.teams_seeded", rows=len(records))
    return len(records)


def assert_team_coverage(session: Session, abbrs: set[str]) -> None:
    """Every abbreviation must exist in `nfl_teams`, or the games FK fails opaquely."""
    known = set(session.scalars(select(NflTeam.team_abbr)))
    missing = sorted(abbrs - known)
    if missing:
        raise ValueError(
            f"{len(missing)} team abbreviation(s) absent from nfl_teams: {missing}. "
            "Run `ffh ingest seed` first."
        )


# --- stadiums ------------------------------------------------------------------------


def seed_stadiums(session: Session, df: pl.DataFrame) -> int:
    """Upsert greerreNFL stadium rows. `altitude` is METRES; `stadiums.altitude_ft` is feet."""
    rows = df.select(
        pl.col("stadium_id"),
        pl.col("stadium_name").alias("name"),
        pl.col("lat").cast(pl.Float64).alias("latitude"),
        pl.col("lon").cast(pl.Float64).alias("longitude"),
        (pl.col("altitude").cast(pl.Float64) * METRES_TO_FEET).round(0).cast(pl.Int32).alias(
            "altitude_ft"
        ),
        pl.col("heading").cast(pl.Float64).alias("heading_deg"),
        pl.col("surface_type"),
        # Outdoors|Dome only — retractable state per game lives in games.roof.
        pl.col("roof_type"),
        pl.col("tz"),
    )
    assert rows.height == df.height, f"row loss mapping stadiums: {df.height} -> {rows.height}"

    records = rows.to_dicts()
    stmt = pg_insert(Stadium).values(records)
    stmt = stmt.on_conflict_do_update(
        index_elements=[Stadium.stadium_id],
        set_={
            c: stmt.excluded[c]
            for c in (
                "name",
                "latitude",
                "longitude",
                "altitude_ft",
                "heading_deg",
                "surface_type",
                "roof_type",
                "tz",
            )
        },
    )
    session.execute(stmt)
    log.info("ingest.reference.stadiums_seeded", rows=len(records))
    return len(records)


def assert_stadium_coverage(session: Session, rows: pl.DataFrame) -> None:
    """Assert every `stadium_id` in `rows` resolves. DATA_SOURCES.md §4 promises 30/30.

    Uses a Polars anti-join and asserts the split accounts for every input row — a silent
    join loss here is exactly the Tier-1 failure AGENTS.md calls out.
    """
    known = pl.DataFrame(
        {"stadium_id": list(session.scalars(select(Stadium.stadium_id)))},
        schema={"stadium_id": pl.String},
    )
    subject = rows.select("stadium_id").drop_nulls()
    unmatched = subject.join(known, on="stadium_id", how="anti")
    matched = subject.join(known, on="stadium_id", how="semi")
    assert matched.height + unmatched.height == subject.height, (
        f"anti/semi join lost rows: {matched.height} + {unmatched.height} != {subject.height}"
    )
    if unmatched.height:
        missing = sorted(set(unmatched["stadium_id"]))
        raise ValueError(
            f"{len(missing)} stadium_id value(s) absent from `stadiums`: {missing}. "
            "Run `ffh ingest run stadiums` first."
        )


@register
class StadiumsJob(HttpIngestJob):
    """greerreNFL stadium coordinates, altitude, heading and tz (DATA_SOURCES.md §4)."""

    name = "stadiums"
    source = "greerre"
    asset = "stadiums"
    seasonal: ClassVar[bool] = False
    REQUIRED_COLUMNS = frozenset(
        {
            "stadium_id",
            "stadium_name",
            "lat",
            "lon",
            "altitude",
            "heading",
            "surface_type",
            "roof_type",
            "tz",
        }
    )

    def url(self) -> str:
        return STADIUMS_CSV_URL

    def partition(self) -> dict[str, str]:
        return {"scrape_date": scrape_date()}

    def parse(self, content: bytes) -> pl.DataFrame:
        return pl.read_csv(io.BytesIO(content), infer_schema_length=None)

    def persist(self, session: Session, df: pl.DataFrame) -> None:
        seed_stadiums(session, df)


# --- sentinel league -----------------------------------------------------------------


def seed_generic_league(session: Session) -> uuid.UUID:
    """Insert the sentinel `leagues` row exactly as DATABASE.md §6 specifies. Idempotent."""
    stmt = pg_insert(League).values(
        league_id=GENERIC_LEAGUE_ID,
        platform="ffh",
        external_id="generic",
        season=0,
        name="Generic PPR",
        num_teams=12,
        league_type="redraft",
        is_superflex=False,
        scoring_settings=GENERIC_SCORING,
        roster_settings=GENERIC_ROSTER,
    )
    session.execute(stmt.on_conflict_do_nothing(index_elements=[League.league_id]))
    log.info("ingest.reference.generic_league_seeded", league_id=str(GENERIC_LEAGUE_ID))
    return GENERIC_LEAGUE_ID
```

- [ ] **Step 6: Run both test files to verify they pass**

Run: `uv run pytest tests/ingest/test_reference.py tests/ingest/test_games.py -v`
Expected: 14 + 13 passed.

- [ ] **Step 7: Verify the gitignore fix actually worked**

```bash
git check-ignore -v backend/src/ffh/data/nfl_teams.csv || echo "NOT IGNORED - good"
```
Expected: `NOT IGNORED - good`.

- [ ] **Step 8: Lint and commit**

```bash
cd backend && uv run ruff check . && uv run ruff format .
cd .. && git add .gitignore backend/src/ffh/data backend/src/ffh/ingest/reference.py backend/tests/ingest/test_reference.py
git commit -m "feat(ingest): nfl_teams/stadiums/sentinel-league seeds and coverage assertions"
```

---

### Task 7: `ffh.features.duck` — in-memory DuckDB views over the latest lake partitions

**Files:**
- Create: `backend/src/ffh/features/duck.py`
- Create: `backend/tests/features/__init__.py` (empty)
- Test: `backend/tests/features/test_duck.py`

**Interfaces:**
- Consumes: `ffh.ingest.lake.parquet_file` (for test setup only); the lake tree on disk.
- Produces:
  - `ffh.features.duck.VIEWS: dict[str, tuple[str, str]]` — view name → `(source, asset)`.
  - `ffh.features.duck.SEASONAL_ASSETS: frozenset[str]`.
  - `ffh.features.duck.latest_partition(lake_root: Path, source: str, asset: str, season: int | None = None) -> Path | None`.
  - `ffh.features.duck.connect(lake_root: Path, season: int) -> duckdb.DuckDBPyConnection`.

**"Latest partition" is defined deterministically as:** among every
`<lake_root>/raw/<source>/<asset>/**/<asset>.parquet`, keep those whose path contains the
literal segment `season=<season>` when the asset is seasonal, then take
`max(candidates, key=lambda p: p.as_posix())`. Because partition keys are emitted in the
order `season=YYYY/scrape_date=YYYY-MM-DD` and both are zero-padded ISO, lexicographic
order over the POSIX path string **is** chronological order. No mtime, no globbing order,
no ambiguity.

- [ ] **Step 1: Write the failing test `backend/tests/features/test_duck.py`**

```python
from pathlib import Path

import duckdb
import polars as pl
import pytest

from ffh.features.duck import VIEWS, connect, latest_partition
from ffh.ingest.lake import parquet_file, write_parquet


def _land(lake: Path, source: str, asset: str, df: pl.DataFrame, **keys) -> Path:
    path = parquet_file(lake, source, asset, **keys)
    write_parquet(df, path)
    return path


@pytest.fixture
def lake(tmp_path: Path) -> Path:
    _land(
        tmp_path,
        "nflverse",
        "players",
        pl.DataFrame({"gsis_id": ["00-0034796", "00-0036322"], "position": ["WR", "RB"]}),
        scrape_date="2026-08-15",
    )
    _land(
        tmp_path,
        "nflverse",
        "stats_player_week",
        pl.DataFrame(
            {
                "player_id": ["00-0034796", "00-0034796"],
                "week": [1, 2],
                "fantasy_points_ppr": [18.4, 22.1],
            }
        ),
        season="2026",
        scrape_date="2026-08-15",
    )
    _land(
        tmp_path,
        "nfldata",
        "games",
        pl.DataFrame({"game_id": ["2026_01_NE_SEA"], "season": [2026], "week": [1]}),
        scrape_date="2026-08-15",
    )
    return tmp_path


def test_latest_partition_picks_the_newest_scrape_date(lake: Path):
    _land(
        lake,
        "nflverse",
        "players",
        pl.DataFrame({"gsis_id": ["00-0039999"], "position": ["TE"]}),
        scrape_date="2026-08-16",
    )
    newest = latest_partition(lake, "nflverse", "players")
    assert newest is not None
    assert newest.parent.name == "scrape_date=2026-08-16"


def test_latest_partition_respects_the_season_filter(lake: Path):
    _land(
        lake,
        "nflverse",
        "stats_player_week",
        pl.DataFrame({"player_id": ["x"], "week": [1], "fantasy_points_ppr": [1.0]}),
        season="2025",
        scrape_date="2026-12-31",
    )
    picked = latest_partition(lake, "nflverse", "stats_player_week", season=2026)
    assert picked is not None
    assert "season=2026" in picked.as_posix()


def test_latest_partition_returns_none_when_nothing_landed(tmp_path: Path):
    assert latest_partition(tmp_path, "nflverse", "injuries", season=2026) is None


def test_connect_creates_views_for_landed_assets(lake: Path):
    con = connect(lake, season=2026)
    try:
        names = {row[0] for row in con.execute("SELECT view_name FROM duckdb_views()").fetchall()}
        assert {"players", "stats_player_week", "games"} <= names
        # not landed in this fixture, so no view is created
        assert "injuries" not in names
    finally:
        con.close()


def test_connect_views_are_queryable(lake: Path):
    con = connect(lake, season=2026)
    try:
        assert con.execute("SELECT count(*) FROM players").fetchone()[0] == 2
        total = con.execute(
            "SELECT round(sum(fantasy_points_ppr), 1) FROM stats_player_week "
            "WHERE player_id = '00-0034796'"
        ).fetchone()[0]
        assert total == pytest.approx(40.5)
        assert con.execute("SELECT game_id FROM games").fetchone()[0] == "2026_01_NE_SEA"
    finally:
        con.close()


def test_connect_never_creates_a_duckdb_file(lake: Path, tmp_path: Path):
    con = connect(lake, season=2026)
    try:
        con.execute("SELECT 1").fetchone()
    finally:
        con.close()
    assert not list(tmp_path.rglob("*.duckdb"))
    assert not list(Path.cwd().glob("*.duckdb"))


def test_connect_on_an_empty_lake_returns_a_usable_connection(tmp_path: Path):
    con = connect(tmp_path, season=2026)
    try:
        assert isinstance(con, duckdb.DuckDBPyConnection)
        assert con.execute("SELECT 42").fetchone()[0] == 42
    finally:
        con.close()


def test_views_cover_every_asset_the_spec_requires():
    assert set(VIEWS) == {
        "players",
        "stats_player_week",
        "snap_counts",
        "depth_charts",
        "injuries",
        "games",
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/features/test_duck.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'ffh.features.duck'`.

- [ ] **Step 3: Write `backend/src/ffh/features/duck.py`**

```python
"""DuckDB over the Parquet lake — read-only, in-memory, no state of its own.

CLAUDE.md rule 5 and DATABASE.md §1: DuckDB owns no state and never writes a `.duckdb`
file. `duckdb.connect()` with no argument is an in-memory database; the views are plain
`read_parquet` over whatever the ingest jobs last landed.
"""

from pathlib import Path

import duckdb
import structlog

log = structlog.get_logger(__name__)

#: view name -> (lake source, lake asset)
VIEWS: dict[str, tuple[str, str]] = {
    "players": ("nflverse", "players"),
    "stats_player_week": ("nflverse", "stats_player_week"),
    "snap_counts": ("nflverse", "snap_counts"),
    "depth_charts": ("nflverse", "depth_charts"),
    "injuries": ("nflverse", "injuries"),
    "games": ("nfldata", "games"),
}

#: assets partitioned by `season=` as well as `scrape_date=`
SEASONAL_ASSETS = frozenset({"stats_player_week", "snap_counts", "depth_charts", "injuries", "pbp"})


def latest_partition(
    lake_root: Path, source: str, asset: str, season: int | None = None
) -> Path | None:
    """The newest landed Parquet for an asset, or None.

    "Newest" is defined without touching the filesystem clock: partition keys are emitted
    as `season=YYYY/scrape_date=YYYY-MM-DD`, both zero-padded ISO, so the lexicographic
    maximum of the POSIX path string is the chronological maximum. Deterministic and
    reproducible from the path alone.
    """
    root = Path(lake_root) / "raw" / source / asset
    if not root.is_dir():
        return None
    candidates = list(root.rglob(f"{asset}.parquet"))
    if season is not None:
        marker = f"/season={season}/"
        candidates = [p for p in candidates if marker in p.as_posix()]
    if not candidates:
        return None
    return max(candidates, key=lambda p: p.as_posix())


def connect(lake_root: Path, season: int) -> duckdb.DuckDBPyConnection:
    """In-memory DuckDB with a `read_parquet` view per landed asset for `season`."""
    con = duckdb.connect()
    for view, (source, asset) in VIEWS.items():
        path = latest_partition(
            lake_root, source, asset, season if asset in SEASONAL_ASSETS else None
        )
        if path is None:
            log.warning("features.duck.view_missing", view=view, source=source, asset=asset)
            continue
        literal = path.as_posix().replace("'", "''")
        con.execute(f"CREATE VIEW {view} AS SELECT * FROM read_parquet('{literal}')")
        log.info("features.duck.view_created", view=view, path=path.as_posix())
    return con
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/features/test_duck.py -v`
Expected: 8 passed.

- [ ] **Step 5: Lint and commit**

```bash
cd backend && uv run ruff check . && uv run ruff format .
cd .. && git add backend/src/ffh/features/duck.py backend/tests/features
git commit -m "feat(features): in-memory duckdb views over the latest lake partitions"
```

---

### Task 8: CLI — `ffh ingest run|list|seed`

**Files:**
- Modify: `backend/src/ffh/cli.py` (replace the placeholder `ingest list` command added in PR ①)
- Test: `backend/tests/test_cli_ingest.py`

**Interfaces:**
- Consumes: `ffh.ingest.base.{JOBS, get_job, STATUS_FAILED}`; `ffh.ingest.nflverse` and `ffh.ingest.games` and `ffh.ingest.reference` (imported for their `@register` side effects); `ffh.ingest.reference.{seed_nfl_teams, seed_generic_league, StadiumsJob}`; `ffh.db.engine.{make_engine, make_session_factory}`; `ffh.config.get_settings`.
- Produces: `ffh ingest list`, `ffh ingest run <job> [--season N]`, `ffh ingest seed`. `run` prints one JSON line and exits 1 when `status == "failed"`.

- [ ] **Step 1: Write the failing test `backend/tests/test_cli_ingest.py`**

```python
import json

import pytest
from typer.testing import CliRunner

from ffh.cli import app
from ffh.ingest.base import IngestRunResult

runner = CliRunner()


def test_ingest_list_shows_every_registered_job():
    result = runner.invoke(app, ["ingest", "list"])
    assert result.exit_code == 0, result.stdout
    for name in (
        "nflverse_players",
        "nflverse_stats_player_week",
        "nflverse_snap_counts",
        "nflverse_depth_charts",
        "nflverse_injuries",
        "nflverse_pbp",
        "nfldata_games",
        "stadiums",
    ):
        assert name in result.stdout
    assert "no ingest jobs registered" not in result.stdout


def test_ingest_run_rejects_an_unknown_job():
    result = runner.invoke(app, ["ingest", "run", "not_a_job"])
    assert result.exit_code != 0
    assert "not_a_job" in result.stdout


def test_ingest_run_prints_json_and_exits_zero_on_success(monkeypatch):
    captured = {}

    def fake_run(self, session, lake_root):
        captured["season"] = self.season
        captured["lake_root"] = lake_root
        return IngestRunResult(status="success", rows_written=272, output_path="/lake/x.parquet")

    monkeypatch.setattr("ffh.ingest.games.NfldataGamesJob.run", fake_run)
    monkeypatch.setattr("ffh.cli._session_scope", _fake_session_scope)

    result = runner.invoke(app, ["ingest", "run", "nfldata_games", "--season", "2026"])
    assert result.exit_code == 0, result.stdout
    payload = json.loads(result.stdout.strip().splitlines()[-1])
    assert payload == {
        "job": "nfldata_games",
        "status": "success",
        "rows_written": 272,
        "output_path": "/lake/x.parquet",
        "error": None,
    }
    assert captured["season"] == 2026


def test_ingest_run_exits_one_on_failed(monkeypatch):
    monkeypatch.setattr(
        "ffh.ingest.games.NfldataGamesJob.run",
        lambda self, session, lake_root: IngestRunResult(status="failed", error="boom"),
    )
    monkeypatch.setattr("ffh.cli._session_scope", _fake_session_scope)
    result = runner.invoke(app, ["ingest", "run", "nfldata_games"])
    assert result.exit_code == 1
    assert "boom" in result.stdout


def test_ingest_run_exits_zero_on_skipped(monkeypatch):
    monkeypatch.setattr(
        "ffh.ingest.nflverse.NflversePbpJob.run",
        lambda self, session, lake_root: IngestRunResult(status="skipped", error="404"),
    )
    monkeypatch.setattr("ffh.cli._session_scope", _fake_session_scope)
    result = runner.invoke(app, ["ingest", "run", "nflverse_pbp", "--season", "2026"])
    assert result.exit_code == 0
    assert "skipped" in result.stdout


def test_ingest_run_defaults_season_to_settings(monkeypatch):
    seen = {}

    def fake_run(self, session, lake_root):
        seen["season"] = self.season
        return IngestRunResult(status="success", rows_written=1)

    monkeypatch.setattr("ffh.ingest.nflverse.NflverseInjuriesJob.run", fake_run)
    monkeypatch.setattr("ffh.cli._session_scope", _fake_session_scope)
    runner.invoke(app, ["ingest", "run", "nflverse_injuries"])
    assert seen["season"] == 2026  # Settings.season default


@pytest.mark.db
def test_ingest_seed_creates_teams_stadiums_and_generic_league(monkeypatch, tmp_path):
    from sqlalchemy import select

    from ffh.db.models import GENERIC_LEAGUE_ID, League, NflTeam

    calls = []
    monkeypatch.setattr(
        "ffh.ingest.reference.StadiumsJob.run",
        lambda self, session, lake_root: calls.append("stadiums")
        or IngestRunResult(status="success", rows_written=62),
    )
    result = runner.invoke(app, ["ingest", "seed"])
    assert result.exit_code == 0, result.stdout
    assert calls == ["stadiums"]

    from ffh.config import get_settings
    from ffh.db.engine import make_engine, make_session_factory

    with make_session_factory(make_engine(get_settings().test_database_url))() as session:
        assert len(list(session.scalars(select(NflTeam)))) == 32
        assert session.get(League, GENERIC_LEAGUE_ID) is not None


class _FakeSession:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def commit(self):
        return None

    def rollback(self):
        return None


def _fake_session_scope():
    return _FakeSession()
```

> Implementation note for the test: `_session_scope` must be a module-level function in
> `ffh.cli` returning a context manager yielding a `Session`, so tests can monkeypatch it
> without a live database. **PRs ④ (`crosswalk seed|verify|report`) and ⑤ (`league load`)
> reuse this exact helper and its imports after rebasing onto ③** — keep the name and
> keep it module-level; ③ owns these lines of `cli.py`. The `db`-marked seed test does **not** patch it and therefore
> runs against `FFH_DATABASE_URL`; set `FFH_DATABASE_URL` to the `_test` database for that
> test via `monkeypatch.setenv` if your compose default points at `ffh`.

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_cli_ingest.py -v`
Expected: FAIL — `ingest list` prints `no ingest jobs registered`, and `ffh.cli._session_scope` does not exist.

- [ ] **Step 3: Rewrite the ingest group in `backend/src/ffh/cli.py`**

Replace the placeholder `ingest_list` command with the block below. `import typer`,
`from ffh import __version__` and the `app`/`ingest_app`/`league_app`/`crosswalk_app`
definitions already exist in `cli.py` from PR ① — **do not duplicate them**; add only the
imports that are new (`json`, `Iterator`, `contextmanager`, `Session`, `get_settings`, the
`ffh.db.engine` and `ffh.ingest.*` lines). Leave `version`, `league_platforms` and
`crosswalk_report` untouched. `ruff check` will flag any duplicate or unused import.

```python
import json
from collections.abc import Iterator
from contextlib import contextmanager

import typer
from sqlalchemy.orm import Session

from ffh import __version__
from ffh.config import get_settings
from ffh.db.engine import make_engine, make_session_factory

# Importing the job modules registers every @register-decorated class in ffh.ingest.base.JOBS.
from ffh.ingest import games as _games  # noqa: F401
from ffh.ingest import nflverse as _nflverse  # noqa: F401
from ffh.ingest import reference as _reference  # noqa: F401
from ffh.ingest.base import JOBS, STATUS_FAILED, get_job
from ffh.ingest.reference import StadiumsJob, seed_generic_league, seed_nfl_teams


@contextmanager
def _session_scope() -> Iterator[Session]:
    """One sync session per CLI invocation. Patched out in unit tests."""
    engine = make_engine()
    factory = make_session_factory(engine)
    session = factory()
    try:
        yield session
    finally:
        session.close()
        engine.dispose()


@ingest_app.command("list")
def ingest_list() -> None:
    """List registered ingest jobs."""
    for name in sorted(JOBS):
        cls = JOBS[name]
        scope = "seasonal" if cls.seasonal else "static"
        typer.echo(f"{name}\t{cls.source}/{cls.asset}\t{scope}")


@ingest_app.command("run")
def ingest_run(
    job: str = typer.Argument(..., help="Job name; see `ffh ingest list`."),
    season: int | None = typer.Option(None, "--season", help="Defaults to FFH_SEASON."),
) -> None:
    """Run one ingest job. Exits non-zero only when the run FAILED."""
    settings = get_settings()
    try:
        cls = get_job(job)
    except KeyError as exc:
        raise typer.BadParameter(str(exc)) from exc

    with _session_scope() as session:
        result = cls(season=season or settings.season).run(session, settings.lake_root)

    typer.echo(
        json.dumps(
            {
                "job": job,
                "status": result.status,
                "rows_written": result.rows_written,
                "output_path": result.output_path,
                "error": result.error,
            }
        )
    )
    if result.status == STATUS_FAILED:
        raise typer.Exit(1)


@ingest_app.command("seed")
def ingest_seed() -> None:
    """Seed nfl_teams, stadiums, and the sentinel generic league. Idempotent."""
    settings = get_settings()
    with _session_scope() as session:
        teams = seed_nfl_teams(session)
        session.commit()
        stadium_result = StadiumsJob().run(session, settings.lake_root)
        league_id = seed_generic_league(session)
        session.commit()

    typer.echo(
        json.dumps(
            {
                "nfl_teams": teams,
                "stadiums_status": stadium_result.status,
                "stadiums_rows": stadium_result.rows_written,
                "generic_league_id": str(league_id),
            }
        )
    )
    if stadium_result.status == STATUS_FAILED:
        raise typer.Exit(1)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_cli_ingest.py tests/test_cli.py -v`
Expected: all pass — including the pre-existing `test_subcommand_groups_exist`.

- [ ] **Step 5: Lint and commit**

```bash
cd backend && uv run ruff check . && uv run ruff format .
cd .. && git add backend/src/ffh/cli.py backend/tests/test_cli_ingest.py
git commit -m "feat(cli): ffh ingest run/list/seed replacing the PR-① placeholder"
```

---

### Task 9: Recorded fixtures, the recorder script, and fixture-backed integration tests

**Files:**
- Create: `backend/scripts/record_nflverse_fixtures.py`
- Create: `backend/tests/fixtures/nflverse/{players,stats_player_week,snap_counts,depth_charts,injuries,pbp}.parquet`
- Create: `backend/tests/fixtures/nfldata/games_sample.csv`
- Create: `backend/tests/fixtures/stadiums/stadiums.csv`
- Test: `backend/tests/ingest/test_fixture_schemas.py`
- Test: `backend/tests/ingest/test_framework_e2e.py`

**Interfaces:**
- Consumes: every job class from Tasks 4–6; `ffh.features.duck.connect`; `respx`.
- Produces: `backend/tests/fixtures/**` and `ffh` has no new runtime API.

- [ ] **Step 1: Write `backend/scripts/record_nflverse_fixtures.py`**

```python
"""Record the small committed test fixtures from the live sources. Run by hand:

    cd backend && uv run python scripts/record_nflverse_fixtures.py

Never run in CI. Re-run when `test_fixture_schemas.py` fails, then review the diff: a
column that disappeared upstream is a real incident, not a fixture to rubber-stamp.
"""

import io
import sys
from pathlib import Path

import polars as pl

from ffh.ingest.games import NfldataGamesJob
from ffh.ingest.http import Fetched, NotFound, get_bytes, make_client
from ffh.ingest.nflverse import (
    NflverseDepthChartsJob,
    NflverseInjuriesJob,
    NflversePbpJob,
    NflversePlayersJob,
    NflverseSnapCountsJob,
    NflverseStatsPlayerWeekJob,
)
from ffh.ingest.reference import StadiumsJob

FIXTURES = Path(__file__).resolve().parents[1] / "tests" / "fixtures"
ROWS = 50

#: Seasonal assets 404 before Week 1, so record from the last completed season.
NFLVERSE_JOBS = [
    (NflversePlayersJob, None),
    (NflverseStatsPlayerWeekJob, 2025),
    (NflverseSnapCountsJob, 2025),
    (NflverseDepthChartsJob, 2026),
    (NflverseInjuriesJob, 2025),
    (NflversePbpJob, 2025),
]


def _write_parquet_fixture(job, content: bytes) -> None:
    df = pl.read_parquet(io.BytesIO(content)).head(ROWS)
    out = FIXTURES / "nflverse" / f"{job.asset}.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(out, compression="zstd")
    print(f"  wrote {out.relative_to(FIXTURES.parent)} ({df.height} rows x {df.width} cols)")


def _write_games_fixture(content: bytes) -> None:
    """Slice the RAW TEXT — a Polars round trip would turn the quoted empty roof into NULL."""
    lines = content.decode().splitlines()
    header, body = lines[0], lines[1:]
    keep = [ln for ln in body if ln.split(",")[1] == "2026" and ln.split(",")[3] in {"1", "2"}]
    keep += [ln for ln in body if ln.split(",")[1] == "2025" and ln.split(",")[3] == "1"]
    out = FIXTURES / "nfldata" / "games_sample.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join([header, *keep]) + "\n", encoding="utf-8")
    assert any('"",' in ln for ln in keep), "expected at least one quoted-empty roof row"
    print(f"  wrote {out.relative_to(FIXTURES.parent)} ({len(keep)} rows)")


def main() -> int:
    with make_client() as client:
        for job_cls, season in NFLVERSE_JOBS:
            job = job_cls(season=season)
            print(f"{job_cls.name} -> {job.url()}")
            result = get_bytes(client, job.url())
            if isinstance(result, NotFound):
                print("  404 - skipped (expected for seasonal assets before Week 1)")
                continue
            assert isinstance(result, Fetched)
            _write_parquet_fixture(job_cls, result.content)

        games = get_bytes(client, NfldataGamesJob(season=2026).url())
        assert isinstance(games, Fetched)
        _write_games_fixture(games.content)

        stadiums = get_bytes(client, StadiumsJob().url())
        assert isinstance(stadiums, Fetched)
        out = FIXTURES / "stadiums" / "stadiums.csv"
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_bytes(stadiums.content)
        print(f"  wrote {out.relative_to(FIXTURES.parent)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 2: Record the fixtures**

```bash
cd backend && uv run python scripts/record_nflverse_fixtures.py
```
Expected output (as of 2026-08-16): `players` and `depth_charts` write; the 2025-season
assets write; `games_sample.csv` and `stadiums.csv` write. Confirm the parquet files are
each well under 1 MB (`ls -la tests/fixtures/nflverse`) and that
`git status --short tests/fixtures` lists them as untracked (proving the `.gitignore`
negation from Task 6 works). If they do **not** appear, re-check Task 6 Step 2.

- [ ] **Step 3: Write `backend/tests/ingest/test_fixture_schemas.py`**

```python
"""Schema-drift canary: the recorded real assets must still satisfy REQUIRED_COLUMNS."""

from pathlib import Path

import polars as pl
import pytest

from ffh.ingest.games import NfldataGamesJob
from ffh.ingest.nflverse import (
    NflverseDepthChartsJob,
    NflverseInjuriesJob,
    NflversePbpJob,
    NflversePlayersJob,
    NflverseSnapCountsJob,
    NflverseStatsPlayerWeekJob,
)
from ffh.ingest.reference import StadiumsJob

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"

PARQUET_JOBS = [
    NflversePlayersJob,
    NflverseStatsPlayerWeekJob,
    NflverseSnapCountsJob,
    NflverseDepthChartsJob,
    NflverseInjuriesJob,
    NflversePbpJob,
]


@pytest.mark.parametrize("cls", PARQUET_JOBS, ids=lambda c: c.name)
def test_recorded_fixture_satisfies_required_columns(cls):
    path = FIXTURES / "nflverse" / f"{cls.asset}.parquet"
    assert path.exists(), (
        f"missing fixture {path}; run `uv run python scripts/record_nflverse_fixtures.py`"
    )
    columns = set(pl.read_parquet_schema(path))
    missing = sorted(cls.REQUIRED_COLUMNS - columns)
    assert not missing, f"{cls.name}: upstream dropped {missing} - investigate before editing"


@pytest.mark.parametrize("cls", PARQUET_JOBS, ids=lambda c: c.name)
def test_recorded_fixture_passes_validate(cls):
    df = pl.read_parquet(FIXTURES / "nflverse" / f"{cls.asset}.parquet")
    cls(season=2025).validate(df)


def test_games_fixture_passes_validate_and_keeps_the_quoted_empty_roof():
    raw = (FIXTURES / "nfldata" / "games_sample.csv").read_bytes()
    df = NfldataGamesJob(season=2026).parse(raw)
    NfldataGamesJob(season=2026).validate(df)
    assert (df["roof"] == "").sum() > 0, "the quoted-empty roof rows must survive recording"


def test_stadiums_fixture_passes_validate_and_has_62_rows():
    raw = (FIXTURES / "stadiums" / "stadiums.csv").read_bytes()
    df = StadiumsJob().parse(raw)
    StadiumsJob().validate(df)
    assert df.height >= 60
    assert df["stadium_id"].n_unique() == df.height


def test_stats_player_week_fixture_has_150_columns():
    schema = pl.read_parquet_schema(FIXTURES / "nflverse" / "stats_player_week.parquet")
    assert len(schema) == 150, "DATA_SOURCES.md §1 records 150 columns"
```

- [ ] **Step 4: Write `backend/tests/ingest/test_framework_e2e.py`**

```python
"""End-to-end: real job classes, respx-mocked servers, real Postgres, real lake on tmp_path."""

from pathlib import Path

import httpx
import polars as pl
import pytest
import respx
from sqlalchemy import select

from ffh.db.models import Game, IngestRun
from ffh.features.duck import connect
from ffh.ingest.games import GAMES_CSV_URL, NfldataGamesJob
from ffh.ingest.nflverse import NflversePbpJob, NflversePlayersJob, NflverseStatsPlayerWeekJob
from ffh.ingest.reference import STADIUMS_CSV_URL, StadiumsJob, seed_nfl_teams

pytestmark = pytest.mark.db

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"
PLAYERS_URL = NflversePlayersJob().url()


def _runs(session, source):
    return list(
        session.scalars(
            select(IngestRun).where(IngestRun.source == source).order_by(IngestRun.started_at)
        )
    )


@respx.mock
def test_players_run_twice_lands_one_partition_and_two_runs(db_session, tmp_path: Path):
    body = (FIXTURES / "nflverse" / "players.parquet").read_bytes()
    respx.get(PLAYERS_URL).mock(
        side_effect=[
            httpx.Response(200, content=body, headers={"ETag": '"players-v1"'}),
            httpx.Response(304),
        ]
    )

    first = NflversePlayersJob().run(db_session, tmp_path)
    second = NflversePlayersJob().run(db_session, tmp_path)

    assert first.status == "success" and first.rows_written == 50
    assert second.status == "skipped_not_modified"
    assert [r.status for r in _runs(db_session, "nflverse")] == [
        "success",
        "skipped_not_modified",
    ]
    assert len(list((tmp_path / "raw" / "nflverse" / "players").rglob("*.parquet"))) == 1


@respx.mock
def test_conditional_request_carries_the_stored_etag(db_session, tmp_path: Path):
    body = (FIXTURES / "nflverse" / "players.parquet").read_bytes()
    route = respx.get(PLAYERS_URL).mock(
        side_effect=[
            httpx.Response(200, content=body, headers={"ETag": '"players-v1"'}),
            httpx.Response(304),
        ]
    )
    NflversePlayersJob().run(db_session, tmp_path)
    NflversePlayersJob().run(db_session, tmp_path)
    assert route.calls[1].request.headers["if-none-match"] == '"players-v1"'


@respx.mock
def test_seasonal_404_is_skipped_not_failed(db_session, tmp_path: Path):
    respx.get(NflversePbpJob(season=2026).url()).mock(return_value=httpx.Response(404))
    result = NflversePbpJob(season=2026).run(db_session, tmp_path)
    assert result.status == "skipped"
    assert not list(tmp_path.rglob("*.parquet"))


@respx.mock
def test_stats_player_week_404_is_skipped_before_week_one(db_session, tmp_path: Path):
    # Verified live 2026-08-16: this asset 404s until Week 1, exactly like pbp.
    respx.get(NflverseStatsPlayerWeekJob(season=2026).url()).mock(
        return_value=httpx.Response(404)
    )
    assert NflverseStatsPlayerWeekJob(season=2026).run(db_session, tmp_path).status == "skipped"


@respx.mock
def test_validate_failure_is_recorded_as_failed(db_session, tmp_path: Path):
    import io

    buf = io.BytesIO()
    pl.DataFrame({"unexpected": [1]}).write_parquet(buf)
    respx.get(PLAYERS_URL).mock(return_value=httpx.Response(200, content=buf.getvalue()))

    result = NflversePlayersJob().run(db_session, tmp_path)
    assert result.status == "failed"
    assert "gsis_id" in result.error
    (run,) = _runs(db_session, "nflverse")
    assert run.status == "failed" and run.rows_written is None and run.output_path is None


@respx.mock
def test_games_job_lands_parquet_and_upserts_postgres(db_session, tmp_path: Path):
    seed_nfl_teams(db_session)
    respx.get(STADIUMS_CSV_URL).mock(
        return_value=httpx.Response(
            200, content=(FIXTURES / "stadiums" / "stadiums.csv").read_bytes()
        )
    )
    assert StadiumsJob().run(db_session, tmp_path).status == "success"

    respx.get(GAMES_CSV_URL).mock(
        return_value=httpx.Response(
            200,
            content=(FIXTURES / "nfldata" / "games_sample.csv").read_bytes(),
            headers={"ETag": '"games-v1"'},
        )
    )
    result = NfldataGamesJob(season=2026).run(db_session, tmp_path)
    assert result.status == "success"

    games = list(db_session.scalars(select(Game)))
    assert games, "the fixture must contain 2026 rows"
    assert all(g.season == 2026 for g in games)
    assert all(g.kickoff_at is not None for g in games)
    # The 100% stadium join is asserted inside upsert_games; reaching here proves it held.
    assert all(g.stadium_id is not None for g in games)


@respx.mock
def test_duckdb_queries_the_landed_fixture(db_session, tmp_path: Path):
    body = (FIXTURES / "nflverse" / "players.parquet").read_bytes()
    respx.get(PLAYERS_URL).mock(return_value=httpx.Response(200, content=body))
    NflversePlayersJob().run(db_session, tmp_path)

    con = connect(tmp_path, season=2026)
    try:
        assert con.execute("SELECT count(*) FROM players").fetchone()[0] == 50
        assert con.execute("SELECT count(*) FROM players WHERE gsis_id IS NOT NULL").fetchone()[
            0
        ] > 0
    finally:
        con.close()
    assert not list(tmp_path.rglob("*.duckdb"))
```

- [ ] **Step 5: Run the whole suite**

Run from `backend/`: `uv run pytest -v`
Expected: everything green, including PR ①/② tests. The `network` marker keeps the
recorder off CI; confirm with `uv run pytest --collect-only -q | tail -3` that no test
performs a live request.

- [ ] **Step 6: Lint and commit**

```bash
cd backend && uv run ruff check . && uv run ruff format .
cd .. && git add backend/scripts/record_nflverse_fixtures.py backend/tests/fixtures backend/tests/ingest/test_fixture_schemas.py backend/tests/ingest/test_framework_e2e.py
git commit -m "test(ingest): recorded nflverse fixtures, schema-drift canary, and e2e run tests"
```

---

### Task 10: Docs — `DATA_SOURCES.md`, `DATABASE.md`, `ROADMAP.md`, and the PR body

**Files:**
- Modify: `docs/DATA_SOURCES.md`
- Modify: `docs/DATABASE.md`
- Modify: `docs/ROADMAP.md`

- [ ] **Step 1: `docs/DATA_SOURCES.md` §1 — replace the "Caveats" bullet list with this**

```markdown
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
```

- [ ] **Step 2: `docs/DATA_SOURCES.md` §2 — append after the "Game totals and spreads" paragraph**

```markdown
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
```

- [ ] **Step 3: `docs/DATA_SOURCES.md` §4 — append after the stadium-coordinates paragraph**

```markdown
**Verified live 2026-08-16:** 62 rows × 35 columns, `stadium_id` unique. The columns this
project uses are `stadium_id, stadium_name, lat, lon, altitude, heading, surface_type,
roof_type, tz` — all non-null for the 30 stadiums referenced by 2026 games, and all 30 join
(0 unmatched, confirming the 30/30 claim).

⚠️ **`altitude` is in METRES, not feet.** `DEN00` = 1583.586 (Mile High is 1,609 m /
5,280 ft); `SEA00` = 5.214 (sea level). `DATABASE.md` §2 declares `stadiums.altitude_ft
INTEGER`, so `ffh.ingest.reference.seed_stadiums` multiplies by 3.280839895 and rounds.
`surface_type` ∈ {`Grass`, `Turf`}; `roof_type` ∈ {`Dome`, `Outdoors`}; `tz` is a valid
IANA name. The column is `stadium_name`, not `name`.
```

- [ ] **Step 4: `docs/DATABASE.md` §1 — replace the "Lake layout" code block with the paths ingest actually emits**

```markdown
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
```

- [ ] **Step 5: `docs/DATABASE.md` §2 — append one line under the `stadiums` DDL block**

```markdown
*Phase 0 note:* greerreNFL's `altitude` column is **metres**; `altitude_ft` is populated by
`ffh.ingest.reference.seed_stadiums` as `round(altitude * 3.280839895)`. Its `name` comes
from the upstream `stadium_name` column.
```

- [ ] **Step 6: `docs/DATABASE.md` §6 — update the sentinel paragraph to point at the shipped function**

Change "PR ③ seeds it via `seed_generic_league(session)` in
`backend/src/ffh/ingest/reference.py` (`seed_nfl_teams`' companion)" to:

```markdown
Because of the FK, a sentinel `leagues` row must exist. It is seeded by
`ffh.ingest.reference.seed_generic_league(session)` (shipped in PR ③, idempotent via
`ON CONFLICT (league_id) DO NOTHING`) and run by `ffh ingest seed` alongside `nfl_teams`
and `stadiums`, with exactly this row:
```

Then, directly **after** the sentinel row block in §6, add this note (PR ⑤ adds the matching
one under the `leagues` DDL in §4):

```markdown
*`roster_settings` has two shapes.* The sentinel row stores a **count map**
(`GENERIC_ROSTER`: `{"QB": 1, "RB": 2, "WR": 2, "TE": 1, "FLEX": 1, "K": 1, "DST": 1,
"BN": …}`). Platform-loaded leagues (`ffh.ingest.platform_sync`, PR ⑤) store
`RosterSettings.model_dump()` — `{"starters": [...], "bench", "ir", "taxi",
"flex_composition", "is_superflex"}`. Consumers must branch on shape (e.g.
`"starters" in roster_settings`), never assume one.
```

- [ ] **Step 7: `docs/ROADMAP.md` — tick two Phase 0 boxes**

④ (crosswalk) and ⑤ (Sleeper adapter) tick the adjacent Phase 0 lines in the same block;
whichever PR lands second gets a trivial rebase conflict here — **keep every tick**.

Change:
```markdown
- [ ] nflverse ingest → Parquet lake (players, stats_player_week, snap counts, depth
      charts, injuries). ⚠️ `nflreadpy`, **not** `nfl_data_py` (archived)
- [ ] `nfldata/games.csv` ingest → schedule + Vegas lines + roof state
```
to:
```markdown
- [x] nflverse ingest → Parquet lake (players, stats_player_week, snap counts, depth
      charts, injuries, pbp). Release Parquet URLs read directly with httpx + Polars —
      **no `nflreadpy`, no `nfl_data_py`** (archived)
- [x] `nfldata/games.csv` ingest → schedule + Vegas lines + roof state
```

- [ ] **Step 8: Full verification before handing off**

```bash
cd backend && uv run ruff check . && uv run ruff format --check . && uv run pytest -v
```
Expected: green. Then confirm the working tree contains only intended files:
`git status --short` should show nothing untracked outside `backend/tests/fixtures`.

- [ ] **Step 9: Commit the docs**

```bash
git add docs/DATA_SOURCES.md docs/DATABASE.md docs/ROADMAP.md
git commit -m "docs(data): record live-verified nflverse 404s, games.csv roof/ET quirks, stadium altitude units"
```

- [ ] **Step 10: Stop — do NOT push and do NOT open the PR**

The controlling session pushes the branch, opens the PR and runs the Codex gate. Report
completion with the commit list (`git log --oneline main..HEAD`) and paste the PR body
below into your final message so the controller can use it verbatim.

```markdown
## Summary
- `ffh.ingest.base` — `IngestJob` ABC + registry; every run writes one `ingest_runs` row
  (`running` → `success|failed|skipped_not_modified|skipped`) with etag, rows, path, error
- `ffh.ingest.http` — one shared httpx client, tenacity retry (429/5xx, max 5, honours
  `Retry-After`), `If-None-Match` from the last successful run's etag, 404 → `NotFound`
- `ffh.ingest.lake` — hive partition paths and a writer that refuses to overwrite
- Six nflverse jobs + `nfldata_games` + `stadiums`; `nfl_teams` and the sentinel generic
  league seeded from `ffh ingest seed`
- `ffh.features.duck.connect()` — in-memory DuckDB views over the latest lake partitions
- CLI: `ffh ingest list | run <job> [--season] | seed`

## Live verification (2026-08-16)
- ⚠️ **All per-season nflverse assets 404 before Week 1, not just pbp** —
  `stats_player_week_2026`, `snap_counts_2026`, `injuries_2026`, `play_by_play_2026` all
  404; `players` and `depth_charts_2026` are 200. Every seasonal job maps 404 → `skipped`.
- ⚠️ **`games.csv` `roof` is the quoted empty string `""`, not NULL**, for retractable
  stadiums with unplayed games (43/272 in 2026). Mapped to NULL explicitly.
- ⚠️ **greerreNFL `altitude` is metres**, not feet (DEN00 = 1583.6 m). Converted.
- ⚠️ **Neutral-site games carry the nominal home team's `stadium_id`** (8 in 2026), so the
  30/30 join succeeds but the coordinates are wrong for those rows.
- `gameday`/`gametime` are Eastern wall-clock → converted to UTC via `America/New_York`.
- `stats_player_week` keys on `player_id` (a GSIS id); `snap_counts` has no GSIS id at all.
- All findings written into `DATA_SOURCES.md` / `DATABASE.md` in this PR.

## Tests
Framework idempotency (two runs → one partition, two `ingest_runs` rows, second
`skipped_not_modified` on 304), 404 → `skipped`, validate failure → `failed` with the
missing column named, per-job `validate` against recorded fixtures, a schema-drift canary,
`upsert_games` idempotency + `updated_at` bump, 100 % stadium coverage with the unmatched
list in the error, and a DuckDB query over the landed fixture. No new dependencies.

Spec: docs/superpowers/specs/2026-08-15-phase0-foundation-design.md §3
Plan: docs/superpowers/plans/2026-08-15-phase0-03-ingest-nflverse-games.md
```

- [ ] **Step 11: Codex adversarial review hunt list** (Chris runs it, per `AGENTS.md`)

Point Codex at: the ET→UTC conversion (DST boundary, the Melbourne 20:35 row); the `""`
roof mapping; `updated_at` in the `ON CONFLICT SET`; the anti-join row-count assertion in
`assert_stadium_coverage`; whether `run()` can leave an `ingest_runs` row stuck at
`running`; whether a retry can re-fire the non-idempotent `persist()`; and the
`latest_partition` tie-break. Resolve every BLOCKING finding or rebut it in writing.
