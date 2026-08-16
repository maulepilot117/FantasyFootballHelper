# PR ④ `feat/crosswalk` — Player ID Crosswalk Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship the player ID crosswalk — the single highest-risk component in the system — as `ffh.crosswalk`: name/team/position normalization, the `players` registry seeded from nflverse, DynastyProcess ids applied to `player_external_ids`, the five-rung resolution ladder (`resolve` / `resolve_many`) that PRs ⑤ and ⑥ consume, a coverage report + CLI, and the two mandatory invariant tests.

**Architecture:** Everything except the last task is *pure of `ffh.ingest`*: functions take a `Session` and Polars `DataFrame`s, never a URL or lake path, so they are testable from hand-built fixture frames with no network. `normalize.py` is pure Python (no DB). `registry.py` upserts `players` from the nflverse players frame. `dynastyprocess.py` fans the DynastyProcess CSV out into `player_external_ids` at rung 1. `resolve.py` walks the ladder strictly in order, persists every rung-3/4 result so the next call is a rung-1 hit, and writes `crosswalk_unmatched` at rung 5. `report.py` + `review.py` back `ffh crosswalk report|seed|verify`. The single `IngestJob` (fetch CSV → lake) is the last task and depends on PR ③.

**Tech Stack:** Python 3.13 · Polars 1.43.2 · rapidfuzz 3.14.5 (`JaroWinkler.normalized_similarity`) · SQLAlchemy 2.0.51 (sync `Session`, `insert(...).on_conflict_do_update`) · Alembic 1.19.0 · structlog 26.1.0 · typer 0.27.1 · pytest 9.1.1 (+ respx for the ingest job test only). **No new dependencies.**

**Spec:** `docs/superpowers/specs/2026-08-15-phase0-foundation-design.md` §4 · authoritative requirements: `docs/DATABASE.md` §2–§3 · overview and locked scope: `docs/superpowers/plans/2026-08-15-phase0-00-overview.md` (section "④ feat/crosswalk").

## Global Constraints

- **Never silently drop a row.** Every filter/join in this package asserts `kept + dropped == input` and logs the dropped count with a breakdown; every unresolved id lands in `crosswalk_unmatched` (DATABASE.md §3 rung 5, ARCHITECTURE.md "Known operational hazards", AGENTS.md Tier 1).
- Polars-native. Never `import pandas`, `nfl_data_py`, or `nflreadpy` (ruff bans them). Every Polars join asserts row counts or passes `validate=`.
- Jaro-Winkler comes from `rapidfuzz.distance.JaroWinkler` (already pinned 3.14.5). **No new dependencies.** If one seems needed, stop and check the 7-day cooldown first — prefer none.
- No network in tests. Fixtures only (`backend/tests/fixtures/dynastyprocess/*.csv`, in-code Polars frames). Anything that hits the wire is `@pytest.mark.network` (never in CI).
- No SQLite, no `.duckdb` file. DB tests are `pytest.mark.db` against Postgres via the `db_session` rollback fixture from `backend/tests/conftest.py`.
- Modules `normalize`, `registry`, `dynastyprocess` (except the job class in Task 8), `resolve`, `report`, `review` have **no import from `ffh.ingest`**. Only Task 8 imports `ffh.ingest.*`, and Task 8 requires PR ③ merged. Task 7 Step 6 (the `cli.py` edit) also requires ③ merged: it reuses ③'s `_session_scope()` and `ffh.features.duck.latest_partition`. **Merge order is ③ → ④ → ⑤**; do Tasks 1–6 and Task 7 Steps 1–5 first, then rebase onto `main` once ③ has landed.
- `backend/src/ffh/cli.py` is shared with ③ and ⑤: ③ owns `_session_scope()` and the shared imports (`get_settings`, `make_engine`, `make_session_factory`, `Session`, `contextmanager`, `json`); ④ only adds its own imports and the `crosswalk_*` commands. `docs/ROADMAP.md` Phase 0 ticks are adjacent lines across ③/④/⑤ — expect a trivial rebase conflict there and **keep every tick**.
- Column names are the **live-verified** ones (2026-08-16): nflverse `players.parquet` and DynastyProcess `db_playerids.csv` — see the "Verified source columns" block below. Do not rename them from memory.
- Every `INSERT ... ON CONFLICT` on `players` sets `updated_at = now()` explicitly (DATABASE.md §2 note; ORM `onupdate` does not fire for upserts).
- Branch `feat/crosswalk` off `main` (after PR ② merged — `ef4b656`). Conventional commits scoped `crosswalk` / `db` / `test` / `docs`. Docs updated in-PR (DATABASE.md §2/§3, DATA_SOURCES.md §5, ROADMAP.md).
- Implementers do not push. The final task ends at "PR body ready"; Chris pushes, opens the PR, and runs the Codex adversarial review (`docs/WORKFLOW.md`).
- All commands below run from `backend/` unless a path says otherwise: `uv run pytest ...`, `uv run ruff check . && uv run ruff format --check .`.

### Verified source columns (live, 2026-08-16)

**nflverse `players/players.parquet`** — 25,033 rows × 39 cols. Columns used here:
`gsis_id` (String, non-null and unique for QB/RB/WR/TE/K/FB rows), `display_name` (e.g. `Kenneth Walker III`, `DJ Moore`, `Ja'Marr Chase`, `Amon-Ra St. Brown` — suffix is *inside* `display_name`; the separate `suffix` column is null for these), `first_name`, `last_name`, `position` (values seen: QB RB WR TE K FB P OT C G OL LS DL DT DE NT LB ILB MLB OLB DB CB S SAF FS), `birth_date` (**String** `YYYY-MM-DD`), `rookie_season` (Int32), `last_season` (Int32), `height` (Int32, inches), `weight` (Int32, lb), `college_name` (String, may be `"Michigan State; Wake Forest"`), `status` (ACT CUT RES DEV RSN NWT PUP RET RSR SUS EXE INA RLS — **not reliable for "currently active"**: a 1989 rookie is `ACT`), `latest_team` (nflverse abbreviations: ARI ATL BAL BUF CAR CHI CIN CLE DAL DEN DET GB HOU IND JAX KC **LA** LAC LV MIA MIN NE NO NYG NYJ PHI PIT SEA SF TB TEN WAS). Also present: `espn_id`, `pfr_id`, `draft_year`, `draft_round`, `draft_pick`, `draft_team`, `headshot`, `jersey_number`, `esb_id`, `nfl_id`, `pff_id`, `otc_id`, `smart_id`, `short_name`, `football_name`, `common_first_name`, `position_group`, `ngs_*`, `college_conference`, `years_of_experience`, `pff_position`, `pff_status`.

**DynastyProcess `files/db_playerids.csv`** — 12,472 rows × 35 cols, `NA` is the null sentinel:
`mfl_id, sportradar_id, fantasypros_id, gsis_id, pff_id, sleeper_id, nfl_id, espn_id, yahoo_id, fleaflicker_id, cbs_id, pfr_id, cfbref_id, rotowire_id, rotoworld_id, ktc_id, stats_id, stats_global_id, fantasy_data_id, swish_id, name, merge_name, position, team, birthdate, age, draft_year, draft_round, draft_pick, draft_ovr, twitter_username, height, weight, college, db_season`.
Gotchas verified in the live file: kickers are `PK` (not `K`); there are **no DST/DEF rows**; `team` uses **MFL codes** (`KCC TBB GBP NEP NOS SFO LVR LAR JAC` + historic `OAK SDC STL RAM`, `FA`, `FA*`); id columns must be read as **text** (`sportradar_id` is a UUID, `pfr_id` is alphanumeric, numeric ids must not become floats); 144 QB/RB/WR/TE/PK rows have `sleeper_id` but no `gsis_id` (2026 rookies / UDFAs); the file contains a handful of **duplicate-id glitches** among fantasy positions (e.g. two rows `Fred Williams`/`Kevin Smith` WR share every id including `gsis_id 00-0031320` but have different `rotowire_id`; `espn_id 2582138` and `pfr_id CartKy01` appear on two different TEs) — Task 4's ambiguity policy exists because of these.

---

## File structure

```
backend/src/ffh/crosswalk/
  __init__.py            (exists, stays empty)
  normalize.py           Task 1  normalize_name / ALIASES / TEAMS / normalize_team / normalize_dst / normalize_position
  registry.py            Task 3  prepare_players_frame / seed_players / seed_dst_players
  dynastyprocess.py      Task 4  read_playerids_csv / apply_playerids / CrosswalkApplyReport / CrosswalkConflictError
                         Task 8  DynastyProcessPlayerIdsJob (③ HttpIngestJob, @register; requires PR ③)
  resolve.py             Task 5  Resolution / ResolveInput / ResolveManyReport / resolve / resolve_many
  report.py              Task 7  CoverageReport / coverage_report
  review.py              Task 7  verify_mapping / reject_mapping
backend/src/ffh/db/models/reference.py   Task 2  Player.team_abbr
backend/alembic/versions/0002_players_team_abbr.py   Task 2
backend/src/ffh/cli.py                   Task 7  crosswalk report|seed|verify (reuses ③'s _session_scope; after ③)
                                         Task 8  eager import of ffh.crosswalk.dynastyprocess
backend/tests/crosswalk/
  __init__.py, conftest.py               Task 3  players_frame fixture, seeded_registry fixture
  test_normalize.py                      Task 1
  test_registry.py                       Task 3
  test_dynastyprocess.py                 Task 4
  test_resolve.py                        Task 5
  test_crosswalk_invariants.py           Task 6  the two mandatory tests
  test_report.py, test_cli_crosswalk.py  Task 7
  test_dynastyprocess_job.py             Task 8
backend/tests/fixtures/dynastyprocess/db_playerids_sample.csv   Task 4
docs/DATABASE.md, docs/DATA_SOURCES.md, docs/ROADMAP.md         Tasks 2, 9
```

Logging convention for every module in this PR: `log = structlog.get_logger(__name__)`; events are dotted strings (`crosswalk.resolve.exact`, `crosswalk.seed_players.dropped`), fields are keyword args.

---

### Task 1: `ffh.crosswalk.normalize` — names, teams, DST, positions

**Files:**
- Create: `backend/src/ffh/crosswalk/normalize.py`
- Create: `backend/tests/crosswalk/__init__.py` (empty)
- Create: `backend/tests/crosswalk/test_normalize.py`

**Interfaces:**
- Consumes: nothing (pure Python).
- Produces:
  - `SUFFIXES: frozenset[str]`, `ALIASES: dict[str, str]`, `TEAMS: tuple[tuple[str, str, str, tuple[str, ...]], ...]` (nflverse abbr, city, nickname, aliases — 32 rows), `FANTASY_POSITIONS: frozenset[str] = {"QB","RB","WR","TE","K","DST"}`.
  - `normalize_name(raw: str) -> str` — canonical person name; `""` for empty input.
  - `normalize_team(raw: str | None) -> str | None` — nflverse abbreviation (`"KC"`, `"LA"`, …) or `None`.
  - `normalize_dst(raw: str | None) -> str | None` — `"<abbr lowercase> dst"` (`"kc dst"`) or `None`.
  - `normalize_position(raw: str | None) -> str | None` — one of `FANTASY_POSITIONS` or `None`.
  - `dst_full_name(abbr: str) -> str` — `"Kansas City Chiefs DST"`.

- [ ] **Step 1: Write the failing tests `backend/tests/crosswalk/test_normalize.py`**

```python
import pytest

from ffh.crosswalk.normalize import (
    ALIASES,
    FANTASY_POSITIONS,
    TEAMS,
    dst_full_name,
    normalize_dst,
    normalize_name,
    normalize_position,
    normalize_team,
)

# Every example from DATABASE.md §3 is here (suffixes, D.J./DJ/D J, Ja'Marr, Amon-Ra,
# Robby/Robert, Cam/Cameron) plus the tricky real names from nflverse/DynastyProcess.
NAME_CASES = [
    ("Odell Beckham Jr.", "odell beckham"),
    ("Odell Beckham Jr", "odell beckham"),
    ("Odell Beckham", "odell beckham"),
    ("Kenneth Walker III", "kenneth walker"),
    ("KENNETH WALKER III", "kenneth walker"),
    ("Kenneth Walker", "kenneth walker"),
    ("Ken Walker", "kenneth walker"),
    ("Amon-Ra St. Brown", "amonra st brown"),
    ("Amon Ra St Brown", "amon ra st brown"),
    ("Ja'Marr Chase", "jamarr chase"),
    ("Ja’Marr Chase", "jamarr chase"),  # curly apostrophe
    ("Jamarr Chase", "jamarr chase"),
    ("D.J. Moore", "dj moore"),
    ("DJ Moore", "dj moore"),
    ("D J Moore", "dj moore"),
    ("A.J. Brown", "aj brown"),
    ("  A.J.   Brown ", "aj brown"),
    ("T.J. Hockenson", "tj hockenson"),
    ("Marvin Harrison Jr.", "marvin harrison"),
    ("Michael Pittman Jr.", "michael pittman"),
    ("Mike Pittman", "michael pittman"),
    ("Brian Robinson Jr.", "brian robinson"),
    ("Travis Etienne Jr.", "travis etienne"),
    ("Robby Anderson", "robert anderson"),
    ("Robbie Anderson", "robert anderson"),
    ("Robert Anderson", "robert anderson"),
    ("Cam Akers", "cameron akers"),
    ("Cameron Akers", "cameron akers"),
    ("Mitch Trubisky", "mitchell trubisky"),
    ("Mitchell Trubisky", "mitchell trubisky"),
    ("Patrick Mahomes II", "patrick mahomes"),
    ("Patrick Mahomes", "patrick mahomes"),
    ("Pat Mahomes", "patrick mahomes"),
    ("Josh Allen", "joshua allen"),
    ("Joshua Allen", "joshua allen"),
    ("Matt Stafford", "matthew stafford"),
    ("Chris Olave", "christopher olave"),
    ("Nick Chubb", "nicholas chubb"),
    ("Will Levis", "william levis"),
    ("Kenny Pickett", "kenneth pickett"),
    ("Tony Pollard", "anthony pollard"),
    ("Dan Campbell", "daniel campbell"),
    ("Gabe Davis", "gabriel davis"),
    ("Ulysses Bentley IV", "ulysses bentley"),
    ("Vinny Anthony II", "vinny anthony"),
    ("Larry Fitzgerald Sr.", "larry fitzgerald"),
    ("Kirk Cousins  ", "kirk cousins"),
    ("De'Von Achane", "devon achane"),
    ("Ray-Ray McCloud", "rayray mccloud"),
    ("Ja'Quinden Jackson", "jaquinden jackson"),
    ("Chig Okonkwo", "chig okonkwo"),
    ("Jr.", "jr"),  # a bare suffix is left alone — never return ""
    ("", ""),
]


@pytest.mark.parametrize(("raw", "expected"), NAME_CASES)
def test_normalize_name(raw: str, expected: str) -> None:
    assert normalize_name(raw) == expected


def test_normalize_name_is_idempotent() -> None:
    for raw, _ in NAME_CASES:
        once = normalize_name(raw)
        assert normalize_name(once) == once


def test_alias_table_has_required_entries() -> None:
    required = {
        "robby": "robert", "rob": "robert", "cam": "cameron", "mitch": "mitchell",
        "josh": "joshua", "mike": "michael", "matt": "matthew", "chris": "christopher",
        "nick": "nicholas", "pat": "patrick", "will": "william", "ken": "kenneth",
        "tony": "anthony", "dan": "daniel",
    }
    for k, v in required.items():
        assert ALIASES[k] == v
    assert len(ALIASES) >= 12
    # keys and values are already-normalized single tokens
    for k, v in ALIASES.items():
        assert k == k.lower() and " " not in k and v == v.lower() and " " not in v


DST_CASES = [
    ("KC", "kc dst"),
    ("KC DST", "kc dst"),
    ("KC D/ST", "kc dst"),
    ("Chiefs D/ST", "kc dst"),
    ("Chiefs DST", "kc dst"),
    ("Chiefs", "kc dst"),
    ("Kansas City", "kc dst"),
    ("Kansas City Chiefs", "kc dst"),
    ("Kansas City Chiefs DST", "kc dst"),
    ("Kansas City Chiefs Defense", "kc dst"),
    ("KAN", "kc dst"),  # PFR
    ("KCC", "kc dst"),  # MFL / DynastyProcess
    ("kc dst", "kc dst"),  # canonical form is a fixed point
    ("Los Angeles Rams", "la dst"),
    ("LA", "la dst"),  # nflverse abbreviation for the Rams
    ("LAR", "la dst"),  # Sleeper / ESPN / Yahoo
    ("Rams", "la dst"),
    ("St. Louis Rams", "la dst"),
    ("LA Chargers", "lac dst"),
    ("Chargers", "lac dst"),
    ("SD", "lac dst"),
    ("Las Vegas Raiders", "lv dst"),
    ("OAK", "lv dst"),
    ("LVR", "lv dst"),
    ("Washington Football Team", "was dst"),
    ("WSH", "was dst"),  # ESPN
    ("Commanders", "was dst"),
    ("49ers", "sf dst"),
    ("San Francisco 49ers D/ST", "sf dst"),
    ("Niners", "sf dst"),
    ("SFO", "sf dst"),
    ("Bucs", "tb dst"),
    ("TBB", "tb dst"),
    ("Tampa Bay Buccaneers", "tb dst"),
    ("GNB", "gb dst"),
    ("GBP", "gb dst"),
    ("Packers", "gb dst"),
    ("JAC", "jax dst"),
    ("Jaguars", "jax dst"),
    ("NY Giants", "nyg dst"),
    ("New York Giants", "nyg dst"),
    ("New York Jets", "nyj dst"),
    ("Jets D/ST", "nyj dst"),
    ("NWE", "ne dst"),
    ("Patriots", "ne dst"),
    ("NOR", "no dst"),
    ("Saints", "no dst"),
    ("New York", None),  # ambiguous city
    ("Los Angeles", None),  # ambiguous city
    ("Josh Allen", None),
    ("FA", None),
    ("FA*", None),
    ("", None),
    (None, None),
]


@pytest.mark.parametrize(("raw", "expected"), DST_CASES)
def test_normalize_dst(raw: str | None, expected: str | None) -> None:
    assert normalize_dst(raw) == expected


@pytest.mark.parametrize(("raw", "expected"), DST_CASES)
def test_normalize_team_matches_dst_table(raw: str | None, expected: str | None) -> None:
    abbr = normalize_team(raw)
    assert abbr == (expected.split()[0].upper() if expected else None)


def test_teams_table_is_complete_and_unique() -> None:
    abbrs = [t[0] for t in TEAMS]
    assert len(abbrs) == 32 and len(set(abbrs)) == 32
    assert set(abbrs) == {
        "ARI", "ATL", "BAL", "BUF", "CAR", "CHI", "CIN", "CLE", "DAL", "DEN", "DET", "GB",
        "HOU", "IND", "JAX", "KC", "LA", "LAC", "LV", "MIA", "MIN", "NE", "NO", "NYG", "NYJ",
        "PHI", "PIT", "SEA", "SF", "TB", "TEN", "WAS",
    }
    for abbr, _city, _nick, _aliases in TEAMS:
        assert normalize_team(abbr) == abbr
        assert normalize_dst(abbr) == f"{abbr.lower()} dst"


def test_dst_full_name() -> None:
    assert dst_full_name("KC") == "Kansas City Chiefs DST"
    assert dst_full_name("LA") == "Los Angeles Rams DST"


POSITION_CASES = [
    ("QB", "QB"), ("qb", "QB"), (" rb ", "RB"), ("WR", "WR"), ("TE", "TE"),
    ("K", "K"), ("PK", "K"),  # DynastyProcess kickers are PK
    ("FB", "RB"), ("HB", "RB"),  # fullbacks live in the RB pool
    ("DST", "DST"), ("DEF", "DST"), ("D/ST", "DST"), ("D ST", "DST"),
    ("OL", None), ("CB", None), ("P", None), ("LB", None), ("XX", None), ("", None), (None, None),
]


@pytest.mark.parametrize(("raw", "expected"), POSITION_CASES)
def test_normalize_position(raw: str | None, expected: str | None) -> None:
    assert normalize_position(raw) == expected
    if expected is not None:
        assert expected in FANTASY_POSITIONS
```

- [ ] **Step 2: Run to verify it fails** — `uv run pytest tests/crosswalk/test_normalize.py -q` → `ModuleNotFoundError: No module named 'ffh.crosswalk.normalize'`.

- [ ] **Step 3: Write `backend/src/ffh/crosswalk/normalize.py`**

```python
"""Name / team / DST / position normalization for the crosswalk (DATABASE.md §3).

Pure functions. No I/O, no ``ffh.db``, no ``ffh.ingest``. Both sides of every match
(registry rows and incoming ids) go through the same functions, so what matters is that
the output is deterministic — not that it looks like the "real" name.
"""

from __future__ import annotations

import re

# ---------------------------------------------------------------------------
# Person names
# ---------------------------------------------------------------------------

SUFFIXES: frozenset[str] = frozenset({"jr", "sr", "ii", "iii", "iv", "v"})

# Applied to the FIRST token only, after suffix stripping. Keys/values are normalized
# single tokens. Documented in DATABASE.md §3 ("alias table location").
ALIASES: dict[str, str] = {
    "robby": "robert",
    "robbie": "robert",
    "rob": "robert",
    "bob": "robert",
    "bobby": "robert",
    "cam": "cameron",
    "mitch": "mitchell",
    "josh": "joshua",
    "mike": "michael",
    "matt": "matthew",
    "chris": "christopher",
    "nick": "nicholas",
    "pat": "patrick",
    "will": "william",
    "ken": "kenneth",
    "kenny": "kenneth",
    "tony": "anthony",
    "dan": "daniel",
    "danny": "daniel",
    "dave": "david",
    "jim": "james",
    "jimmy": "james",
    "joe": "joseph",
    "zach": "zachary",
    "zack": "zachary",
    "ben": "benjamin",
    "gabe": "gabriel",
    "jon": "jonathan",
}

_DROP_CHARS_RE = re.compile(r"[.'’\-]")  # D.J. -> DJ, Ja'Marr -> JaMarr, Amon-Ra -> AmonRa
_NON_ALNUM_RE = re.compile(r"[^a-z0-9 ]")


def _merge_initials(tokens: list[str]) -> list[str]:
    """['d', 'j', 'moore'] -> ['dj', 'moore'] so 'D J Moore' == 'D.J. Moore' == 'DJ Moore'."""
    out: list[str] = []
    buf = ""
    for tok in tokens:
        if len(tok) == 1:
            buf += tok
        else:
            if buf:
                out.append(buf)
                buf = ""
            out.append(tok)
    if buf:
        out.append(buf)
    return out


def normalize_name(raw: str) -> str:
    """Lowercase; drop periods/apostrophes/hyphens; collapse whitespace; merge initials;
    strip trailing suffixes (Jr, Sr, II, III, IV, V); alias the first token."""
    s = _DROP_CHARS_RE.sub("", raw.lower().strip())
    s = _NON_ALNUM_RE.sub(" ", s)
    tokens = _merge_initials(s.split())
    while len(tokens) > 1 and tokens[-1] in SUFFIXES:
        tokens.pop()
    if tokens:
        tokens[0] = ALIASES.get(tokens[0], tokens[0])
    return " ".join(tokens)


# ---------------------------------------------------------------------------
# Teams and DST
# ---------------------------------------------------------------------------

# (nflverse abbr, city, nickname, extra aliases). Aliases cover MFL/DynastyProcess
# (KCC TBB GBP NEP NOS SFO LVR LAR JAC SDC STL RAM OAK), PFR (KAN GNB NWE NOR TAM SFO
# LVR SDG), ESPN (WSH LAR), Sleeper/Yahoo (LAR JAX) and common nicknames.
TEAMS: tuple[tuple[str, str, str, tuple[str, ...]], ...] = (
    ("ARI", "Arizona", "Cardinals", ("ARZ", "Cards")),
    ("ATL", "Atlanta", "Falcons", ()),
    ("BAL", "Baltimore", "Ravens", ("BLT",)),
    ("BUF", "Buffalo", "Bills", ()),
    ("CAR", "Carolina", "Panthers", ()),
    ("CHI", "Chicago", "Bears", ()),
    ("CIN", "Cincinnati", "Bengals", ()),
    ("CLE", "Cleveland", "Browns", ("CLV",)),
    ("DAL", "Dallas", "Cowboys", ()),
    ("DEN", "Denver", "Broncos", ()),
    ("DET", "Detroit", "Lions", ()),
    ("GB", "Green Bay", "Packers", ("GNB", "GBP")),
    ("HOU", "Houston", "Texans", ("HST",)),
    ("IND", "Indianapolis", "Colts", ()),
    ("JAX", "Jacksonville", "Jaguars", ("JAC", "Jags")),
    ("KC", "Kansas City", "Chiefs", ("KAN", "KCC")),
    ("LA", "Los Angeles", "Rams", ("LAR", "RAM", "STL", "St. Louis Rams", "LA Rams")),
    ("LAC", "Los Angeles", "Chargers", ("SD", "SDG", "SDC", "San Diego Chargers", "LA Chargers")),
    ("LV", "Las Vegas", "Raiders", ("LVR", "OAK", "Oakland Raiders")),
    ("MIA", "Miami", "Dolphins", ()),
    ("MIN", "Minnesota", "Vikings", ()),
    ("NE", "New England", "Patriots", ("NWE", "NEP", "Pats")),
    ("NO", "New Orleans", "Saints", ("NOR", "NOS")),
    ("NYG", "New York", "Giants", ("NY Giants",)),
    ("NYJ", "New York", "Jets", ("NY Jets",)),
    ("PHI", "Philadelphia", "Eagles", ()),
    ("PIT", "Pittsburgh", "Steelers", ()),
    ("SF", "San Francisco", "49ers", ("SFO", "Niners", "San Francisco Forty Niners")),
    ("SEA", "Seattle", "Seahawks", ()),
    ("TB", "Tampa Bay", "Buccaneers", ("TAM", "TBB", "Bucs")),
    ("TEN", "Tennessee", "Titans", ("OTI",)),
    ("WAS", "Washington", "Commanders", ("WSH", "Washington Football Team")),
)

# Tokens that mean "the defense" and carry no team information.
_DST_TOKENS: frozenset[str] = frozenset(
    {"dst", "def", "defense", "defence", "d", "st", "special", "teams", "team", "ds"}
)


def _clean(raw: str) -> str:
    s = raw.lower().replace("/", " ").replace("&", " ")
    s = re.sub(r"[^a-z0-9 ]", " ", s)
    return " ".join(s.split())


def _build_team_lookup() -> dict[str, str]:
    lookup: dict[str, str] = {}
    cities = [city for _, city, _, _ in TEAMS]
    for abbr, city, nickname, aliases in TEAMS:
        keys = [abbr, nickname, f"{city} {nickname}", *aliases]
        if cities.count(city) == 1:  # "New York" / "Los Angeles" alone are ambiguous
            keys.append(city)
        for key in keys:
            k = _clean(key)
            assert k not in lookup or lookup[k] == abbr, f"team alias collision: {key!r}"
            lookup[k] = abbr
    return lookup


_TEAM_LOOKUP: dict[str, str] = _build_team_lookup()
_TEAM_BY_ABBR: dict[str, tuple[str, str]] = {abbr: (city, nick) for abbr, city, nick, _ in TEAMS}


def normalize_team(raw: str | None) -> str | None:
    """Any team spelling → nflverse abbreviation, or None (unknown, FA, ambiguous city)."""
    if not raw:
        return None
    s = _clean(raw)
    if not s:
        return None
    if s in _TEAM_LOOKUP:
        return _TEAM_LOOKUP[s]
    stripped = " ".join(t for t in s.split() if t not in _DST_TOKENS)
    return _TEAM_LOOKUP.get(stripped)


def normalize_dst(raw: str | None) -> str | None:
    """Any DST spelling → canonical ``"<abbr lowercase> dst"`` (e.g. ``"kc dst"``), or None."""
    abbr = normalize_team(raw)
    return f"{abbr.lower()} dst" if abbr else None


def dst_full_name(abbr: str) -> str:
    city, nickname = _TEAM_BY_ABBR[abbr]
    return f"{city} {nickname} DST"


# ---------------------------------------------------------------------------
# Positions
# ---------------------------------------------------------------------------

FANTASY_POSITIONS: frozenset[str] = frozenset({"QB", "RB", "WR", "TE", "K", "DST"})

_POSITION_MAP: dict[str, str] = {
    "QB": "QB",
    "RB": "RB",
    "FB": "RB",
    "HB": "RB",
    "WR": "WR",
    "TE": "TE",
    "K": "K",
    "PK": "K",
    "DST": "DST",
    "DEF": "DST",
}


def normalize_position(raw: str | None) -> str | None:
    """'PK'→'K', 'FB'→'RB', 'DEF'/'D/ST'→'DST'; anything non-fantasy → None."""
    if not raw:
        return None
    key = re.sub(r"[^A-Z]", "", raw.upper())
    return _POSITION_MAP.get(key)
```

- [ ] **Step 4: Run to verify it passes** — `uv run pytest tests/crosswalk/test_normalize.py -q` → all pass (≈ 55 name cases + 55 DST/team cases + positions). If `test_normalize_name["Amon Ra St Brown"]` surprises you: hyphen removal joins `Amon-Ra`→`amonra`, but a space keeps `amon ra` — that asymmetry is intended and recorded in DATABASE.md §3 (Task 9); sources spell it with the hyphen.

- [ ] **Step 5: Lint and commit**

```bash
uv run ruff check src/ffh/crosswalk tests/crosswalk && uv run ruff format src/ffh/crosswalk tests/crosswalk
git add src/ffh/crosswalk/normalize.py tests/crosswalk/__init__.py tests/crosswalk/test_normalize.py
git commit -m "feat(crosswalk): name/team/DST/position normalization with alias and 32-team tables"
```

---

### Task 2: `players.team_abbr` — model + Alembic migration 0002 (rung-3 tie-breaker)

**Why (deviation from DATABASE.md §3, recorded in §2/§3 in this task and Task 9):** the ladder's rung 3 is "exact `(normalized_name, position, team)`", but `players` has no team column. Without one, `Marvin Harrison Jr.` (WR, ARI) and `Marvin Harrison` (WR, IND, 1996) collide on `("marvin harrison", "WR")` and can never resolve at rung 3. The minimal fix is a nullable `players.team_abbr TEXT` (nflverse `latest_team`, refreshed by every `seed_players` run) used **only** as a tie-breaker inside the crosswalk — never as roster truth (rosters come from platforms).

**Files:**
- Modify: `backend/src/ffh/db/models/reference.py` (class `Player`, after `status`)
- Create: `backend/alembic/versions/0002_players_team_abbr.py`
- Modify: `backend/tests/db/test_models_reference.py` (`test_players_table_shape`)
- Modify: `docs/DATABASE.md` §2 `players` DDL

**Interfaces:**
- Produces: `Player.team_abbr: Mapped[str | None]` — used by Tasks 3, 4, 5.

- [ ] **Step 1: Extend the failing test** — in `backend/tests/db/test_models_reference.py::test_players_table_shape` add:

```python
    assert c["team_abbr"].nullable  # crosswalk rung-3 tie-breaker; DATABASE.md §2 Phase 0 note
```

- [ ] **Step 2: Run to verify it fails** — `uv run pytest tests/db/test_models_reference.py::test_players_table_shape -q` → `KeyError: 'team_abbr'`.

- [ ] **Step 3: Add the column to the model** — in `backend/src/ffh/db/models/reference.py`, class `Player`, directly after the `status` line:

```python
    status: Mapped[str | None] = mapped_column(Text)
    # Phase 0 addition (DATABASE.md §2 note): nflverse latest_team, refreshed by
    # ffh.crosswalk.registry.seed_players. Crosswalk rung-3 tie-breaker ONLY — never roster truth.
    team_abbr: Mapped[str | None] = mapped_column(Text)
```

- [ ] **Step 4: Write the migration `backend/alembic/versions/0002_players_team_abbr.py`**

```python
"""players.team_abbr — crosswalk rung-3 tie-breaker

Revision ID: 0002
Revises: 0001
Create Date: 2026-08-16

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0002"
down_revision: str | Sequence[str] | None = "0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("players", sa.Column("team_abbr", sa.Text(), nullable=True))


def downgrade() -> None:
    op.drop_column("players", "team_abbr")
```

- [ ] **Step 5: Run the DB tests** — `uv run pytest tests/db -q` (compose Postgres up). `test_migrations.py` proves `upgrade head` → `alembic check` (no drift) → `downgrade base` all succeed with 0002 in the chain; `test_players_table_shape` passes.

- [ ] **Step 6: DATABASE.md §2** — in the `players` DDL block add after `status`:

```sql
    status          TEXT,                     -- Active, IR, PUP, Retired, ...
    team_abbr       TEXT,                     -- Phase 0: nflverse latest_team; crosswalk rung-3 tie-breaker ONLY, never roster truth
```

and immediately after the `players_normalized_name_pos_idx` line add the paragraph:

> *Phase 0 note (PR ④):* `team_abbr` was added because DATABASE.md §3 rung 3 matches on `(normalized_name, position, team)` and the table had no team column. It is refreshed from nflverse `latest_team` by `seed_players` (and set from the DynastyProcess `team` for rows created there); it is used only inside `ffh.crosswalk.resolve` to break ties and is **not** a roster field. Migration `0002_players_team_abbr`.

- [ ] **Step 7: Commit**

```bash
uv run ruff check . && uv run ruff format --check .
git add src/ffh/db/models/reference.py alembic/versions/0002_players_team_abbr.py tests/db/test_models_reference.py ../docs/DATABASE.md
git commit -m "feat(db): players.team_abbr for crosswalk rung-3 tie-break (migration 0002)"
```

---

### Task 3: `ffh.crosswalk.registry` — seed `players` from the nflverse frame (+ 32 DST rows)

**Files:**
- Create: `backend/src/ffh/crosswalk/registry.py`
- Create: `backend/tests/crosswalk/conftest.py`
- Create: `backend/tests/crosswalk/test_registry.py`

**Interfaces:**
- Consumes: Task 1 (`normalize_name`, `normalize_team`, `normalize_dst`, `normalize_position`, `dst_full_name`, `TEAMS`, `FANTASY_POSITIONS`); Task 2 (`Player.team_abbr`); `ffh.db.models.Player`.
- Produces:
  - `PLAYERS_REQUIRED_COLUMNS: frozenset[str]` (nflverse names).
  - `class RegistryError(RuntimeError)`.
  - `prepare_players_frame(players_df: pl.DataFrame) -> tuple[pl.DataFrame, dict[str, int]]` — pure; returns the frame in `players` column names (`gsis_id, full_name, first_name, last_name, normalized_name, position, birth_date, rookie_year, height_in, weight_lb, college, status, team_abbr`) and `dropped_by_position` counts.
  - `seed_players(session: Session, players_df: pl.DataFrame) -> int` — upsert on `gsis_id`, then `seed_dst_players`; returns `rows upserted + 32`.
  - `seed_dst_players(session: Session) -> int` — creates any missing team-DST `players` rows (`gsis_id NULL`, `position='DST'`, `normalized_name = normalize_dst(abbr)`, `full_name = dst_full_name(abbr)`, `team_abbr = abbr`); returns rows created.
  - Test fixtures (conftest): `players_frame() -> pl.DataFrame` (17 nflverse-shaped rows, 14 fantasy), `seeded_registry(db_session) -> dict[str, uuid.UUID]` mapping `gsis_id`/`"kc dst"`… → `player_id`.

- [ ] **Step 1: Write the fixtures `backend/tests/crosswalk/conftest.py`**

```python
"""Shared crosswalk fixtures. Real nflverse values (verified 2026-08-16) except rows marked FAKE."""

import uuid

import polars as pl
import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from ffh.db.models import Player

# nflverse players.parquet column names and dtypes (birth_date is a String in the file).
PLAYERS_ROWS: list[dict[str, object]] = [
    dict(gsis_id="00-0033873", display_name="Patrick Mahomes", first_name="Patrick", last_name="Mahomes", position="QB", birth_date="1995-09-17", rookie_season=2017, height=74, weight=225, college_name="Texas Tech", status="ACT", latest_team="KC"),
    dict(gsis_id="00-0036900", display_name="Ja'Marr Chase", first_name="Ja'Marr", last_name="Chase", position="WR", birth_date="2000-03-01", rookie_season=2021, height=72, weight=205, college_name="LSU", status="ACT", latest_team="CIN"),
    dict(gsis_id="00-0034827", display_name="DJ Moore", first_name="Denniston", last_name="Moore", position="WR", birth_date="1997-04-14", rookie_season=2018, height=72, weight=213, college_name="Maryland", status="ACT", latest_team="BUF"),
    dict(gsis_id="00-0038134", display_name="Kenneth Walker III", first_name="Kenneth", last_name="Walker", position="RB", birth_date="2000-10-20", rookie_season=2022, height=69, weight=211, college_name="Michigan State; Wake Forest", status="ACT", latest_team="KC"),
    dict(gsis_id="00-0033303", display_name="Harrison Butker", first_name="Harrison", last_name="Butker", position="K", birth_date="1995-07-14", rookie_season=2017, height=76, weight=205, college_name="Georgia Tech", status="ACT", latest_team="KC"),
    dict(gsis_id="00-0036963", display_name="Amon-Ra St. Brown", first_name="Amon-Ra", last_name="St. Brown", position="WR", birth_date="1999-10-24", rookie_season=2021, height=72, weight=202, college_name="USC", status="ACT", latest_team="DET"),
    dict(gsis_id="00-0039849", display_name="Marvin Harrison Jr.", first_name="Marvin", last_name="Harrison", position="WR", birth_date="2002-08-11", rookie_season=2024, height=75, weight=220, college_name="Ohio State", status="ACT", latest_team="ARI"),
    dict(gsis_id="00-0007024", display_name="Marvin Harrison", first_name="Marvin", last_name="Harrison", position="WR", birth_date="1972-08-25", rookie_season=1996, height=72, weight=185, college_name="Syracuse", status="ACT", latest_team="IND"),
    dict(gsis_id="00-0029892", display_name="Kyle Juszczyk", first_name="Kyle", last_name="Juszczyk", position="FB", birth_date="1991-04-23", rookie_season=2013, height=74, weight=235, college_name="Harvard", status="ACT", latest_team="SF"),
    dict(gsis_id="00-0032688", display_name="Robbie Chosen", first_name="Robert", last_name="Chosen", position="WR", birth_date="1993-05-09", rookie_season=2016, height=75, weight=185, college_name="Temple", status="DEV", latest_team="WAS"),
    dict(gsis_id="00-0034796", display_name="Lamar Jackson", first_name="Lamar", last_name="Jackson", position="QB", birth_date="1997-01-07", rookie_season=2018, height=74, weight=205, college_name="Louisville", status="ACT", latest_team="BAL"),
    dict(gsis_id="00-0036152", display_name="Lamar Jackson", first_name="Lamar", last_name="Jackson", position="CB", birth_date="1998-04-13", rookie_season=2020, height=74, weight=212, college_name="Nebraska", status="DEV", latest_team="ATL"),
    dict(gsis_id="00-0034857", display_name="Josh Allen", first_name="Joshua", last_name="Allen", position="QB", birth_date="1996-05-21", rookie_season=2018, height=77, weight=237, college_name="Wyoming; Reedley", status="ACT", latest_team="BUF"),
    dict(gsis_id="00-0030833", display_name="Josh Allen", first_name="Joshua", last_name="Allen", position="C", birth_date="1991-12-30", rookie_season=2014, height=75, weight=315, college_name="Louisiana-Monroe", status="DEV", latest_team="TB"),
    dict(gsis_id="00-0036613", display_name="Jaylen Waddle", first_name="Jaylen", last_name="Waddle", position="WR", birth_date="1998-11-25", rookie_season=2021, height=70, weight=185, college_name="Alabama", status="ACT", latest_team="DEN"),
    dict(gsis_id="00-0033869", display_name="Mitchell Trubisky", first_name="Mitchell", last_name="Trubisky", position="QB", birth_date="1994-08-20", rookie_season=2017, height=74, weight=222, college_name="North Carolina", status="ACT", latest_team="TEN"),
    # FAKE punter — proves non-fantasy positions are dropped and reported.
    dict(gsis_id="00-0000001", display_name="Test Punter", first_name="Test", last_name="Punter", position="P", birth_date="1990-01-01", rookie_season=2012, height=72, weight=200, college_name="Nowhere", status="ACT", latest_team="SEA"),
]
FANTASY_ROW_COUNT = 14  # 17 rows minus CB, C, P


@pytest.fixture
def players_frame() -> pl.DataFrame:
    return pl.DataFrame(
        PLAYERS_ROWS,
        schema={
            "gsis_id": pl.Utf8, "display_name": pl.Utf8, "first_name": pl.Utf8,
            "last_name": pl.Utf8, "position": pl.Utf8, "birth_date": pl.Utf8,
            "rookie_season": pl.Int32, "height": pl.Int32, "weight": pl.Int32,
            "college_name": pl.Utf8, "status": pl.Utf8, "latest_team": pl.Utf8,
        },
    )


@pytest.fixture
def seeded_registry(db_session: Session, players_frame: pl.DataFrame) -> dict[str, uuid.UUID]:
    """Seed the 14 fantasy players + 32 DSTs; return {gsis_id or 'kc dst': player_id}."""
    from ffh.crosswalk.registry import seed_players

    seed_players(db_session, players_frame)
    out: dict[str, uuid.UUID] = {}
    for pid, gsis, nn in db_session.execute(
        select(Player.player_id, Player.gsis_id, Player.normalized_name)
    ):
        out[gsis if gsis else nn] = pid
    return out
```

- [ ] **Step 2: Write the failing tests `backend/tests/crosswalk/test_registry.py`**

```python
import polars as pl
import pytest
from sqlalchemy import func, select

from ffh.crosswalk.registry import (
    PLAYERS_REQUIRED_COLUMNS,
    RegistryError,
    prepare_players_frame,
    seed_dst_players,
    seed_players,
)
from ffh.db.models import Player
from tests.crosswalk.conftest import FANTASY_ROW_COUNT

pytestmark = pytest.mark.db


def test_prepare_filters_positions_and_reports_dropped(players_frame):
    frame, dropped = prepare_players_frame(players_frame)
    assert frame.height == FANTASY_ROW_COUNT
    assert dropped == {"CB": 1, "C": 1, "P": 1}
    assert frame.height + sum(dropped.values()) == players_frame.height
    row = frame.filter(pl.col("gsis_id") == "00-0029892").row(0, named=True)
    assert row["position"] == "RB"  # FB → RB
    assert row["full_name"] == "Kyle Juszczyk" and row["team_abbr"] == "SF"
    walker = frame.filter(pl.col("gsis_id") == "00-0038134").row(0, named=True)
    assert walker["normalized_name"] == "kenneth walker"
    assert walker["birth_date"].isoformat() == "2000-10-20"
    assert walker["rookie_year"] == 2022 and walker["height_in"] == 69 and walker["weight_lb"] == 211
    assert set(frame.columns) == {
        "gsis_id", "full_name", "first_name", "last_name", "normalized_name", "position",
        "birth_date", "rookie_year", "height_in", "weight_lb", "college", "status", "team_abbr",
    }


def test_prepare_raises_on_missing_columns(players_frame):
    with pytest.raises(RegistryError, match="latest_team"):
        prepare_players_frame(players_frame.drop("latest_team"))
    assert "latest_team" in PLAYERS_REQUIRED_COLUMNS


def test_prepare_raises_on_duplicate_gsis(players_frame):
    dup = pl.concat([players_frame, players_frame.head(1)])
    with pytest.raises(RegistryError, match="duplicate gsis_id"):
        prepare_players_frame(dup)


def test_prepare_raises_on_null_gsis(players_frame):
    bad = players_frame.with_columns(
        pl.when(pl.col("display_name") == "Patrick Mahomes").then(None).otherwise(pl.col("gsis_id")).alias("gsis_id")
    )
    with pytest.raises(RegistryError, match="null gsis_id"):
        prepare_players_frame(bad)


def test_seed_players_is_idempotent(db_session, players_frame):
    n1 = seed_players(db_session, players_frame)
    count1 = db_session.scalar(select(func.count()).select_from(Player))
    n2 = seed_players(db_session, players_frame)
    count2 = db_session.scalar(select(func.count()).select_from(Player))
    assert n1 == n2 == FANTASY_ROW_COUNT + 32
    assert count1 == count2 == FANTASY_ROW_COUNT + 32


def test_seed_players_updates_changed_fields(db_session, players_frame):
    seed_players(db_session, players_frame)
    moved = players_frame.with_columns(
        pl.when(pl.col("gsis_id") == "00-0033873").then(pl.lit("DEN")).otherwise(pl.col("latest_team")).alias("latest_team"),
        pl.when(pl.col("gsis_id") == "00-0033873").then(pl.lit("RET")).otherwise(pl.col("status")).alias("status"),
    )
    seed_players(db_session, moved)
    p = db_session.scalar(select(Player).where(Player.gsis_id == "00-0033873"))
    assert p.team_abbr == "DEN" and p.status == "RET"


def test_seed_creates_exactly_32_dst_rows(db_session, players_frame):
    seed_players(db_session, players_frame)
    dst = db_session.scalars(select(Player).where(Player.position == "DST")).all()
    assert len(dst) == 32
    kc = next(p for p in dst if p.normalized_name == "kc dst")
    assert kc.gsis_id is None and kc.team_abbr == "KC"
    assert kc.full_name == "Kansas City Chiefs DST"
    assert kc.first_name == "Kansas City" and kc.last_name == "Chiefs"
    assert seed_dst_players(db_session) == 0  # nothing missing on a second call
    assert db_session.scalar(select(func.count()).select_from(Player).where(Player.position == "DST")) == 32
```

- [ ] **Step 3: Run to verify it fails** — `uv run pytest tests/crosswalk/test_registry.py -q` → `ModuleNotFoundError: ffh.crosswalk.registry`.

- [ ] **Step 4: Write `backend/src/ffh/crosswalk/registry.py`**

```python
"""Seed the canonical ``players`` registry from the nflverse players frame (DATABASE.md §2).

Takes a DataFrame — never a URL or lake path — so it is testable without ``ffh.ingest``.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from typing import Any

import polars as pl
import structlog
from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from ffh.crosswalk.normalize import (
    FANTASY_POSITIONS,
    TEAMS,
    dst_full_name,
    normalize_dst,
    normalize_name,
    normalize_position,
    normalize_team,
)
from ffh.db.models import Player

log = structlog.get_logger(__name__)

# Live-verified nflverse column names (2026-08-16).
PLAYERS_REQUIRED_COLUMNS: frozenset[str] = frozenset(
    {
        "gsis_id",
        "display_name",
        "first_name",
        "last_name",
        "position",
        "birth_date",
        "rookie_season",
        "height",
        "weight",
        "college_name",
        "status",
        "latest_team",
    }
)

_UPSERT_CHUNK = 1000


class RegistryError(RuntimeError):
    """The players frame is not usable as-is (missing columns, null/duplicate gsis_id)."""


def _chunks(rows: list[dict[str, Any]], size: int) -> Iterator[list[dict[str, Any]]]:
    for i in range(0, len(rows), size):
        yield rows[i : i + size]


def prepare_players_frame(players_df: pl.DataFrame) -> tuple[pl.DataFrame, dict[str, int]]:
    """nflverse frame → frame in ``players`` column names + dropped-by-position counts.

    Never silently drops: kept + dropped == input, and the dropped breakdown is returned.
    """
    missing = PLAYERS_REQUIRED_COLUMNS - set(players_df.columns)
    if missing:
        raise RegistryError(f"players frame missing columns: {sorted(missing)}")

    n_in = players_df.height
    df = players_df.with_columns(
        pl.col("position")
        .map_elements(normalize_position, return_dtype=pl.Utf8)
        .alias("position_norm")
    )
    keep = pl.col("position_norm").is_in(sorted(FANTASY_POSITIONS)).fill_null(False)
    kept = df.filter(keep)
    dropped = df.filter(~keep)
    assert kept.height + dropped.height == n_in, "row loss in position filter"
    dropped_by_position = {
        str(row["position"]): int(row["len"])
        for row in dropped.group_by("position").len().sort("position").iter_rows(named=True)
    }

    if kept["gsis_id"].null_count():
        raise RegistryError(f"{kept['gsis_id'].null_count()} fantasy rows have null gsis_id")
    if kept["gsis_id"].n_unique() != kept.height:
        raise RegistryError("duplicate gsis_id in players frame")

    birth = pl.col("birth_date")
    birth_expr = (
        birth.str.to_date("%Y-%m-%d", strict=False)
        if kept.schema["birth_date"] == pl.Utf8
        else birth.cast(pl.Date)
    )
    out = kept.select(
        pl.col("gsis_id"),
        pl.col("display_name").alias("full_name"),
        pl.col("first_name"),
        pl.col("last_name"),
        pl.col("display_name").map_elements(normalize_name, return_dtype=pl.Utf8).alias("normalized_name"),
        pl.col("position_norm").alias("position"),
        birth_expr.alias("birth_date"),
        pl.col("rookie_season").cast(pl.Int32).alias("rookie_year"),
        pl.col("height").cast(pl.Int32).alias("height_in"),
        pl.col("weight").cast(pl.Int32).alias("weight_lb"),
        pl.col("college_name").alias("college"),
        pl.col("status"),
        pl.col("latest_team").map_elements(normalize_team, return_dtype=pl.Utf8).alias("team_abbr"),
    )
    empty_names = out.filter(pl.col("normalized_name") == "").height
    if empty_names:
        raise RegistryError(f"{empty_names} rows normalize to an empty name")
    unparsed = kept["birth_date"].drop_nulls().len() - out["birth_date"].drop_nulls().len()
    if unparsed:
        log.warning("crosswalk.seed_players.unparsed_birth_dates", count=unparsed)
    unknown_team = out.filter(pl.col("team_abbr").is_null() & pl.col("gsis_id").is_not_null()).height
    log.info(
        "crosswalk.seed_players.prepared",
        input=n_in,
        kept=out.height,
        dropped_by_position=dropped_by_position,
        rows_without_team=unknown_team,
    )
    return out, dropped_by_position


_UPDATE_COLUMNS: tuple[str, ...] = (
    "full_name",
    "first_name",
    "last_name",
    "normalized_name",
    "position",
    "birth_date",
    "rookie_year",
    "height_in",
    "weight_lb",
    "college",
    "status",
    "team_abbr",
)


def seed_players(session: Session, players_df: pl.DataFrame) -> int:
    """Upsert ``players`` on ``gsis_id`` from the nflverse frame, then ensure 32 DST rows.

    Idempotent. Sets ``updated_at = now()`` explicitly on conflict (DATABASE.md §2).
    Returns rows upserted + 32.
    """
    frame, _dropped = prepare_players_frame(players_df)
    rows = frame.to_dicts()
    for chunk in _chunks(rows, _UPSERT_CHUNK):
        stmt = insert(Player).values(chunk)
        stmt = stmt.on_conflict_do_update(
            index_elements=[Player.gsis_id],
            set_={**{c: getattr(stmt.excluded, c) for c in _UPDATE_COLUMNS}, "updated_at": func.now()},
        )
        session.execute(stmt)
    created_dst = seed_dst_players(session)
    session.flush()
    log.info("crosswalk.seed_players.done", upserted=len(rows), dst_created=created_dst)
    return len(rows) + len(TEAMS)


def seed_dst_players(session: Session) -> int:
    """One ``players`` row per team DST: gsis NULL, position DST, normalized_name 'kc dst'."""
    existing = set(session.scalars(select(Player.normalized_name).where(Player.position == "DST")))
    new: list[Player] = []
    for abbr, city, nickname, _aliases in TEAMS:
        nn = normalize_dst(abbr)
        assert nn is not None
        if nn in existing:
            continue
        new.append(
            Player(
                gsis_id=None,
                full_name=dst_full_name(abbr),
                first_name=city,
                last_name=nickname,
                normalized_name=nn,
                position="DST",
                team_abbr=abbr,
            )
        )
    session.add_all(new)
    session.flush()
    return len(new)


def iter_gsis_to_player_id(session: Session) -> Iterable[tuple[str, Any]]:
    """(gsis_id, player_id) for every registry row that has a gsis_id. Used by Task 4."""
    return session.execute(select(Player.gsis_id, Player.player_id).where(Player.gsis_id.is_not(None))).all()
```

- [ ] **Step 5: Run to verify it passes** — `uv run pytest tests/crosswalk/test_registry.py -q` → 7 passed.

- [ ] **Step 6: Lint and commit**

```bash
uv run ruff check src/ffh/crosswalk tests/crosswalk && uv run ruff format src/ffh/crosswalk tests/crosswalk
git add src/ffh/crosswalk/registry.py tests/crosswalk/conftest.py tests/crosswalk/test_registry.py
git commit -m "feat(crosswalk): seed players registry from nflverse frame with DST rows and drop reporting"
```

---

### Task 4: `ffh.crosswalk.dynastyprocess` — apply `db_playerids.csv` ids to `player_external_ids` (rung 1)

**Files:**
- Create: `backend/src/ffh/crosswalk/dynastyprocess.py` (pure part; the `IngestJob` class is added by Task 8)
- Create: `backend/tests/fixtures/dynastyprocess/db_playerids_sample.csv`
- Create: `backend/tests/crosswalk/test_dynastyprocess.py`

**Interfaces:**
- Consumes: Task 1 (`normalize_name`, `normalize_team`, `normalize_dst`, `normalize_position`, `FANTASY_POSITIONS`); Task 3 (`seed_players`, `iter_gsis_to_player_id`, `seeded_registry` fixture); `ffh.db.models.Player`, `PlayerExternalId`.
- Produces:
  - `DP_ID_COLUMNS: dict[str, str]` — CSV column → crosswalk `source`: `sleeper_id→sleeper, espn_id→espn, yahoo_id→yahoo, pfr_id→pfr, fantasypros_id→fantasypros, sportradar_id→sportradar, rotowire_id→rotowire`.
  - `DP_REQUIRED_COLUMNS: frozenset[str]`, `DP_TEXT_COLUMNS: frozenset[str]`.
  - `class DynastyProcessError(RuntimeError)`; `class CrosswalkConflictError(RuntimeError)` with `.conflicts: list[tuple[str, str, uuid.UUID, uuid.UUID]]` (source, external_id, existing player_id, incoming player_id).
  - `read_playerids_csv(raw: bytes) -> pl.DataFrame` — id columns as `pl.Utf8`, `NA` → null.
  - `@dataclass(frozen=True) CrosswalkApplyReport(inserted: int, updated: int, unchanged: int, created_players: int, skipped_no_ids: int, skipped_position: int, ambiguous: tuple[tuple[str, str], ...])`.
  - `apply_playerids(session: Session, df: pl.DataFrame) -> CrosswalkApplyReport`.

**Policies (all recorded in DATABASE.md §3 by Task 9):**
1. Positions normalized (`PK→K`, `FB→RB`, `DEF→DST`); non-fantasy rows are counted in `skipped_position` — never silently dropped.
2. Rows with **no** id in any `DP_ID_COLUMNS` are `skipped_no_ids`.
3. Player assignment per row: `gsis_id` present and in registry → that player; else any of the row's ids already in `player_external_ids` → that player (makes re-runs idempotent for rookies); else a **placeholder** (`gsis:<gsis>` if the row has a gsis, otherwise `mfl:<mfl_id>`) that becomes a new `players` row only if the row still has ≥1 id after the ambiguity pass. DST rows (`position == DST`) map to the seeded DST player via `normalize_dst(team) or normalize_dst(name)`.
4. **Ambiguity (intra-file):** after unpivoting to `(source, external_id, player_key)` and de-duplicating, any `(source, external_id)` pointing at >1 player key, and any `(source, player_key)` holding >1 external id, is dropped from the batch and listed in `report.ambiguous`. A player holds at most **one** id per source (that is what `test_crosswalk_no_duplicate_player_ids` asserts).
5. **Conflict (vs the DB):** an existing `(source, external_id)` row that points at a *different* `player_id` raises `CrosswalkConflictError` **before any write** — DP is never allowed to silently re-point an id. Fix by hand (`ffh crosswalk verify --reject`, then re-run) after deciding who is right.
6. Existing row, same player: `match_method='dynastyprocess' AND confidence=1.0` → `unchanged`; otherwise upgraded to `dynastyprocess/1.0` → `updated` (rung 1 outranks 3/4).
7. Ids are stored as TEXT exactly as in the CSV (`sportradar_id` UUIDs, `pfr_id` alphanumerics, numeric ids never pass through a float).

- [ ] **Step 1: Write the fixture `backend/tests/fixtures/dynastyprocess/db_playerids_sample.csv`** — a 16-column subset of the real 35-column header (extra real columns are irrelevant; `read_playerids_csv` requires only `DP_REQUIRED_COLUMNS`). Rows are live 2026-08-16 values except the two marked in the test docstring (Nobody Nowhere, Kansas City Chiefs DEF).

```csv
mfl_id,sportradar_id,fantasypros_id,gsis_id,sleeper_id,espn_id,yahoo_id,pfr_id,rotowire_id,name,merge_name,position,team,birthdate,draft_year,college
13116,11cad59d-90dd-449c-a839-dddaba4fe16c,16413,00-0033873,4046,3139477,30123,MahoPa00,11839,Patrick Mahomes,patrick mahomes,QB,KCC,1995-09-17,2017,Texas Tech
15281,fa99e984-d63b-4ef4-a164-407f68a7eeaf,19788,00-0036900,7564,4362628,33393,ChasJa00,15183,Ja'Marr Chase,jamarr chase,WR,CIN,2000-03-01,2021,LSU
13635,d8202e6d-d03b-4cd1-a793-ff8fd39d9755,17265,00-0034827,4983,3915416,30994,MoorD.00,12477,D.J. Moore,dj moore,WR,BUF,1997-04-14,2018,Maryland
15711,22ee9bac-a64c-4d44-94fc-51d775465b3b,23021,00-0038134,8151,4567048,33996,WalkKe00,15909,Kenneth Walker III,kenneth walker,RB,KCC,2000-10-20,2022,Michigan State
13354,4ceb866c-8eaf-49b5-9043-56228e43a2e5,16712,00-0033303,4227,3055899,30346,ButkHa00,11783,Harrison Butker,harrison butker,PK,KCC,1995-07-14,2017,Georgia Tech
17471,125e9b80-426b-11f1-9d61-c7109dbf1b70,28082,NA,13427,5084180,NA,NA,19290,Diego Pavia,diego pavia,QB,FA,2002-02-16,2026,Vanderbilt
11367,67da5b5c-0db9-4fbc-b98d-7eb8e97b69f6,11798,00-0029892,1379,16002,26753,JuszKy00,8930,Kyle Juszczyk,kyle juszczyk,RB,SFO,1991-04-23,2013,Harvard
16614,e8da21a8-796d-48a2-b644-57d08983ae01,23064,00-0039849,11628,4432708,40893,HarrMa09,17674,Marvin Harrison Jr.,marvin harrison,WR,ARI,2002-08-11,2024,Ohio State
17556,8095554c-7eae-4e7c-9213-01f24ceb0185,28053,00-0041509,13377,4950400,NA,ReesAr00,19273,Arvell Reese,arvell reese,LB,NYG,2005-08-30,2026,Ohio State
12459,2092561a-fc19-4c9a-9695-2f5a537717be,NA,00-0031320,2295,17257,28118,SmitKe04,9898,Fred Williams,fred williams,WR,KCC,1988-04-15,2014,St. Cloud State
12571,2092561a-fc19-4c9a-9695-2f5a537717be,NA,00-0031320,2295,17257,28118,SmitKe04,10167,Kevin Smith,kevin smith,WR,SEA,1991-12-21,2014,Washington
99900,NA,NA,NA,NA,NA,NA,NA,NA,Nobody Nowhere,nobody nowhere,QB,FA,NA,NA,NA
99901,NA,NA,NA,KC,-16012,NA,NA,NA,Kansas City Chiefs,kansas city chiefs,DEF,KCC,NA,NA,NA
```

Expected outcome against `seeded_registry` (14 players + 32 DST) — work it by hand once, it is the oracle for the tests:
- `skipped_position = 1` (Arvell Reese, LB). 12 rows continue.
- `skipped_no_ids = 1` (Nobody Nowhere). 11 rows continue.
- Registry hits via gsis: Mahomes, Chase, DJ Moore, Walker, Butker, Juszczyk, MHJ → 7 × 7 ids = 49 rows.
- Diego Pavia: no gsis → placeholder `mfl:17471`; ids: sportradar, fantasypros, sleeper, espn, rotowire = 5 → new player.
- Fred Williams / Kevin Smith: same gsis `00-0031320` not in registry → one placeholder `gsis:00-0031320`; shared ids dedupe to 5 (sportradar, sleeper, espn, yahoo, pfr); rotowire has 2 distinct ids for one player → **both ambiguous** and dropped → 5 rows; one new player (named from the first row, "Fred Williams", gsis set, team `KC`).
- Chiefs DEF → DST player `kc dst`; ids sleeper `KC`, espn `-16012` = 2 rows.
- Totals: `inserted = 49 + 5 + 5 + 2 = 61`, `created_players = 2`, `ambiguous = (("rotowire", "10167"), ("rotowire", "9898"))`, `updated = 0`, `unchanged = 0`.

- [ ] **Step 2: Write the failing tests `backend/tests/crosswalk/test_dynastyprocess.py`**

```python
"""apply_playerids on the 13-row sample. Rows 'Nobody Nowhere' (mfl 99900) and
'Kansas City Chiefs' DEF (mfl 99901) are fabricated; everything else is live DP data."""

import uuid
from pathlib import Path

import polars as pl
import pytest
from sqlalchemy import func, select

from ffh.crosswalk.dynastyprocess import (
    DP_ID_COLUMNS,
    CrosswalkApplyReport,
    CrosswalkConflictError,
    DynastyProcessError,
    apply_playerids,
    read_playerids_csv,
)
from ffh.db.models import Player, PlayerExternalId

pytestmark = pytest.mark.db

FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "dynastyprocess" / "db_playerids_sample.csv"


@pytest.fixture
def dp_frame() -> pl.DataFrame:
    return read_playerids_csv(FIXTURE.read_bytes())


def _count_ids(session) -> int:
    return session.scalar(select(func.count()).select_from(PlayerExternalId))


def test_read_csv_keeps_ids_as_text_and_na_as_null(dp_frame):
    for col in DP_ID_COLUMNS:
        assert dp_frame.schema[col] == pl.Utf8, col
    assert dp_frame.schema["gsis_id"] == pl.Utf8
    mahomes = dp_frame.filter(pl.col("mfl_id") == "13116").row(0, named=True)
    assert mahomes["yahoo_id"] == "30123" and mahomes["espn_id"] == "3139477"
    assert mahomes["sportradar_id"] == "11cad59d-90dd-449c-a839-dddaba4fe16c"
    pavia = dp_frame.filter(pl.col("mfl_id") == "17471").row(0, named=True)
    assert pavia["gsis_id"] is None and pavia["yahoo_id"] is None and pavia["pfr_id"] is None
    assert dp_frame.height == 13


def test_read_csv_rejects_missing_required_columns():
    with pytest.raises(DynastyProcessError, match="sleeper_id"):
        read_playerids_csv(b"mfl_id,gsis_id,name,position,team\n1,NA,x,QB,FA\n")


def test_apply_populates_external_ids(db_session, seeded_registry, dp_frame):
    report = apply_playerids(db_session, dp_frame)
    assert report == CrosswalkApplyReport(
        inserted=61,
        updated=0,
        unchanged=0,
        created_players=2,
        skipped_no_ids=1,
        skipped_position=1,
        ambiguous=(("rotowire", "10167"), ("rotowire", "9898")),
    )
    assert _count_ids(db_session) == 61
    rows = db_session.scalars(select(PlayerExternalId)).all()
    assert all(r.confidence == 1.0 and r.match_method == "dynastyprocess" for r in rows)
    mahomes = seeded_registry["00-0033873"]
    by_key = {(r.source, r.external_id): r.player_id for r in rows}
    assert by_key[("sleeper", "4046")] == mahomes
    assert by_key[("espn", "3139477")] == mahomes
    assert by_key[("sportradar", "11cad59d-90dd-449c-a839-dddaba4fe16c")] == mahomes
    assert by_key[("pfr", "MoorD.00")] == seeded_registry["00-0034827"]
    # PK → K: Butker's ids land on the K registry row
    assert by_key[("sleeper", "4227")] == seeded_registry["00-0033303"]
    # DST row maps to the seeded 'kc dst' player
    assert by_key[("sleeper", "KC")] == seeded_registry["kc dst"]
    assert by_key[("espn", "-16012")] == seeded_registry["kc dst"]
    # Ambiguous rotowire ids were NOT written
    assert ("rotowire", "9898") not in by_key and ("rotowire", "10167") not in by_key


def test_apply_creates_rookie_player_without_gsis(db_session, seeded_registry, dp_frame):
    apply_playerids(db_session, dp_frame)
    pavia = db_session.scalar(select(Player).where(Player.normalized_name == "diego pavia"))
    assert pavia is not None
    assert pavia.gsis_id is None and pavia.position == "QB"
    assert pavia.full_name == "Diego Pavia" and pavia.first_name == "Diego" and pavia.last_name == "Pavia"
    assert pavia.rookie_year == 2026 and pavia.college == "Vanderbilt"
    assert pavia.birth_date.isoformat() == "2002-02-16"
    assert pavia.team_abbr is None  # FA
    link = db_session.get(PlayerExternalId, ("sleeper", "13427"))
    assert link.player_id == pavia.player_id
    # The glitch pair shares a gsis → exactly one player, carrying that gsis
    fred = db_session.scalar(select(Player).where(Player.gsis_id == "00-0031320"))
    assert fred.full_name == "Fred Williams" and fred.team_abbr == "KC"
    assert db_session.get(PlayerExternalId, ("sleeper", "2295")).player_id == fred.player_id


def test_apply_is_idempotent(db_session, seeded_registry, dp_frame):
    first = apply_playerids(db_session, dp_frame)
    second = apply_playerids(db_session, dp_frame)
    assert second.inserted == 0 and second.created_players == 0 and second.updated == 0
    assert second.unchanged == first.inserted == 61
    assert second.ambiguous == first.ambiguous
    assert _count_ids(db_session) == 61
    assert db_session.scalar(select(func.count()).select_from(Player)) == 14 + 32 + 2


def test_apply_upgrades_lower_rung_row_for_same_player(db_session, seeded_registry, dp_frame):
    db_session.add(
        PlayerExternalId(
            player_id=seeded_registry["00-0033873"], source="sleeper", external_id="4046",
            confidence=0.95, match_method="exact_name",
        )
    )
    db_session.flush()
    report = apply_playerids(db_session, dp_frame)
    assert report.updated == 1 and report.inserted == 60
    row = db_session.get(PlayerExternalId, ("sleeper", "4046"))
    db_session.refresh(row)
    assert row.match_method == "dynastyprocess" and row.confidence == 1.0


def test_apply_raises_on_conflicting_existing_mapping(db_session, seeded_registry, dp_frame):
    # sleeper 4046 is Mahomes in DP; pre-point it at Chase.
    db_session.add(
        PlayerExternalId(
            player_id=seeded_registry["00-0036900"], source="sleeper", external_id="4046",
            confidence=1.0, match_method="manual",
        )
    )
    db_session.flush()
    with pytest.raises(CrosswalkConflictError) as exc:
        apply_playerids(db_session, dp_frame)
    (src, ext, existing, incoming), *_ = exc.value.conflicts
    assert (src, ext) == ("sleeper", "4046")
    assert existing == seeded_registry["00-0036900"] and incoming == seeded_registry["00-0033873"]
    assert isinstance(existing, uuid.UUID)
    # Nothing else was written: only the pre-existing manual row is present.
    assert _count_ids(db_session) == 1


def test_apply_rejects_frame_missing_columns(db_session, dp_frame):
    with pytest.raises(DynastyProcessError):
        apply_playerids(db_session, dp_frame.drop("position"))
```

- [ ] **Step 3: Run to verify it fails** — `uv run pytest tests/crosswalk/test_dynastyprocess.py -q` → `ModuleNotFoundError: ffh.crosswalk.dynastyprocess`.

- [ ] **Step 4: Write `backend/src/ffh/crosswalk/dynastyprocess.py`**

```python
"""DynastyProcess ``db_playerids.csv`` → ``player_external_ids`` (rung 1 of the ladder).

Pure with respect to ``ffh.ingest``: takes a DataFrame. The IngestJob that fetches the CSV
into the lake is appended to this module in Task 8 (requires PR ③).
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import date

import polars as pl
import structlog
from sqlalchemy import select, update
from sqlalchemy.orm import Session

from ffh.crosswalk.normalize import (
    FANTASY_POSITIONS,
    normalize_dst,
    normalize_name,
    normalize_position,
    normalize_team,
)
from ffh.crosswalk.registry import iter_gsis_to_player_id
from ffh.db.models import Player, PlayerExternalId

log = structlog.get_logger(__name__)

# CSV column → crosswalk source (DATABASE.md §2 player_external_ids.source).
DP_ID_COLUMNS: dict[str, str] = {
    "sleeper_id": "sleeper",
    "espn_id": "espn",
    "yahoo_id": "yahoo",
    "pfr_id": "pfr",
    "fantasypros_id": "fantasypros",
    "sportradar_id": "sportradar",
    "rotowire_id": "rotowire",
}
DP_TEXT_COLUMNS: frozenset[str] = frozenset({"mfl_id", "gsis_id", *DP_ID_COLUMNS})
DP_REQUIRED_COLUMNS: frozenset[str] = frozenset(
    {"mfl_id", "gsis_id", "name", "position", "team", "birthdate", "draft_year", "college", *DP_ID_COLUMNS}
)
DP_METHOD = "dynastyprocess"
DP_CONFIDENCE = 1.0


class DynastyProcessError(RuntimeError):
    """The DP frame is missing required columns."""


class CrosswalkConflictError(RuntimeError):
    """An existing (source, external_id) row points at a different player than DP says."""

    def __init__(self, conflicts: list[tuple[str, str, uuid.UUID, uuid.UUID]]) -> None:
        self.conflicts = conflicts
        shown = ", ".join(f"{s}:{e} db={a} dp={b}" for s, e, a, b in conflicts[:10])
        super().__init__(
            f"{len(conflicts)} DynastyProcess id(s) conflict with existing crosswalk rows "
            f"(first: {shown}). Resolve by hand (ffh crosswalk verify --reject) and re-run."
        )


@dataclass(frozen=True)
class CrosswalkApplyReport:
    inserted: int
    updated: int
    unchanged: int
    created_players: int
    skipped_no_ids: int
    skipped_position: int
    ambiguous: tuple[tuple[str, str], ...]


def read_playerids_csv(raw: bytes) -> pl.DataFrame:
    """Parse the CSV with every id column as text and ``NA`` as null."""
    header = raw.split(b"\n", 1)[0].decode("utf-8").strip().split(",")
    missing = DP_REQUIRED_COLUMNS - set(header)
    if missing:
        raise DynastyProcessError(f"db_playerids.csv missing columns: {sorted(missing)}")
    return pl.read_csv(
        raw,
        null_values=["NA", ""],
        schema_overrides={c: pl.Utf8 for c in DP_TEXT_COLUMNS},
        infer_schema_length=20000,
    )


def _validate(df: pl.DataFrame) -> pl.DataFrame:
    missing = DP_REQUIRED_COLUMNS - set(df.columns)
    if missing:
        raise DynastyProcessError(f"DynastyProcess frame missing columns: {sorted(missing)}")
    # Defensive: a Parquet round-trip could have typed ids numerically. Store as text.
    return df.with_columns([pl.col(c).cast(pl.Utf8) for c in DP_TEXT_COLUMNS])


def _split_name(name: str) -> tuple[str | None, str | None]:
    parts = name.strip().split(maxsplit=1)
    if len(parts) == 2:
        return parts[0], parts[1]
    return (parts[0], None) if parts else (None, None)


def _parse_date(raw: str | None) -> date | None:
    if not raw:
        return None
    try:
        return date.fromisoformat(raw)
    except ValueError:
        return None


def apply_playerids(session: Session, df: pl.DataFrame) -> CrosswalkApplyReport:
    """Populate ``player_external_ids`` at confidence 1.0 / ``dynastyprocess`` from a DP frame.

    Policies 1–7 are in the plan (Task 4) and DATABASE.md §3. Runs inside the caller's
    transaction; raises ``CrosswalkConflictError`` before writing any id row.
    """
    df = _validate(df)
    n_in = df.height

    # 1. positions
    df = df.with_columns(
        pl.col("position").map_elements(normalize_position, return_dtype=pl.Utf8).alias("position_norm")
    )
    keep_pos = pl.col("position_norm").is_in(sorted(FANTASY_POSITIONS)).fill_null(False)
    kept = df.filter(keep_pos)
    skipped_position = df.filter(~keep_pos).height
    assert kept.height + skipped_position == n_in, "row loss in position filter"

    # 2. rows without any id
    has_id = pl.any_horizontal([pl.col(c).is_not_null() for c in DP_ID_COLUMNS])
    with_ids = kept.filter(has_id)
    skipped_no_ids = kept.filter(~has_id).height
    assert with_ids.height + skipped_no_ids == kept.height, "row loss in id filter"

    # 3. player assignment
    gsis_to_pid: dict[str, uuid.UUID] = dict(iter_gsis_to_player_id(session))
    dst_to_pid: dict[str, uuid.UUID] = dict(
        session.execute(
            select(Player.normalized_name, Player.player_id).where(Player.position == "DST")
        ).all()
    )
    existing: dict[tuple[str, str], PlayerExternalId] = {
        (r.source, r.external_id): r
        for r in session.scalars(
            select(PlayerExternalId).where(
                PlayerExternalId.source.in_(sorted(DP_ID_COLUMNS.values()))
            )
        )
    }
    player_key: list[str] = []  # str(uuid) for known players, "gsis:…"/"mfl:…" placeholders
    for row in with_ids.iter_rows(named=True):
        key: str | None = None
        if row["position_norm"] == "DST":
            nn = normalize_dst(row["team"]) or normalize_dst(row["name"])
            pid = dst_to_pid.get(nn) if nn else None
            key = str(pid) if pid else None
        elif row["gsis_id"] and row["gsis_id"] in gsis_to_pid:
            key = str(gsis_to_pid[row["gsis_id"]])
        if key is None:
            for col, source in DP_ID_COLUMNS.items():
                ext = row[col]
                hit = existing.get((source, ext)) if ext else None
                if hit is not None:
                    key = str(hit.player_id)
                    break
        if key is None:
            key = f"gsis:{row['gsis_id']}" if row["gsis_id"] else f"mfl:{row['mfl_id']}"
        player_key.append(key)
    with_ids = with_ids.with_columns(pl.Series("player_key", player_key, dtype=pl.Utf8))

    # 4. unpivot + ambiguity (one id per (source, player); one player per (source, id))
    long = (
        with_ids.select(["mfl_id", "player_key", *DP_ID_COLUMNS])
        .unpivot(
            on=list(DP_ID_COLUMNS),
            index=["mfl_id", "player_key"],
            variable_name="col",
            value_name="external_id",
        )
        .drop_nulls("external_id")
        .with_columns(pl.col("col").replace_strict(DP_ID_COLUMNS).alias("source"))
        .select(["source", "external_id", "player_key"])
        .unique()
    )
    n_long = long.height
    many_players = (
        long.group_by(["source", "external_id"])
        .agg(pl.col("player_key").n_unique().alias("n"))
        .filter(pl.col("n") > 1)
        .select(["source", "external_id"])
    )
    many_ids = (
        long.group_by(["source", "player_key"])
        .agg(pl.col("external_id").n_unique().alias("n"))
        .filter(pl.col("n") > 1)
        .select(["source", "player_key"])
    )
    bad = pl.concat(
        [
            long.join(many_players, on=["source", "external_id"], how="semi"),
            long.join(many_ids, on=["source", "player_key"], how="semi"),
        ]
    ).unique()
    bad_keys = bad.select(["source", "external_id"]).unique()
    n_bad_rows = long.join(bad_keys, on=["source", "external_id"], how="semi").height
    clean = long.join(bad_keys, on=["source", "external_id"], how="anti")
    assert clean.height + n_bad_rows == n_long, "row loss in ambiguity pass"
    ambiguous = tuple(sorted((r["source"], r["external_id"]) for r in bad_keys.iter_rows(named=True)))
    if ambiguous:
        log.warning("crosswalk.dynastyprocess.ambiguous_ids", count=len(ambiguous), sample=ambiguous[:10])

    # 5. create players for placeholders that still hold ≥ 1 id
    is_placeholder = pl.col("player_key").str.starts_with("gsis:") | pl.col("player_key").str.starts_with("mfl:")
    needed = sorted(set(clean.filter(is_placeholder)["player_key"].to_list()))
    created: dict[str, uuid.UUID] = {}
    first_rows = with_ids.filter(pl.col("player_key").is_in(needed)).unique(subset=["player_key"], keep="first", maintain_order=True)
    for row in first_rows.iter_rows(named=True):
        first, last = _split_name(row["name"])
        player = Player(
            gsis_id=row["gsis_id"],
            full_name=row["name"],
            first_name=first,
            last_name=last,
            normalized_name=normalize_name(row["name"]),
            position=row["position_norm"],
            birth_date=_parse_date(row["birthdate"]),
            rookie_year=int(row["draft_year"]) if row["draft_year"] is not None else None,
            college=row["college"],
            team_abbr=normalize_team(row["team"]),
        )
        session.add(player)
        session.flush()
        created[row["player_key"]] = player.player_id
    log.info("crosswalk.dynastyprocess.created_players", count=len(created))

    # 6. partition against existing rows — check everything, then write
    inserts: list[dict[str, object]] = []
    updates: list[tuple[str, str]] = []
    unchanged = 0
    conflicts: list[tuple[str, str, uuid.UUID, uuid.UUID]] = []
    for r in clean.iter_rows(named=True):
        pid = created.get(r["player_key"]) or uuid.UUID(r["player_key"])
        ex = existing.get((r["source"], r["external_id"]))
        if ex is None:
            inserts.append(
                {
                    "player_id": pid,
                    "source": r["source"],
                    "external_id": r["external_id"],
                    "confidence": DP_CONFIDENCE,
                    "match_method": DP_METHOD,
                }
            )
        elif ex.player_id != pid:
            conflicts.append((r["source"], r["external_id"], ex.player_id, pid))
        elif ex.match_method == DP_METHOD and ex.confidence == DP_CONFIDENCE:
            unchanged += 1
        else:
            updates.append((r["source"], r["external_id"]))
    if conflicts:
        log.error("crosswalk.dynastyprocess.conflicts", count=len(conflicts))
        raise CrosswalkConflictError(conflicts)
    assert len(inserts) + len(updates) + unchanged == clean.height, "row loss in partition"

    if inserts:
        session.execute(PlayerExternalId.__table__.insert(), inserts)
    for source, ext in updates:
        session.execute(
            update(PlayerExternalId)
            .where(PlayerExternalId.source == source, PlayerExternalId.external_id == ext)
            .values(match_method=DP_METHOD, confidence=DP_CONFIDENCE)
        )
    session.flush()

    report = CrosswalkApplyReport(
        inserted=len(inserts),
        updated=len(updates),
        unchanged=unchanged,
        created_players=len(created),
        skipped_no_ids=skipped_no_ids,
        skipped_position=skipped_position,
        ambiguous=ambiguous,
    )
    log.info("crosswalk.dynastyprocess.applied", **report.__dict__)
    return report
```

- [ ] **Step 5: Run to verify it passes** — `uv run pytest tests/crosswalk/test_dynastyprocess.py -q` → 8 passed. If `test_apply_populates_external_ids` reports a different `inserted`, print `long` and `bad` — do not change the expected number until the by-hand oracle above is proven wrong.

- [ ] **Step 6: Lint and commit**

```bash
uv run ruff check src/ffh/crosswalk tests/crosswalk && uv run ruff format src/ffh/crosswalk tests/crosswalk
git add src/ffh/crosswalk/dynastyprocess.py tests/fixtures/dynastyprocess/db_playerids_sample.csv tests/crosswalk/test_dynastyprocess.py
git commit -m "feat(crosswalk): apply DynastyProcess ids at rung 1 with ambiguity and conflict policies"
```

---

### Task 5: `ffh.crosswalk.resolve` — the ladder, strictly in order

**Files:**
- Create: `backend/src/ffh/crosswalk/resolve.py`
- Create: `backend/tests/crosswalk/test_resolve.py`

**Interfaces:**
- Consumes: Task 1 (`normalize_name`, `normalize_dst`, `normalize_position`, `normalize_team`); Task 2 (`Player.team_abbr`); Task 3 fixture `seeded_registry`; `ffh.db.models.Player`, `PlayerExternalId`, `CrosswalkUnmatched`.
- Produces (PRs ⑤ and ⑥ import exactly these):
  - `EXACT_CONFIDENCE = 0.95`, `FUZZY_THRESHOLD = 0.92`, `FUZZY_CAP = 0.89`, `FUZZY_TIE_MARGIN = 0.01`, `USABLE_CONFIDENCE = 0.9`.
  - `@dataclass(frozen=True) Resolution(player_id: uuid.UUID, method: str, confidence: float)`.
  - `@dataclass(frozen=True) ResolveInput(source: str, external_id: str, raw_name: str | None = None, raw_position: str | None = None, raw_team: str | None = None, gsis_id: str | None = None, birth_date: date | None = None, college: str | None = None)` with `.key -> tuple[str, str]`.
  - `@dataclass ResolveManyReport(resolved: dict[tuple[str, str], Resolution], unmatched: list[tuple[str, str]], pending_review: list[tuple[str, str]], by_method: Counter[str])`.
  - `is_usable(confidence: float, verified_at: datetime | None) -> bool` — `confidence >= 0.9 or verified_at is not None`. **This is the consumer filter rule**: anything reading `player_external_ids` directly (SQL) must apply `confidence >= 0.9 OR verified_at IS NOT NULL`.
  - `resolve(session, source, external_id, raw_name=None, raw_position=None, raw_team=None, *, gsis_id=None, birth_date=None, college=None) -> Resolution | None`.
  - `resolve_many(session, rows: Iterable[ResolveInput]) -> ResolveManyReport`.

**Ladder semantics (record in DATABASE.md §3 in Task 9):**
1. **Rung 1** — existing `player_external_ids` row for `(source, external_id)`. Usable (`is_usable`) → return its `match_method`/`confidence` (whatever rung wrote it: `dynastyprocess`, `gsis`, `exact_name`, verified `fuzzy`, `manual`). Not usable (fuzzy awaiting review) → return `None`, outcome `pending_review`, **no re-guessing and no unmatched row**.
2. **Rung 2** — gsis direct: `gsis = gsis_id or (external_id if source == "gsis")`. Found → confidence 1.0, `gsis`; if `source != "gsis"` persist a `(source, external_id)` row so the next call is rung 1.
3. **Rung 3** — canonical name: DST positions use `normalize_dst(raw_name) or normalize_dst(raw_team) or normalize_dst(external_id)`; everyone else `normalize_name(raw_name)`. Candidates: `players` rows with equal `(normalized_name, position)` **that do not already hold an id for this source** (one id per source per player — this is how the no-duplicates invariant is enforced by construction). Team rule: `team = normalize_team(raw_team)`; if `team is None` → match iff exactly one candidate; else keep candidates whose `team_abbr` is `team` or `NULL` and match iff exactly one remains. Persist `exact_name` 0.95, return it.
4. **Rung 4** — `rapidfuzz.process.extract(name, {player_id: normalized_name}, scorer=JaroWinkler.normalized_similarity, score_cutoff=0.92, limit=None)` over same-position candidates not already mapped for the source. If `birth_date` given, drop candidates whose stored `birth_date` is non-null and different; same for `college` (case-insensitive substring, because nflverse stores `"Michigan State; Wake Forest"`). Two survivors within `0.01` of each other → **tie → not matched** (falls to rung 5). Otherwise persist `fuzzy` with `confidence = min(similarity, 0.89)`, `verified_at NULL`, and return `None` (outcome `pending_review`). A human runs `ffh crosswalk verify <source> <id>`; after that rung 1 returns it.
5. **Rung 5** — upsert `crosswalk_unmatched` (`first_seen` default on insert; `last_seen = clock_timestamp()` and `resolved = false` on conflict; raw fields refreshed) and return `None`.

- [ ] **Step 1: Write the failing tests `backend/tests/crosswalk/test_resolve.py`**

```python
from datetime import UTC, date, datetime

import pytest
from sqlalchemy import func, select

from ffh.crosswalk.resolve import (
    EXACT_CONFIDENCE,
    FUZZY_CAP,
    ResolveInput,
    Resolution,
    is_usable,
    resolve,
    resolve_many,
)
from ffh.db.models import CrosswalkUnmatched, Player, PlayerExternalId

pytestmark = pytest.mark.db


def _ids(session) -> list[PlayerExternalId]:
    return session.scalars(select(PlayerExternalId)).all()


def _unmatched(session) -> list[CrosswalkUnmatched]:
    return session.scalars(select(CrosswalkUnmatched)).all()


def _add_fake_player(session, name: str, position: str, team: str | None, gsis: str) -> Player:
    from ffh.crosswalk.normalize import normalize_name

    p = Player(
        gsis_id=gsis, full_name=name, normalized_name=normalize_name(name), position=position, team_abbr=team
    )
    session.add(p)
    session.flush()
    return p


def test_is_usable_rule():
    assert is_usable(1.0, None) and is_usable(0.95, None) and is_usable(0.9, None)
    assert not is_usable(0.89, None)
    assert is_usable(0.89, datetime.now(UTC))


def test_rung1_existing_row_wins_over_everything(db_session, seeded_registry):
    mahomes = seeded_registry["00-0033873"]
    db_session.add(PlayerExternalId(player_id=mahomes, source="sleeper", external_id="4046", confidence=1.0, match_method="dynastyprocess"))
    db_session.flush()
    # A misleading name/position/team must not matter once the id is known.
    res = resolve(db_session, "sleeper", "4046", "Some Other Name", "WR", "DEN")
    assert res == Resolution(mahomes, "dynastyprocess", 1.0)
    assert len(_ids(db_session)) == 1 and _unmatched(db_session) == []


def test_rung2_gsis_direct_and_persists_for_other_sources(db_session, seeded_registry):
    mahomes = seeded_registry["00-0033873"]
    assert resolve(db_session, "gsis", "00-0033873") == Resolution(mahomes, "gsis", 1.0)
    assert _ids(db_session) == []  # gsis lives on players; nothing to persist
    res = resolve(db_session, "sleeper", "4046", "Patrick Mahomes", "QB", "KC", gsis_id="00-0033873")
    assert res == Resolution(mahomes, "gsis", 1.0)
    row = db_session.get(PlayerExternalId, ("sleeper", "4046"))
    assert row.player_id == mahomes and row.match_method == "gsis" and row.confidence == 1.0
    # next call is a rung-1 hit with the persisted method
    assert resolve(db_session, "sleeper", "4046") == Resolution(mahomes, "gsis", 1.0)


def test_rung3_exact_name_persists_then_rung1(db_session, seeded_registry):
    mahomes = seeded_registry["00-0033873"]
    res = resolve(db_session, "sleeper", "4046", "Patrick Mahomes II", "QB", "KC")
    assert res == Resolution(mahomes, "exact_name", EXACT_CONFIDENCE)
    row = db_session.get(PlayerExternalId, ("sleeper", "4046"))
    assert row.match_method == "exact_name" and row.confidence == pytest.approx(0.95)
    assert row.verified_at is None  # ≥ 0.9 needs no verification
    again = resolve(db_session, "sleeper", "4046", "Patrick Mahomes II", "QB", "KC")
    # REAL round-trips 0.95 as float32 → compare with approx, not ==
    assert again is not None and again.player_id == mahomes and again.method == "exact_name"
    assert again.confidence == pytest.approx(EXACT_CONFIDENCE)
    assert len(_ids(db_session)) == 1


def test_ladder_order_rung3_beats_rung4(db_session, seeded_registry):
    """A name matching both an exact registry row and a near-identical fake resolves exact."""
    _add_fake_player(db_session, "DJ Moor", "WR", "BUF", "FAKE-DJ")
    res = resolve(db_session, "espn", "3915416", "D.J. Moore", "WR", "BUF")
    assert res is not None and res.method == "exact_name"
    assert res.player_id == seeded_registry["00-0034827"]


def test_rung3_team_disambiguates_same_name_same_position(db_session, seeded_registry):
    jr, sr = seeded_registry["00-0039849"], seeded_registry["00-0007024"]
    res = resolve(db_session, "sleeper", "11628", "Marvin Harrison Jr.", "WR", "ARI")
    assert res is not None and res.player_id == jr and res.method == "exact_name"
    res_sr = resolve(db_session, "pfr", "HarrMa00", "Marvin Harrison", "WR", "IND")
    assert res_sr is not None and res_sr.player_id == sr


def test_rung3_without_team_and_two_candidates_is_not_exact_and_fuzzy_ties(db_session, seeded_registry):
    # Both Harrisons are 'marvin harrison' WR; no team → rung 3 ambiguous → rung 4 tie → unmatched
    res = resolve(db_session, "yahoo", "40893", "Marvin Harrison Jr.", "WR", None)
    assert res is None
    assert _ids(db_session) == []
    (u,) = _unmatched(db_session)
    assert (u.source, u.external_id, u.raw_name) == ("yahoo", "40893", "Marvin Harrison Jr.")


def test_rung3_team_mismatch_falls_to_fuzzy_pending(db_session, seeded_registry):
    # Only one 'patrick mahomes' QB, but registry says KC and caller says DEN → not exact.
    res = resolve(db_session, "sleeper", "4046", "Patrick Mahomes", "QB", "DEN")
    assert res is None
    row = db_session.get(PlayerExternalId, ("sleeper", "4046"))
    assert row.match_method == "fuzzy" and row.verified_at is None
    assert row.confidence == pytest.approx(FUZZY_CAP)  # similarity 1.0 capped at 0.89


def test_rung3_excludes_players_already_mapped_for_source(db_session, seeded_registry):
    mahomes = seeded_registry["00-0033873"]
    db_session.add(PlayerExternalId(player_id=mahomes, source="sleeper", external_id="4046", confidence=1.0, match_method="dynastyprocess"))
    db_session.flush()
    # A second sleeper id claiming to be Mahomes must not attach: one id per source per player.
    res = resolve(db_session, "sleeper", "9999", "Patrick Mahomes", "QB", "KC")
    assert res is None
    assert db_session.get(PlayerExternalId, ("sleeper", "9999")) is None
    assert [(u.source, u.external_id) for u in _unmatched(db_session)] == [("sleeper", "9999")]


def test_rung4_fuzzy_persists_pending_and_returns_none_until_verified(db_session, seeded_registry):
    lamar = seeded_registry["00-0034796"]
    res = resolve(db_session, "sleeper", "4881", "Lamarr Jackson", "QB", "BAL")  # JW ≈ 0.9857
    assert res is None
    row = db_session.get(PlayerExternalId, ("sleeper", "4881"))
    assert row.player_id == lamar and row.match_method == "fuzzy"
    assert row.confidence == pytest.approx(FUZZY_CAP) and row.verified_at is None
    assert _unmatched(db_session) == []  # pending review is not "unmatched"
    # Second call: rung 1 sees the unverified row → still None, no duplicate, no re-guess
    assert resolve(db_session, "sleeper", "4881", "Lamarr Jackson", "QB", "BAL") is None
    assert len(_ids(db_session)) == 1
    # Human verifies → usable
    row.verified_at = datetime.now(UTC)
    db_session.flush()
    res = resolve(db_session, "sleeper", "4881", "Lamarr Jackson", "QB", "BAL")
    assert res is not None and res.player_id == lamar and res.method == "fuzzy"
    assert res.confidence == pytest.approx(FUZZY_CAP)


def test_rung4_tie_is_unmatched(db_session, seeded_registry):
    _add_fake_player(db_session, "Jaylon Waddle", "WR", "DEN", "FAKE-JW")
    # 'jaylin waddle' vs 'jaylen waddle' = 0.9692 and vs 'jaylon waddle' = 0.9692 → tie
    res = resolve(db_session, "sleeper", "7526", "Jaylin Waddle", "WR", "DEN")
    assert res is None
    assert _ids(db_session) == []
    (u,) = _unmatched(db_session)
    assert (u.source, u.external_id) == ("sleeper", "7526") and u.resolved is False


def test_rung4_birth_date_breaks_tie(db_session, seeded_registry):
    _add_fake_player(db_session, "Jaylon Waddle", "WR", "DEN", "FAKE-JW")
    res = resolve(db_session, "sleeper", "7526", "Jaylin Waddle", "WR", "DEN", birth_date=date(1998, 11, 25))
    assert res is None  # still pending review …
    row = db_session.get(PlayerExternalId, ("sleeper", "7526"))
    assert row.player_id == seeded_registry["00-0036613"]  # … but pointed at the real Waddle
    assert _unmatched(db_session) == []


def test_dst_resolves_at_rung3_from_any_spelling(db_session, seeded_registry):
    kc = seeded_registry["kc dst"]
    a = resolve(db_session, "sleeper", "KC", "Kansas City Chiefs", "DEF", "KC")
    b = resolve(db_session, "espn", "-16012", "Chiefs D/ST", "D/ST", None)
    c = resolve(db_session, "yahoo", "100012", None, "DEF", "KC")  # name missing → team → 'kc dst'
    for r in (a, b, c):
        assert r is not None and r.player_id == kc and r.method == "exact_name"


def test_unmatched_created_then_bumped(db_session, seeded_registry):
    assert resolve(db_session, "sleeper", "99999", "Nobody Nowhere", "QB", "FA") is None
    (u,) = _unmatched(db_session)
    first_seen, last_seen = u.first_seen, u.last_seen
    assert u.resolved is False and u.raw_position == "QB" and u.raw_team == "FA"
    assert resolve(db_session, "sleeper", "99999", "Nobody Nowhere", "QB", "FA") is None
    db_session.refresh(u)
    assert len(_unmatched(db_session)) == 1
    assert u.first_seen == first_seen and u.last_seen > last_seen


def test_missing_name_and_position_goes_straight_to_unmatched(db_session, seeded_registry):
    assert resolve(db_session, "sleeper", "424242", None, None, None) is None
    assert [(u.source, u.external_id) for u in _unmatched(db_session)] == [("sleeper", "424242")]


def test_resolve_many_report(db_session, seeded_registry):
    rows = [
        ResolveInput("sleeper", "4046", "Patrick Mahomes", "QB", "KC"),  # exact
        ResolveInput("sleeper", "7564", "Ja'Marr Chase", "WR", "CIN"),  # exact
        ResolveInput("sleeper", "4881", "Lamarr Jackson", "QB", "BAL"),  # fuzzy pending
        ResolveInput("sleeper", "99999", "Nobody Nowhere", "QB", "FA"),  # unmatched
        ResolveInput("gsis", "00-0038134"),  # gsis
    ]
    rep = resolve_many(db_session, rows)
    assert set(rep.resolved) == {("sleeper", "4046"), ("sleeper", "7564"), ("gsis", "00-0038134")}
    assert rep.pending_review == [("sleeper", "4881")]
    assert rep.unmatched == [("sleeper", "99999")]
    assert rep.by_method == {"exact_name": 2, "gsis": 1, "fuzzy_pending": 1, "unmatched": 1}
    assert db_session.scalar(select(func.count()).select_from(CrosswalkUnmatched)) == 1
```

- [ ] **Step 2: Run to verify it fails** — `uv run pytest tests/crosswalk/test_resolve.py -q` → `ModuleNotFoundError: ffh.crosswalk.resolve`.

- [ ] **Step 3: Write `backend/src/ffh/crosswalk/resolve.py`**

```python
"""The resolution ladder (DATABASE.md §3) — strictly in order, records which rung won.

Consumers (platform_sync in PR ⑤, ADP ingest in PR ⑥) call ``resolve`` / ``resolve_many``
and never touch ``player_external_ids`` directly. If you must query the table in SQL, apply
``confidence >= 0.9 OR verified_at IS NOT NULL`` (see ``is_usable``).
"""

from __future__ import annotations

import uuid
from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Literal

import structlog
from rapidfuzz import process
from rapidfuzz.distance import JaroWinkler
from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from ffh.crosswalk.normalize import (
    normalize_dst,
    normalize_name,
    normalize_position,
    normalize_team,
)
from ffh.db.models import CrosswalkUnmatched, Player, PlayerExternalId

log = structlog.get_logger(__name__)

EXACT_CONFIDENCE = 0.95
FUZZY_THRESHOLD = 0.92
FUZZY_CAP = 0.89
FUZZY_TIE_MARGIN = 0.01
USABLE_CONFIDENCE = 0.9

Outcome = Literal["resolved", "pending_review", "unmatched"]


@dataclass(frozen=True)
class Resolution:
    player_id: uuid.UUID
    method: str
    confidence: float


@dataclass(frozen=True)
class ResolveInput:
    source: str
    external_id: str
    raw_name: str | None = None
    raw_position: str | None = None
    raw_team: str | None = None
    gsis_id: str | None = None
    birth_date: date | None = None
    college: str | None = None

    @property
    def key(self) -> tuple[str, str]:
        return (self.source, self.external_id)


@dataclass
class ResolveManyReport:
    resolved: dict[tuple[str, str], Resolution] = field(default_factory=dict)
    unmatched: list[tuple[str, str]] = field(default_factory=list)
    pending_review: list[tuple[str, str]] = field(default_factory=list)
    by_method: Counter[str] = field(default_factory=Counter)


def is_usable(confidence: float, verified_at: datetime | None) -> bool:
    """DATABASE.md §3: confidence < 0.9 rows require human review before use."""
    return confidence >= USABLE_CONFIDENCE or verified_at is not None


def resolve(
    session: Session,
    source: str,
    external_id: str,
    raw_name: str | None = None,
    raw_position: str | None = None,
    raw_team: str | None = None,
    *,
    gsis_id: str | None = None,
    birth_date: date | None = None,
    college: str | None = None,
) -> Resolution | None:
    """Walk the ladder for one id. ``None`` means unmatched OR pending human review — both
    are recorded (``crosswalk_unmatched`` / unverified ``player_external_ids`` row)."""
    res, _outcome = _resolve(
        session,
        ResolveInput(source, external_id, raw_name, raw_position, raw_team, gsis_id, birth_date, college),
    )
    return res


def resolve_many(session: Session, rows: Iterable[ResolveInput]) -> ResolveManyReport:
    report = ResolveManyReport()
    for inp in rows:
        res, outcome = _resolve(session, inp)
        if res is not None:
            report.resolved[inp.key] = res
            report.by_method[res.method] += 1
        elif outcome == "pending_review":
            report.pending_review.append(inp.key)
            report.by_method["fuzzy_pending"] += 1
        else:
            report.unmatched.append(inp.key)
            report.by_method["unmatched"] += 1
    log.info(
        "crosswalk.resolve_many",
        resolved=len(report.resolved),
        pending_review=len(report.pending_review),
        unmatched=len(report.unmatched),
        by_method=dict(report.by_method),
    )
    return report


# ---------------------------------------------------------------------------


def _resolve(session: Session, inp: ResolveInput) -> tuple[Resolution | None, Outcome]:
    # Rung 1 — an existing crosswalk row (dynastyprocess, gsis, exact_name, verified fuzzy, manual)
    row = session.get(PlayerExternalId, (inp.source, inp.external_id))
    if row is not None:
        if is_usable(row.confidence, row.verified_at):
            return Resolution(row.player_id, row.match_method, float(row.confidence)), "resolved"
        log.info("crosswalk.resolve.pending_review", source=inp.source, external_id=inp.external_id)
        return None, "pending_review"

    # Rung 2 — gsis direct
    gsis = inp.gsis_id or (inp.external_id if inp.source == "gsis" else None)
    if gsis:
        pid = session.scalar(select(Player.player_id).where(Player.gsis_id == gsis))
        if pid is not None:
            if inp.source != "gsis":
                _persist(session, inp, pid, "gsis", 1.0)
            log.info("crosswalk.resolve.gsis", source=inp.source, external_id=inp.external_id)
            return Resolution(pid, "gsis", 1.0), "resolved"

    position = normalize_position(inp.raw_position)
    name = _canonical_name(inp, position)
    if not name or not position:
        _record_unmatched(session, inp, reason="no_name_or_position")
        return None, "unmatched"

    # Rung 3 — exact (normalized_name, position[, team])
    pid = _exact(session, inp, name, position)
    if pid is not None:
        _persist(session, inp, pid, "exact_name", EXACT_CONFIDENCE)
        log.info("crosswalk.resolve.exact", source=inp.source, external_id=inp.external_id, name=name)
        return Resolution(pid, "exact_name", EXACT_CONFIDENCE), "resolved"

    # Rung 4 — Jaro-Winkler ≥ 0.92, persisted for review, never returned unverified
    fuzzy = _fuzzy(session, inp, name, position)
    if fuzzy is not None:
        pid, similarity = fuzzy
        _persist(session, inp, pid, "fuzzy", min(similarity, FUZZY_CAP))
        log.info(
            "crosswalk.resolve.fuzzy_pending",
            source=inp.source,
            external_id=inp.external_id,
            name=name,
            similarity=round(similarity, 4),
        )
        return None, "pending_review"

    # Rung 5 — unmatched, never silently dropped
    _record_unmatched(session, inp, reason="no_candidate")
    return None, "unmatched"


def _canonical_name(inp: ResolveInput, position: str | None) -> str | None:
    if position == "DST":
        return normalize_dst(inp.raw_name) or normalize_dst(inp.raw_team) or normalize_dst(inp.external_id)
    return normalize_name(inp.raw_name) if inp.raw_name else None


def _mapped_for_source(source: str):
    return select(PlayerExternalId.player_id).where(PlayerExternalId.source == source)


def _exact(session: Session, inp: ResolveInput, name: str, position: str) -> uuid.UUID | None:
    cands = session.execute(
        select(Player.player_id, Player.team_abbr).where(
            Player.normalized_name == name,
            Player.position == position,
            Player.player_id.not_in(_mapped_for_source(inp.source)),
        )
    ).all()
    if not cands:
        return None
    team = normalize_team(inp.raw_team)
    if team is None:
        return cands[0].player_id if len(cands) == 1 else None
    compatible = [c for c in cands if c.team_abbr in (team, None)]
    return compatible[0].player_id if len(compatible) == 1 else None


def _fuzzy(session: Session, inp: ResolveInput, name: str, position: str) -> tuple[uuid.UUID, float] | None:
    rows = session.execute(
        select(Player.player_id, Player.normalized_name, Player.birth_date, Player.college).where(
            Player.position == position,
            Player.player_id.not_in(_mapped_for_source(inp.source)),
        )
    ).all()
    if not rows:
        return None
    meta = {r.player_id: (r.birth_date, r.college) for r in rows}
    choices = {r.player_id: r.normalized_name for r in rows}
    hits = process.extract(
        name, choices, scorer=JaroWinkler.normalized_similarity, score_cutoff=FUZZY_THRESHOLD, limit=None
    )
    survivors: list[tuple[uuid.UUID, float]] = []
    for _choice, score, pid in hits:
        bd, college = meta[pid]
        if inp.birth_date is not None and bd is not None and bd != inp.birth_date:
            continue
        if inp.college and college and inp.college.strip().lower() not in college.lower():
            continue
        survivors.append((pid, float(score)))
    if not survivors:
        return None
    survivors.sort(key=lambda t: t[1], reverse=True)
    if len(survivors) > 1 and survivors[0][1] - survivors[1][1] < FUZZY_TIE_MARGIN:
        log.info(
            "crosswalk.resolve.fuzzy_tie",
            source=inp.source,
            external_id=inp.external_id,
            name=name,
            top=[(str(p), round(s, 4)) for p, s in survivors[:3]],
        )
        return None
    return survivors[0]


def _persist(session: Session, inp: ResolveInput, pid: uuid.UUID, method: str, confidence: float) -> None:
    session.add(
        PlayerExternalId(
            player_id=pid,
            source=inp.source,
            external_id=inp.external_id,
            confidence=confidence,
            match_method=method,
            verified_at=None,
        )
    )
    session.flush()


def _record_unmatched(session: Session, inp: ResolveInput, *, reason: str) -> None:
    stmt = insert(CrosswalkUnmatched).values(
        source=inp.source,
        external_id=inp.external_id,
        raw_name=inp.raw_name,
        raw_position=inp.raw_position,
        raw_team=inp.raw_team,
    )
    stmt = stmt.on_conflict_do_update(
        index_elements=["source", "external_id"],
        set_={
            "raw_name": stmt.excluded.raw_name,
            "raw_position": stmt.excluded.raw_position,
            "raw_team": stmt.excluded.raw_team,
            "last_seen": func.clock_timestamp(),  # not now(): must advance inside one transaction
            "resolved": False,
        },
    )
    session.execute(stmt)
    session.flush()
    log.warning(
        "crosswalk.resolve.unmatched",
        source=inp.source,
        external_id=inp.external_id,
        raw_name=inp.raw_name,
        raw_position=inp.raw_position,
        raw_team=inp.raw_team,
        reason=reason,
    )
```

- [ ] **Step 4: Run to verify it passes** — `uv run pytest tests/crosswalk/test_resolve.py -q` → 16 passed. Watch specifically: `test_rung3_team_mismatch_falls_to_fuzzy_pending` (proves rung 3 is strict on team) and `test_rung4_tie_is_unmatched` (the JW numbers 0.9692/0.9692 were computed with rapidfuzz 3.14.5 — do not "fix" the test by loosening the margin).

- [ ] **Step 5: Lint and commit**

```bash
uv run ruff check src/ffh/crosswalk tests/crosswalk && uv run ruff format src/ffh/crosswalk tests/crosswalk
git add src/ffh/crosswalk/resolve.py tests/crosswalk/test_resolve.py
git commit -m "feat(crosswalk): five-rung resolution ladder with persisted rungs and unmatched upsert"
```

---

### Task 6: The mandatory invariant tests (DATABASE.md §3 "Mandatory tests")

Two of the four mandatory tests land here. **`test_crosswalk_covers_all_rostered_players` lands in PR ⑤** (it needs the Sleeper fixture league) and **`test_crosswalk_covers_top_300_adp` lands in PR ⑥** (it needs the ADP snapshot). Say so in the PR body.

**Files:**
- Create: `backend/tests/crosswalk/test_crosswalk_invariants.py`

**Interfaces:**
- Consumes: Task 3 `seeded_registry`; Task 4 `apply_playerids`, `read_playerids_csv`; Task 5 `resolve`, `resolve_many`, `ResolveInput`, `is_usable`; Task 7 `coverage_report` (imported lazily inside one test — write that assertion now, it goes green after Task 7).

- [ ] **Step 1: Write the tests**

```python
"""DATABASE.md §3 mandatory tests that do not need platform/ADP fixtures.

test_crosswalk_covers_all_rostered_players → PR ⑤ (needs the Sleeper league fixture)
test_crosswalk_covers_top_300_adp          → PR ⑥ (needs the ADP snapshot)
"""

from pathlib import Path

import pytest
from sqlalchemy import text

from ffh.crosswalk.dynastyprocess import apply_playerids, read_playerids_csv
from ffh.crosswalk.resolve import ResolveInput, is_usable, resolve, resolve_many

pytestmark = pytest.mark.db

FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "dynastyprocess" / "db_playerids_sample.csv"


@pytest.fixture
def populated(db_session, seeded_registry):
    """Registry + DP ids + a spread of ladder outcomes (exact, fuzzy pending, unmatched)."""
    apply_playerids(db_session, read_playerids_csv(FIXTURE.read_bytes()))
    resolve(db_session, "sleeper", "4881", "Lamarr Jackson", "QB", "BAL")  # fuzzy → pending
    resolve(db_session, "espn", "3916387", "Lamar Jackson", "QB", "BAL")  # exact 0.95
    resolve(db_session, "sleeper", "KC", "Kansas City Chiefs", "DEF", "KC")  # rung 1 (from DP row)
    resolve(db_session, "sleeper", "99999", "Nobody Nowhere", "QB", "FA")  # unmatched
    return seeded_registry


def test_crosswalk_no_duplicate_player_ids(db_session, populated):
    """No two external ids from one source map to one player_id, and vice-versa."""
    many_ids_per_player = db_session.execute(
        text(
            """
            SELECT source, player_id, count(*) AS n
            FROM player_external_ids
            GROUP BY source, player_id
            HAVING count(*) > 1
            """
        )
    ).all()
    assert many_ids_per_player == [], many_ids_per_player

    many_players_per_id = db_session.execute(
        text(
            """
            SELECT source, external_id, count(DISTINCT player_id) AS n
            FROM player_external_ids
            GROUP BY source, external_id
            HAVING count(DISTINCT player_id) > 1
            """
        )
    ).all()
    assert many_players_per_id == [], many_players_per_id  # PK guarantees this; asserted anyway

    # And the ladder cannot break it: a second sleeper id claiming an already-mapped player
    # is refused (rungs 3/4 exclude players that already hold an id for the source).
    assert resolve(db_session, "sleeper", "4046-dupe", "Patrick Mahomes", "QB", "KC") is None
    assert (
        db_session.execute(
            text("SELECT count(*) FROM player_external_ids WHERE source='sleeper' AND player_id = :pid"),
            {"pid": populated["00-0033873"]},
        ).scalar_one()
        == 1
    )


def test_crosswalk_low_confidence_reviewed(db_session, populated):
    """No confidence < 0.9 row is used unverified: resolve never returns one."""
    unverified = db_session.execute(
        text(
            "SELECT source, external_id, player_id FROM player_external_ids "
            "WHERE confidence < 0.9 AND verified_at IS NULL"
        )
    ).all()
    assert len(unverified) >= 1  # the fixture created a pending fuzzy row on purpose

    for source, external_id, _pid in unverified:
        assert resolve(db_session, source, external_id) is None

    rep = resolve_many(db_session, [ResolveInput(s, e) for s, e, _ in unverified])
    assert rep.resolved == {}
    assert sorted(rep.pending_review) == sorted((s, e) for s, e, _ in unverified)

    # Every Resolution the ladder hands out passes the consumer filter rule.
    all_rows = db_session.execute(
        text("SELECT source, external_id, confidence, verified_at FROM player_external_ids")
    ).all()
    for source, external_id, confidence, verified_at in all_rows:
        res = resolve(db_session, source, external_id)
        assert (res is not None) == is_usable(confidence, verified_at)

    # The report surfaces them (goes green after Task 7).
    from ffh.crosswalk.report import coverage_report

    report = coverage_report(db_session)
    assert {(r.source, r.external_id) for r in report.unverified_low_confidence} == {(s, e) for s, e, _ in unverified}
    assert report.ok is False
```

- [ ] **Step 2: Run** — `uv run pytest tests/crosswalk/test_crosswalk_invariants.py -q` → `test_crosswalk_no_duplicate_player_ids` passes; `test_crosswalk_low_confidence_reviewed` fails only at the final `ffh.crosswalk.report` import (Task 7 turns it green). Do not skip or comment out that assertion.

- [ ] **Step 3: Commit**

```bash
git add tests/crosswalk/test_crosswalk_invariants.py
git commit -m "test(crosswalk): mandatory no-duplicate-ids and low-confidence-reviewed invariants"
```

---

### Task 7: `ffh.crosswalk.report` + `ffh.crosswalk.review` + CLI `ffh crosswalk report|seed|verify`

**Files:**
- Create: `backend/src/ffh/crosswalk/report.py`
- Create: `backend/src/ffh/crosswalk/review.py`
- Modify: `backend/src/ffh/cli.py` (replace the placeholder `crosswalk_report`; add `seed`, `verify`)
- Create: `backend/tests/crosswalk/test_report.py`
- Create: `backend/tests/crosswalk/test_cli_crosswalk.py`

**Interfaces:**
- Consumes: Tasks 3–5; ③'s `ffh.cli._session_scope()` (context manager yielding a sync `Session`; does not commit) and ③'s `ffh.features.duck.latest_partition` (Step 6 only — **Step 6 requires PR ③ merged**; Steps 1–5 are pure).
- Produces:
  - `@dataclass(frozen=True) UnverifiedRow(source, external_id, player_id, full_name, position, confidence, created_at)`; `UnmatchedRow(source, external_id, raw_name, raw_position, raw_team, first_seen, last_seen)`.
  - `@dataclass(frozen=True) CoverageReport(players_total: int, players_by_position: dict[str, int], ids_by_source: dict[str, int], ids_by_source_method: dict[str, dict[str, int]], unverified_low_confidence: tuple[UnverifiedRow, ...], unmatched: tuple[UnmatchedRow, ...])` with `.ok -> bool`, `.to_dict() -> dict`, `.render() -> str`.
  - `coverage_report(session: Session) -> CoverageReport`.
  - `verify_mapping(session, source, external_id) -> bool` (sets `verified_at = now()`; False if no row); `reject_mapping(session, source, external_id) -> bool` (deletes the row and upserts a `crosswalk_unmatched` row so it is not forgotten; False if no row).
  - CLI: `ffh crosswalk report [--json]` (exit 1 if `unmatched > 0` or `unverified_low_confidence > 0`); `ffh crosswalk seed [--players <players.parquet>] [--playerids <db_playerids.csv|.parquet>]` (`--players` defaults to ③'s `latest_partition(settings.lake_root, "nflverse", "players")`; exit 1 with the `ffh ingest run nflverse_players` remedy if the lake is empty); `ffh crosswalk verify SOURCE EXTERNAL_ID [--reject]`.

- [ ] **Step 1: Write the failing tests `backend/tests/crosswalk/test_report.py`**

```python
import json
from pathlib import Path

import pytest
from sqlalchemy import select

from ffh.crosswalk.dynastyprocess import apply_playerids, read_playerids_csv
from ffh.crosswalk.report import CoverageReport, coverage_report
from ffh.crosswalk.resolve import resolve
from ffh.crosswalk.review import reject_mapping, verify_mapping
from ffh.db.models import CrosswalkUnmatched, PlayerExternalId

pytestmark = pytest.mark.db

FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "dynastyprocess" / "db_playerids_sample.csv"


def test_report_on_empty_db_is_ok(db_session):
    rep = coverage_report(db_session)
    assert isinstance(rep, CoverageReport)
    assert rep.ok and rep.players_total == 0 and rep.unmatched == () and rep.unverified_low_confidence == ()
    assert "unmatched: 0" in rep.render()
    json.dumps(rep.to_dict())  # serializable


def test_report_counts_and_flags(db_session, seeded_registry):
    apply_playerids(db_session, read_playerids_csv(FIXTURE.read_bytes()))
    resolve(db_session, "sleeper", "4881", "Lamarr Jackson", "QB", "BAL")  # fuzzy pending
    resolve(db_session, "espn", "3916387", "Lamar Jackson", "QB", "BAL")  # exact
    resolve(db_session, "sleeper", "99999", "Nobody Nowhere", "QB", "FA")  # unmatched
    rep = coverage_report(db_session)
    assert rep.players_total == 14 + 32 + 2
    assert rep.players_by_position["DST"] == 32 and rep.players_by_position["QB"] == 5
    # DP wrote 10 sleeper ids (Mahomes, Chase, DJ Moore, Walker, Butker, Juszczyk, MHJ, Pavia,
    # Fred 2295, Chiefs KC) and 10 espn ids; see the Task 4 oracle.
    assert rep.ids_by_source["sleeper"] == 11  # 10 DP + the pending fuzzy row
    assert rep.ids_by_source_method["sleeper"] == {"dynastyprocess": 10, "fuzzy": 1}
    assert rep.ids_by_source_method["espn"] == {"dynastyprocess": 10, "exact_name": 1}
    assert [(r.source, r.external_id, r.full_name) for r in rep.unverified_low_confidence] == [
        ("sleeper", "4881", "Lamar Jackson")
    ]
    assert rep.unverified_low_confidence[0].confidence == pytest.approx(0.89)
    assert [(r.source, r.external_id, r.raw_name) for r in rep.unmatched] == [("sleeper", "99999", "Nobody Nowhere")]
    assert rep.ok is False
    d = rep.to_dict()
    assert d["ok"] is False and d["unmatched"][0]["external_id"] == "99999"
    text = rep.render()
    assert "unverified low-confidence: 1" in text and "unmatched: 1" in text and "Nobody Nowhere" in text


def test_verify_and_reject(db_session, seeded_registry):
    resolve(db_session, "sleeper", "4881", "Lamarr Jackson", "QB", "BAL")
    assert verify_mapping(db_session, "sleeper", "4881") is True
    row = db_session.get(PlayerExternalId, ("sleeper", "4881"))
    db_session.refresh(row)
    assert row.verified_at is not None and row.verified_at.tzinfo is not None
    assert resolve(db_session, "sleeper", "4881") is not None  # now usable
    assert coverage_report(db_session).ok
    assert verify_mapping(db_session, "sleeper", "nope") is False

    assert reject_mapping(db_session, "sleeper", "4881") is True
    assert db_session.get(PlayerExternalId, ("sleeper", "4881")) is None
    u = db_session.scalar(select(CrosswalkUnmatched).where(CrosswalkUnmatched.external_id == "4881"))
    assert u is not None and u.source == "sleeper" and u.resolved is False
    assert reject_mapping(db_session, "sleeper", "4881") is False
```

- [ ] **Step 2: Write the failing CLI tests `backend/tests/crosswalk/test_cli_crosswalk.py`**

```python
import json
from datetime import UTC, datetime

from typer.testing import CliRunner

import ffh.cli as cli
from ffh.crosswalk.report import CoverageReport, UnmatchedRow

runner = CliRunner()


def _fake_report(unmatched: int) -> CoverageReport:
    rows = tuple(
        UnmatchedRow("sleeper", str(i), "Nobody", "QB", "FA", datetime.now(UTC), datetime.now(UTC))
        for i in range(unmatched)
    )
    return CoverageReport(
        players_total=1,
        players_by_position={"QB": 1},
        ids_by_source={},
        ids_by_source_method={},
        unverified_low_confidence=(),
        unmatched=rows,
    )


def test_report_exit_0_when_clean(monkeypatch):
    monkeypatch.setattr(cli, "_coverage_report_for_cli", lambda: _fake_report(0))
    result = runner.invoke(cli.app, ["crosswalk", "report"])
    assert result.exit_code == 0, result.output
    assert "unmatched: 0" in result.output


def test_report_exit_1_when_unmatched(monkeypatch):
    monkeypatch.setattr(cli, "_coverage_report_for_cli", lambda: _fake_report(2))
    result = runner.invoke(cli.app, ["crosswalk", "report"])
    assert result.exit_code == 1
    assert "unmatched: 2" in result.output


def test_report_json(monkeypatch):
    monkeypatch.setattr(cli, "_coverage_report_for_cli", lambda: _fake_report(1))
    result = runner.invoke(cli.app, ["crosswalk", "report", "--json"])
    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert payload["ok"] is False and len(payload["unmatched"]) == 1


def test_crosswalk_help_lists_commands():
    result = runner.invoke(cli.app, ["crosswalk", "--help"])
    assert result.exit_code == 0
    for cmd in ("report", "seed", "verify"):
        assert cmd in result.output
```

- [ ] **Step 3: Run to verify they fail** — `uv run pytest tests/crosswalk/test_report.py tests/crosswalk/test_cli_crosswalk.py -q` → import errors.

- [ ] **Step 4: Write `backend/src/ffh/crosswalk/report.py`**

```python
"""Coverage report for the crosswalk: what is mapped, what needs review, what is unmatched."""

from __future__ import annotations

import uuid
from dataclasses import asdict, dataclass
from datetime import datetime

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from ffh.crosswalk.resolve import USABLE_CONFIDENCE
from ffh.db.models import CrosswalkUnmatched, Player, PlayerExternalId


@dataclass(frozen=True)
class UnverifiedRow:
    source: str
    external_id: str
    player_id: uuid.UUID
    full_name: str
    position: str
    confidence: float
    created_at: datetime


@dataclass(frozen=True)
class UnmatchedRow:
    source: str
    external_id: str
    raw_name: str | None
    raw_position: str | None
    raw_team: str | None
    first_seen: datetime
    last_seen: datetime


@dataclass(frozen=True)
class CoverageReport:
    players_total: int
    players_by_position: dict[str, int]
    ids_by_source: dict[str, int]
    ids_by_source_method: dict[str, dict[str, int]]
    unverified_low_confidence: tuple[UnverifiedRow, ...]
    unmatched: tuple[UnmatchedRow, ...]

    @property
    def ok(self) -> bool:
        return not self.unverified_low_confidence and not self.unmatched

    def to_dict(self) -> dict[str, object]:
        d = asdict(self)
        d["ok"] = self.ok
        for key in ("unverified_low_confidence", "unmatched"):
            d[key] = [
                {k: (str(v) if isinstance(v, uuid.UUID | datetime) else v) for k, v in row.items()}
                for row in d[key]
            ]
        return d

    def render(self) -> str:
        lines = [
            f"players: {self.players_total} " + " ".join(f"{p}={n}" for p, n in sorted(self.players_by_position.items())),
            "external ids by source / method:",
        ]
        for source in sorted(self.ids_by_source):
            methods = ", ".join(f"{m}={n}" for m, n in sorted(self.ids_by_source_method[source].items()))
            lines.append(f"  {source:<12}{self.ids_by_source[source]:>7}  ({methods})")
        lines.append(f"unverified low-confidence: {len(self.unverified_low_confidence)}")
        for r in self.unverified_low_confidence:
            lines.append(f"  {r.source}:{r.external_id} -> {r.full_name} ({r.position}) conf={r.confidence:.2f}")
        lines.append(f"unmatched: {len(self.unmatched)}")
        for r in self.unmatched:
            lines.append(
                f"  {r.source}:{r.external_id} {r.raw_name!r} {r.raw_position} {r.raw_team} "
                f"first={r.first_seen:%Y-%m-%d} last={r.last_seen:%Y-%m-%d}"
            )
        lines.append("OK" if self.ok else "ATTENTION REQUIRED")
        return "\n".join(lines)


def coverage_report(session: Session) -> CoverageReport:
    players_total = session.scalar(select(func.count()).select_from(Player)) or 0
    players_by_position = {
        pos: n for pos, n in session.execute(select(Player.position, func.count()).group_by(Player.position))
    }
    by_source_method: dict[str, dict[str, int]] = {}
    for source, method, n in session.execute(
        select(PlayerExternalId.source, PlayerExternalId.match_method, func.count()).group_by(
            PlayerExternalId.source, PlayerExternalId.match_method
        )
    ):
        by_source_method.setdefault(source, {})[method] = n
    ids_by_source = {s: sum(m.values()) for s, m in by_source_method.items()}

    unverified = tuple(
        UnverifiedRow(e.source, e.external_id, e.player_id, p.full_name, p.position, float(e.confidence), e.created_at)
        for e, p in session.execute(
            select(PlayerExternalId, Player)
            .join(Player, Player.player_id == PlayerExternalId.player_id)
            .where(PlayerExternalId.confidence < USABLE_CONFIDENCE, PlayerExternalId.verified_at.is_(None))
            .order_by(PlayerExternalId.source, PlayerExternalId.external_id)
        )
    )
    unmatched = tuple(
        UnmatchedRow(u.source, u.external_id, u.raw_name, u.raw_position, u.raw_team, u.first_seen, u.last_seen)
        for u in session.scalars(
            select(CrosswalkUnmatched)
            .where(CrosswalkUnmatched.resolved.is_(False))
            .order_by(CrosswalkUnmatched.source, CrosswalkUnmatched.external_id)
        )
    )
    return CoverageReport(
        players_total=players_total,
        players_by_position=players_by_position,
        ids_by_source=ids_by_source,
        ids_by_source_method=by_source_method,
        unverified_low_confidence=unverified,
        unmatched=unmatched,
    )
```

- [ ] **Step 5: Write `backend/src/ffh/crosswalk/review.py`**

```python
"""Human review actions for rung-4 (fuzzy) rows and bad mappings."""

from __future__ import annotations

import structlog
from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from ffh.db.models import CrosswalkUnmatched, PlayerExternalId

log = structlog.get_logger(__name__)


def verify_mapping(session: Session, source: str, external_id: str) -> bool:
    """Mark a mapping as human-verified (usable regardless of confidence)."""
    row = session.get(PlayerExternalId, (source, external_id))
    if row is None:
        return False
    row.verified_at = func.now()
    session.flush()
    log.info("crosswalk.review.verified", source=source, external_id=external_id, player_id=str(row.player_id))
    return True


def reject_mapping(session: Session, source: str, external_id: str) -> bool:
    """Delete a wrong mapping and park the id in crosswalk_unmatched so it is not forgotten."""
    row = session.get(PlayerExternalId, (source, external_id))
    if row is None:
        return False
    session.delete(row)
    stmt = insert(CrosswalkUnmatched).values(source=source, external_id=external_id)
    stmt = stmt.on_conflict_do_update(
        index_elements=["source", "external_id"],
        set_={"last_seen": func.clock_timestamp(), "resolved": False},
    )
    session.execute(stmt)
    session.flush()
    log.info("crosswalk.review.rejected", source=source, external_id=external_id)
    return True


def mark_unmatched_resolved(session: Session, source: str, external_id: str) -> bool:
    """Flip crosswalk_unmatched.resolved after a mapping was created by hand."""
    u = session.scalar(
        select(CrosswalkUnmatched).where(
            CrosswalkUnmatched.source == source, CrosswalkUnmatched.external_id == external_id
        )
    )
    if u is None:
        return False
    u.resolved = True
    session.flush()
    return True
```

- [ ] **Step 6: Replace the placeholder in `backend/src/ffh/cli.py`** — **requires PR ③ merged; rebase onto `main` first** (`git fetch origin && git rebase origin/main`). After the rebase `cli.py` already contains ③'s `_session_scope()` context manager and the shared imports (`json`, `contextmanager`, `Iterator`, `Session`, `get_settings`, `make_engine`, `make_session_factory`) — ③ owns those lines. **Reuse `_session_scope()`; do not add a second session helper and do not re-import what ③ already imports** (`ruff check` flags duplicates). ③'s helper does not commit, so every write path below calls `session.commit()` explicitly. Delete the `crosswalk_report` placeholder (the last block of the file) and add only:

```python
from pathlib import Path

from ffh.features.duck import latest_partition

# ... ③'s imports, `_session_scope`, and the app/typer group definitions stay as they are ...


def _coverage_report_for_cli():
    """Indirection so tests can monkeypatch the report without a database."""
    from ffh.crosswalk.report import coverage_report

    with _session_scope() as session:
        return coverage_report(session)


@crosswalk_app.command("report")
def crosswalk_report(
    json_out: bool = typer.Option(False, "--json", help="Machine-readable output."),
) -> None:
    """Crosswalk coverage report. Exit 1 if anything is unmatched or awaiting review."""
    rep = _coverage_report_for_cli()
    typer.echo(json.dumps(rep.to_dict(), indent=2, default=str) if json_out else rep.render())
    raise typer.Exit(code=0 if rep.ok else 1)


@crosswalk_app.command("seed")
def crosswalk_seed(
    players: Path | None = typer.Option(
        None,
        "--players",
        exists=True,
        help="nflverse players.parquet. Default: the newest raw/nflverse/players partition in "
        "FFH_LAKE_ROOT (landed by `ffh ingest run nflverse_players`, PR ③).",
    ),
    playerids: Path | None = typer.Option(
        None, "--playerids", exists=True, help="DynastyProcess db_playerids (.csv or .parquet)"
    ),
) -> None:
    """Seed the players registry (and DSTs) from nflverse; optionally apply DynastyProcess ids."""
    import polars as pl

    from ffh.crosswalk.dynastyprocess import apply_playerids, read_playerids_csv
    from ffh.crosswalk.registry import seed_players

    if players is None:
        # ③'s ffh.features.duck.latest_partition — the same "newest scrape_date" rule the
        # DuckDB views use; returns None when nothing has landed yet.
        players = latest_partition(get_settings().lake_root, "nflverse", "players")
        if players is None:
            typer.echo(
                "no nflverse players partition in the lake; run `ffh ingest run "
                "nflverse_players` first or pass --players",
                err=True,
            )
            raise typer.Exit(code=1)

    with _session_scope() as session:
        n = seed_players(session, pl.read_parquet(players))
        typer.echo(f"players upserted (incl. 32 DST): {n}")
        if playerids is not None:
            frame = (
                read_playerids_csv(playerids.read_bytes())
                if playerids.suffix == ".csv"
                else pl.read_parquet(playerids)
            )
            report = apply_playerids(session, frame)
            typer.echo(json.dumps(report.__dict__, indent=2))
        session.commit()


@crosswalk_app.command("verify")
def crosswalk_verify(
    source: str = typer.Argument(..., help="sleeper|espn|yahoo|pfr|fantasypros|sportradar|rotowire"),
    external_id: str = typer.Argument(...),
    reject: bool = typer.Option(False, "--reject", help="Delete the mapping instead of verifying it."),
) -> None:
    """Human review of a crosswalk row: mark verified (default) or reject."""
    from ffh.crosswalk.review import reject_mapping, verify_mapping

    with _session_scope() as session:
        ok = (reject_mapping if reject else verify_mapping)(session, source, external_id)
        if ok:
            session.commit()
    if not ok:
        typer.echo(f"no crosswalk row for {source}:{external_id}", err=True)
        raise typer.Exit(code=1)
    typer.echo(("rejected " if reject else "verified ") + f"{source}:{external_id}")
```

`latest_partition(lake_root, source, asset, season=None) -> Path | None` is ③'s
(`ffh.features.duck`, its Task 7) and returns the path of the newest
`.../scrape_date=YYYY-MM-DD/players.parquet` **file** (lexicographic max, which is
chronological). Keep ③'s `ingest_*` commands and ⑤'s `league_platforms` placeholder
untouched. `tests/test_cli.py::test_subcommand_groups_exist` and ③'s
`tests/test_cli_ingest.py` must still pass.

Add to `backend/tests/crosswalk/test_cli_crosswalk.py`:

```python
def test_seed_without_players_and_empty_lake_exits_1(monkeypatch, tmp_path):
    monkeypatch.setenv("FFH_LAKE_ROOT", str(tmp_path))
    result = runner.invoke(cli.app, ["crosswalk", "seed"])
    assert result.exit_code == 1
    assert "ffh ingest run nflverse_players" in result.output
```

(`get_settings` is `lru_cache`d and the repo's `conftest.py` clears it around every test, so
the env override is picked up.)

- [ ] **Step 7: Run everything** — `uv run pytest tests/crosswalk tests/test_cli.py -q` → all pass, including `test_crosswalk_low_confidence_reviewed` from Task 6.

- [ ] **Step 8: Lint and commit**

```bash
uv run ruff check . && uv run ruff format .
git add src/ffh/crosswalk/report.py src/ffh/crosswalk/review.py src/ffh/cli.py tests/crosswalk/test_report.py tests/crosswalk/test_cli_crosswalk.py
git commit -m "feat(crosswalk): coverage report, review actions, and ffh crosswalk report|seed|verify"
```

---

### Task 8: `DynastyProcessPlayerIdsJob` (IngestJob) — **requires PR ③ merged; rebase onto `main` first**

Before starting: `git fetch origin && git rebase origin/main`, then **open `backend/src/ffh/ingest/base.py`, `http.py`, `lake.py` and the nflverse jobs in `nflverse.py`** and mirror their exact API. ③'s merged contract (its plan Tasks 1–3), which the code below already follows:

- `ffh.ingest.base.HttpIngestJob(IngestJob)` — subclass **this**, not the bare `IngestJob`: it supplies `fetch()` (built on `make_client()` + `get_bytes(client, url, etag)`) and asks only for `url(self) -> str`. Do **not** write a custom `fetch`; ③'s `get_bytes` signature is `get_bytes(client: httpx.Client, url: str, etag: str | None = None)`, not `get_bytes(url, etag=)`.
- ClassVars: `name: str` (**required** — `run()` logs it and the registry keys on it), `source`, `asset`, `REQUIRED_COLUMNS: frozenset[str]` (checked by the base `validate()` along with the empty-frame case; both raise `IngestValidationError`).
- Registration is the **`@register` class decorator** from `ffh.ingest.base` (raises on a duplicate `name`); there is no `JOBS[...] = ...` assignment. Job modules are imported eagerly in `backend/src/ffh/cli.py` (③'s `from ffh.ingest import games as _games  # noqa: F401` block).
- `partition()` returns `{"scrape_date": scrape_date()}` using **③'s `ffh.ingest.lake.scrape_date`** (UTC) — never a local `date.today()`/`datetime.now()`.
- The bytes→DataFrame hook is `parse(self, content: bytes) -> pl.DataFrame`; the result dataclass is `IngestRunResult(status, rows_written, output_path, error, run_id)`; statuses are `success | failed | skipped_not_modified | skipped`.

**Files:**
- Modify: `backend/src/ffh/crosswalk/dynastyprocess.py` (append the job class at the bottom — the only `ffh.ingest` import in the package)
- Create: `backend/tests/crosswalk/test_dynastyprocess_job.py`
- Modify: `backend/src/ffh/cli.py` — one eager import line next to ③'s job-module imports (Step 3)

**Interfaces:**
- Consumes: PR ③ `ffh.ingest.base.{HttpIngestJob, IngestValidationError, register, get_job}`, `ffh.ingest.lake.scrape_date`; Task 4 `read_playerids_csv`, `DP_REQUIRED_COLUMNS`, `DP_TEXT_COLUMNS`.
- Produces: job `name = "dynastyprocess_playerids"` → lands `LAKE_ROOT/raw/dynastyprocess/playerids/scrape_date=YYYY-MM-DD/playerids.parquet` (new partition per scrape, never overwrite); `ffh ingest run dynastyprocess_playerids`.

- [ ] **Step 1: Write the failing test `backend/tests/crosswalk/test_dynastyprocess_job.py`**

```python
from pathlib import Path

import httpx
import polars as pl
import pytest
import respx

from ffh.crosswalk.dynastyprocess import DP_URL, DynastyProcessPlayerIdsJob
from ffh.ingest.base import IngestValidationError, get_job
from ffh.ingest.lake import scrape_date

pytestmark = pytest.mark.db

FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "dynastyprocess" / "db_playerids_sample.csv"


def test_job_is_registered():
    # ③'s @register keys JOBS on the `name` ClassVar.
    assert DynastyProcessPlayerIdsJob.name == "dynastyprocess_playerids"
    assert get_job("dynastyprocess_playerids") is DynastyProcessPlayerIdsJob
    job = DynastyProcessPlayerIdsJob()
    assert (job.source, job.asset) == ("dynastyprocess", "playerids")
    assert job.url() == DP_URL
    # ③'s UTC clock, the same key every other lake partition uses.
    assert job.partition() == {"scrape_date": scrape_date()}


def test_validate_requires_columns_and_text_ids():
    job = DynastyProcessPlayerIdsJob()
    df = job.parse(FIXTURE.read_bytes())
    job.validate(df)
    assert df.schema["sleeper_id"] == pl.Utf8
    # ③'s contract: validate() raises IngestValidationError so run() maps it to `failed`.
    with pytest.raises(IngestValidationError, match="sleeper_id"):
        job.validate(df.drop("sleeper_id"))
    with pytest.raises(IngestValidationError, match="0 rows"):
        job.validate(df.head(0))
    with pytest.raises(IngestValidationError, match="non-text"):
        job.validate(df.with_columns(pl.col("espn_id").cast(pl.Int64, strict=False)))


@respx.mock
def test_run_lands_parquet_then_304_is_skipped(db_session, tmp_path):
    route = respx.get(DP_URL).mock(
        return_value=httpx.Response(200, content=FIXTURE.read_bytes(), headers={"ETag": '"abc"'})
    )
    job = DynastyProcessPlayerIdsJob()
    result = job.run(db_session, tmp_path)
    assert result.status == "success" and result.rows_written == 13
    out = Path(result.output_path)
    assert out.exists() and out.parts[-4:-1] == ("dynastyprocess", "playerids", out.parts[-2])
    assert out.parts[-2].startswith("scrape_date=")
    landed = pl.read_parquet(out)
    assert landed.schema["sportradar_id"] == pl.Utf8 and landed.height == 13

    route.mock(return_value=httpx.Response(304))
    second = job.run(db_session, tmp_path)
    assert second.status == "skipped_not_modified"
    assert second.rows_written is None and second.output_path is None
```

The 304 arrives because ③'s `HttpIngestJob.fetch` sends `If-None-Match` with the ETag of
the last **successful** `ingest_runs` row for `(dynastyprocess, playerids)` — the first run
above stored `"abc"`. The assertions (success + rows + partition path + 304 →
`skipped_not_modified`) are the contract.

- [ ] **Step 2: Append to `backend/src/ffh/crosswalk/dynastyprocess.py`**

```python
# ---------------------------------------------------------------------------
# IngestJob — the only ffh.ingest dependency in ffh.crosswalk (requires PR ③).
# ---------------------------------------------------------------------------

from ffh.ingest.base import HttpIngestJob, IngestValidationError, register  # noqa: E402
from ffh.ingest.lake import scrape_date  # noqa: E402

DP_URL = "https://raw.githubusercontent.com/dynastyprocess/data/master/files/db_playerids.csv"


@register
class DynastyProcessPlayerIdsJob(HttpIngestJob):
    """Fetch db_playerids.csv (ETag-aware via ③'s HttpIngestJob.fetch) and land it as
    Parquet with text id columns."""

    name: ClassVar[str] = "dynastyprocess_playerids"
    source: ClassVar[str] = "dynastyprocess"
    asset: ClassVar[str] = "playerids"
    REQUIRED_COLUMNS: ClassVar[frozenset[str]] = DP_REQUIRED_COLUMNS

    def url(self) -> str:
        return DP_URL

    def partition(self) -> dict[str, str]:
        # ③'s UTC clock — the same key every other lake partition uses.
        return {"scrape_date": scrape_date()}

    def parse(self, content: bytes) -> pl.DataFrame:
        return read_playerids_csv(content)

    def validate(self, df: pl.DataFrame) -> None:
        # ③'s base checks REQUIRED_COLUMNS and the empty frame (IngestValidationError).
        super().validate(df)
        wrong = [c for c in sorted(DP_TEXT_COLUMNS) if df.schema[c] != pl.Utf8]
        if wrong:
            raise IngestValidationError(
                f"{type(self).name}: id columns must be text, got non-text: {wrong}"
            )
```

Add `from typing import ClassVar` to the module's top-of-file stdlib imports (Task 4 wrote
none from `typing`). The `ffh.ingest` imports sit at the module tail so the pure part of `dynastyprocess.py`
stays import-light; keep the `noqa: E402`s, or move them to the top **only if** the pure
tests in Tasks 4–7 still pass with ③'s modules importable — they will be, once ③ is
merged, so prefer top-of-module imports and delete the `noqa`s.

- [ ] **Step 3: Register/import** — `@register` fills ③'s `JOBS` at import time, so add
`from ffh.crosswalk import dynastyprocess as _dynastyprocess  # noqa: F401` to
`backend/src/ffh/cli.py` next to ③'s `from ffh.ingest import games as _games  # noqa: F401`
/ `nflverse` / `reference` lines (that is where ③ collects jobs; if ③ moved the block to
`ffh/ingest/__init__.py`, put it there) so `ffh ingest list` shows `dynastyprocess_playerids`.

- [ ] **Step 4: Run** — `uv run pytest tests/crosswalk tests/test_cli_ingest.py -q && uv run ruff check . && uv run ruff format --check .` → green. Then a manual smoke against local compose (network, not a test): `uv run ffh ingest run dynastyprocess_playerids` → prints the landed path; `uv run ffh crosswalk seed --playerids <landed playerids.parquet>` (`--players` defaults to ③'s latest nflverse `players` partition; see Task 7); `uv run ffh crosswalk report` — expect `unmatched: 0`, `unverified low-confidence: 0` and roughly `sleeper ≈ 5,600` DP ids (5,673 fantasy-position rows in the live file, minus the ambiguous handful).

- [ ] **Step 5: Commit**

```bash
git add src/ffh/crosswalk/dynastyprocess.py tests/crosswalk/test_dynastyprocess_job.py src/ffh/cli.py
git commit -m "feat(crosswalk): dynastyprocess_playerids ingest job landing db_playerids.csv to the lake"
```

---

### Task 9: Docs (DATABASE.md §3, DATA_SOURCES.md §5, ROADMAP), full verification, PR body

**Files:**
- Modify: `docs/DATABASE.md` §3
- Modify: `docs/DATA_SOURCES.md` §5 (DynastyProcess row) — **§1 is ③'s**: its "Verified schemas" table already records the `players.parquet` columns; do not add a second note there
- Modify: `docs/ROADMAP.md` (tick "★ Player ID crosswalk ★")

- [ ] **Step 1: DATABASE.md §3** — after "### Name normalization must handle", add a new subsection:

```markdown
### Phase 0 implementation notes (PR ④, `ffh.crosswalk`)

- **Alias table:** `ffh.crosswalk.normalize.ALIASES` (first token only, after suffix
  stripping; ≥ 28 entries incl. Robby/Robbie/Rob→Robert, Cam→Cameron, Mitch→Mitchell,
  Josh→Joshua, Mike→Michael, Matt→Matthew, Chris→Christopher, Nick→Nicholas, Pat→Patrick,
  Will→William, Ken/Kenny→Kenneth, Tony→Anthony, Dan→Daniel). Team spellings live in
  `normalize.TEAMS` (32 rows: nflverse abbr, city, nickname, MFL/PFR/ESPN aliases).
- **`normalize_name`:** lowercase → drop `.` `'` `’` `-` → non-alphanumerics to space →
  merge single-letter runs (`D J`→`dj`) → strip trailing `jr sr ii iii iv v` → alias first
  token. `Amon-Ra`→`amonra` but `Amon Ra`→`amon ra` (hyphen removed, space kept) — sources
  spell it hyphenated.
- **DST canonical form:** `players.normalized_name = "<abbr lowercase> dst"` (`kc dst`),
  `position = 'DST'`, `full_name = "<City> <Nickname> DST"`, `gsis_id NULL`, `team_abbr`
  set. 32 rows created by `seed_dst_players`. Any spelling (`KC`, `KC DST`, `Chiefs D/ST`,
  `Kansas City`, `KCC`, `KAN`, `LAR`, `WSH`, `Bucs`, …) resolves via `normalize_dst`.
  Position aliases: `DEF`/`D/ST`→`DST`, `PK`→`K`, `FB`/`HB`→`RB`.
- **Rung 3 and team:** `players.team_abbr` (nflverse `latest_team`, refreshed by
  `seed_players`) is the tie-breaker. Candidates = same `(normalized_name, position)` that
  do not already hold an id for the source. No team given → match iff exactly one
  candidate. Team given → keep candidates whose `team_abbr` equals it or is NULL, match
  iff exactly one remains. A team disagreement never matches at rung 3 (falls to rung 4).
- **Rung 4 semantics:** rapidfuzz Jaro-Winkler `≥ 0.92` on same-position candidates not
  already mapped for the source, filtered by `birth_date` / `college` when the caller
  supplies them; two survivors within `0.01` = tie = not matched. A hit is **persisted**
  with `match_method='fuzzy'`, `confidence = min(sim, 0.89)`, `verified_at NULL` and
  `resolve` returns `None` ("pending review", not "unmatched"). `ffh crosswalk verify
  <source> <id>` sets `verified_at`; `--reject` deletes the row and parks the id in
  `crosswalk_unmatched`.
- **Consumer filter rule:** a row is usable iff `confidence >= 0.9 OR verified_at IS NOT
  NULL` (`ffh.crosswalk.resolve.is_usable`). `resolve`/`resolve_many` already apply it;
  any direct SQL over `player_external_ids` must too.
- **One id per source per player** — enforced by construction (rungs 3/4 exclude players
  already mapped for the source; DynastyProcess apply drops intra-file ambiguities) and by
  `test_crosswalk_no_duplicate_player_ids`. Not a DB constraint (schema unchanged).
- **DynastyProcess apply policy:** positions normalized; rows with no id skipped and
  counted; `gsis` → registry player, else an existing id → its player, else a new
  `players` row (rookies; one per gsis or per `mfl_id`); ids appearing on >1 player, or
  a player holding >1 id for a source, are dropped and reported (`ambiguous`); an existing
  row pointing at a *different* player raises `CrosswalkConflictError` before any write;
  same player at a lower rung is upgraded to `dynastyprocess/1.0`. Ids stored as TEXT
  exactly as in the CSV. Team codes are MFL-style and go through `normalize_team`.
- **Rung 5:** `crosswalk_unmatched` upsert — `first_seen` on insert, `last_seen =
  clock_timestamp()` and `resolved = false` on every re-sighting, raw fields refreshed.
- **`ffh crosswalk report`** exits 1 if `unmatched > 0` or unverified `< 0.9` rows exist.
- **Mandatory tests:** `test_crosswalk_no_duplicate_player_ids` and
  `test_crosswalk_low_confidence_reviewed` live in
  `backend/tests/crosswalk/test_crosswalk_invariants.py`;
  `test_crosswalk_covers_all_rostered_players` lands with the Sleeper adapter (PR ⑤),
  `test_crosswalk_covers_top_300_adp` with ADP ingest (PR ⑥).
```

Also, in the "Resolution order" list, append to item 3: "(team via `players.team_abbr`; see notes below)" and to item 4: "(persisted unverified; `resolve` returns `None` until a human verifies)".

- [ ] **Step 2: DATA_SOURCES.md** — §5 DynastyProcess row: append to the "Use for" cell: "Verified 2026-08-16: 35 columns, 12,472 rows, `NA` = null; id columns `mfl_id sportradar_id fantasypros_id gsis_id pff_id sleeper_id nfl_id espn_id yahoo_id fleaflicker_id cbs_id pfr_id cfbref_id rotowire_id rotoworld_id ktc_id stats_id stats_global_id fantasy_data_id swish_id` + `name merge_name position team birthdate age draft_year draft_round draft_pick draft_ovr twitter_username height weight college db_season`. Kickers are `PK`; no DST rows; `team` is MFL-style (`KCC`, `TBB`, `GBP`, `NEP`, `NOS`, `SFO`, `LVR`, `LAR`, `JAC`); read ids as text; a few duplicate-id glitches exist (handled by `ffh.crosswalk.dynastyprocess`)." **Do not touch §1** — ③ is authoritative there (its "Verified schemas (2026-08-16)" table already lists the `players.parquet` gotchas: `display_name`, `college_name`, `rookie_season`, `birth_date` String); a second note would drift.

- [ ] **Step 3: ROADMAP.md** — tick `- [x] **★ Player ID crosswalk ★** — …`. ③ (nflverse ingest, games.csv) and ⑤ (Sleeper adapter) tick the adjacent Phase 0 lines; expect a trivial rebase conflict in this block and **keep every tick** when resolving.

- [ ] **Step 4: Full verification**

```bash
uv run ruff check . && uv run ruff format --check . && uv run pytest -q
```

Everything green including `tests/db/test_migrations.py` (0002 in the chain), `tests/test_engine_purity.py`, `tests/test_cli.py`.

- [ ] **Step 5: Commit docs**

```bash
git add ../docs/DATABASE.md ../docs/DATA_SOURCES.md ../docs/ROADMAP.md
git commit -m "docs(crosswalk): record alias table, DST form, rung-3 team rule, review flow; tick roadmap"
```

- [ ] **Step 6: PR body ready (do NOT push — Chris pushes and opens the PR, then runs the Codex review)**

```
feat(crosswalk): player ID crosswalk — normalization, registry, DynastyProcess, ladder, report (Phase 0 ④)

## Summary
- ffh.crosswalk.normalize: name normalization (suffixes, initials, apostrophes/hyphens, 28-entry alias table), 32-team table, DST canonical form `<abbr> dst`, position aliases (PK→K, FB→RB, DEF→DST)
- ffh.crosswalk.registry: seed `players` from nflverse players.parquet (upsert on gsis_id, updated_at=now()), 32 DST rows, dropped-row reporting
- ffh.crosswalk.dynastyprocess: db_playerids.csv → player_external_ids at rung 1 (text ids, rookies without gsis, ambiguity + conflict policies) + `dynastyprocess_playerids` IngestJob
- ffh.crosswalk.resolve: five-rung ladder strictly in order; rungs 2–4 persist so the next call is rung 1; fuzzy is pending review until verified; unmatched upsert
- ffh.crosswalk.report / review + CLI `ffh crosswalk report|seed|verify` (report exits 1 on unmatched or unverified low-confidence)
- Migration 0002: `players.team_abbr` (rung-3 tie-breaker) — deviation recorded in DATABASE.md §2/§3

## Mandatory tests
- test_crosswalk_no_duplicate_player_ids ✅ (backend/tests/crosswalk/test_crosswalk_invariants.py)
- test_crosswalk_low_confidence_reviewed ✅ (same file)
- test_crosswalk_covers_all_rostered_players → PR ⑤ (needs Sleeper fixture league)
- test_crosswalk_covers_top_300_adp → PR ⑥ (needs ADP snapshot)

## Live-verified sources (2026-08-16)
players.parquet 25,033 × 39; db_playerids.csv 12,472 × 35 — column lists in DATA_SOURCES.md.

## Codex hunt list
silent row loss in any Polars filter/join (each asserts counts); rung ordering; a path where an unverified <0.9 row is returned; a path that writes two ids from one source to one player; PK→K / FB→RB / DST mapping; float-mangled ids.

Spec: docs/superpowers/specs/2026-08-15-phase0-foundation-design.md §4
Plan: docs/superpowers/plans/2026-08-15-phase0-04-crosswalk.md

🤖 Generated with [Claude Code](https://claude.com/claude-code)

https://claude.ai/code/session_018YN1sk8qzRpGdzgXYHBPgP
```

---

## Self-review (done by the plan author; fix inline)

**Spec coverage (spec §4 + DATABASE.md §3 + overview ④):**
- normalize (suffixes, D.J./DJ/D J, apostrophes, hyphens, alias table, DST) → Task 1 ✅ (≥ 40 cases: 55 name + 55 DST/team + 21 position)
- registry seed on gsis, `normalized_name`, DST rows, idempotent, `updated_at` explicit → Task 3 ✅
- DynastyProcess job + `apply_playerids` (7 sources, 1.0/`dynastyprocess`, rookies create players, text ids, DST, conflict raise) → Tasks 4, 8 ✅
- ladder strictly in order, `match_method` recorded, rung-3 persist, rung-4 persist unverified + tie → unmatched, rung-5 upsert with `last_seen` bump, `resolve_many` → Task 5 ✅
- `ffh crosswalk report` (counts by method, unverified rows, unmatched, exit 1, `--json`) → Task 7 ✅
- mandatory tests present here (2) and explicitly deferred (2) → Task 6 ✅
- docs (alias table location, DST form, rung-3 team decision, consumer filter rule, ROADMAP tick, PR body) → Task 9 ✅
- Global: no `ffh.ingest` import outside Task 8 ✅; no pandas ✅; no network in tests ✅ (Task 8's test uses respx); no new deps ✅.

**Placeholder scan:** no TBD/TODO; every step has code/commands. Task 8 and Task 7 Step 6 use ③'s exact merged names (`HttpIngestJob.url`, `@register`, `name` ClassVar, `IngestValidationError`, `scrape_date`, `_session_scope`, `latest_partition`) and say to re-check them against `main` after the rebase.

**Type consistency:** `seeded_registry` keys are `gsis_id` or DST `normalized_name` (Tasks 3→4→5→6→7 all use `["00-0033873"]`, `["kc dst"]`); `CrosswalkApplyReport` fields identical in Task 4 code, tests, and CLI echo; `Resolution(player_id, method, confidence)` positional order identical in Task 5 code and tests; `CoverageReport` field names identical between `report.py`, `test_report.py`, and `test_cli_crosswalk.py`'s `_fake_report`; `is_usable(confidence, verified_at)` used with the same order in Tasks 5–7; `iter_gsis_to_player_id` defined in Task 3 and consumed in Task 4.

**Deviations introduced vs DATABASE.md §3 (all recorded in Task 2/9):** (1) `players.team_abbr` column added (migration 0002) for rung 3; (2) rung 4 hits are persisted unverified and `resolve` returns `None` for them (DATABASE.md says "require human review before use" — this is how); (3) `crosswalk_unmatched.last_seen` bumps with `clock_timestamp()` rather than `now()`; (4) one-id-per-source-per-player enforced by construction and test, not by a new DB constraint.
