# PR ② `feat/db-schema` — Postgres Schema + Initial Alembic Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** SQLAlchemy 2 models mirroring `docs/DATABASE.md` §2–7 table-for-table, one initial Alembic migration, engine/session helpers, and DB test fixtures that every later PR uses.

**Architecture:** `ffh.db.base.Base` (DeclarativeBase) + domain model modules under `ffh/db/models/`. Sync engine on `postgresql+psycopg://` for Alembic, CLI jobs, and tests; an async engine factory for the API (used from Phase 1). Alembic `env.py` reads the URL from `FFH_DATABASE_URL` or an `-x url=` override. Tests get a session-scoped fixture that rebuilds `ffh_test` via `alembic downgrade base && upgrade head`, and a per-test transactional session that rolls back.

**Tech Stack:** SQLAlchemy 2.0.51 · Alembic 1.19.0 · psycopg 3.3.4 · Postgres 17.

**Spec:** `docs/superpowers/specs/2026-08-15-phase0-foundation-design.md` §2

## Global Constraints

- Table and column names, types, nullability, defaults, PKs, uniques, and indexes are **exactly** those in `docs/DATABASE.md` §2–7 — that document is authoritative. Where this plan and DATABASE.md disagree, DATABASE.md wins and this plan has a bug; fix the plan.
- One Alembic migration in this PR. Never edit it after it merges.
- No SQLite. Tests run against Postgres `ffh_test` (compose or CI service).
- Deviation recorded in DATABASE.md: `nfl_teams.bye_week` stays but is left NULL in Phase 0; byes derive from `games`.
- Branch: `feat/db-schema` off `main` after PR ① merges.

Type mapping used throughout (SQL → SQLAlchemy):

| SQL | SQLAlchemy |
|---|---|
| `UUID` | `sqlalchemy.dialects.postgresql.UUID(as_uuid=True)` — alias `PG_UUID` |
| `TEXT` | `Text` |
| `SMALLINT` | `SmallInteger` |
| `INTEGER` | `Integer` |
| `BIGSERIAL` | `BigInteger` with `Identity()` (Postgres identity is the modern equivalent; DATABASE.md is updated to say so) |
| `REAL` | `REAL` (from `sqlalchemy.dialects.postgresql`) |
| `DOUBLE PRECISION` | `Double` |
| `NUMERIC(10,6)` | `Numeric(10, 6)` |
| `BOOLEAN` | `Boolean` |
| `DATE` | `Date` |
| `TIMESTAMPTZ` | `DateTime(timezone=True)` |
| `JSONB` | `JSONB` (from `sqlalchemy.dialects.postgresql`) |
| `DEFAULT now()` | `server_default=func.now()` |
| `DEFAULT gen_random_uuid()` | `server_default=text("gen_random_uuid()")` |

---

### Task 1: `ffh.db` base, engine factories, session helpers

**Files:**
- Create: `backend/src/ffh/db/base.py`
- Create: `backend/src/ffh/db/engine.py`
- Create: `backend/tests/db/test_engine.py`

**Interfaces:**
- Produces: `ffh.db.base.Base` (DeclarativeBase, `metadata` with naming convention); `ffh.db.engine.make_engine(url: str | None = None) -> Engine`; `ffh.db.engine.make_async_engine(url: str | None = None) -> AsyncEngine`; `ffh.db.engine.SessionLocal` factory via `make_session_factory(engine) -> sessionmaker[Session]`.

- [ ] **Step 1: Write the failing test `backend/tests/db/test_engine.py`**

```python
import pytest
from sqlalchemy import text

from ffh.db.base import Base
from ffh.db.engine import make_engine, make_session_factory

pytestmark = pytest.mark.db


def test_make_engine_defaults_to_settings_database_url(monkeypatch):
    monkeypatch.setenv("FFH_DATABASE_URL", "postgresql+psycopg://ffh:ffh@localhost:5432/ffh_test")
    engine = make_engine()
    assert engine.url.database == "ffh_test"
    with engine.connect() as conn:
        assert conn.execute(text("select 1")).scalar_one() == 1


def test_session_factory_yields_working_session():
    engine = make_engine("postgresql+psycopg://ffh:ffh@localhost:5432/ffh_test")
    factory = make_session_factory(engine)
    with factory() as session:
        assert session.execute(text("select 2")).scalar_one() == 2


def test_metadata_naming_convention_is_set():
    assert "fk" in Base.metadata.naming_convention
    assert "ix" in Base.metadata.naming_convention
```

- [ ] **Step 2: Run to verify it fails** — `uv run pytest tests/db/test_engine.py -v` → `ModuleNotFoundError: ffh.db.base`.

- [ ] **Step 3: Write `backend/src/ffh/db/base.py`**

```python
from sqlalchemy import MetaData
from sqlalchemy.orm import DeclarativeBase

# Deterministic constraint names so Alembic migrations are stable across autogenerate runs.
NAMING_CONVENTION = {
    "ix": "%(table_name)s_%(column_0_N_name)s_idx",
    "uq": "%(table_name)s_%(column_0_N_name)s_key",
    "ck": "%(table_name)s_%(constraint_name)s_check",
    "fk": "%(table_name)s_%(column_0_name)s_fkey",
    "pk": "%(table_name)s_pkey",
}


class Base(DeclarativeBase):
    metadata = MetaData(naming_convention=NAMING_CONVENTION)
```

- [ ] **Step 4: Write `backend/src/ffh/db/engine.py`**

```python
from sqlalchemy import Engine, create_engine
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.orm import Session, sessionmaker

from ffh.config import get_settings


def make_engine(url: str | None = None) -> Engine:
    """Sync engine (Alembic, CLI ingest jobs, tests)."""
    return create_engine(url or get_settings().database_url, pool_pre_ping=True)


def make_async_engine(url: str | None = None) -> AsyncEngine:
    """Async engine for FastAPI request paths. Same psycopg driver, async mode."""
    return create_async_engine(url or get_settings().database_url, pool_pre_ping=True)


def make_session_factory(engine: Engine) -> sessionmaker[Session]:
    return sessionmaker(bind=engine, expire_on_commit=False)
```

- [ ] **Step 5: Run to verify it passes** — `uv run pytest tests/db/test_engine.py -v` → 3 passed.

- [ ] **Step 6: Commit**

```bash
git add backend/src/ffh/db/base.py backend/src/ffh/db/engine.py backend/tests/db/test_engine.py
git commit -m "feat(db): declarative base with naming convention, engine + session factories"
```

---

### Task 2: Models — reference tables (§2) and crosswalk (§3)

**Files:**
- Create: `backend/src/ffh/db/models/__init__.py`
- Create: `backend/src/ffh/db/models/reference.py`
- Create: `backend/src/ffh/db/models/crosswalk.py`
- Create: `backend/tests/db/test_models_reference.py`

**Interfaces:**
- Produces: ORM classes `Player`, `PlayerExternalId`, `NflTeam`, `Stadium`, `Game`, `GameWeatherForecast`, `CrosswalkUnmatched`, importable from `ffh.db.models`.

- [ ] **Step 1: Write the failing test `backend/tests/db/test_models_reference.py`**

```python
from sqlalchemy.dialects import postgresql

from ffh.db.base import Base
from ffh.db import models  # noqa: F401  (registers tables)


def _cols(table: str) -> dict[str, object]:
    return {c.name: c for c in Base.metadata.tables[table].columns}


def test_players_table_shape():
    c = _cols("players")
    assert c["player_id"].primary_key
    assert c["gsis_id"].unique
    assert not c["full_name"].nullable
    assert not c["normalized_name"].nullable
    assert not c["position"].nullable
    assert "created_at" in c and "updated_at" in c


def test_player_external_ids_pk_is_source_external_id():
    t = Base.metadata.tables["player_external_ids"]
    assert [c.name for c in t.primary_key.columns] == ["source", "external_id"]
    assert not _cols("player_external_ids")["match_method"].nullable


def test_games_has_brin_index_on_kickoff():
    t = Base.metadata.tables["games"]
    brin = [i for i in t.indexes if i.dialect_options["postgresql"].get("using") == "brin"]
    assert brin and [c.name for c in brin[0].columns] == ["kickoff_at"]


def test_games_neutral_site_defaults_false():
    c = _cols("games")["neutral_site"]
    assert not c.nullable and c.server_default is not None


def test_crosswalk_unmatched_unique_source_external_id():
    t = Base.metadata.tables["crosswalk_unmatched"]
    uniques = [tuple(c.name for c in u.columns) for u in t.constraints if u.__class__.__name__ == "UniqueConstraint"]
    assert ("source", "external_id") in uniques


def test_jsonb_and_uuid_types_used():
    assert isinstance(_cols("player_external_ids")["player_id"].type, postgresql.UUID)
```

- [ ] **Step 2: Run to verify it fails** — `uv run pytest tests/db/test_models_reference.py -v` → `ModuleNotFoundError: ffh.db.models`.

- [ ] **Step 3: Write `backend/src/ffh/db/models/reference.py`**

```python
import uuid
from datetime import date, datetime

from sqlalchemy import (
    Boolean,
    Date,
    DateTime,
    Double,
    ForeignKey,
    Index,
    Integer,
    SmallInteger,
    Text,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import REAL, UUID as PG_UUID
from sqlalchemy.orm import Mapped, mapped_column

from ffh.db.base import Base


class Player(Base):
    """Canonical player identity. One row per human being, ever. (DATABASE.md §2)"""

    __tablename__ = "players"

    player_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    gsis_id: Mapped[str | None] = mapped_column(Text, unique=True)
    full_name: Mapped[str] = mapped_column(Text, nullable=False)
    first_name: Mapped[str | None] = mapped_column(Text)
    last_name: Mapped[str | None] = mapped_column(Text)
    normalized_name: Mapped[str] = mapped_column(Text, nullable=False)
    position: Mapped[str] = mapped_column(Text, nullable=False)
    birth_date: Mapped[date | None] = mapped_column(Date)
    rookie_year: Mapped[int | None] = mapped_column(SmallInteger)
    height_in: Mapped[int | None] = mapped_column(SmallInteger)
    weight_lb: Mapped[int | None] = mapped_column(SmallInteger)
    college: Mapped[str | None] = mapped_column(Text)
    status: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )

    __table_args__ = (Index("players_normalized_name_pos_idx", "normalized_name", "position"),)


class PlayerExternalId(Base):
    """★ THE CROSSWALK ★ (DATABASE.md §3). Highest-risk table in the system."""

    __tablename__ = "player_external_ids"

    player_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("players.player_id", ondelete="CASCADE"),
        nullable=False,
    )
    source: Mapped[str] = mapped_column(Text, primary_key=True)
    external_id: Mapped[str] = mapped_column(Text, primary_key=True)
    confidence: Mapped[float] = mapped_column(REAL, nullable=False, server_default=text("1.0"))
    match_method: Mapped[str] = mapped_column(Text, nullable=False)
    verified_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (Index("player_external_ids_player_idx", "player_id"),)


class NflTeam(Base):
    __tablename__ = "nfl_teams"

    team_abbr: Mapped[str] = mapped_column(Text, primary_key=True)
    espn_id: Mapped[int | None] = mapped_column(Integer, unique=True)
    full_name: Mapped[str] = mapped_column(Text, nullable=False)
    conference: Mapped[str] = mapped_column(Text, nullable=False)
    division: Mapped[str] = mapped_column(Text, nullable=False)
    # Per-season data on a static table. Left NULL in Phase 0; byes derive from `games`.
    bye_week: Mapped[int | None] = mapped_column(SmallInteger)


class Stadium(Base):
    __tablename__ = "stadiums"

    stadium_id: Mapped[str] = mapped_column(Text, primary_key=True)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    latitude: Mapped[float] = mapped_column(Double, nullable=False)
    longitude: Mapped[float] = mapped_column(Double, nullable=False)
    altitude_ft: Mapped[int | None] = mapped_column(Integer)
    heading_deg: Mapped[float | None] = mapped_column(REAL)
    surface_type: Mapped[str | None] = mapped_column(Text)
    roof_type: Mapped[str | None] = mapped_column(Text)
    tz: Mapped[str] = mapped_column(Text, nullable=False)


class Game(Base):
    __tablename__ = "games"

    game_id: Mapped[str] = mapped_column(Text, primary_key=True)
    season: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    week: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    season_type: Mapped[str] = mapped_column(Text, nullable=False)
    kickoff_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    home_team: Mapped[str] = mapped_column(Text, ForeignKey("nfl_teams.team_abbr"), nullable=False)
    away_team: Mapped[str] = mapped_column(Text, ForeignKey("nfl_teams.team_abbr"), nullable=False)
    stadium_id: Mapped[str | None] = mapped_column(Text, ForeignKey("stadiums.stadium_id"))
    spread_line: Mapped[float | None] = mapped_column(REAL)
    total_line: Mapped[float | None] = mapped_column(REAL)
    home_moneyline: Mapped[int | None] = mapped_column(Integer)
    away_moneyline: Mapped[int | None] = mapped_column(Integer)
    roof: Mapped[str | None] = mapped_column(Text)
    surface: Mapped[str | None] = mapped_column(Text)
    div_game: Mapped[bool | None] = mapped_column(Boolean)
    home_rest: Mapped[int | None] = mapped_column(SmallInteger)
    away_rest: Mapped[int | None] = mapped_column(SmallInteger)
    neutral_site: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("false"))
    home_score: Mapped[int | None] = mapped_column(SmallInteger)
    away_score: Mapped[int | None] = mapped_column(SmallInteger)
    temp_f: Mapped[float | None] = mapped_column(REAL)
    wind_mph: Mapped[float | None] = mapped_column(REAL)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )

    __table_args__ = (
        Index("games_season_week_idx", "season", "week"),
        Index("games_kickoff_brin", "kickoff_at", postgresql_using="brin"),
    )


class GameWeatherForecast(Base):
    __tablename__ = "game_weather_forecasts"

    game_id: Mapped[str] = mapped_column(Text, ForeignKey("games.game_id"), primary_key=True)
    forecast_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), primary_key=True)
    temp_f: Mapped[float | None] = mapped_column(REAL)
    wind_mph: Mapped[float | None] = mapped_column(REAL)
    wind_gust_mph: Mapped[float | None] = mapped_column(REAL)
    wind_dir_deg: Mapped[float | None] = mapped_column(REAL)
    precip_mm: Mapped[float | None] = mapped_column(REAL)
    precip_prob: Mapped[float | None] = mapped_column(REAL)
```

- [ ] **Step 4: Write `backend/src/ffh/db/models/crosswalk.py`**

```python
from datetime import datetime

from sqlalchemy import BigInteger, Boolean, DateTime, Identity, Text, UniqueConstraint, func, text
from sqlalchemy.orm import Mapped, mapped_column

from ffh.db.base import Base


class CrosswalkUnmatched(Base):
    """Rung 5 of the resolution ladder: never silently dropped (DATABASE.md §3)."""

    __tablename__ = "crosswalk_unmatched"

    id: Mapped[int] = mapped_column(BigInteger, Identity(), primary_key=True)
    source: Mapped[str] = mapped_column(Text, nullable=False)
    external_id: Mapped[str] = mapped_column(Text, nullable=False)
    raw_name: Mapped[str | None] = mapped_column(Text)
    raw_position: Mapped[str | None] = mapped_column(Text)
    raw_team: Mapped[str | None] = mapped_column(Text)
    first_seen: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    last_seen: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    resolved: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("false"))

    __table_args__ = (UniqueConstraint("source", "external_id"),)
```

- [ ] **Step 5: Write `backend/src/ffh/db/models/__init__.py`** (extended in later tasks)

```python
"""ORM models. Import this package to register every table on Base.metadata."""

from ffh.db.models.crosswalk import CrosswalkUnmatched
from ffh.db.models.reference import (
    Game,
    GameWeatherForecast,
    NflTeam,
    Player,
    PlayerExternalId,
    Stadium,
)

__all__ = [
    "CrosswalkUnmatched",
    "Game",
    "GameWeatherForecast",
    "NflTeam",
    "Player",
    "PlayerExternalId",
    "Stadium",
]
```

- [ ] **Step 6: Run to verify it passes** — `uv run pytest tests/db/test_models_reference.py -v` → 6 passed. `uv run ruff check .` clean.

- [ ] **Step 7: Commit**

```bash
git add backend/src/ffh/db/models backend/tests/db/test_models_reference.py
git commit -m "feat(db): reference + crosswalk models (players, external ids, teams, stadiums, games, unmatched)"
```

---

### Task 3: Models — league state (§4) and draft (§5)

**Files:**
- Create: `backend/src/ffh/db/models/league.py`
- Create: `backend/src/ffh/db/models/draft.py`
- Modify: `backend/src/ffh/db/models/__init__.py`
- Create: `backend/tests/db/test_models_league.py`

**Interfaces:**
- Produces: `League`, `LeagueTeam`, `RosterSlot`, `Matchup`, `Transaction`, `Draft`, `DraftPick`, `Adp`.

- [ ] **Step 1: Write the failing test `backend/tests/db/test_models_league.py`**

```python
from sqlalchemy.dialects import postgresql

from ffh.db.base import Base
from ffh.db import models  # noqa: F401


def _uniques(table: str) -> set[tuple[str, ...]]:
    t = Base.metadata.tables[table]
    return {
        tuple(c.name for c in u.columns)
        for u in t.constraints
        if u.__class__.__name__ == "UniqueConstraint"
    }


def test_leagues_unique_and_jsonb_settings():
    assert ("platform", "external_id", "season") in _uniques("leagues")
    t = Base.metadata.tables["leagues"]
    assert isinstance(t.c.scoring_settings.type, postgresql.JSONB)
    assert not t.c.scoring_settings.nullable
    assert not t.c.roster_settings.nullable


def test_roster_slots_pk():
    t = Base.metadata.tables["roster_slots"]
    assert [c.name for c in t.primary_key.columns] == ["league_team_id", "week", "player_id"]


def test_draft_picks_pk_and_index():
    t = Base.metadata.tables["draft_picks"]
    assert [c.name for c in t.primary_key.columns] == ["draft_id", "pick_no"]
    assert any([c.name for c in i.columns] == ["player_id"] for i in t.indexes)


def test_adp_pk_and_stdev_present():
    t = Base.metadata.tables["adp"]
    assert [c.name for c in t.primary_key.columns] == [
        "source", "format", "num_teams", "scrape_date", "player_id",
    ]
    assert "adp_stdev" in t.c
```

- [ ] **Step 2: Run to verify it fails** — KeyError `leagues`.

- [ ] **Step 3: Write `backend/src/ffh/db/models/league.py`**

```python
import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    Boolean,
    DateTime,
    ForeignKey,
    Integer,
    SmallInteger,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, REAL, UUID as PG_UUID
from sqlalchemy.orm import Mapped, mapped_column

from ffh.db.base import Base


class League(Base):
    __tablename__ = "leagues"

    league_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    platform: Mapped[str] = mapped_column(Text, nullable=False)
    external_id: Mapped[str] = mapped_column(Text, nullable=False)
    season: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    name: Mapped[str | None] = mapped_column(Text)
    num_teams: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    # ALWAYS fetched from the platform, NEVER hardcoded.
    scoring_settings: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    roster_settings: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    league_type: Mapped[str] = mapped_column(Text, nullable=False)
    is_superflex: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("false"))
    playoff_teams: Mapped[int | None] = mapped_column(SmallInteger)
    playoff_start_wk: Mapped[int | None] = mapped_column(SmallInteger)
    faab_budget: Mapped[int | None] = mapped_column(Integer)
    my_team_id: Mapped[uuid.UUID | None] = mapped_column(PG_UUID(as_uuid=True))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (UniqueConstraint("platform", "external_id", "season"),)


class LeagueTeam(Base):
    __tablename__ = "league_teams"

    league_team_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    league_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("leagues.league_id", ondelete="CASCADE"), nullable=False
    )
    external_id: Mapped[str] = mapped_column(Text, nullable=False)
    display_name: Mapped[str | None] = mapped_column(Text)
    manager_name: Mapped[str | None] = mapped_column(Text)
    draft_slot: Mapped[int | None] = mapped_column(SmallInteger)
    faab_remaining: Mapped[int | None] = mapped_column(Integer)
    waiver_priority: Mapped[int | None] = mapped_column(SmallInteger)
    is_me: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("false"))

    __table_args__ = (UniqueConstraint("league_id", "external_id"),)


class RosterSlot(Base):
    """Roster snapshots — one row per player per team per week; keep the history."""

    __tablename__ = "roster_slots"

    league_team_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("league_teams.league_team_id", ondelete="CASCADE"),
        primary_key=True,
    )
    week: Mapped[int] = mapped_column(SmallInteger, primary_key=True)
    player_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("players.player_id"), primary_key=True
    )
    slot: Mapped[str] = mapped_column(Text, nullable=False)
    is_starter: Mapped[bool] = mapped_column(Boolean, nullable=False)
    captured_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class Matchup(Base):
    __tablename__ = "matchups"

    league_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("leagues.league_id", ondelete="CASCADE"), primary_key=True
    )
    week: Mapped[int] = mapped_column(SmallInteger, primary_key=True)
    matchup_no: Mapped[int] = mapped_column(SmallInteger, primary_key=True)
    home_team_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("league_teams.league_team_id"), nullable=False
    )
    away_team_id: Mapped[uuid.UUID | None] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("league_teams.league_team_id")
    )
    home_points: Mapped[float | None] = mapped_column(REAL)
    away_points: Mapped[float | None] = mapped_column(REAL)


class Transaction(Base):
    __tablename__ = "transactions"

    transaction_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    league_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("leagues.league_id", ondelete="CASCADE"), nullable=False
    )
    external_id: Mapped[str | None] = mapped_column(Text)
    type: Mapped[str] = mapped_column(Text, nullable=False)
    week: Mapped[int | None] = mapped_column(SmallInteger)
    executed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    faab_spent: Mapped[int | None] = mapped_column(Integer)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)

    __table_args__ = (UniqueConstraint("league_id", "external_id"),)
```

- [ ] **Step 4: Write `backend/src/ffh/db/models/draft.py`**

```python
import uuid
from datetime import date, datetime

from sqlalchemy import (
    Boolean,
    Date,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    SmallInteger,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import REAL, UUID as PG_UUID
from sqlalchemy.orm import Mapped, mapped_column

from ffh.db.base import Base


class Draft(Base):
    __tablename__ = "drafts"

    draft_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    league_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("leagues.league_id", ondelete="CASCADE"), nullable=False
    )
    external_id: Mapped[str] = mapped_column(Text, nullable=False)
    draft_type: Mapped[str] = mapped_column(Text, nullable=False)
    rounds: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    my_slot: Mapped[int | None] = mapped_column(SmallInteger)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    __table_args__ = (UniqueConstraint("league_id", "external_id"),)


class DraftPick(Base):
    __tablename__ = "draft_picks"

    draft_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("drafts.draft_id", ondelete="CASCADE"), primary_key=True
    )
    pick_no: Mapped[int] = mapped_column(SmallInteger, primary_key=True)
    round: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    draft_slot: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    league_team_id: Mapped[uuid.UUID | None] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("league_teams.league_team_id")
    )
    player_id: Mapped[uuid.UUID | None] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("players.player_id")
    )
    is_keeper: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("false"))
    auction_amount: Mapped[int | None] = mapped_column(Integer)
    picked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    __table_args__ = (Index("draft_picks_player_idx", "player_id"),)


class Adp(Base):
    """ADP by format. adp_stdev is REQUIRED for VONA (ENGINE.md §2); enforced at ingest."""

    __tablename__ = "adp"

    source: Mapped[str] = mapped_column(Text, primary_key=True)
    format: Mapped[str] = mapped_column(Text, primary_key=True)
    num_teams: Mapped[int] = mapped_column(SmallInteger, primary_key=True)
    scrape_date: Mapped[date] = mapped_column(Date, primary_key=True)
    player_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("players.player_id"), primary_key=True
    )
    adp: Mapped[float] = mapped_column(REAL, nullable=False)
    adp_stdev: Mapped[float | None] = mapped_column(REAL)
    times_drafted: Mapped[int | None] = mapped_column(Integer)
```

- [ ] **Step 5: Extend `backend/src/ffh/db/models/__init__.py`** — add imports and `__all__` entries for `League, LeagueTeam, RosterSlot, Matchup, Transaction` (from `.league`) and `Draft, DraftPick, Adp` (from `.draft`).

- [ ] **Step 6: Run** — `uv run pytest tests/db/test_models_league.py -v` → 4 passed; ruff clean.

- [ ] **Step 7: Commit**

```bash
git add backend/src/ffh/db/models backend/tests/db/test_models_league.py
git commit -m "feat(db): league, roster, matchup, transaction, draft, adp models"
```

---

### Task 4: Models — projections/stats (§6), decisions/debates/ingest (§7)

**Files:**
- Create: `backend/src/ffh/db/models/projections.py`
- Create: `backend/src/ffh/db/models/decisions.py`
- Modify: `backend/src/ffh/db/models/__init__.py`
- Create: `backend/tests/db/test_models_projections.py`

**Interfaces:**
- Produces: `Projection`, `ProjectionCorrelation`, `PlayerWeekActual`, `PlayerInjuryStatus`, `Recommendation`, `AiDebate`, `IngestRun`.

- [ ] **Step 1: Write the failing test `backend/tests/db/test_models_projections.py`**

```python
from ffh.db.base import Base
from ffh.db import models  # noqa: F401

EXPECTED_TABLES = {
    "players", "player_external_ids", "nfl_teams", "stadiums", "games",
    "game_weather_forecasts", "crosswalk_unmatched",
    "leagues", "league_teams", "roster_slots", "matchups", "transactions",
    "drafts", "draft_picks", "adp",
    "projections", "projection_correlations", "player_week_actuals", "player_injury_status",
    "recommendations", "ai_debates", "ingest_runs",
}


def test_all_database_md_tables_are_registered():
    assert set(Base.metadata.tables) == EXPECTED_TABLES


def test_projections_carry_gamma_params_not_null():
    t = Base.metadata.tables["projections"]
    for col in ("mean_points", "gamma_shape", "gamma_scale", "model_version", "inputs"):
        assert not t.c[col].nullable, col


def test_projection_correlations_check_canonical_order():
    t = Base.metadata.tables["projection_correlations"]
    checks = [c for c in t.constraints if c.__class__.__name__ == "CheckConstraint"]
    assert any("player_a < player_b" in str(c.sqltext) for c in checks)


def test_ingest_runs_index():
    t = Base.metadata.tables["ingest_runs"]
    assert any(i.name == "ingest_runs_source_idx" for i in t.indexes)
```

- [ ] **Step 2: Run to verify it fails** — set mismatch on the first test.

- [ ] **Step 3: Write `backend/src/ffh/db/models/projections.py`**

```python
import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    SmallInteger,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, REAL, UUID as PG_UUID
from sqlalchemy.orm import Mapped, mapped_column

from ffh.db.base import Base

# Sentinel league_id for league-agnostic ("generic PPR") rows where a NOT NULL key is required.
GENERIC_LEAGUE_ID = uuid.UUID(int=0)


class Projection(Base):
    """A projection is a DISTRIBUTION. Never store or pass only the mean (DATABASE.md §6)."""

    __tablename__ = "projections"

    projection_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    player_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("players.player_id"), nullable=False
    )
    season: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    week: Mapped[int] = mapped_column(SmallInteger, nullable=False)  # 0 = full season
    league_id: Mapped[uuid.UUID | None] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("leagues.league_id")
    )
    source: Mapped[str] = mapped_column(Text, nullable=False)
    model_version: Mapped[str] = mapped_column(Text, nullable=False)
    mean_points: Mapped[float] = mapped_column(REAL, nullable=False)
    gamma_shape: Mapped[float] = mapped_column(REAL, nullable=False)
    gamma_scale: Mapped[float] = mapped_column(REAL, nullable=False)
    floor_p10: Mapped[float | None] = mapped_column(REAL)
    ceiling_p90: Mapped[float | None] = mapped_column(REAL)
    inputs: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    computed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        UniqueConstraint("player_id", "season", "week", "league_id", "source", "model_version"),
        Index("projections_lookup_idx", "season", "week", "source", "model_version"),
    )


class ProjectionCorrelation(Base):
    __tablename__ = "projection_correlations"

    season: Mapped[int] = mapped_column(SmallInteger, primary_key=True)
    week: Mapped[int] = mapped_column(SmallInteger, primary_key=True)
    player_a: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("players.player_id"), primary_key=True
    )
    player_b: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("players.player_id"), primary_key=True
    )
    rho: Mapped[float] = mapped_column(REAL, nullable=False)
    reason: Mapped[str] = mapped_column(Text, nullable=False)

    __table_args__ = (CheckConstraint("player_a < player_b", name="canonical_pair_order"),)


class PlayerWeekActual(Base):
    __tablename__ = "player_week_actuals"

    player_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("players.player_id"), primary_key=True
    )
    season: Mapped[int] = mapped_column(SmallInteger, primary_key=True)
    week: Mapped[int] = mapped_column(SmallInteger, primary_key=True)
    # NOT NULL because it is part of the PK; generic-PPR rows use GENERIC_LEAGUE_ID.
    league_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("leagues.league_id"), primary_key=True
    )
    game_id: Mapped[str | None] = mapped_column(Text, ForeignKey("games.game_id"))
    fantasy_points: Mapped[float] = mapped_column(REAL, nullable=False)
    snap_pct: Mapped[float | None] = mapped_column(REAL)
    target_share: Mapped[float | None] = mapped_column(REAL)
    carry_share: Mapped[float | None] = mapped_column(REAL)
    rz_touches: Mapped[int | None] = mapped_column(SmallInteger)


class PlayerInjuryStatus(Base):
    __tablename__ = "player_injury_status"

    player_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("players.player_id"), primary_key=True
    )
    season: Mapped[int] = mapped_column(SmallInteger, primary_key=True)
    week: Mapped[int] = mapped_column(SmallInteger, primary_key=True)
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), primary_key=True)
    source: Mapped[str] = mapped_column(Text, primary_key=True)
    report_status: Mapped[str | None] = mapped_column(Text)
    practice_status: Mapped[str | None] = mapped_column(Text)
    injury_body_part: Mapped[str | None] = mapped_column(Text)
    notes: Mapped[str | None] = mapped_column(Text)
```

Note on `player_week_actuals.league_id`: DATABASE.md puts a nullable column in the PK. Postgres forbids NULL in a primary key, so the model makes it NOT NULL and defines the sentinel `GENERIC_LEAGUE_ID` above for "generic PPR" rows. Task 7 records this in DATABASE.md.

- [ ] **Step 4: Write `backend/src/ffh/db/models/decisions.py`**

```python
import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    SmallInteger,
    Text,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, REAL, UUID as PG_UUID
from sqlalchemy.orm import Mapped, mapped_column

from ffh.db.base import Base


class Recommendation(Base):
    """Every recommendation is logged with inputs and outcome (CLAUDE.md rule 8)."""

    __tablename__ = "recommendations"

    recommendation_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    league_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True), ForeignKey("leagues.league_id"), nullable=False
    )
    module: Mapped[str] = mapped_column(Text, nullable=False)
    season: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    week: Mapped[int | None] = mapped_column(SmallInteger)
    context: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    engine_output: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    final_output: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    engine_version: Mapped[str] = mapped_column(Text, nullable=False)
    debate_id: Mapped[uuid.UUID | None] = mapped_column(PG_UUID(as_uuid=True))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    action_taken: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    outcome: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    outcome_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    __table_args__ = (Index("recommendations_module_idx", "module", "season", "week"),)


class AiDebate(Base):
    __tablename__ = "ai_debates"

    debate_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    module: Mapped[str] = mapped_column(Text, nullable=False)
    evidence_packet: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    provider_a: Mapped[str] = mapped_column(Text, nullable=False)
    provider_b: Mapped[str] = mapped_column(Text, nullable=False)
    model_a: Mapped[str] = mapped_column(Text, nullable=False)
    model_b: Mapped[str] = mapped_column(Text, nullable=False)
    round1_a: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    round1_b: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    round2_a: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    round2_b: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    judge_provider: Mapped[str] = mapped_column(Text, nullable=False)
    judge_model: Mapped[str] = mapped_column(Text, nullable=False)
    verdict: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    consensus_score: Mapped[float] = mapped_column(REAL, nullable=False)
    disagreement_axis: Mapped[str | None] = mapped_column(Text)
    latency_ms: Mapped[int] = mapped_column(Integer, nullable=False)
    cost_usd: Mapped[float | None] = mapped_column(Numeric(10, 6))
    tokens_in: Mapped[int | None] = mapped_column(Integer)
    tokens_out: Mapped[int | None] = mapped_column(Integer)
    cache_hit: Mapped[bool | None] = mapped_column(Boolean)
    error: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (Index("ai_debates_consensus_idx", "consensus_score"),)


class IngestRun(Base):
    """Ingest provenance and watermarks — makes ingest idempotent and resumable."""

    __tablename__ = "ingest_runs"

    run_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True), primary_key=True, server_default=text("gen_random_uuid()")
    )
    source: Mapped[str] = mapped_column(Text, nullable=False)
    asset: Mapped[str] = mapped_column(Text, nullable=False)
    season: Mapped[int | None] = mapped_column(SmallInteger)
    week: Mapped[int | None] = mapped_column(SmallInteger)
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    status: Mapped[str] = mapped_column(Text, nullable=False)
    rows_written: Mapped[int | None] = mapped_column(Integer)
    source_etag: Mapped[str | None] = mapped_column(Text)
    source_mtime: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    output_path: Mapped[str | None] = mapped_column(Text)
    error: Mapped[str | None] = mapped_column(Text)

    # DATABASE.md writes `(source, asset, started_at DESC)`. A btree index scans backward for
    # free, and a plain column list keeps `alembic check` drift-free, so DESC is omitted here.
    # Task 7 records this in DATABASE.md.
    __table_args__ = (Index("ingest_runs_source_idx", "source", "asset", "started_at"),)
```

- [ ] **Step 5: Extend `backend/src/ffh/db/models/__init__.py`** with `Projection, ProjectionCorrelation, PlayerWeekActual, PlayerInjuryStatus, GENERIC_LEAGUE_ID` and `Recommendation, AiDebate, IngestRun`.

- [ ] **Step 6: Run** — `uv run pytest tests/db -v` → all pass; ruff clean.

- [ ] **Step 7: Commit**

```bash
git add backend/src/ffh/db/models backend/tests/db/test_models_projections.py
git commit -m "feat(db): projection, actuals, injury, recommendation, debate, ingest_run models"
```

---

### Task 5: Alembic setup and the initial migration

**Files:**
- Create: `backend/alembic.ini`
- Create: `backend/alembic/env.py`, `backend/alembic/script.py.mako`
- Create: `backend/alembic/versions/0001_initial_schema.py` (autogenerated, then hand-reviewed)
- Create: `backend/tests/db/test_migrations.py`

**Interfaces:**
- Produces: `uv run alembic upgrade head` from `backend/`; `-x url=<sqlalchemy-url>` overrides the target DB.

- [ ] **Step 1: Init alembic** — from `backend/`: `uv run alembic init alembic`. Move the generated `alembic.ini` to `backend/alembic.ini` (it lands there already) and set `script_location = alembic`. Remove `sqlalchemy.url` from `alembic.ini` (URL comes from env/`-x`).

- [ ] **Step 2: Replace `backend/alembic/env.py`**

```python
from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool

from ffh.config import get_settings
from ffh.db import models  # noqa: F401  registers all tables
from ffh.db.base import Base

config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def _url() -> str:
    override = context.get_x_argument(as_dictionary=True).get("url")
    return override or get_settings().database_url


def run_migrations_offline() -> None:
    context.configure(
        url=_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    section = config.get_section(config.config_ini_section, {})
    section["sqlalchemy.url"] = _url()
    connectable = engine_from_config(section, prefix="sqlalchemy.", poolclass=pool.NullPool)
    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata, compare_type=True)
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
```

- [ ] **Step 3: Write the failing migration test `backend/tests/db/test_migrations.py`**

```python
import subprocess
import sys

import pytest
from sqlalchemy import inspect, text

from ffh.config import get_settings
from ffh.db.engine import make_engine

pytestmark = pytest.mark.db


def _alembic(*args: str) -> subprocess.CompletedProcess[str]:
    url = get_settings().test_database_url
    return subprocess.run(
        [sys.executable, "-m", "alembic", "-x", f"url={url}", *args],
        capture_output=True, text=True, check=False,
    )


def _reset_schema() -> None:
    engine = make_engine(get_settings().test_database_url)
    with engine.begin() as conn:
        conn.execute(text("DROP SCHEMA public CASCADE; CREATE SCHEMA public;"))
    engine.dispose()


@pytest.fixture(autouse=True)
def _leave_schema_at_head():
    """Other tests (db_session fixture) expect ffh_test at head after this module runs."""
    yield
    _reset_schema()
    assert _alembic("upgrade", "head").returncode == 0


def test_upgrade_head_creates_all_tables_and_check_reports_no_drift():
    _reset_schema()
    up = _alembic("upgrade", "head")
    assert up.returncode == 0, up.stderr
    engine = make_engine(get_settings().test_database_url)
    tables = set(inspect(engine).get_table_names())
    assert {"players", "player_external_ids", "games", "leagues", "draft_picks", "adp",
            "projections", "recommendations", "ai_debates", "ingest_runs"} <= tables
    check = _alembic("check")
    assert check.returncode == 0, check.stdout + check.stderr


def test_downgrade_base_removes_everything():
    _reset_schema()
    assert _alembic("upgrade", "head").returncode == 0
    down = _alembic("downgrade", "base")
    assert down.returncode == 0, down.stderr
    engine = make_engine(get_settings().test_database_url)
    assert set(inspect(engine).get_table_names()) == {"alembic_version"}
```

- [ ] **Step 4: Run to verify it fails** — `uv run pytest tests/db/test_migrations.py -v` → fails (no revision → `alembic check` complains or tables missing).

- [ ] **Step 5: Autogenerate the initial migration**

From `backend/` with compose up: `uv run alembic -x url=postgresql+psycopg://ffh:ffh@localhost:5432/ffh_test revision --autogenerate -m "initial schema" --rev-id 0001`.
Then open `alembic/versions/0001_initial_schema.py` and hand-check:
  - every one of the 22 tables is created;
  - `games_kickoff_brin` is created with `postgresql_using="brin"`;
  - `projection_correlations` has the `player_a < player_b` CHECK;
  - `crosswalk_unmatched.id` uses `sa.Identity()`;
  - `server_default=sa.text("gen_random_uuid()")` on every UUID PK;
  - the `downgrade()` drops in dependency order (autogenerate does this).

- [ ] **Step 6: Run the migration tests** — `uv run pytest tests/db/test_migrations.py -v` → 2 passed. Then run the whole suite `uv run pytest -v` → green.

- [ ] **Step 7: Commit**

```bash
git add backend/alembic.ini backend/alembic backend/tests/db/test_migrations.py
git commit -m "feat(db): alembic env and initial schema migration (0001)"
```

---

### Task 6: Shared DB test fixtures for later PRs

**Files:**
- Modify: `backend/tests/conftest.py`

**Interfaces:**
- Produces: session-scoped fixture `migrated_engine: Engine` (fresh `ffh_test` at `alembic head`), function-scoped `db_session: Session` (transaction rolled back after each test). Marker `db` applied automatically to tests using them.

- [ ] **Step 1: Write a test that uses the fixture first — `backend/tests/db/test_fixtures.py`**

```python
import pytest
from sqlalchemy import select

from ffh.db.models import NflTeam

pytestmark = pytest.mark.db


def test_db_session_rolls_back_between_tests_1(db_session):
    db_session.add(NflTeam(team_abbr="ZZZ", full_name="Test", conference="AFC", division="North"))
    db_session.flush()
    assert db_session.scalar(select(NflTeam).where(NflTeam.team_abbr == "ZZZ")) is not None


def test_db_session_rolls_back_between_tests_2(db_session):
    assert db_session.scalar(select(NflTeam).where(NflTeam.team_abbr == "ZZZ")) is None
```

- [ ] **Step 2: Run to verify it fails** — fixture `db_session` not found.

- [ ] **Step 3: Extend `backend/tests/conftest.py`**

```python
import subprocess
import sys

import pytest
from sqlalchemy import text
from sqlalchemy.orm import Session

from ffh.config import get_settings
from ffh.db.engine import make_engine


@pytest.fixture(autouse=True)
def _clear_settings_cache():
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


@pytest.fixture(scope="session")
def migrated_engine():
    url = get_settings().test_database_url
    engine = make_engine(url)
    with engine.begin() as conn:
        conn.execute(text("DROP SCHEMA public CASCADE; CREATE SCHEMA public;"))
    r = subprocess.run(
        [sys.executable, "-m", "alembic", "-x", f"url={url}", "upgrade", "head"],
        capture_output=True, text=True, check=False,
    )
    assert r.returncode == 0, r.stderr
    yield engine
    engine.dispose()


@pytest.fixture
def db_session(migrated_engine):
    """A session inside a transaction that is always rolled back."""
    conn = migrated_engine.connect()
    trans = conn.begin()
    session = Session(bind=conn, join_transaction_mode="create_savepoint")
    try:
        yield session
    finally:
        session.close()
        trans.rollback()
        conn.close()
```

Note: `test_migrations.py` drops the schema, but its autouse `_leave_schema_at_head` fixture restores head after each test, so `db_session` works regardless of test order.

- [ ] **Step 4: Run** — `uv run pytest tests/db -v` → all pass, including both fixture tests in either order (`-p no:randomly` not needed; try `uv run pytest tests/db/test_fixtures.py -v` alone too).

- [ ] **Step 5: Commit**

```bash
git add backend/tests/conftest.py backend/tests/db/test_fixtures.py backend/tests/db/test_migrations.py
git commit -m "test(db): migrated_engine + rollback db_session fixtures"
```

---

### Task 7: Docs, ROADMAP tick, PR

**Files:**
- Modify: `docs/DATABASE.md`
- Modify: `docs/ROADMAP.md`

- [ ] **Step 1: DATABASE.md edits** (same PR as the migration — WORKFLOW.md rule):
  1. Under `## 2` after the `nfl_teams` block add: "*Phase 0 note:* `bye_week` is per-season data on a static table; it is left NULL and byes derive from `games` at query time. Revisit if a `season_team_meta` table is added."
  2. In `crosswalk_unmatched`, change `id BIGSERIAL PRIMARY KEY` to `id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY` (what the migration creates).
  3. Under `## 6` `player_week_actuals`: "`league_id` participates in the PK and is therefore NOT NULL. The generic-PPR row uses sentinel `00000000-0000-0000-0000-000000000000` (`ffh.db.models.GENERIC_LEAGUE_ID`)." Update the DDL to `league_id UUID NOT NULL REFERENCES leagues(league_id)`. Note the FK means a sentinel `leagues` row must exist — **add** to the note: "PR ③ seeds the sentinel league row (`platform='ffh'`, `external_id='generic'`) in `seed_nfl_teams`' companion `seed_reference()`", and put that seeding in PR ③'s locked scope (overview plan) — done in this PR by editing `docs/superpowers/plans/2026-08-15-phase0-00-overview.md` ③ bullet for `reference.py`: add `seed_generic_league(session)`.
  4. Add a line at the top of `## 1`: "Migrations live in `backend/alembic/versions/`; run `uv run alembic upgrade head` from `backend/`."
  5. In `ingest_runs`, change the index DDL to `(source, asset, started_at)` with a note: "DESC omitted — btree scans backward; keeps autogenerate drift-free."

- [ ] **Step 2: ROADMAP.md** — tick "Postgres schema + initial Alembic migration".

- [ ] **Step 3: Full verification** — `uv run ruff check . && uv run ruff format --check . && uv run pytest -v` green.

- [ ] **Step 4: Push and open PR**

```bash
git add docs/DATABASE.md docs/ROADMAP.md docs/superpowers/plans/2026-08-15-phase0-00-overview.md
git commit -m "docs(db): record identity/sentinel/bye_week decisions; tick schema roadmap item"
git push -u origin feat/db-schema
gh pr create --title "feat(db): SQLAlchemy models + initial Alembic migration (Phase 0 ②)" --body-file - <<'EOF'
## Summary
- 22 tables from docs/DATABASE.md §2–7 as SQLAlchemy 2 models
- Alembic env (URL from FFH_DATABASE_URL or -x url=) and migration 0001
- Engine/session factories; migrated_engine + rollback db_session test fixtures
- Tests: table shapes, BRIN index, CHECK constraint, upgrade/check/downgrade round trip

Deviations recorded in DATABASE.md: identity vs bigserial; player_week_actuals.league_id NOT NULL with sentinel; nfl_teams.bye_week left NULL.

Spec: docs/superpowers/specs/2026-08-15-phase0-foundation-design.md §2
Plan: docs/superpowers/plans/2026-08-15-phase0-02-db-schema.md

🤖 Generated with [Claude Code](https://claude.com/claude-code)

https://claude.ai/code/session_018YN1sk8qzRpGdzgXYHBPgP
EOF
gh pr checks --watch
```

- [ ] **Step 5: Codex adversarial review** (Chris runs it) — hunt list: nullable/PK mismatches vs DATABASE.md, missing indexes, wrong FK `ondelete`. Resolve BLOCKING findings; merge.
