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
from sqlalchemy.dialects.postgresql import REAL
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
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
    # Phase 0 addition (DATABASE.md §2 note): nflverse latest_team, refreshed by
    # ffh.crosswalk.registry.seed_players. Crosswalk rung-3 tie-breaker ONLY — never roster truth.
    team_abbr: Mapped[str | None] = mapped_column(Text)
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

    __table_args__ = (
        Index("player_external_ids_player_idx", "player_id"),
        # One external id per source per player_id (DATABASE.md §3 invariant, made real —
        # see migration 0002 docstring: resolve._persist / apply_playerids must pre-check
        # (source, player_id) and route the loser to crosswalk_unmatched / the ambiguity
        # report rather than relying on an IntegrityError).
        Index("player_external_ids_source_player_uidx", "source", "player_id", unique=True),
    )


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
    neutral_site: Mapped[bool] = mapped_column(
        Boolean, nullable=False, server_default=text("false")
    )
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
