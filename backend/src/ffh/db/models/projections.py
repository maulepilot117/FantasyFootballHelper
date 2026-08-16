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
from sqlalchemy.dialects.postgresql import JSONB, REAL
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
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
        UniqueConstraint(
            "player_id",
            "season",
            "week",
            "league_id",
            "source",
            "model_version",
            postgresql_nulls_not_distinct=True,
        ),
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
