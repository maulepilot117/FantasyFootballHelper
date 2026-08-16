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
from sqlalchemy.dialects.postgresql import JSONB, REAL
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
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
    is_superflex: Mapped[bool] = mapped_column(
        Boolean, nullable=False, server_default=text("false")
    )
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
