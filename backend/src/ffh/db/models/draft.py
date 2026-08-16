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
from sqlalchemy.dialects.postgresql import REAL
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
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
