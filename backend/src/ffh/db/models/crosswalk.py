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
