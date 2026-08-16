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
from sqlalchemy.dialects.postgresql import JSONB, REAL
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
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
