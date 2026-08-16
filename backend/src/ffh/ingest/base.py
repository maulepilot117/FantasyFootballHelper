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
            raise IngestValidationError(f"{type(self).name}: missing required columns {missing}")
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
        except Exception as exc:  # broad on purpose: the status field is the error channel
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
