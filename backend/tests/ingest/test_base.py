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
            select(IngestRun)
            .where(IngestRun.source == source)
            .order_by(IngestRun.started_at, IngestRun.finished_at.nulls_last())
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


def test_persist_failure_is_failed_and_lands_nothing(db_session, tmp_path: Path):
    class Exploding(FakeJob):
        name = "exploding_persist"

        def persist(self, session, df):
            raise RuntimeError("upsert blew up")

    result = Exploding(script=[Fetched(content=b"x", etag=None, mtime=None)]).run(
        db_session, tmp_path
    )
    assert result.status == "failed"
    assert "upsert blew up" in result.error
    assert not list(tmp_path.rglob("*.parquet"))
    (run,) = _runs(db_session)
    assert run.status == "failed" and "upsert blew up" in run.error


def test_persist_runs_before_landing_so_skipped_partition_still_persists(
    db_session, tmp_path: Path
):
    calls: list[int] = []

    class Counting(FakeJob):
        name = "counting_persist"

        def persist(self, session, df):
            calls.append(df.height)

    FakeJob(script=[Fetched(content=b"x", etag=None, mtime=None)]).run(db_session, tmp_path)
    result = Counting(script=[Fetched(content=b"x", etag=None, mtime=None)]).run(
        db_session, tmp_path
    )
    assert result.status == "skipped"
    assert calls == [1]


def test_ingest_validation_error_is_a_value_error():
    assert issubclass(IngestValidationError, ValueError)
