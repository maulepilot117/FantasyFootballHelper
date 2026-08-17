"""`sleeper_players` lifecycle against Postgres (ingest_runs row + never-overwrite guard)."""

import json

import httpx
import polars as pl
import pytest
from sqlalchemy import select

from ffh.db.models import IngestRun
from ffh.ingest.lake import parquet_file, write_parquet
from ffh.ingest.sleeper_players import PLAYER_COLUMNS, SleeperPlayersJob

pytestmark = pytest.mark.db


def test_first_run_lands_the_partition_and_records_the_digest(
    tmp_path, db_session, sleeper_mock, sleeper_fixture
):
    body = json.dumps(sleeper_fixture("players_slice")).encode()
    sleeper_mock.get("/players/nfl").mock(return_value=httpx.Response(200, content=body))
    job = SleeperPlayersJob()

    result = job.run(db_session, tmp_path)
    assert result.status == "success" and result.rows_written == 25
    landed = parquet_file(tmp_path, "sleeper", "players", **job.partition())
    assert result.output_path == str(landed)
    assert pl.read_parquet(landed).columns == list(PLAYER_COLUMNS)
    run = db_session.scalars(select(IngestRun).where(IngestRun.run_id == result.run_id)).one()
    assert run.source_etag is not None and run.source_etag.startswith("sha256:")


def test_second_run_on_the_same_day_is_skipped_never_overwritten(
    tmp_path, db_session, sleeper_mock, sleeper_fixture
):
    """③'s lifecycle owns the guard: write_parquet raises PartitionExistsError for today's
    file and IngestJob.run maps that to status="skipped" (an ingest_runs row is still
    written). No run() override here — the base lifecycle is the contract."""
    body = json.dumps(sleeper_fixture("players_slice")).encode()
    sleeper_mock.get("/players/nfl").mock(return_value=httpx.Response(200, content=body))
    job = SleeperPlayersJob()
    today = job.partition()["scrape_date"]
    landed = parquet_file(tmp_path, "sleeper", "players", scrape_date=today)
    write_parquet(pl.DataFrame({"player_id": ["sentinel"]}), landed)

    result = job.run(db_session, tmp_path)
    assert result.status == "skipped" and result.rows_written is None
    assert "already exists" in result.error
    # The original partition survives untouched (DATABASE.md §1: never overwrite).
    assert pl.read_parquet(landed)["player_id"].to_list() == ["sentinel"]
    assert len(list(tmp_path.rglob("*.parquet"))) == 1
