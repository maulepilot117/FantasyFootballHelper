"""`sleeper_players` IngestJob: pure tests (no Postgres).

The db-marked lifecycle tests live in test_sleeper_players_db.py.
"""

import hashlib
import json

import httpx
import polars as pl
import pytest

from ffh.adapters.sleeper.catalog import REQUIRED_COLUMNS
from ffh.ingest.base import JOBS, Fetched, IngestValidationError, NotModified, get_job
from ffh.ingest.lake import scrape_date
from ffh.ingest.sleeper_players import PLAYER_COLUMNS, SleeperPlayersJob, players_to_frame


def test_job_is_registered_under_its_name():
    # ③'s @register decorator keys JOBS on the class's `name` ClassVar.
    assert SleeperPlayersJob.name == "sleeper_players"
    assert SleeperPlayersJob.source == "sleeper" and SleeperPlayersJob.asset == "players"
    assert get_job("sleeper_players") is SleeperPlayersJob


def test_cli_imports_the_job_module():
    # `ffh ingest run sleeper_players` only works if the CLI eagerly imports the module.
    import ffh.cli  # noqa: F401

    assert JOBS["sleeper_players"] is SleeperPlayersJob


def test_partition_is_todays_utc_scrape_date():
    # Same clock as every other lake partition (③ `ffh.ingest.lake.scrape_date`, UTC).
    assert SleeperPlayersJob().partition() == {"scrape_date": scrape_date()}


def test_player_columns_start_with_the_catalog_contract():
    # ONE column contract: the catalog's REQUIRED_COLUMNS lead PLAYER_COLUMNS verbatim and
    # the job's validate() requires exactly those.
    assert PLAYER_COLUMNS[: len(REQUIRED_COLUMNS)] == tuple(REQUIRED_COLUMNS)
    assert SleeperPlayersJob.REQUIRED_COLUMNS == frozenset(REQUIRED_COLUMNS)


def test_frame_is_all_utf8_with_normalized_name_and_position(sleeper_fixture):
    df = players_to_frame(sleeper_fixture("players_slice"))
    assert df.columns == list(PLAYER_COLUMNS)
    assert set(df.schema.values()) == {pl.Utf8}
    assert df.height == 25
    row = df.filter(pl.col("player_id") == "1").row(0, named=True)
    assert row["name"] == "Fixture Quarterback" and row["position"] == "QB"
    assert row["espn_id"] == "9000001" and row["active"] == "true"
    assert row["fantasy_positions"] == "QB"
    # DEF entries: position becomes DST and the name is the team abbreviation, which is
    # the one form the crosswalk's normalize_dst can canonicalize.
    dst = df.filter(pl.col("player_id") == "KC").row(0, named=True)
    assert dst["position"] == "DST" and dst["name"] == "KC" and dst["gsis_id"] is None


def test_frame_rejects_an_empty_payload():
    with pytest.raises(ValueError, match="empty payload"):
        players_to_frame({})


def test_validate_requires_the_crosswalk_columns_and_rows(sleeper_fixture):
    job = SleeperPlayersJob()
    good = players_to_frame(sleeper_fixture("players_slice"))
    job.validate(good)
    # ③'s contract: validate() raises IngestValidationError (never a bare assert), so
    # IngestJob.run maps it to status="failed" with the message in ingest_runs.error.
    with pytest.raises(IngestValidationError, match="missing required columns"):
        job.validate(pl.DataFrame({"player_id": []}, schema={"player_id": pl.Utf8}))
    with pytest.raises(IngestValidationError, match="0 rows"):
        job.validate(good.head(0))


def test_validate_rejects_duplicate_and_null_player_ids(sleeper_fixture):
    job = SleeperPlayersJob()
    good = players_to_frame(sleeper_fixture("players_slice"))
    with pytest.raises(IngestValidationError, match="duplicate player_id"):
        job.validate(pl.concat([good, good.head(1)]))
    nulled = good.with_columns(
        pl.when(pl.col("player_id") == "1")
        .then(None)
        .otherwise(pl.col("player_id"))
        .alias("player_id")
    )
    with pytest.raises(IngestValidationError, match="null player_id"):
        job.validate(nulled)


def test_fetch_hashes_the_body_and_returns_not_modified_when_unchanged(
    sleeper_mock, sleeper_fixture
):
    # fetch() is SYNC: ③'s IngestJob.run calls `self.fetch(etag)` directly. Sleeper ignores
    # If-None-Match, so freshness is a sha256 of the body, not the server's ETag.
    body = json.dumps(sleeper_fixture("players_slice")).encode()
    route = sleeper_mock.get("/players/nfl").mock(
        return_value=httpx.Response(200, content=body, headers={"ETag": 'W/"server-etag"'})
    )
    job = SleeperPlayersJob()
    first = job.fetch(None)
    assert isinstance(first, Fetched)
    assert first.etag == f"sha256:{hashlib.sha256(body).hexdigest()}"
    assert first.content == body and first.mtime is not None
    # Goes through the shared ingest client: our User-Agent, never a bare httpx.Client.
    assert route.calls.last.request.headers["user-agent"].startswith("ffh/")
    assert "if-none-match" not in route.calls.last.request.headers

    second = job.fetch(first.etag)
    assert isinstance(second, NotModified) and second.etag == first.etag

    # A different stored digest means the blob changed -> Fetched again.
    third = job.fetch("sha256:" + "0" * 64)
    assert isinstance(third, Fetched) and third.etag == first.etag


def test_fetch_retries_transient_statuses_through_the_ingest_http_layer(
    sleeper_mock, sleeper_fixture, monkeypatch
):
    monkeypatch.setattr("ffh.ingest.http._RETRY_WAIT_CAP", 0.0)
    body = json.dumps(sleeper_fixture("players_slice")).encode()
    route = sleeper_mock.get("/players/nfl").mock(
        side_effect=[httpx.Response(503), httpx.Response(200, content=body)]
    )
    result = SleeperPlayersJob().fetch(None)
    assert isinstance(result, Fetched) and route.call_count == 2


def test_parse_round_trips_the_json_body(sleeper_fixture):
    payload = sleeper_fixture("players_slice")
    df = SleeperPlayersJob().parse(json.dumps(payload).encode())
    assert df.height == len(payload) and df.columns == list(PLAYER_COLUMNS)


def test_landed_partition_is_readable_by_the_lake_player_catalog(tmp_path, sleeper_fixture):
    """Task 5's LakePlayerCatalog and this job must agree on path and columns."""
    import asyncio

    from ffh.adapters.sleeper.catalog import LakePlayerCatalog
    from ffh.ingest.lake import parquet_file, write_parquet

    df = players_to_frame(sleeper_fixture("players_slice"))
    write_parquet(df, parquet_file(tmp_path, "sleeper", "players", scrape_date="2026-08-16"))
    refs = asyncio.run(LakePlayerCatalog(tmp_path).all_players())
    assert refs["KC"].position == "DST" and refs["1"].name == "Fixture Quarterback"
    assert len(refs) == 25
