"""End-to-end: real job classes, respx-mocked servers, real Postgres, real lake on tmp_path."""

from pathlib import Path

import httpx
import polars as pl
import pytest
import respx
from sqlalchemy import select

from ffh.db.models import Game, IngestRun
from ffh.features.duck import connect
from ffh.ingest.games import GAMES_CSV_URL, NfldataGamesJob
from ffh.ingest.nflverse import NflversePbpJob, NflversePlayersJob, NflverseStatsPlayerWeekJob
from ffh.ingest.reference import STADIUMS_CSV_URL, StadiumsJob, seed_nfl_teams

pytestmark = pytest.mark.db

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"
PLAYERS_URL = NflversePlayersJob().url()


def _runs(session, source):
    return list(
        session.scalars(
            select(IngestRun).where(IngestRun.source == source).order_by(IngestRun.started_at)
        )
    )


@respx.mock
def test_players_run_twice_lands_one_partition_and_two_runs(db_session, tmp_path: Path):
    body = (FIXTURES / "nflverse" / "players.parquet").read_bytes()
    respx.get(PLAYERS_URL).mock(
        side_effect=[
            httpx.Response(200, content=body, headers={"ETag": '"players-v1"'}),
            httpx.Response(304),
        ]
    )

    first = NflversePlayersJob().run(db_session, tmp_path)
    second = NflversePlayersJob().run(db_session, tmp_path)

    assert first.status == "success" and first.rows_written == 50
    assert second.status == "skipped_not_modified"
    assert [r.status for r in _runs(db_session, "nflverse")] == [
        "success",
        "skipped_not_modified",
    ]
    assert len(list((tmp_path / "raw" / "nflverse" / "players").rglob("*.parquet"))) == 1


@respx.mock
def test_conditional_request_carries_the_stored_etag(db_session, tmp_path: Path):
    body = (FIXTURES / "nflverse" / "players.parquet").read_bytes()
    route = respx.get(PLAYERS_URL).mock(
        side_effect=[
            httpx.Response(200, content=body, headers={"ETag": '"players-v1"'}),
            httpx.Response(304),
        ]
    )
    NflversePlayersJob().run(db_session, tmp_path)
    NflversePlayersJob().run(db_session, tmp_path)
    assert route.calls[1].request.headers["if-none-match"] == '"players-v1"'


@respx.mock
def test_seasonal_404_is_skipped_not_failed(db_session, tmp_path: Path):
    respx.get(NflversePbpJob(season=2026).url()).mock(return_value=httpx.Response(404))
    result = NflversePbpJob(season=2026).run(db_session, tmp_path)
    assert result.status == "skipped"
    assert not list(tmp_path.rglob("*.parquet"))


@respx.mock
def test_stats_player_week_404_is_skipped_before_week_one(db_session, tmp_path: Path):
    # Verified live 2026-08-16: this asset 404s until Week 1, exactly like pbp.
    respx.get(NflverseStatsPlayerWeekJob(season=2026).url()).mock(return_value=httpx.Response(404))
    assert NflverseStatsPlayerWeekJob(season=2026).run(db_session, tmp_path).status == "skipped"


@respx.mock
def test_validate_failure_is_recorded_as_failed(db_session, tmp_path: Path):
    import io

    buf = io.BytesIO()
    pl.DataFrame({"unexpected": [1]}).write_parquet(buf)
    respx.get(PLAYERS_URL).mock(return_value=httpx.Response(200, content=buf.getvalue()))

    result = NflversePlayersJob().run(db_session, tmp_path)
    assert result.status == "failed"
    assert "gsis_id" in result.error
    (run,) = _runs(db_session, "nflverse")
    assert run.status == "failed" and run.rows_written is None and run.output_path is None


@respx.mock
def test_games_job_lands_parquet_and_upserts_postgres(db_session, tmp_path: Path):
    seed_nfl_teams(db_session)
    respx.get(STADIUMS_CSV_URL).mock(
        return_value=httpx.Response(
            200, content=(FIXTURES / "stadiums" / "stadiums.csv").read_bytes()
        )
    )
    assert StadiumsJob().run(db_session, tmp_path).status == "success"

    respx.get(GAMES_CSV_URL).mock(
        return_value=httpx.Response(
            200,
            content=(FIXTURES / "nfldata" / "games_sample.csv").read_bytes(),
            headers={"ETag": '"games-v1"'},
        )
    )
    result = NfldataGamesJob(season=2026).run(db_session, tmp_path)
    assert result.status == "success"

    games = list(db_session.scalars(select(Game)))
    assert games, "the fixture must contain 2026 rows"
    assert all(g.season == 2026 for g in games)
    assert all(g.kickoff_at is not None for g in games)
    # The 100% stadium join is asserted inside upsert_games; reaching here proves it held.
    assert all(g.stadium_id is not None for g in games)


@respx.mock
def test_same_day_changed_fetch_updates_postgres_but_not_the_lake(db_session, tmp_path: Path):
    """Pins the daily-snapshot lake grain (DATABASE.md §1, decided in the PR ③ review).

    Second run on the same day with CHANGED content: Postgres is upserted (persist runs
    before land), the partition already exists -> `skipped`, the lake keeps the first
    snapshot, and the watermark stays at the last *landed* ETag.
    """
    from ffh.ingest.base import last_successful_etag

    seed_nfl_teams(db_session)
    respx.get(STADIUMS_CSV_URL).mock(
        return_value=httpx.Response(
            200, content=(FIXTURES / "stadiums" / "stadiums.csv").read_bytes()
        )
    )
    assert StadiumsJob().run(db_session, tmp_path).status == "success"

    v1 = (FIXTURES / "nfldata" / "games_sample.csv").read_bytes()
    # v2: bump every 2026 Week-1 total_line to 99.5 by rewriting the raw text
    df = pl.read_csv(v1, infer_schema_length=None)
    changed = df.with_columns(
        pl.when((pl.col("season") == 2026) & (pl.col("week") == 1))
        .then(pl.lit(99.5))
        .otherwise(pl.col("total_line"))
        .alias("total_line")
    )
    v2 = changed.write_csv().encode()

    route = respx.get(GAMES_CSV_URL).mock(
        side_effect=[
            httpx.Response(200, content=v1, headers={"ETag": '"games-v1"'}),
            httpx.Response(200, content=v2, headers={"ETag": '"games-v2"'}),
        ]
    )
    first = NfldataGamesJob(season=2026).run(db_session, tmp_path)
    second = NfldataGamesJob(season=2026).run(db_session, tmp_path)
    assert first.status == "success"
    assert second.status == "skipped" and "already exists" in second.error

    # Postgres reflects v2 ...
    db_session.expire_all()
    opener = db_session.get(Game, "2026_01_NE_SEA")
    assert opener.total_line == pytest.approx(99.5)
    # ... the lake still holds exactly one (v1) partition for today ...
    parts = list((tmp_path / "raw" / "nfldata" / "games").rglob("games.parquet"))
    assert len(parts) == 1
    landed = pl.read_parquet(parts[0])
    assert landed.filter(pl.col("game_id") == "2026_01_NE_SEA")["total_line"].item() != 99.5
    # ... and the watermark is still v1, so the second request carried v1, not v2.
    assert route.calls[1].request.headers["if-none-match"] == '"games-v1"'
    assert last_successful_etag(db_session, "nfldata", "games", 2026) == '"games-v1"'


@respx.mock
def test_duckdb_queries_the_landed_fixture(db_session, tmp_path: Path):
    body = (FIXTURES / "nflverse" / "players.parquet").read_bytes()
    respx.get(PLAYERS_URL).mock(return_value=httpx.Response(200, content=body))
    NflversePlayersJob().run(db_session, tmp_path)

    con = connect(tmp_path, season=2026)
    try:
        assert con.execute("SELECT count(*) FROM players").fetchone()[0] == 50
        assert (
            con.execute("SELECT count(*) FROM players WHERE gsis_id IS NOT NULL").fetchone()[0] > 0
        )
    finally:
        con.close()
    assert not list(tmp_path.rglob("*.duckdb"))
