from pathlib import Path

import duckdb
import polars as pl
import pytest

from ffh.features.duck import VIEWS, connect, latest_partition
from ffh.ingest.lake import parquet_file, write_parquet


def _land(lake: Path, source: str, asset: str, df: pl.DataFrame, **keys) -> Path:
    path = parquet_file(lake, source, asset, **keys)
    write_parquet(df, path)
    return path


@pytest.fixture
def lake(tmp_path: Path) -> Path:
    _land(
        tmp_path,
        "nflverse",
        "players",
        pl.DataFrame({"gsis_id": ["00-0034796", "00-0036322"], "position": ["WR", "RB"]}),
        scrape_date="2026-08-15",
    )
    _land(
        tmp_path,
        "nflverse",
        "stats_player_week",
        pl.DataFrame(
            {
                "player_id": ["00-0034796", "00-0034796"],
                "week": [1, 2],
                "fantasy_points_ppr": [18.4, 22.1],
            }
        ),
        season="2026",
        scrape_date="2026-08-15",
    )
    _land(
        tmp_path,
        "nfldata",
        "games",
        pl.DataFrame({"game_id": ["2026_01_NE_SEA"], "season": [2026], "week": [1]}),
        scrape_date="2026-08-15",
    )
    return tmp_path


def test_latest_partition_picks_the_newest_scrape_date(lake: Path):
    _land(
        lake,
        "nflverse",
        "players",
        pl.DataFrame({"gsis_id": ["00-0039999"], "position": ["TE"]}),
        scrape_date="2026-08-16",
    )
    newest = latest_partition(lake, "nflverse", "players")
    assert newest is not None
    assert newest.parent.name == "scrape_date=2026-08-16"


def test_latest_partition_respects_the_season_filter(lake: Path):
    _land(
        lake,
        "nflverse",
        "stats_player_week",
        pl.DataFrame({"player_id": ["x"], "week": [1], "fantasy_points_ppr": [1.0]}),
        season="2025",
        scrape_date="2026-12-31",
    )
    picked = latest_partition(lake, "nflverse", "stats_player_week", season=2026)
    assert picked is not None
    assert "season=2026" in picked.as_posix()


def test_latest_partition_returns_none_when_nothing_landed(tmp_path: Path):
    assert latest_partition(tmp_path, "nflverse", "injuries", season=2026) is None


def test_connect_creates_views_for_landed_assets(lake: Path):
    con = connect(lake, season=2026)
    try:
        names = {row[0] for row in con.execute("SELECT view_name FROM duckdb_views()").fetchall()}
        assert {"players", "stats_player_week", "games"} <= names
        # not landed in this fixture, so no view is created
        assert "injuries" not in names
    finally:
        con.close()


def test_connect_views_are_queryable(lake: Path):
    con = connect(lake, season=2026)
    try:
        assert con.execute("SELECT count(*) FROM players").fetchone()[0] == 2
        total = con.execute(
            "SELECT round(sum(fantasy_points_ppr), 1) FROM stats_player_week "
            "WHERE player_id = '00-0034796'"
        ).fetchone()[0]
        assert total == pytest.approx(40.5)
        assert con.execute("SELECT game_id FROM games").fetchone()[0] == "2026_01_NE_SEA"
    finally:
        con.close()


def test_connect_never_creates_a_duckdb_file(lake: Path, tmp_path: Path):
    con = connect(lake, season=2026)
    try:
        con.execute("SELECT 1").fetchone()
    finally:
        con.close()
    assert not list(tmp_path.rglob("*.duckdb"))
    assert not list(Path.cwd().glob("*.duckdb"))


def test_connect_on_an_empty_lake_returns_a_usable_connection(tmp_path: Path):
    con = connect(tmp_path, season=2026)
    try:
        assert isinstance(con, duckdb.DuckDBPyConnection)
        assert con.execute("SELECT 42").fetchone()[0] == 42
    finally:
        con.close()


def test_views_cover_every_asset_the_spec_requires():
    assert set(VIEWS) == {
        "players",
        "stats_player_week",
        "snap_counts",
        "depth_charts",
        "injuries",
        "games",
    }
