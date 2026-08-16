"""Schema-drift canary: the recorded real assets must still satisfy REQUIRED_COLUMNS."""

from pathlib import Path

import polars as pl
import pytest

from ffh.ingest.games import NfldataGamesJob
from ffh.ingest.nflverse import (
    NflverseDepthChartsJob,
    NflverseInjuriesJob,
    NflversePbpJob,
    NflversePlayersJob,
    NflverseSnapCountsJob,
    NflverseStatsPlayerWeekJob,
)
from ffh.ingest.reference import StadiumsJob

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"

PARQUET_JOBS = [
    NflversePlayersJob,
    NflverseStatsPlayerWeekJob,
    NflverseSnapCountsJob,
    NflverseDepthChartsJob,
    NflverseInjuriesJob,
    NflversePbpJob,
]


@pytest.mark.parametrize("cls", PARQUET_JOBS, ids=lambda c: c.name)
def test_recorded_fixture_satisfies_required_columns(cls):
    path = FIXTURES / "nflverse" / f"{cls.asset}.parquet"
    assert path.exists(), (
        f"missing fixture {path}; run `uv run python scripts/record_nflverse_fixtures.py`"
    )
    columns = set(pl.read_parquet_schema(path))
    missing = sorted(cls.REQUIRED_COLUMNS - columns)
    assert not missing, f"{cls.name}: upstream dropped {missing} - investigate before editing"


@pytest.mark.parametrize("cls", PARQUET_JOBS, ids=lambda c: c.name)
def test_recorded_fixture_passes_validate(cls):
    df = pl.read_parquet(FIXTURES / "nflverse" / f"{cls.asset}.parquet")
    cls(season=2025).validate(df)


def test_games_fixture_passes_validate_and_keeps_the_quoted_empty_roof():
    raw = (FIXTURES / "nfldata" / "games_sample.csv").read_bytes()
    df = NfldataGamesJob(season=2026).parse(raw)
    NfldataGamesJob(season=2026).validate(df)
    assert (df["roof"] == "").sum() > 0, "the quoted-empty roof rows must survive recording"


def test_stadiums_fixture_passes_validate_and_has_62_rows():
    raw = (FIXTURES / "stadiums" / "stadiums.csv").read_bytes()
    df = StadiumsJob().parse(raw)
    StadiumsJob().validate(df)
    assert df.height >= 60
    assert df["stadium_id"].n_unique() == df.height


def test_stats_player_week_fixture_has_150_columns():
    schema = pl.read_parquet_schema(FIXTURES / "nflverse" / "stats_player_week.parquet")
    assert len(schema) == 150, "DATA_SOURCES.md §1 records 150 columns"
