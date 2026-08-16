import io

import polars as pl
import pytest

from ffh.ingest.base import JOBS, IngestValidationError
from ffh.ingest.lake import scrape_date
from ffh.ingest.nflverse import (
    NflverseDepthChartsJob,
    NflverseInjuriesJob,
    NflversePbpJob,
    NflversePlayersJob,
    NflverseSnapCountsJob,
    NflverseStatsPlayerWeekJob,
)

SEASONAL = [
    NflverseStatsPlayerWeekJob,
    NflverseSnapCountsJob,
    NflverseDepthChartsJob,
    NflverseInjuriesJob,
    NflversePbpJob,
]


def test_all_six_jobs_are_registered():
    assert {
        "nflverse_players",
        "nflverse_stats_player_week",
        "nflverse_snap_counts",
        "nflverse_depth_charts",
        "nflverse_injuries",
        "nflverse_pbp",
    } <= set(JOBS)


def test_urls_match_the_verified_nflverse_release_paths():
    assert NflversePlayersJob().url() == (
        "https://github.com/nflverse/nflverse-data/releases/download/players/players.parquet"
    )
    assert NflverseStatsPlayerWeekJob(season=2026).url() == (
        "https://github.com/nflverse/nflverse-data/releases/download/"
        "stats_player/stats_player_week_2026.parquet"
    )
    assert NflverseSnapCountsJob(season=2026).url().endswith("snap_counts/snap_counts_2026.parquet")
    assert (
        NflverseDepthChartsJob(season=2026).url().endswith("depth_charts/depth_charts_2026.parquet")
    )
    assert NflverseInjuriesJob(season=2026).url().endswith("injuries/injuries_2026.parquet")
    assert NflversePbpJob(season=2026).url().endswith("pbp/play_by_play_2026.parquet")


def test_stats_player_week_uses_the_live_asset_not_the_frozen_one():
    url = NflverseStatsPlayerWeekJob(season=2026).url()
    assert "stats_player/stats_player_week_" in url
    assert "player_stats" not in url  # frozen at 2025-05-07 (DATA_SOURCES.md warning 2)


@pytest.mark.parametrize("cls", SEASONAL)
def test_every_seasonal_job_skips_on_404(cls):
    assert cls.seasonal is True
    assert cls.season_scoped is True
    assert cls.skip_on_404 is True, "seasonal assets 404 before Week 1 (verified 2026-08-16)"


def test_players_job_is_not_seasonal_and_does_not_skip_on_404():
    assert NflversePlayersJob.seasonal is False
    assert NflversePlayersJob.skip_on_404 is False


def test_partitions_are_hive_keys_in_path_order():
    assert NflversePlayersJob().partition() == {"scrape_date": scrape_date()}
    assert NflverseInjuriesJob(season=2026).partition() == {
        "season": "2026",
        "scrape_date": scrape_date(),
    }


def test_parse_reads_parquet_bytes():
    buf = io.BytesIO()
    pl.DataFrame({"gsis_id": ["00-0034796"]}).write_parquet(buf)
    df = NflversePlayersJob().parse(buf.getvalue())
    assert df["gsis_id"].to_list() == ["00-0034796"]


def _frame(cls) -> pl.DataFrame:
    return pl.DataFrame({c: ["x"] for c in sorted(cls.REQUIRED_COLUMNS)})


@pytest.mark.parametrize(
    "cls",
    [
        NflversePlayersJob,
        NflverseStatsPlayerWeekJob,
        NflverseSnapCountsJob,
        NflverseDepthChartsJob,
        NflverseInjuriesJob,
        NflversePbpJob,
    ],
)
def test_validate_accepts_a_frame_with_exactly_the_required_columns(cls):
    cls(season=2026).validate(_frame(cls))


@pytest.mark.parametrize(
    "cls",
    [
        NflversePlayersJob,
        NflverseStatsPlayerWeekJob,
        NflverseSnapCountsJob,
        NflverseDepthChartsJob,
        NflverseInjuriesJob,
        NflversePbpJob,
    ],
)
def test_validate_rejects_a_frame_missing_one_required_column(cls):
    df = _frame(cls)
    dropped = sorted(cls.REQUIRED_COLUMNS)[0]
    with pytest.raises(IngestValidationError, match=dropped):
        cls(season=2026).validate(df.drop(dropped))


def test_stats_player_week_keys_on_player_id_not_gsis_id():
    # verified 2026-08-16: the 150-column asset has `player_id` (a GSIS id), no `gsis_id`
    assert "player_id" in NflverseStatsPlayerWeekJob.REQUIRED_COLUMNS
    assert "gsis_id" not in NflverseStatsPlayerWeekJob.REQUIRED_COLUMNS


def test_snap_counts_keys_on_pfr_player_id():
    # verified 2026-08-16: snap_counts carries no GSIS id at all
    assert "pfr_player_id" in NflverseSnapCountsJob.REQUIRED_COLUMNS
    assert "gsis_id" not in NflverseSnapCountsJob.REQUIRED_COLUMNS


def test_players_job_uses_nflverse_field_names_not_table_names():
    required = NflversePlayersJob.REQUIRED_COLUMNS
    assert {"display_name", "college_name", "rookie_season", "height", "weight"} <= required
    assert not ({"full_name", "college", "rookie_year", "height_in", "weight_lb"} & required)
