"""nflverse release Parquet jobs (DATA_SOURCES.md §1).

No API key, no rate limit. Read the release Parquet URLs directly with httpx + Polars —
`nflreadpy` is deliberately not a dependency and `nfl_data_py` is archived.

REQUIRED_COLUMNS were verified live on 2026-08-16 against:
  players.parquet (39 cols) · stats_player_week_2025.parquet (150) ·
  snap_counts_2025.parquet (16) · depth_charts_2026.parquet (12) ·
  injuries_2025.parquet (16) · play_by_play_2025.parquet (372)
Re-verify with `uv run python scripts/record_nflverse_fixtures.py` before changing them.
"""

import io
from typing import ClassVar

import polars as pl

from ffh.ingest.base import HttpIngestJob, IngestValidationError, register
from ffh.ingest.lake import scrape_date

NFLVERSE_RELEASE_BASE = "https://github.com/nflverse/nflverse-data/releases/download"


class NflverseParquetJob(HttpIngestJob):
    """One nflverse release asset. Lands the file verbatim — renaming belongs to PR ④."""

    source: ClassVar[str] = "nflverse"
    release: ClassVar[str]
    #: str.format template; seasonal assets use {season}
    filename: ClassVar[str]

    def url(self) -> str:
        return f"{NFLVERSE_RELEASE_BASE}/{self.release}/{self.filename.format(season=self.season)}"

    def partition(self) -> dict[str, str]:
        if type(self).seasonal:
            return {"season": str(self.season), "scrape_date": scrape_date()}
        return {"scrape_date": scrape_date()}

    def parse(self, content: bytes) -> pl.DataFrame:
        return pl.read_parquet(io.BytesIO(content))


class SeasonalNflverseJob(NflverseParquetJob):
    """Per-season assets. Verified 2026-08-16: these 404 before Week 1, so 404 -> skipped."""

    seasonal: ClassVar[bool] = True
    season_scoped: ClassVar[bool] = True
    skip_on_404: ClassVar[bool] = True

    def validate(self, df: pl.DataFrame) -> None:
        """Required columns + non-empty, then: every `season` value must be the requested one.

        The payload lands under `season=<requested>`; a wrong-season payload (a bad filename
        template, an upstream mislabel) must never be filed as the requested season.
        depth_charts carries no `season` column and is exempt.
        """
        super().validate(df)
        if "season" in df.columns:
            found = sorted(df["season"].drop_nulls().unique().to_list())
            if found != [self.season]:
                raise IngestValidationError(
                    f"{type(self).name}: requested season {self.season} but payload has "
                    f"season values {found}"
                )


@register
class NflversePlayersJob(NflverseParquetJob):
    """Canonical player registry. Published daily, year-round — a 404 here is a real fault."""

    name = "nflverse_players"
    asset = "players"
    release = "players"
    filename = "players.parquet"
    REQUIRED_COLUMNS = frozenset(
        {
            "gsis_id",
            "display_name",
            "first_name",
            "last_name",
            "position",
            "position_group",
            "birth_date",
            "college_name",
            "height",
            "weight",
            "status",
            "rookie_season",
            "latest_team",
            "espn_id",
            "pfr_id",
        }
    )


@register
class NflverseStatsPlayerWeekJob(SeasonalNflverseJob):
    """150-column weekly stats. NOTE the asset path: `player_stats/` is frozen since 2025."""

    name = "nflverse_stats_player_week"
    asset = "stats_player_week"
    release = "stats_player"
    filename = "stats_player_week_{season}.parquet"
    REQUIRED_COLUMNS = frozenset(
        {
            "player_id",  # this IS the GSIS id; there is no `gsis_id` column
            "player_display_name",
            "position",
            "season",
            "week",
            "season_type",
            "game_id",
            "team",
            "opponent_team",
            "completions",
            "attempts",
            "passing_yards",
            "passing_tds",
            "passing_interceptions",
            "passing_epa",
            "carries",
            "rushing_yards",
            "rushing_tds",
            "rushing_epa",
            "receptions",
            "targets",
            "receiving_yards",
            "receiving_tds",
            "receiving_air_yards",
            "receiving_epa",
            "target_share",
            "air_yards_share",
            "wopr",
            "racr",
            "fantasy_points",
            "fantasy_points_ppr",
        }
    )


@register
class NflverseSnapCountsJob(SeasonalNflverseJob):
    """PFR snap counts — the in-season route-participation proxy (DATA_SOURCES.md §1)."""

    name = "nflverse_snap_counts"
    asset = "snap_counts"
    release = "snap_counts"
    filename = "snap_counts_{season}.parquet"
    REQUIRED_COLUMNS = frozenset(
        {
            "game_id",
            "pfr_game_id",
            "season",
            "game_type",
            "week",
            "player",
            "pfr_player_id",  # the only player key here — no GSIS id in this asset
            "position",
            "team",
            "opponent",
            "offense_snaps",
            "offense_pct",
            "defense_snaps",
            "defense_pct",
            "st_snaps",
            "st_pct",
        }
    )


@register
class NflverseDepthChartsJob(SeasonalNflverseJob):
    """Daily snapshots, not current state. `dt` is a String ISO-8601 UTC stamp."""

    name = "nflverse_depth_charts"
    asset = "depth_charts"
    release = "depth_charts"
    filename = "depth_charts_{season}.parquet"
    REQUIRED_COLUMNS = frozenset(
        {
            "dt",
            "team",
            "player_name",
            "espn_id",
            "gsis_id",
            "pos_grp",
            "pos_abb",
            "pos_name",
            "pos_slot",
            "pos_rank",
        }
    )


@register
class NflverseInjuriesJob(SeasonalNflverseJob):
    """Historical practice participation. 2025+ dropped `date_modified` — do not require it."""

    name = "nflverse_injuries"
    asset = "injuries"
    release = "injuries"
    filename = "injuries_{season}.parquet"
    REQUIRED_COLUMNS = frozenset(
        {
            "season",
            "season_type",
            "game_type",
            "team",
            "week",
            "gsis_id",
            "position",
            "full_name",
            "first_name",
            "last_name",
            "report_primary_injury",
            "report_secondary_injury",
            "report_status",
            "practice_primary_injury",
            "practice_secondary_injury",
            "practice_status",
        }
    )


@register
class NflversePbpJob(SeasonalNflverseJob):
    """Full play-by-play. 404s until Week 1 (verified 2026-08-16) -> `skipped`."""

    name = "nflverse_pbp"
    asset = "pbp"
    release = "pbp"
    filename = "play_by_play_{season}.parquet"
    REQUIRED_COLUMNS = frozenset(
        {
            "play_id",
            "game_id",
            "season",
            "week",
            "season_type",
            "posteam",
            "defteam",
            "play_type",
            "desc",
            "down",
            "ydstogo",
            "qtr",
            "yardline_100",
            "game_seconds_remaining",
            "yards_gained",
            "air_yards",
            "epa",
            "wp",
            "success",
            "pass_attempt",
            "rush_attempt",
            "complete_pass",
            "touchdown",
            "passer_player_id",
            "rusher_player_id",
            "receiver_player_id",
        }
    )
