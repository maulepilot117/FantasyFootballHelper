"""nfldata games.csv -> lake Parquet + the `games` table (DATA_SOURCES.md §2).

Live gotchas verified 2026-08-16 and encoded below:
  * `gameday`/`gametime` are Eastern wall-clock, not UTC.
  * `roof` is the literal quoted empty string "" (not NULL) for retractable-roof stadiums
    whose game has not been played — 43 of 272 rows in 2026.
  * `game_type` is REG|WC|DIV|CON|SB; `games.season_type` is REG|POST.
  * neutral-site games carry the nominal HOME team's stadium_id.
"""

import io
from typing import ClassVar

import polars as pl
import structlog
from sqlalchemy import func
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from ffh.config import get_settings
from ffh.db.models import Game
from ffh.ingest.base import HttpIngestJob, register
from ffh.ingest.lake import scrape_date
from ffh.ingest.reference import assert_stadium_coverage, assert_team_coverage

log = structlog.get_logger(__name__)

GAMES_CSV_URL = "https://raw.githubusercontent.com/nflverse/nfldata/master/data/games.csv"

EASTERN = "America/New_York"

GAME_COLUMNS = (
    "game_id",
    "season",
    "week",
    "season_type",
    "kickoff_at",
    "home_team",
    "away_team",
    "stadium_id",
    "spread_line",
    "total_line",
    "home_moneyline",
    "away_moneyline",
    "roof",
    "surface",
    "div_game",
    "home_rest",
    "away_rest",
    "neutral_site",
    "home_score",
    "away_score",
    "temp_f",
    "wind_mph",
)


def _blank_to_null(name: str) -> pl.Expr:
    """games.csv writes an empty *quoted* string where R had `""` — that is not NULL."""
    return (
        pl.when(pl.col(name).str.strip_chars() == "").then(None).otherwise(pl.col(name)).alias(name)
    )


def to_game_rows(df: pl.DataFrame, season: int) -> pl.DataFrame:
    """Map raw games.csv rows for one season onto the `games` table's columns."""
    season_df = df.filter(pl.col("season") == season)
    if season_df.height == 0:
        raise ValueError(f"games.csv has no rows for season {season}")

    kickoff = (
        pl.concat_str([pl.col("gameday"), pl.lit(" "), pl.col("gametime")])
        .str.to_datetime("%Y-%m-%d %H:%M", time_unit="us")
        .dt.replace_time_zone(EASTERN, ambiguous="earliest", non_existent="raise")
        .dt.convert_time_zone("UTC")
        .alias("kickoff_at")
    )

    rows = season_df.select(
        pl.col("game_id"),
        pl.col("season").cast(pl.Int16),
        pl.col("week").cast(pl.Int16),
        pl.when(pl.col("game_type") == "REG")
        .then(pl.lit("REG"))
        .otherwise(pl.lit("POST"))
        .alias("season_type"),
        kickoff,
        pl.col("home_team"),
        pl.col("away_team"),
        _blank_to_null("stadium_id"),
        pl.col("spread_line").cast(pl.Float64),
        pl.col("total_line").cast(pl.Float64),
        pl.col("home_moneyline").cast(pl.Int32),
        pl.col("away_moneyline").cast(pl.Int32),
        _blank_to_null("roof"),
        _blank_to_null("surface"),
        (pl.col("div_game") == 1).alias("div_game"),
        pl.col("home_rest").cast(pl.Int16),
        pl.col("away_rest").cast(pl.Int16),
        (pl.col("location") == "Neutral").alias("neutral_site"),
        pl.col("home_score").cast(pl.Int16),
        pl.col("away_score").cast(pl.Int16),
        pl.col("temp").cast(pl.Float64).alias("temp_f"),
        pl.col("wind").cast(pl.Float64).alias("wind_mph"),
    )
    assert rows.height == season_df.height, (
        f"row loss mapping games.csv: {season_df.height} in, {rows.height} out"
    )
    assert rows["kickoff_at"].null_count() == 0, "every scheduled game must have a kickoff time"
    return rows.select(GAME_COLUMNS)


def upsert_games(session: Session, df: pl.DataFrame, season: int) -> int:
    """Upsert one season of games.csv into `games`. Returns the number of rows upserted."""
    rows = to_game_rows(df, season)

    assert_team_coverage(session, set(rows["home_team"]) | set(rows["away_team"]))
    assert_stadium_coverage(session, rows)

    records = rows.to_dicts()
    assert len(records) == rows.height, "row loss converting the frame to records"

    stmt = pg_insert(Game).values(records)
    updatable = {c: stmt.excluded[c] for c in GAME_COLUMNS if c != "game_id"}
    stmt = stmt.on_conflict_do_update(
        index_elements=[Game.game_id],
        # ORM onupdate does not fire for INSERT ... ON CONFLICT (DATABASE.md §2).
        set_={**updatable, "updated_at": func.now()},
    )
    session.execute(stmt)
    log.info("ingest.games.upserted", season=season, rows=len(records))
    return len(records)


@register
class NfldataGamesJob(HttpIngestJob):
    """Schedule + Vegas lines + per-game roof state. Refreshes every 5 minutes in season."""

    name = "nfldata_games"
    source = "nfldata"
    asset = "games"
    seasonal: ClassVar[bool] = False
    season_scoped: ClassVar[bool] = True
    REQUIRED_COLUMNS = frozenset(
        {
            "game_id",
            "season",
            "game_type",
            "week",
            "gameday",
            "gametime",
            "away_team",
            "home_team",
            "away_score",
            "home_score",
            "location",
            "away_rest",
            "home_rest",
            "away_moneyline",
            "home_moneyline",
            "spread_line",
            "total_line",
            "div_game",
            "roof",
            "surface",
            "temp",
            "wind",
            "stadium_id",
        }
    )

    def url(self) -> str:
        return GAMES_CSV_URL

    def partition(self) -> dict[str, str]:
        return {"scrape_date": scrape_date()}

    def parse(self, content: bytes) -> pl.DataFrame:
        # infer_schema_length=None scans every row: the 2026 rows are almost all-null and a
        # short inference window types spread_line/total_line as String.
        return pl.read_csv(io.BytesIO(content), infer_schema_length=None)

    def persist(self, session: Session, df: pl.DataFrame) -> None:
        upsert_games(session, df, self.season or get_settings().season)
