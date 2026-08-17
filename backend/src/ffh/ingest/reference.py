"""Static reference seeds: nfl_teams, stadiums, and the sentinel generic league.

`nfl_teams` ships as a checked-in CSV (32 rows never change mid-season). `stadiums` is a
real ingest job over greerreNFL's CSV. The sentinel `leagues` row exists so the NOT NULL
FKs on `projections.league_id` and `player_week_actuals.league_id` can hold league-agnostic
rows (DATABASE.md §6).
"""

import io
import uuid
from importlib.resources import files
from typing import Any, ClassVar

import polars as pl
import structlog
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from ffh.db.models import GENERIC_LEAGUE_ID, League, NflTeam, Stadium
from ffh.ingest.base import HttpIngestJob, register
from ffh.ingest.lake import scrape_date

log = structlog.get_logger(__name__)

STADIUMS_CSV_URL = "https://raw.githubusercontent.com/greerreNFL/stadiums/main/data/stadiums.csv"

NFL_TEAMS_CSV = files("ffh.data") / "nfl_teams.csv"

METRES_TO_FEET = 3.280839895

#: Canonical full-PPR reference scoring. NOT a default for real leagues — those are always
#: platform-fetched (DATABASE.md §6, ARCHITECTURE.md adapter contract).
GENERIC_SCORING: dict[str, Any] = {
    "pass_yd": 0.04,
    "pass_td": 4,
    "pass_int": -2,
    "rush_yd": 0.1,
    "rush_td": 6,
    "rec": 1,
    "rec_yd": 0.1,
    "rec_td": 6,
    "fum_lost": -2,
    "two_pt": 2,
}

GENERIC_ROSTER: dict[str, Any] = {
    "QB": 1,
    "RB": 2,
    "WR": 2,
    "TE": 1,
    "FLEX": 1,
    "K": 1,
    "DST": 1,
    "BN": 6,
}


# --- nfl_teams -----------------------------------------------------------------------


def load_nfl_teams() -> pl.DataFrame:
    """The packaged 32-row team table (nflverse abbreviations, ESPN numeric ids)."""
    return pl.read_csv(io.BytesIO(NFL_TEAMS_CSV.read_bytes()))


def seed_nfl_teams(session: Session) -> int:
    """Upsert the 32 NFL teams. `bye_week` stays NULL (Phase 0 deviation, DATABASE.md §2)."""
    df = load_nfl_teams()
    if df.height != 32:
        raise ValueError(f"nfl_teams.csv must have exactly 32 rows, found {df.height}")

    records = df.select("team_abbr", "espn_id", "full_name", "conference", "division").to_dicts()
    stmt = pg_insert(NflTeam).values(records)
    stmt = stmt.on_conflict_do_update(
        index_elements=[NflTeam.team_abbr],
        set_={
            "espn_id": stmt.excluded.espn_id,
            "full_name": stmt.excluded.full_name,
            "conference": stmt.excluded.conference,
            "division": stmt.excluded.division,
        },
    )
    session.execute(stmt)
    log.info("ingest.reference.teams_seeded", rows=len(records))
    return len(records)


def assert_team_coverage(session: Session, abbrs: set[str]) -> None:
    """Every abbreviation must exist in `nfl_teams`, or the games FK fails opaquely."""
    known = set(session.scalars(select(NflTeam.team_abbr)))
    missing = sorted(abbrs - known)
    if missing:
        raise ValueError(
            f"{len(missing)} team abbreviation(s) absent from nfl_teams: {missing}. "
            "Run `ffh ingest seed` first."
        )


# --- stadiums ------------------------------------------------------------------------


def seed_stadiums(session: Session, df: pl.DataFrame) -> int:
    """Upsert greerreNFL stadium rows. `altitude` is METRES; `stadiums.altitude_ft` is feet."""
    rows = df.select(
        pl.col("stadium_id"),
        pl.col("stadium_name").alias("name"),
        pl.col("lat").cast(pl.Float64).alias("latitude"),
        pl.col("lon").cast(pl.Float64).alias("longitude"),
        (pl.col("altitude").cast(pl.Float64) * METRES_TO_FEET)
        .round(0)
        .cast(pl.Int32)
        .alias("altitude_ft"),
        pl.col("heading").cast(pl.Float64).alias("heading_deg"),
        pl.col("surface_type"),
        # Outdoors|Dome only — retractable state per game lives in games.roof.
        pl.col("roof_type"),
        pl.col("tz"),
    )
    assert rows.height == df.height, f"row loss mapping stadiums: {df.height} -> {rows.height}"

    records = rows.to_dicts()
    stmt = pg_insert(Stadium).values(records)
    stmt = stmt.on_conflict_do_update(
        index_elements=[Stadium.stadium_id],
        set_={
            c: stmt.excluded[c]
            for c in (
                "name",
                "latitude",
                "longitude",
                "altitude_ft",
                "heading_deg",
                "surface_type",
                "roof_type",
                "tz",
            )
        },
    )
    session.execute(stmt)
    log.info("ingest.reference.stadiums_seeded", rows=len(records))
    return len(records)


def assert_stadium_coverage(session: Session, rows: pl.DataFrame) -> None:
    """Assert every `stadium_id` in `rows` resolves. DATA_SOURCES.md §4 promises 30/30.

    Uses a Polars anti-join and asserts the split accounts for every input row — a silent
    join loss here is exactly the Tier-1 failure AGENTS.md calls out.
    """
    known = pl.DataFrame(
        {"stadium_id": list(session.scalars(select(Stadium.stadium_id)))},
        schema={"stadium_id": pl.String},
    )
    # A NULL stadium_id (games.csv quoted-empty -> NULL) would silently pass a drop_nulls()
    # coverage check and land as NULL in a nullable column. Reject it by name instead.
    null_ids = rows.filter(pl.col("stadium_id").is_null())
    if null_ids.height:
        games = sorted(null_ids["game_id"].to_list()) if "game_id" in rows.columns else []
        raise ValueError(
            f"{null_ids.height} game row(s) have no stadium_id: {games[:20]}. "
            "games.csv promises a stadium_id for every game (DATA_SOURCES.md §4)."
        )
    subject = rows.select("stadium_id")
    assert subject.height == rows.height, "coverage subject must cover every input row"
    unmatched = subject.join(known, on="stadium_id", how="anti")
    matched = subject.join(known, on="stadium_id", how="semi")
    assert matched.height + unmatched.height == rows.height, (
        f"anti/semi join lost rows: {matched.height} + {unmatched.height} != {rows.height}"
    )
    if unmatched.height:
        missing = sorted(set(unmatched["stadium_id"]))
        raise ValueError(
            f"{len(missing)} stadium_id value(s) absent from `stadiums`: {missing}. "
            "Run `ffh ingest run stadiums` first."
        )


@register
class StadiumsJob(HttpIngestJob):
    """greerreNFL stadium coordinates, altitude, heading and tz (DATA_SOURCES.md §4)."""

    name = "stadiums"
    source = "greerre"
    asset = "stadiums"
    seasonal: ClassVar[bool] = False
    REQUIRED_COLUMNS = frozenset(
        {
            "stadium_id",
            "stadium_name",
            "lat",
            "lon",
            "altitude",
            "heading",
            "surface_type",
            "roof_type",
            "tz",
        }
    )

    def url(self) -> str:
        return STADIUMS_CSV_URL

    def partition(self) -> dict[str, str]:
        return {"scrape_date": scrape_date()}

    def parse(self, content: bytes) -> pl.DataFrame:
        return pl.read_csv(io.BytesIO(content), infer_schema_length=None)

    def persist(self, session: Session, df: pl.DataFrame) -> None:
        seed_stadiums(session, df)


# --- sentinel league -----------------------------------------------------------------


def seed_generic_league(session: Session) -> uuid.UUID:
    """Insert the sentinel `leagues` row exactly as DATABASE.md §6 specifies. Idempotent."""
    stmt = pg_insert(League).values(
        league_id=GENERIC_LEAGUE_ID,
        platform="ffh",
        external_id="generic",
        season=0,
        name="Generic PPR",
        num_teams=12,
        league_type="redraft",
        is_superflex=False,
        scoring_settings=GENERIC_SCORING,
        roster_settings=GENERIC_ROSTER,
    )
    session.execute(stmt.on_conflict_do_nothing(index_elements=[League.league_id]))
    log.info("ingest.reference.generic_league_seeded", league_id=str(GENERIC_LEAGUE_ID))
    return GENERIC_LEAGUE_ID
