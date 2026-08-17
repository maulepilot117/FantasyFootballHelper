"""Shared crosswalk fixtures. Real nflverse values (verified 2026-08-16) except rows marked FAKE.

Sibling test packages (tests/adapters, tests/ingest) import the plain helpers
``build_players_frame`` / ``seed_fixture_registry`` directly — pytest scopes the
*fixtures* to this directory, but the helpers are importable from anywhere.
"""

import uuid
from pathlib import Path

import polars as pl
import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from ffh.db.models import Player

# backend/tests/fixtures/ — Task 4 adds dynastyprocess CSVs here.
FIXTURE_DIR = Path(__file__).resolve().parents[1] / "fixtures"

DP_SAMPLE_CSV = FIXTURE_DIR / "dynastyprocess" / "db_playerids_sample.csv"

# nflverse players.parquet column names and dtypes (birth_date is a String in the file).
# fmt: off
PLAYERS_ROWS: list[dict[str, object]] = [
    dict(gsis_id="00-0033873", display_name="Patrick Mahomes", first_name="Patrick", last_name="Mahomes", position="QB", birth_date="1995-09-17", rookie_season=2017, height=74, weight=225, college_name="Texas Tech", status="ACT", latest_team="KC"),
    dict(gsis_id="00-0036900", display_name="Ja'Marr Chase", first_name="Ja'Marr", last_name="Chase", position="WR", birth_date="2000-03-01", rookie_season=2021, height=72, weight=205, college_name="LSU", status="ACT", latest_team="CIN"),
    dict(gsis_id="00-0034827", display_name="DJ Moore", first_name="Denniston", last_name="Moore", position="WR", birth_date="1997-04-14", rookie_season=2018, height=72, weight=213, college_name="Maryland", status="ACT", latest_team="BUF"),
    dict(gsis_id="00-0038134", display_name="Kenneth Walker III", first_name="Kenneth", last_name="Walker", position="RB", birth_date="2000-10-20", rookie_season=2022, height=69, weight=211, college_name="Michigan State; Wake Forest", status="ACT", latest_team="KC"),
    dict(gsis_id="00-0033303", display_name="Harrison Butker", first_name="Harrison", last_name="Butker", position="K", birth_date="1995-07-14", rookie_season=2017, height=76, weight=205, college_name="Georgia Tech", status="ACT", latest_team="KC"),
    dict(gsis_id="00-0036963", display_name="Amon-Ra St. Brown", first_name="Amon-Ra", last_name="St. Brown", position="WR", birth_date="1999-10-24", rookie_season=2021, height=72, weight=202, college_name="USC", status="ACT", latest_team="DET"),
    dict(gsis_id="00-0039849", display_name="Marvin Harrison Jr.", first_name="Marvin", last_name="Harrison", position="WR", birth_date="2002-08-11", rookie_season=2024, height=75, weight=220, college_name="Ohio State", status="ACT", latest_team="ARI"),
    dict(gsis_id="00-0007024", display_name="Marvin Harrison", first_name="Marvin", last_name="Harrison", position="WR", birth_date="1972-08-25", rookie_season=1996, height=72, weight=185, college_name="Syracuse", status="ACT", latest_team="IND"),
    dict(gsis_id="00-0029892", display_name="Kyle Juszczyk", first_name="Kyle", last_name="Juszczyk", position="FB", birth_date="1991-04-23", rookie_season=2013, height=74, weight=235, college_name="Harvard", status="ACT", latest_team="SF"),
    dict(gsis_id="00-0032688", display_name="Robbie Chosen", first_name="Robert", last_name="Chosen", position="WR", birth_date="1993-05-09", rookie_season=2016, height=75, weight=185, college_name="Temple", status="DEV", latest_team="WAS"),
    dict(gsis_id="00-0034796", display_name="Lamar Jackson", first_name="Lamar", last_name="Jackson", position="QB", birth_date="1997-01-07", rookie_season=2018, height=74, weight=205, college_name="Louisville", status="ACT", latest_team="BAL"),
    dict(gsis_id="00-0036152", display_name="Lamar Jackson", first_name="Lamar", last_name="Jackson", position="CB", birth_date="1998-04-13", rookie_season=2020, height=74, weight=212, college_name="Nebraska", status="DEV", latest_team="ATL"),
    dict(gsis_id="00-0034857", display_name="Josh Allen", first_name="Joshua", last_name="Allen", position="QB", birth_date="1996-05-21", rookie_season=2018, height=77, weight=237, college_name="Wyoming; Reedley", status="ACT", latest_team="BUF"),
    dict(gsis_id="00-0030833", display_name="Josh Allen", first_name="Joshua", last_name="Allen", position="C", birth_date="1991-12-30", rookie_season=2014, height=75, weight=315, college_name="Louisiana-Monroe", status="DEV", latest_team="TB"),
    dict(gsis_id="00-0036613", display_name="Jaylen Waddle", first_name="Jaylen", last_name="Waddle", position="WR", birth_date="1998-11-25", rookie_season=2021, height=70, weight=185, college_name="Alabama", status="ACT", latest_team="DEN"),
    dict(gsis_id="00-0033869", display_name="Mitchell Trubisky", first_name="Mitchell", last_name="Trubisky", position="QB", birth_date="1994-08-20", rookie_season=2017, height=74, weight=222, college_name="North Carolina", status="ACT", latest_team="TEN"),
    # FAKE punter — proves non-fantasy positions are dropped and reported.
    dict(gsis_id="00-0000001", display_name="Test Punter", first_name="Test", last_name="Punter", position="P", birth_date="1990-01-01", rookie_season=2012, height=72, weight=200, college_name="Nowhere", status="ACT", latest_team="SEA"),
]
# fmt: on
FANTASY_ROW_COUNT = 14  # 17 rows minus CB, C, P


def build_players_frame() -> pl.DataFrame:
    """The 17-row nflverse-shaped fixture frame. Plain function for cross-package import."""
    return pl.DataFrame(
        PLAYERS_ROWS,
        schema={
            "gsis_id": pl.Utf8,
            "display_name": pl.Utf8,
            "first_name": pl.Utf8,
            "last_name": pl.Utf8,
            "position": pl.Utf8,
            "birth_date": pl.Utf8,
            "rookie_season": pl.Int32,
            "height": pl.Int32,
            "weight": pl.Int32,
            "college_name": pl.Utf8,
            "status": pl.Utf8,
            "latest_team": pl.Utf8,
        },
    )


def seed_fixture_registry(session: Session) -> dict[str, uuid.UUID]:
    """Seed the 14 fantasy players + 32 DSTs; return {gsis_id or 'kc dst': player_id}."""
    from ffh.crosswalk.registry import seed_players

    seed_players(session, build_players_frame())
    out: dict[str, uuid.UUID] = {}
    for pid, gsis, nn in session.execute(
        select(Player.player_id, Player.gsis_id, Player.normalized_name)
    ):
        out[gsis if gsis else nn] = pid
    return out


@pytest.fixture
def players_frame() -> pl.DataFrame:
    return build_players_frame()


@pytest.fixture
def seeded_registry(db_session: Session) -> dict[str, uuid.UUID]:
    return seed_fixture_registry(db_session)


@pytest.fixture
def dp_frame() -> pl.DataFrame:
    """The 13-row DynastyProcess sample parsed exactly as apply_playerids receives it."""
    from ffh.crosswalk.dynastyprocess import read_playerids_csv

    return read_playerids_csv(DP_SAMPLE_CSV.read_bytes())
