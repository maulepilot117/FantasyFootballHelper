import uuid

import polars as pl
import pytest
from sqlalchemy import select

from ffh.db.models import GENERIC_LEAGUE_ID, League, NflTeam, Stadium
from ffh.ingest.reference import (
    GENERIC_ROSTER,
    GENERIC_SCORING,
    STADIUMS_CSV_URL,
    StadiumsJob,
    assert_stadium_coverage,
    assert_team_coverage,
    load_nfl_teams,
    seed_generic_league,
    seed_nfl_teams,
    seed_stadiums,
)

pytestmark = pytest.mark.db

# Verified rows from greerreNFL/stadiums on 2026-08-16 (altitude is in METRES).
STADIUMS_CSV = (
    "stadium_id,stadium_name,lat,lon,altitude,heading,surface_type,roof_type,tz\n"
    "DEN00,Empower Field at Mile High,39.7439402,-105.0201065,1583.586238,0,Grass,Outdoors,"
    "America/Denver\n"
    "SEA00,Lumen Field,47.5951513,-122.3316259,5.213504872,0,Turf,Outdoors,"
    "America/Los_Angeles\n"
    "PHO00,State Farm Stadium,33.5277555,-112.2625948,325.7411644,328,Grass,Dome,"
    "America/Phoenix\n"
)


def _stadiums() -> pl.DataFrame:
    return pl.read_csv(STADIUMS_CSV.encode())


def test_packaged_nfl_teams_csv_has_32_unique_rows():
    df = load_nfl_teams()
    assert df.height == 32
    assert df["team_abbr"].n_unique() == 32
    assert df["espn_id"].n_unique() == 32


def test_nfl_teams_uses_nflverse_abbreviations():
    abbrs = set(load_nfl_teams()["team_abbr"])
    assert {"LA", "LAC", "LV", "WAS", "JAX", "GB", "NO", "SF", "TB", "KC", "NE"} <= abbrs
    assert "LAR" not in abbrs and "WSH" not in abbrs and "OAK" not in abbrs and "SD" not in abbrs


def test_nfl_teams_has_four_teams_in_every_division():
    df = load_nfl_teams()
    counts = df.group_by(["conference", "division"]).len().sort(["conference", "division"])
    assert counts.height == 8
    assert set(counts["len"].to_list()) == {4}


def test_seed_nfl_teams_is_idempotent(db_session):
    assert seed_nfl_teams(db_session) == 32
    db_session.flush()
    assert seed_nfl_teams(db_session) == 32
    db_session.flush()
    assert len(list(db_session.scalars(select(NflTeam)))) == 32
    rams = db_session.get(NflTeam, "LA")
    assert (rams.espn_id, rams.full_name, rams.conference, rams.division) == (
        14,
        "Los Angeles Rams",
        "NFC",
        "West",
    )
    assert rams.bye_week is None  # Phase 0 deviation: derived from `games` at query time


def test_stadiums_job_url_and_partition():
    assert STADIUMS_CSV_URL == (
        "https://raw.githubusercontent.com/greerreNFL/stadiums/main/data/stadiums.csv"
    )
    assert StadiumsJob().url() == STADIUMS_CSV_URL
    assert set(StadiumsJob().partition()) == {"scrape_date"}
    assert StadiumsJob.seasonal is False


def test_seed_stadiums_converts_altitude_metres_to_feet(db_session):
    assert seed_stadiums(db_session, _stadiums()) == 3
    db_session.flush()
    denver = db_session.get(Stadium, "DEN00")
    # 1583.586238 m * 3.280839895 = 5195.5 ft -> 5195 (Mile High really is ~5,280 ft)
    assert denver.altitude_ft == 5195
    assert db_session.get(Stadium, "SEA00").altitude_ft == 17


def test_seed_stadiums_maps_names_and_is_idempotent(db_session):
    seed_stadiums(db_session, _stadiums())
    db_session.flush()
    assert seed_stadiums(db_session, _stadiums()) == 3
    db_session.flush()
    assert len(list(db_session.scalars(select(Stadium)))) == 3
    sea = db_session.get(Stadium, "SEA00")
    assert sea.name == "Lumen Field"
    assert sea.tz == "America/Los_Angeles"
    assert sea.roof_type == "Outdoors" and sea.surface_type == "Turf"
    assert sea.heading_deg == pytest.approx(0.0)
    assert sea.latitude == pytest.approx(47.5951513)


def test_seed_generic_league_matches_database_md_section_6(db_session):
    league_id = seed_generic_league(db_session)
    db_session.flush()
    assert league_id == GENERIC_LEAGUE_ID == uuid.UUID("00000000-0000-0000-0000-000000000000")
    league = db_session.get(League, GENERIC_LEAGUE_ID)
    assert league.platform == "ffh"
    assert league.external_id == "generic"
    assert league.season == 0
    assert league.name == "Generic PPR"
    assert league.num_teams == 12
    assert league.league_type == "redraft"
    assert league.is_superflex is False
    assert league.scoring_settings == GENERIC_SCORING
    assert league.roster_settings == GENERIC_ROSTER
    assert league.playoff_teams is None
    assert league.playoff_start_wk is None
    assert league.faab_budget is None
    assert league.my_team_id is None


def test_generic_scoring_is_canonical_full_ppr():
    assert GENERIC_SCORING == {
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
    assert GENERIC_ROSTER == {
        "QB": 1,
        "RB": 2,
        "WR": 2,
        "TE": 1,
        "FLEX": 1,
        "K": 1,
        "DST": 1,
        "BN": 6,
    }


def test_seed_generic_league_is_idempotent(db_session):
    seed_generic_league(db_session)
    db_session.flush()
    seed_generic_league(db_session)
    db_session.flush()
    assert len(list(db_session.scalars(select(League)))) == 1


def test_assert_team_coverage_names_the_missing_abbreviations(db_session):
    seed_nfl_teams(db_session)
    db_session.flush()
    assert_team_coverage(db_session, {"KC", "LA", "WAS"})
    with pytest.raises(ValueError, match="LAR"):
        assert_team_coverage(db_session, {"KC", "LAR"})


def test_assert_stadium_coverage_passes_when_every_id_matches(db_session):
    seed_stadiums(db_session, _stadiums())
    db_session.flush()
    rows = pl.DataFrame({"game_id": ["a", "b"], "stadium_id": ["DEN00", "SEA00"]})
    assert_stadium_coverage(db_session, rows)


def test_assert_stadium_coverage_raises_with_the_unmatched_list(db_session):
    seed_stadiums(db_session, _stadiums())
    db_session.flush()
    rows = pl.DataFrame({"game_id": ["a", "b"], "stadium_id": ["DEN00", "NOPE1"]})
    with pytest.raises(ValueError) as excinfo:
        assert_stadium_coverage(db_session, rows)
    assert "NOPE1" in str(excinfo.value)
    assert "DEN00" not in str(excinfo.value)
