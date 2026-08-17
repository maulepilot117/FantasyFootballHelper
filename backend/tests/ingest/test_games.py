from datetime import UTC, datetime

import polars as pl
import pytest
from sqlalchemy import select, text

from ffh.db.models import Game
from ffh.ingest.games import GAMES_CSV_URL, NfldataGamesJob, to_game_rows, upsert_games
from ffh.ingest.reference import seed_nfl_teams, seed_stadiums

pytestmark = pytest.mark.db

# Real rows copied verbatim from games.csv on 2026-08-16, including the quoted-empty roof.
GAMES_CSV = (
    "game_id,season,game_type,week,gameday,weekday,gametime,away_team,away_score,home_team,"
    "home_score,location,result,total,overtime,old_game_id,gsis,nfl_detail_id,pfr,pff,espn,"
    "ftn,away_rest,home_rest,away_moneyline,home_moneyline,spread_line,away_spread_odds,"
    "home_spread_odds,total_line,under_odds,over_odds,div_game,roof,surface,temp,wind,"
    "away_qb_id,home_qb_id,away_qb_name,home_qb_name,away_coach,home_coach,referee,stadium_id,"
    "stadium\n"
    "2026_01_NE_SEA,2026,REG,1,2026-09-09,Wednesday,20:20,NE,,SEA,,Home,,,,2026090900,,,"
    "202609090sea,,401872656,,7,7,154,-185,3.5,-110,-110,44.5,-110,-110,0,outdoors,fieldturf,"
    ",,,,,,Mike Vrabel,Mike Macdonald,,SEA00,Lumen Field\n"
    "2026_01_SF_LA,2026,REG,1,2026-09-10,Thursday,20:35,SF,,LA,,Neutral,,,,2026091000,,,"
    "202609100ram,,401872657,,7,7,160,-192,3.5,-110,-110,48.5,-112,-108,1,dome,matrixturf,"
    ",,,,,,Kyle Shanahan,Sean McVay,,LAX01,Melbourne Cricket Ground\n"
    "2026_01_BAL_IND,2026,REG,1,2026-09-13,Sunday,13:00,BAL,,IND,,Home,,,,2026091304,,,"
    '202609130clt,,401872659,,7,7,-175,145,-3.5,-108,-112,48.5,-110,-110,0,"",fieldturf,'
    ",,,,,,Jesse Minter,Shane Steichen,,IND00,Lucas Oil Stadium\n"
    "2025_01_DAL_PHI,2025,REG,1,2025-09-04,Thursday,20:20,DAL,20,PHI,24,Home,4,44,0,"
    "2025090400,,,202509040phi,,401772510,,7,7,330,-400,-8.5,-110,-110,47.5,-110,-110,1,"
    "outdoors,grass,72,6,,,,,Brian Schottenheimer,Nick Sirianni,,PHI00,Lincoln Financial Field\n"
    "2025_10_LV_DEN,2025,REG,10,2025-11-06,Thursday,20:15,LV,7,DEN,10,Home,3,17,0,2025110600,"
    "59978,,202511060den,28553,401772944,6869,4,4,380,-500,9.5,-112,-108,42.5,-110,-110,1,"
    "outdoors,grass,60,10,00-0030565,00-0039732,Geno Smith,Bo Nix,Pete Carroll,Sean Payton,"
    "Bill Vinovich,DEN00,Empower Field at Mile High\n"
)

STADIUMS_CSV = (
    "stadium_id,stadium_name,lat,lon,altitude,heading,surface_type,roof_type,tz\n"
    "SEA00,Lumen Field,47.5951513,-122.3316259,5.213504872,0,Turf,Outdoors,America/Los_Angeles\n"
    "LAX01,SoFi Stadium,33.9534635,-118.3392382,32.0,120,Turf,Dome,America/Los_Angeles\n"
    "IND00,Lucas Oil Stadium,39.7601008,-86.1638573,220.0,90,Turf,Dome,America/Indianapolis\n"
    "PHI00,Lincoln Financial Field,39.9008358,-75.1674627,4.0,20,Grass,Outdoors,America/New_York\n"
)


def _raw() -> pl.DataFrame:
    return NfldataGamesJob(season=2026).parse(GAMES_CSV.encode())


@pytest.fixture
def seeded(db_session):
    seed_nfl_teams(db_session)
    seed_stadiums(db_session, pl.read_csv(STADIUMS_CSV.encode()))
    db_session.flush()
    return db_session


def test_job_url_and_registration():
    assert GAMES_CSV_URL == (
        "https://raw.githubusercontent.com/nflverse/nfldata/master/data/games.csv"
    )
    assert NfldataGamesJob(season=2026).url() == GAMES_CSV_URL
    assert NfldataGamesJob.source == "nfldata" and NfldataGamesJob.asset == "games"
    assert NfldataGamesJob.seasonal is False and NfldataGamesJob.season_scoped is True


def test_parse_keeps_the_quoted_empty_roof_as_an_empty_string():
    raw = _raw()
    assert raw.filter(pl.col("game_id") == "2026_01_BAL_IND")["roof"].item() == ""
    assert raw["roof"].null_count() == 0


def test_kickoff_at_converts_eastern_wall_clock_to_utc():
    rows = to_game_rows(_raw(), 2026)
    opener = rows.filter(pl.col("game_id") == "2026_01_NE_SEA")["kickoff_at"].item()
    # 2026-09-09 20:20 ET (EDT, UTC-4) -> 2026-09-10 00:20 UTC
    assert opener == datetime(2026, 9, 10, 0, 20, tzinfo=UTC)
    # Melbourne game: games.csv still records the ET wall clock, so 20:35 ET -> 00:35 UTC.
    melbourne = rows.filter(pl.col("game_id") == "2026_01_SF_LA")["kickoff_at"].item()
    assert melbourne == datetime(2026, 9, 11, 0, 35, tzinfo=UTC)


def test_kickoff_at_uses_est_after_the_november_dst_change():
    rows = to_game_rows(_raw(), 2025)
    tnf = rows.filter(pl.col("game_id") == "2025_10_LV_DEN")["kickoff_at"].item()
    # 2025-11-06 20:15 ET is EST (UTC-5; DST ended 2025-11-02) -> 2025-11-07 01:15 UTC.
    # A fixed UTC-4 offset would yield 00:15 and fail here.
    assert tnf == datetime(2025, 11, 7, 1, 15, tzinfo=UTC)


def test_empty_roof_becomes_null_not_empty_string():
    rows = to_game_rows(_raw(), 2026)
    assert rows.filter(pl.col("game_id") == "2026_01_BAL_IND")["roof"].item() is None
    assert rows.filter(pl.col("game_id") == "2026_01_NE_SEA")["roof"].item() == "outdoors"


def test_neutral_site_comes_from_location():
    rows = to_game_rows(_raw(), 2026)
    neutral = dict(zip(rows["game_id"], rows["neutral_site"], strict=True))
    assert neutral["2026_01_SF_LA"] is True
    assert neutral["2026_01_NE_SEA"] is False


def test_season_filter_and_season_type_mapping():
    rows_2026 = to_game_rows(_raw(), 2026)
    assert rows_2026.height == 3
    assert set(rows_2026["season_type"]) == {"REG"}
    assert to_game_rows(_raw(), 2025).height == 2


def test_div_game_and_rest_and_lines_map_through():
    row = to_game_rows(_raw(), 2026).filter(pl.col("game_id") == "2026_01_SF_LA").to_dicts()[0]
    assert row["div_game"] is True
    assert row["home_rest"] == 7 and row["away_rest"] == 7
    assert row["spread_line"] == pytest.approx(3.5)
    assert row["total_line"] == pytest.approx(48.5)
    assert row["home_moneyline"] == -192 and row["away_moneyline"] == 160


def test_post_game_actuals_map_through_for_a_played_game():
    row = to_game_rows(_raw(), 2025).to_dicts()[0]
    assert (row["home_score"], row["away_score"]) == (24, 20)
    assert row["temp_f"] == pytest.approx(72.0) and row["wind_mph"] == pytest.approx(6.0)


def test_upsert_games_inserts_and_is_idempotent(seeded):
    assert upsert_games(seeded, _raw(), 2026) == 3
    seeded.flush()
    assert seeded.scalar(select(Game.game_id).where(Game.game_id == "2026_01_NE_SEA"))
    first = seeded.get(Game, "2026_01_NE_SEA").updated_at

    assert upsert_games(seeded, _raw(), 2026) == 3
    seeded.flush()
    assert len(list(seeded.scalars(select(Game)))) == 3
    seeded.expire_all()
    assert seeded.get(Game, "2026_01_NE_SEA").updated_at >= first


def test_upsert_games_bumps_updated_at_on_conflict(seeded):
    upsert_games(seeded, _raw(), 2026)
    seeded.flush()
    # Postgres now() is frozen at transaction start and this whole test is one transaction,
    # so backdate the row: the ON CONFLICT ... SET updated_at = now() must move it forward.
    seeded.execute(
        text("UPDATE games SET updated_at = now() - interval '1 day' WHERE game_id = :g"),
        {"g": "2026_01_BAL_IND"},
    )
    seeded.expire_all()
    before = seeded.get(Game, "2026_01_BAL_IND").updated_at

    changed = _raw().with_columns(
        pl.when(pl.col("game_id") == "2026_01_BAL_IND")
        .then(pl.lit(51.5))
        .otherwise(pl.col("total_line"))
        .alias("total_line")
    )
    upsert_games(seeded, changed, 2026)
    seeded.flush()
    seeded.expire_all()
    game = seeded.get(Game, "2026_01_BAL_IND")
    assert game.total_line == pytest.approx(51.5)
    assert game.updated_at > before


def test_upsert_games_raises_with_the_unmatched_stadium_list(db_session):
    seed_nfl_teams(db_session)
    seed_stadiums(
        db_session,
        pl.read_csv(
            b"stadium_id,stadium_name,lat,lon,altitude,heading,surface_type,roof_type,tz\n"
            b"SEA00,Lumen Field,47.6,-122.3,5.2,0,Turf,Outdoors,America/Los_Angeles\n"
        ),
    )
    db_session.flush()
    with pytest.raises(ValueError, match="IND00"):
        upsert_games(db_session, _raw(), 2026)


def test_upsert_games_rejects_a_season_with_no_rows(seeded):
    with pytest.raises(ValueError, match="no rows for season 1999"):
        upsert_games(seeded, _raw(), 1999)


def test_persist_upserts_for_the_jobs_season(seeded, monkeypatch):
    job = NfldataGamesJob(season=2026)
    job.persist(seeded, _raw())
    seeded.flush()
    assert len(list(seeded.scalars(select(Game)))) == 3


def test_upsert_games_rejects_a_blank_stadium_id(seeded):
    blanked = _raw().with_columns(
        pl.when(pl.col("game_id") == "2026_01_NE_SEA")
        .then(pl.lit(""))
        .otherwise(pl.col("stadium_id"))
        .alias("stadium_id")
    )
    with pytest.raises(ValueError, match="2026_01_NE_SEA"):
        upsert_games(seeded, blanked, 2026)
