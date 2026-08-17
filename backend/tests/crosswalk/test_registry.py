import polars as pl
import pytest
from sqlalchemy import func, select

from ffh.crosswalk.registry import (
    PLAYERS_REQUIRED_COLUMNS,
    RegistryError,
    iter_gsis_to_player_id,
    prepare_players_frame,
    seed_dst_players,
    seed_players,
)
from ffh.db.models import Player
from tests.crosswalk.conftest import FANTASY_ROW_COUNT

# NOTE: no module-level pytest.mark.db — the prepare_* tests are pure and must run
# without Postgres. Only the seed/iter tests below carry the marker.


def test_prepare_filters_positions_and_reports_dropped(players_frame):
    frame, dropped = prepare_players_frame(players_frame)
    assert frame.height == FANTASY_ROW_COUNT
    assert dropped == {"CB": 1, "C": 1, "P": 1}
    assert frame.height + sum(dropped.values()) == players_frame.height
    row = frame.filter(pl.col("gsis_id") == "00-0029892").row(0, named=True)
    assert row["position"] == "RB"  # FB → RB
    assert row["full_name"] == "Kyle Juszczyk" and row["team_abbr"] == "SF"
    walker = frame.filter(pl.col("gsis_id") == "00-0038134").row(0, named=True)
    assert walker["normalized_name"] == "kenneth walker"
    assert walker["birth_date"].isoformat() == "2000-10-20"
    assert (
        walker["rookie_year"] == 2022 and walker["height_in"] == 69 and walker["weight_lb"] == 211
    )
    assert set(frame.columns) == {
        "gsis_id", "full_name", "first_name", "last_name", "normalized_name", "position",
        "birth_date", "rookie_year", "height_in", "weight_lb", "college", "status", "team_abbr",
    }  # fmt: skip


def test_prepare_raises_on_missing_columns(players_frame):
    with pytest.raises(RegistryError, match="latest_team"):
        prepare_players_frame(players_frame.drop("latest_team"))
    assert "latest_team" in PLAYERS_REQUIRED_COLUMNS


def test_prepare_raises_on_duplicate_gsis(players_frame):
    dup = pl.concat([players_frame, players_frame.head(1)])
    with pytest.raises(RegistryError, match="duplicate gsis_id"):
        prepare_players_frame(dup)


def test_prepare_raises_on_null_gsis(players_frame):
    bad = players_frame.with_columns(
        pl.when(pl.col("display_name") == "Patrick Mahomes")
        .then(None)
        .otherwise(pl.col("gsis_id"))
        .alias("gsis_id")
    )
    with pytest.raises(RegistryError, match="null gsis_id"):
        prepare_players_frame(bad)


@pytest.mark.db
def test_seed_players_is_idempotent(db_session, players_frame):
    n1 = seed_players(db_session, players_frame)
    count1 = db_session.scalar(select(func.count()).select_from(Player))
    n2 = seed_players(db_session, players_frame)
    count2 = db_session.scalar(select(func.count()).select_from(Player))
    assert n1 == n2 == FANTASY_ROW_COUNT + 32
    assert count1 == count2 == FANTASY_ROW_COUNT + 32


@pytest.mark.db
def test_seed_players_updates_changed_fields(db_session, players_frame):
    seed_players(db_session, players_frame)
    moved = players_frame.with_columns(
        pl.when(pl.col("gsis_id") == "00-0033873")
        .then(pl.lit("DEN"))
        .otherwise(pl.col("latest_team"))
        .alias("latest_team"),
        pl.when(pl.col("gsis_id") == "00-0033873")
        .then(pl.lit("RET"))
        .otherwise(pl.col("status"))
        .alias("status"),
    )
    seed_players(db_session, moved)
    p = db_session.scalar(select(Player).where(Player.gsis_id == "00-0033873"))
    assert p.team_abbr == "DEN" and p.status == "RET"


@pytest.mark.db
def test_seed_creates_exactly_32_dst_rows(db_session, players_frame):
    seed_players(db_session, players_frame)
    dst = db_session.scalars(select(Player).where(Player.position == "DST")).all()
    assert len(dst) == 32
    kc = next(p for p in dst if p.normalized_name == "kc dst")
    assert kc.gsis_id is None and kc.team_abbr == "KC"
    assert kc.full_name == "Kansas City Chiefs DST"
    assert kc.first_name == "Kansas City" and kc.last_name == "Chiefs"
    assert seed_dst_players(db_session) == 0  # nothing missing on a second call
    assert (
        db_session.scalar(select(func.count()).select_from(Player).where(Player.position == "DST"))
        == 32
    )


@pytest.mark.db
def test_iter_gsis_to_player_id(db_session, players_frame):
    seed_players(db_session, players_frame)
    pairs = dict(iter_gsis_to_player_id(db_session))
    # Every fantasy row with a gsis_id appears; the 32 DST rows (gsis NULL) do not.
    assert len(pairs) == FANTASY_ROW_COUNT
    assert all(isinstance(g, str) for g in pairs)
    expected = db_session.scalar(select(Player.player_id).where(Player.gsis_id == "00-0033873"))
    assert pairs["00-0033873"] == expected
