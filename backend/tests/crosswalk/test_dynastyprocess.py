"""apply_playerids on the 13-row sample. Rows 'Nobody Nowhere' (mfl 99900) and
'Kansas City Chiefs' DEF (mfl 99901) are fabricated; everything else is live DP data.

The ``dp_frame`` fixture lives in tests/crosswalk/conftest.py (reads
tests/fixtures/dynastyprocess/db_playerids_sample.csv via FIXTURE_DIR).
"""

import uuid

import polars as pl
import pytest
from sqlalchemy import func, select

from ffh.crosswalk.dynastyprocess import (
    DP_ID_COLUMNS,
    CrosswalkApplyReport,
    CrosswalkConflictError,
    DynastyProcessError,
    apply_playerids,
    read_playerids_csv,
)
from ffh.db.models import Player, PlayerExternalId

pytestmark = pytest.mark.db


def _count_ids(session) -> int:
    return session.scalar(select(func.count()).select_from(PlayerExternalId))


def _count_players(session) -> int:
    return session.scalar(select(func.count()).select_from(Player))


def test_read_csv_keeps_ids_as_text_and_na_as_null(dp_frame):
    for col in DP_ID_COLUMNS:
        assert dp_frame.schema[col] == pl.Utf8, col
    assert dp_frame.schema["gsis_id"] == pl.Utf8
    mahomes = dp_frame.filter(pl.col("mfl_id") == "13116").row(0, named=True)
    assert mahomes["yahoo_id"] == "30123" and mahomes["espn_id"] == "3139477"
    assert mahomes["sportradar_id"] == "11cad59d-90dd-449c-a839-dddaba4fe16c"
    pavia = dp_frame.filter(pl.col("mfl_id") == "17471").row(0, named=True)
    assert pavia["gsis_id"] is None and pavia["yahoo_id"] is None and pavia["pfr_id"] is None
    assert dp_frame.height == 13


def test_read_csv_rejects_missing_required_columns():
    with pytest.raises(DynastyProcessError, match="sleeper_id"):
        read_playerids_csv(b"mfl_id,gsis_id,name,position,team\n1,NA,x,QB,FA\n")


def test_apply_populates_external_ids(db_session, seeded_registry, dp_frame):
    report = apply_playerids(db_session, dp_frame)
    assert report == CrosswalkApplyReport(
        inserted=61,
        updated=0,
        unchanged=0,
        created_players=2,
        skipped_no_ids=1,
        skipped_position=1,
        skipped_dst=0,
        ambiguous=(("rotowire", "10167"), ("rotowire", "9898")),
    )
    assert _count_ids(db_session) == 61
    rows = db_session.scalars(select(PlayerExternalId)).all()
    assert all(r.confidence == 1.0 and r.match_method == "dynastyprocess" for r in rows)
    mahomes = seeded_registry["00-0033873"]
    by_key = {(r.source, r.external_id): r.player_id for r in rows}
    assert by_key[("sleeper", "4046")] == mahomes
    assert by_key[("espn", "3139477")] == mahomes
    assert by_key[("sportradar", "11cad59d-90dd-449c-a839-dddaba4fe16c")] == mahomes
    assert by_key[("pfr", "MoorD.00")] == seeded_registry["00-0034827"]
    # PK → K: Butker's ids land on the K registry row
    assert by_key[("sleeper", "4227")] == seeded_registry["00-0033303"]
    # DST row maps to the seeded 'kc dst' player
    assert by_key[("sleeper", "KC")] == seeded_registry["kc dst"]
    assert by_key[("espn", "-16012")] == seeded_registry["kc dst"]
    # Ambiguous rotowire ids were NOT written
    assert ("rotowire", "9898") not in by_key and ("rotowire", "10167") not in by_key


def test_apply_creates_rookie_player_without_gsis(db_session, seeded_registry, dp_frame):
    apply_playerids(db_session, dp_frame)
    pavia = db_session.scalar(select(Player).where(Player.normalized_name == "diego pavia"))
    assert pavia is not None
    assert pavia.gsis_id is None and pavia.position == "QB"
    assert pavia.full_name == "Diego Pavia"
    assert pavia.first_name == "Diego" and pavia.last_name == "Pavia"
    assert pavia.rookie_year == 2026 and pavia.college == "Vanderbilt"
    assert pavia.birth_date.isoformat() == "2002-02-16"
    assert pavia.team_abbr is None  # FA
    link = db_session.get(PlayerExternalId, ("sleeper", "13427"))
    assert link.player_id == pavia.player_id
    # The glitch pair shares a gsis → exactly one player, carrying that gsis
    fred = db_session.scalar(select(Player).where(Player.gsis_id == "00-0031320"))
    assert fred.full_name == "Fred Williams" and fred.team_abbr == "KC"
    assert db_session.get(PlayerExternalId, ("sleeper", "2295")).player_id == fred.player_id


def test_apply_is_idempotent(db_session, seeded_registry, dp_frame):
    first = apply_playerids(db_session, dp_frame)
    second = apply_playerids(db_session, dp_frame)
    assert second.inserted == 0 and second.created_players == 0 and second.updated == 0
    assert second.unchanged == first.inserted == 61
    assert second.ambiguous == first.ambiguous
    assert _count_ids(db_session) == 61
    assert _count_players(db_session) == 14 + 32 + 2


def test_apply_upgrades_lower_rung_row_for_same_player(db_session, seeded_registry, dp_frame):
    db_session.add(
        PlayerExternalId(
            player_id=seeded_registry["00-0033873"],
            source="sleeper",
            external_id="4046",
            confidence=0.95,
            match_method="exact_name",
        )
    )
    db_session.flush()
    report = apply_playerids(db_session, dp_frame)
    assert report.updated == 1 and report.inserted == 60
    row = db_session.get(PlayerExternalId, ("sleeper", "4046"))
    db_session.refresh(row)
    assert row.match_method == "dynastyprocess" and row.confidence == 1.0


def test_apply_raises_on_conflicting_existing_mapping(db_session, seeded_registry, dp_frame):
    # sleeper 4046 is Mahomes in DP; pre-point it at Chase.
    db_session.add(
        PlayerExternalId(
            player_id=seeded_registry["00-0036900"],
            source="sleeper",
            external_id="4046",
            confidence=1.0,
            match_method="manual",
        )
    )
    db_session.flush()
    players_before = _count_players(db_session)
    with pytest.raises(CrosswalkConflictError) as exc:
        apply_playerids(db_session, dp_frame)
    (src, ext, existing, incoming), *_ = exc.value.conflicts
    assert (src, ext) == ("sleeper", "4046")
    assert existing == seeded_registry["00-0036900"] and incoming == seeded_registry["00-0033873"]
    assert isinstance(existing, uuid.UUID)
    # Nothing was written: only the pre-existing manual row, and no placeholder players
    # were created before the conflict scan ("raises before any write" is literal).
    assert _count_ids(db_session) == 1
    assert _count_players(db_session) == players_before == 14 + 32


def test_apply_routes_second_id_per_source_and_player_to_ambiguous(
    db_session, seeded_registry, dp_frame
):
    # Mahomes already holds a *different* sleeper id. The unique index
    # player_external_ids_source_player_uidx forbids a second sleeper row for him, so
    # DP's sleeper 4046 must be reported ambiguous — never an IntegrityError.
    db_session.add(
        PlayerExternalId(
            player_id=seeded_registry["00-0033873"],
            source="sleeper",
            external_id="9999",
            confidence=1.0,
            match_method="manual",
        )
    )
    db_session.flush()
    report = apply_playerids(db_session, dp_frame)
    assert ("sleeper", "4046") in report.ambiguous
    assert report.inserted == 60
    assert db_session.get(PlayerExternalId, ("sleeper", "4046")) is None
    kept = db_session.get(PlayerExternalId, ("sleeper", "9999"))
    assert kept.player_id == seeded_registry["00-0033873"]
    assert kept.match_method == "manual"


def test_apply_never_creates_a_player_for_an_unmapped_dst_row(
    db_session, seeded_registry, dp_frame
):
    # Break the fabricated Chiefs DEF row so neither team nor name resolves to a
    # seeded DST player: the row must be counted in skipped_dst, not become a player.
    doctored = dp_frame.with_columns(
        pl.when(pl.col("mfl_id") == "99901")
        .then(pl.lit("London Monarchs"))
        .otherwise(pl.col("team"))
        .alias("team"),
        pl.when(pl.col("mfl_id") == "99901")
        .then(pl.lit("London Monarchs"))
        .otherwise(pl.col("name"))
        .alias("name"),
    )
    report = apply_playerids(db_session, doctored)
    assert report.skipped_dst == 1
    assert report.inserted == 59  # the two Chiefs DEF ids were skipped with the row
    assert report.created_players == 2  # Pavia + the glitch pair; never a DST placeholder
    assert _count_players(db_session) == 14 + 32 + 2
    assert db_session.get(PlayerExternalId, ("sleeper", "KC")) is None


def test_apply_rejects_frame_missing_columns(db_session, dp_frame):
    with pytest.raises(DynastyProcessError):
        apply_playerids(db_session, dp_frame.drop("position"))
