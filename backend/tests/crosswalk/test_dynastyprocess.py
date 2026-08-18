"""apply_playerids on the 13-row sample. Rows 'Nobody Nowhere' (mfl 99900) and
'Kansas City Chiefs' DEF (mfl 99901) are fabricated; everything else is live DP data.

The ``dp_frame`` fixture lives in tests/crosswalk/conftest.py (reads
tests/fixtures/dynastyprocess/db_playerids_sample.csv via FIXTURE_DIR).
"""

import uuid
from datetime import UTC, datetime

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
from ffh.crosswalk.report import coverage_report
from ffh.crosswalk.review import reject_mapping
from ffh.db.models import CrosswalkUnmatched, Player, PlayerExternalId
from tests.crosswalk.conftest import DP_SAMPLE_CSV

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


def test_read_csv_tolerates_utf8_bom():
    bom = bytes([0xEF, 0xBB, 0xBF])
    df = read_playerids_csv(bom + DP_SAMPLE_CSV.read_bytes())
    assert df.height == 13 and df.columns[0] == "mfl_id"


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
        skipped_no_person_key=0,
        ambiguous_in_file=(("rotowire", "10167"), ("rotowire", "9898")),
        blocked_by_existing=(),
        blocked_by_rejection=(),
        displaced=(),
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
    assert second.ambiguous_in_file == first.ambiguous_in_file
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
    # `blocked_by_existing`, not the file-ambiguity bucket: an operator must be able to
    # tell "DP contradicts itself" from "a DB row blocked DP".
    assert report.blocked_by_existing == (("sleeper", "4046"),)
    assert ("sleeper", "4046") not in report.ambiguous_in_file
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


def test_apply_coerces_integral_float_ids_without_mangling(db_session, seeded_registry, dp_frame):
    # A Parquet round-trip can type a numeric id column as Float64. Integral floats
    # must come back as clean digit strings — never "30123.0".
    floaty = dp_frame.with_columns(pl.col("yahoo_id").cast(pl.Float64))
    report = apply_playerids(db_session, floaty)
    assert report.inserted == 61
    mahomes = seeded_registry["00-0033873"]
    assert db_session.get(PlayerExternalId, ("yahoo", "30123")).player_id == mahomes
    assert db_session.get(PlayerExternalId, ("yahoo", "30123.0")) is None


def test_apply_rejects_non_integral_float_ids(db_session, dp_frame):
    bad = dp_frame.with_columns((pl.col("yahoo_id").cast(pl.Float64) + 0.5).alias("yahoo_id"))
    with pytest.raises(DynastyProcessError, match="yahoo_id"):
        apply_playerids(db_session, bad)


def test_apply_rejects_frame_missing_columns(db_session, dp_frame):
    with pytest.raises(DynastyProcessError):
        apply_playerids(db_session, dp_frame.drop("position"))


# ---------------------------------------------------------------------------
# Fix wave: every id DP refuses to write reaches crosswalk_unmatched, the 1.0
# authority rule, rejection tombstones, and the "mfl:None" placeholder collapse.
# ---------------------------------------------------------------------------


def _open_queue(session) -> list[tuple[str, str]]:
    return [
        (u.source, u.external_id)
        for u in session.scalars(
            select(CrosswalkUnmatched)
            .where(CrosswalkUnmatched.resolved.is_(False))
            .order_by(CrosswalkUnmatched.source, CrosswalkUnmatched.external_id)
        )
    ]


def test_apply_queues_file_ambiguous_ids_with_their_raw_context(
    db_session, seeded_registry, dp_frame
):
    """An ambiguity the gate cannot see is an unmapped fantasy id that leaves
    `ffh crosswalk report` exiting 0 (Global Constraint / DATABASE.md §3 rung 5)."""
    report = apply_playerids(db_session, dp_frame)
    assert report.ambiguous_in_file == (("rotowire", "10167"), ("rotowire", "9898"))
    assert _open_queue(db_session) == [("rotowire", "10167"), ("rotowire", "9898")]
    u = db_session.scalar(
        select(CrosswalkUnmatched).where(CrosswalkUnmatched.external_id == "9898")
    )
    assert (u.raw_name, u.raw_position) == ("Fred Williams", "WR")
    assert coverage_report(db_session).ok is False


def test_apply_queues_ids_blocked_by_an_existing_row(db_session, seeded_registry, dp_frame):
    """The DB-blocked bucket is queued too — and is reported separately from the
    file-ambiguity bucket so an operator can tell the two causes apart."""
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
    assert report.blocked_by_existing == (("sleeper", "4046"),)
    assert ("sleeper", "4046") in _open_queue(db_session)
    u = db_session.scalar(
        select(CrosswalkUnmatched).where(CrosswalkUnmatched.external_id == "4046")
    )
    assert (u.raw_name, u.raw_team) == ("Patrick Mahomes", "KCC")
    assert coverage_report(db_session).ok is False


def test_apply_displaces_an_unverified_guess_but_not_a_human_row(
    db_session, seeded_registry, dp_frame
):
    """Authority rule: DP's 1.0 fact beats an unverified `exact_name` 0.95 incumbent
    (which used to win merely by occupying the slot); the displaced id goes on the gate."""
    mahomes = seeded_registry["00-0033873"]
    db_session.add(
        PlayerExternalId(
            player_id=mahomes,
            source="sleeper",
            external_id="GUESS",
            confidence=0.95,
            match_method="exact_name",
        )
    )
    db_session.flush()
    report = apply_playerids(db_session, dp_frame)
    assert report.displaced == (("sleeper", "GUESS"),)
    assert report.blocked_by_existing == ()
    assert db_session.get(PlayerExternalId, ("sleeper", "GUESS")) is None
    assert db_session.get(PlayerExternalId, ("sleeper", "4046")).player_id == mahomes
    assert ("sleeper", "GUESS") in _open_queue(db_session)


def test_apply_does_not_displace_a_verified_low_confidence_row(
    db_session, seeded_registry, dp_frame
):
    mahomes = seeded_registry["00-0033873"]
    db_session.add(
        PlayerExternalId(
            player_id=mahomes,
            source="sleeper",
            external_id="GUESS",
            confidence=0.89,
            match_method="fuzzy",
            verified_at=datetime.now(UTC),
        )
    )
    db_session.flush()
    report = apply_playerids(db_session, dp_frame)
    assert report.displaced == ()
    assert report.blocked_by_existing == (("sleeper", "4046"),)
    assert db_session.get(PlayerExternalId, ("sleeper", "GUESS")) is not None


def test_apply_never_remints_a_rejected_pairing(db_session, seeded_registry, dp_frame):
    """The DP path must honour the tombstone too — otherwise the next
    `ffh crosswalk seed` silently re-creates the mapping a human rejected."""
    apply_playerids(db_session, dp_frame)
    assert reject_mapping(db_session, "sleeper", "4046") is True

    report = apply_playerids(db_session, dp_frame)
    assert report.blocked_by_rejection == (("sleeper", "4046"),)
    row = db_session.get(PlayerExternalId, ("sleeper", "4046"))
    assert row.match_method == "rejected" and row.confidence == 0.0
    assert ("sleeper", "4046") in _open_queue(db_session)
    assert coverage_report(db_session).ok is False


def test_apply_rejects_rows_with_neither_gsis_nor_mfl_id(db_session, seeded_registry, dp_frame):
    """`mfl:None` was a real placeholder key: every gsis-less, mfl-less row collapsed onto
    it, so ONE invented `players` row accumulated ids from several different people. The
    ambiguity pass catches the overlapping case, never this disjoint one."""
    ghosts = pl.DataFrame(
        [
            {
                "mfl_id": None,
                "sportradar_id": None,
                "fantasypros_id": None,
                "gsis_id": None,
                "sleeper_id": "G1",
                "espn_id": None,
                "yahoo_id": None,
                "pfr_id": None,
                "rotowire_id": None,
                "name": "Ghost One",
                "merge_name": "ghost one",
                "position": "WR",
                "team": "FA",
                "birthdate": "1999-01-01",
                "draft_year": 2024,
                "college": "Nowhere",
            },
            {
                "mfl_id": None,
                "sportradar_id": None,
                "fantasypros_id": None,
                "gsis_id": None,
                "sleeper_id": None,
                "espn_id": "G2",
                "yahoo_id": None,
                "pfr_id": None,
                "rotowire_id": None,
                "name": "Ghost Two",
                "merge_name": "ghost two",
                "position": "RB",
                "team": "FA",
                "birthdate": "1998-01-01",
                "draft_year": 2023,
                "college": "Elsewhere",
            },
        ],
        schema=dp_frame.schema,
    )
    report = apply_playerids(db_session, pl.concat([dp_frame, ghosts]))
    assert report.skipped_no_person_key == 2
    # Same counts as the clean frame: the two disjoint ghosts never became one player.
    assert report.inserted == 61 and report.created_players == 2
    assert db_session.get(PlayerExternalId, ("sleeper", "G1")) is None
    assert db_session.get(PlayerExternalId, ("espn", "G2")) is None
    assert (
        db_session.scalar(
            select(func.count())
            .select_from(Player)
            .where(Player.full_name.in_(["Ghost One", "Ghost Two"]))
        )
        == 0
    )


def test_rejecting_a_rookie_id_does_not_mint_a_duplicate_player_on_the_next_seed(
    db_session, seeded_registry, dp_frame
):
    """Diego Pavia has no gsis_id: his `players` row exists only because DP created it,
    and the next seed re-finds him through his already-mapped ids. A tombstone must keep
    supplying that identity — otherwise the row takes the placeholder path again and mints
    a SECOND Diego Pavia (silent duplicate players), which is worse than the wrong id."""
    apply_playerids(db_session, dp_frame)
    pavia = db_session.scalar(select(Player).where(Player.normalized_name == "diego pavia"))
    assert reject_mapping(db_session, "sleeper", "13427") is True

    report = apply_playerids(db_session, dp_frame)
    assert report.created_players == 0
    assert (
        db_session.scalar(
            select(func.count()).select_from(Player).where(Player.normalized_name == "diego pavia")
        )
        == 1
    )
    # The rejected id stays rejected and on the gate; his other ids still point at him.
    assert report.blocked_by_rejection == (("sleeper", "13427"),)
    assert db_session.get(PlayerExternalId, ("sleeper", "13427")).match_method == "rejected"
    assert db_session.get(PlayerExternalId, ("espn", "5084180")).player_id == pavia.player_id
    assert ("sleeper", "13427") in _open_queue(db_session)
