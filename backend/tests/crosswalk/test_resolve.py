"""Task 5 — the five-rung resolution ladder (DATABASE.md §3), strictly in order."""

import struct
from datetime import UTC, date, datetime

import pytest
import structlog.testing
from sqlalchemy import func, select

from ffh.crosswalk.resolve import (
    CONFIDENCE_EPSILON,
    EXACT_CONFIDENCE,
    FUZZY_CAP,
    USABLE_CONFIDENCE,
    Resolution,
    ResolveInput,
    is_usable,
    resolve,
    resolve_many,
    upsert_unmatched,
)
from ffh.db.models import CrosswalkUnmatched, Player, PlayerExternalId

pytestmark = pytest.mark.db


def _ids(session) -> list[PlayerExternalId]:
    return session.scalars(select(PlayerExternalId)).all()


def _unmatched(session) -> list[CrosswalkUnmatched]:
    return session.scalars(select(CrosswalkUnmatched)).all()


def _add_fake_player(session, name: str, position: str, team: str | None, gsis: str) -> Player:
    from ffh.crosswalk.normalize import normalize_name

    p = Player(
        gsis_id=gsis,
        full_name=name,
        normalized_name=normalize_name(name),
        position=position,
        team_abbr=team,
    )
    session.add(p)
    session.flush()
    return p


def test_is_usable_rule():
    assert is_usable(1.0, None) and is_usable(0.95, None) and is_usable(0.9, None)
    assert not is_usable(0.89, None)
    assert is_usable(0.89, datetime.now(UTC))


def test_is_usable_tolerates_float4_roundtrip():
    # player_external_ids.confidence is Postgres REAL: 0.9 comes back as float32(0.9).
    f32_09 = struct.unpack("f", struct.pack("f", 0.9))[0]
    assert f32_09 < USABLE_CONFIDENCE  # the raw comparison would wrongly fail …
    assert is_usable(f32_09, None)  # … and the epsilon absorbs it
    assert not is_usable(USABLE_CONFIDENCE - 2 * CONFIDENCE_EPSILON, None)


def test_rung1_existing_row_wins_over_everything(db_session, seeded_registry):
    mahomes = seeded_registry["00-0033873"]
    db_session.add(
        PlayerExternalId(
            player_id=mahomes,
            source="sleeper",
            external_id="4046",
            confidence=1.0,
            match_method="dynastyprocess",
        )
    )
    db_session.flush()
    # A misleading name/position/team must not matter once the id is known.
    res = resolve(db_session, "sleeper", "4046", "Some Other Name", "WR", "DEN")
    assert res == Resolution(mahomes, "dynastyprocess", 1.0)
    assert len(_ids(db_session)) == 1 and _unmatched(db_session) == []


def test_rung1_full_confidence_row_is_never_reconsulted(db_session, seeded_registry):
    mahomes, chase = seeded_registry["00-0033873"], seeded_registry["00-0036900"]
    db_session.add(
        PlayerExternalId(
            player_id=mahomes,
            source="sleeper",
            external_id="4046",
            confidence=1.0,
            match_method="dynastyprocess",
        )
    )
    db_session.flush()
    # Even a contradicting gsis_id does not reopen a confidence-1.0 mapping.
    res = resolve(db_session, "sleeper", "4046", gsis_id="00-0036900")
    assert res == Resolution(mahomes, "dynastyprocess", 1.0)
    row = db_session.get(PlayerExternalId, ("sleeper", "4046"))
    assert row.player_id == mahomes and row.player_id != chase


def test_rung1_low_confidence_upgraded_when_gsis_confirms_same_player(db_session, seeded_registry):
    mahomes = seeded_registry["00-0033873"]
    db_session.add(
        PlayerExternalId(
            player_id=mahomes,
            source="sleeper",
            external_id="4046",
            confidence=0.95,
            match_method="exact_name",
        )
    )
    db_session.flush()
    with structlog.testing.capture_logs() as logs:
        res = resolve(db_session, "sleeper", "4046", gsis_id="00-0033873")
    assert res == Resolution(mahomes, "gsis", 1.0)
    row = db_session.get(PlayerExternalId, ("sleeper", "4046"))
    assert row.player_id == mahomes and row.match_method == "gsis" and row.confidence == 1.0
    assert any(e["event"] == "crosswalk.resolve.upgraded" for e in logs)
    assert len(_ids(db_session)) == 1 and _unmatched(db_session) == []


def test_rung1_stale_low_confidence_row_corrected_when_gsis_points_elsewhere(
    db_session, seeded_registry
):
    mahomes, chase = seeded_registry["00-0033873"], seeded_registry["00-0036900"]
    db_session.add(
        PlayerExternalId(
            player_id=chase,  # stale guess: this sleeper id is actually Mahomes
            source="sleeper",
            external_id="4046",
            confidence=0.95,
            match_method="exact_name",
        )
    )
    db_session.flush()
    with structlog.testing.capture_logs() as logs:
        res = resolve(db_session, "sleeper", "4046", gsis_id="00-0033873")
    assert res == Resolution(mahomes, "gsis", 1.0)
    row = db_session.get(PlayerExternalId, ("sleeper", "4046"))
    assert row.player_id == mahomes and row.match_method == "gsis" and row.confidence == 1.0
    assert any(e["event"] == "crosswalk.resolve.upgraded" for e in logs)
    assert len(_ids(db_session)) == 1 and _unmatched(db_session) == []


def test_rung2_gsis_direct_and_persists_for_other_sources(db_session, seeded_registry):
    mahomes = seeded_registry["00-0033873"]
    assert resolve(db_session, "gsis", "00-0033873") == Resolution(mahomes, "gsis", 1.0)
    assert _ids(db_session) == []  # gsis lives on players; nothing to persist
    res = resolve(
        db_session, "sleeper", "4046", "Patrick Mahomes", "QB", "KC", gsis_id="00-0033873"
    )
    assert res == Resolution(mahomes, "gsis", 1.0)
    row = db_session.get(PlayerExternalId, ("sleeper", "4046"))
    assert row.player_id == mahomes and row.match_method == "gsis" and row.confidence == 1.0
    # next call is a rung-1 hit with the persisted method
    assert resolve(db_session, "sleeper", "4046") == Resolution(mahomes, "gsis", 1.0)


def test_rung2_persist_guard_incumbent_wins(db_session, seeded_registry):
    mahomes = seeded_registry["00-0033873"]
    db_session.add(
        PlayerExternalId(
            player_id=mahomes,
            source="sleeper",
            external_id="4046",
            confidence=1.0,
            match_method="dynastyprocess",
        )
    )
    db_session.flush()
    # A second sleeper id whose gsis points at Mahomes: the unique (source, player_id)
    # index forbids a second row — the incumbent wins, the new id goes to unmatched.
    with structlog.testing.capture_logs() as logs:
        res = resolve(
            db_session, "sleeper", "9999", "Pat Mahomes", "QB", "KC", gsis_id="00-0033873"
        )
    assert res is None
    assert db_session.get(PlayerExternalId, ("sleeper", "9999")) is None
    assert [(u.source, u.external_id) for u in _unmatched(db_session)] == [("sleeper", "9999")]
    assert any(
        e["event"] == "crosswalk.resolve.duplicate_for_source" and e["log_level"] == "warning"
        for e in logs
    )


def test_rung3_exact_name_persists_then_rung1(db_session, seeded_registry):
    mahomes = seeded_registry["00-0033873"]
    res = resolve(db_session, "sleeper", "4046", "Patrick Mahomes II", "QB", "KC")
    assert res == Resolution(mahomes, "exact_name", EXACT_CONFIDENCE)
    row = db_session.get(PlayerExternalId, ("sleeper", "4046"))
    assert row.match_method == "exact_name" and row.confidence == pytest.approx(0.95)
    assert row.verified_at is None  # ≥ 0.9 needs no verification
    again = resolve(db_session, "sleeper", "4046", "Patrick Mahomes II", "QB", "KC")
    # REAL round-trips 0.95 as float32 → compare with approx, not ==
    assert again is not None and again.player_id == mahomes and again.method == "exact_name"
    assert again.confidence == pytest.approx(EXACT_CONFIDENCE)
    assert len(_ids(db_session)) == 1


def test_ladder_order_rung3_beats_rung4(db_session, seeded_registry):
    """A name matching both an exact registry row and a near-identical fake resolves exact."""
    _add_fake_player(db_session, "DJ Moor", "WR", "BUF", "FAKE-DJ")
    res = resolve(db_session, "espn", "3915416", "D.J. Moore", "WR", "BUF")
    assert res is not None and res.method == "exact_name"
    assert res.player_id == seeded_registry["00-0034827"]


def test_rung3_team_disambiguates_same_name_same_position(db_session, seeded_registry):
    jr, sr = seeded_registry["00-0039849"], seeded_registry["00-0007024"]
    res = resolve(db_session, "sleeper", "11628", "Marvin Harrison Jr.", "WR", "ARI")
    assert res is not None and res.player_id == jr and res.method == "exact_name"
    res_sr = resolve(db_session, "pfr", "HarrMa00", "Marvin Harrison", "WR", "IND")
    assert res_sr is not None and res_sr.player_id == sr


def test_rung3_without_team_and_two_candidates_is_not_exact_and_fuzzy_ties(
    db_session, seeded_registry
):
    # Both Harrisons are 'marvin harrison' WR; no team → rung 3 ambiguous → rung 4 tie → unmatched
    with structlog.testing.capture_logs() as logs:
        res = resolve(db_session, "yahoo", "40893", "Marvin Harrison Jr.", "WR", None)
    assert res is None
    assert _ids(db_session) == []
    (u,) = _unmatched(db_session)
    assert (u.source, u.external_id, u.raw_name) == ("yahoo", "40893", "Marvin Harrison Jr.")
    # Rung 4 was actually reached and declared the tie (not skipped).
    assert any(e["event"] == "crosswalk.resolve.fuzzy_tie" for e in logs)


def test_rung3_team_mismatch_falls_to_fuzzy_pending(db_session, seeded_registry):
    # Only one 'patrick mahomes' QB, but registry says KC and caller says DEN → not exact.
    res = resolve(db_session, "sleeper", "4046", "Patrick Mahomes", "QB", "DEN")
    assert res is None
    row = db_session.get(PlayerExternalId, ("sleeper", "4046"))
    assert row.match_method == "fuzzy" and row.verified_at is None
    assert row.confidence == pytest.approx(FUZZY_CAP)  # similarity 1.0 capped at 0.89


def test_rung3_excludes_players_already_mapped_for_source(db_session, seeded_registry):
    mahomes = seeded_registry["00-0033873"]
    db_session.add(
        PlayerExternalId(
            player_id=mahomes,
            source="sleeper",
            external_id="4046",
            confidence=1.0,
            match_method="dynastyprocess",
        )
    )
    db_session.flush()
    # A second sleeper id claiming to be Mahomes must not attach: one id per source per player.
    res = resolve(db_session, "sleeper", "9999", "Patrick Mahomes", "QB", "KC")
    assert res is None
    assert db_session.get(PlayerExternalId, ("sleeper", "9999")) is None
    assert [(u.source, u.external_id) for u in _unmatched(db_session)] == [("sleeper", "9999")]


def test_rung4_fuzzy_persists_pending_and_returns_none_until_verified(db_session, seeded_registry):
    lamar = seeded_registry["00-0034796"]
    res = resolve(db_session, "sleeper", "4881", "Lamarr Jackson", "QB", "BAL")  # JW ≈ 0.9857
    assert res is None
    row = db_session.get(PlayerExternalId, ("sleeper", "4881"))
    assert row.player_id == lamar and row.match_method == "fuzzy"
    assert row.confidence == pytest.approx(FUZZY_CAP) and row.verified_at is None
    assert _unmatched(db_session) == []  # pending review is not "unmatched"
    # Second call: rung 1 sees the unverified row → still None, no duplicate, no re-guess
    assert resolve(db_session, "sleeper", "4881", "Lamarr Jackson", "QB", "BAL") is None
    assert len(_ids(db_session)) == 1
    # Human verifies → usable
    row.verified_at = datetime.now(UTC)
    db_session.flush()
    res = resolve(db_session, "sleeper", "4881", "Lamarr Jackson", "QB", "BAL")
    assert res is not None and res.player_id == lamar and res.method == "fuzzy"
    assert res.confidence == pytest.approx(FUZZY_CAP)


def test_rung4_tie_is_unmatched(db_session, seeded_registry):
    _add_fake_player(db_session, "Jaylon Waddle", "WR", "DEN", "FAKE-JW")
    # 'jaylin waddle' vs 'jaylen waddle' = 0.9692 and vs 'jaylon waddle' = 0.9692 → tie
    res = resolve(db_session, "sleeper", "7526", "Jaylin Waddle", "WR", "DEN")
    assert res is None
    assert _ids(db_session) == []
    (u,) = _unmatched(db_session)
    assert (u.source, u.external_id) == ("sleeper", "7526") and u.resolved is False


def test_rung4_birth_date_breaks_tie(db_session, seeded_registry):
    _add_fake_player(db_session, "Jaylon Waddle", "WR", "DEN", "FAKE-JW")
    res = resolve(
        db_session, "sleeper", "7526", "Jaylin Waddle", "WR", "DEN", birth_date=date(1998, 11, 25)
    )
    assert res is None  # still pending review …
    row = db_session.get(PlayerExternalId, ("sleeper", "7526"))
    assert row.player_id == seeded_registry["00-0036613"]  # … but pointed at the real Waddle
    assert _unmatched(db_session) == []


def test_rung4_college_breaks_tie(db_session, seeded_registry):
    _add_fake_player(db_session, "Jaylon Waddle", "WR", "DEN", "FAKE-JW")
    res = resolve(db_session, "sleeper", "7526", "Jaylin Waddle", "WR", "DEN", college="Alabama")
    assert res is None  # pending review, pointed at the real (Alabama) Waddle
    row = db_session.get(PlayerExternalId, ("sleeper", "7526"))
    assert row.player_id == seeded_registry["00-0036613"]
    assert _unmatched(db_session) == []


def test_dst_resolves_at_rung3_from_any_spelling(db_session, seeded_registry):
    kc = seeded_registry["kc dst"]
    a = resolve(db_session, "sleeper", "KC", "Kansas City Chiefs", "DEF", "KC")
    b = resolve(db_session, "espn", "-16012", "Chiefs D/ST", "D/ST", None)
    c = resolve(db_session, "yahoo", "100012", None, "DEF", "KC")  # name missing → team → 'kc dst'
    for r in (a, b, c):
        assert r is not None and r.player_id == kc and r.method == "exact_name"


def test_unmatched_created_then_bumped(db_session, seeded_registry):
    assert resolve(db_session, "sleeper", "99999", "Nobody Nowhere", "QB", "FA") is None
    (u,) = _unmatched(db_session)
    first_seen, last_seen = u.first_seen, u.last_seen
    assert u.resolved is False and u.raw_position == "QB" and u.raw_team == "FA"
    assert resolve(db_session, "sleeper", "99999", "Nobody Nowhere", "QB", "FA") is None
    db_session.refresh(u)
    assert len(_unmatched(db_session)) == 1
    assert u.first_seen == first_seen and u.last_seen > last_seen


def test_upsert_unmatched_helper_refreshes_raw_fields(db_session):
    # Task 7's review.py imports this exact helper — one writer for crosswalk_unmatched.
    upsert_unmatched(db_session, "sleeper", "42", raw_name="Foo")
    upsert_unmatched(db_session, "sleeper", "42", raw_name="Foo Bar", raw_team="KC")
    (u,) = _unmatched(db_session)
    assert (u.raw_name, u.raw_position, u.raw_team) == ("Foo Bar", None, "KC")
    assert u.resolved is False


def test_missing_name_and_position_goes_straight_to_unmatched(db_session, seeded_registry):
    assert resolve(db_session, "sleeper", "424242", None, None, None) is None
    assert [(u.source, u.external_id) for u in _unmatched(db_session)] == [("sleeper", "424242")]


def test_position_optional_gsis_resolves_and_no_gsis_lands_unmatched(db_session, seeded_registry):
    # Cross-PR contract: a sibling adapter can produce position=None.
    walker = seeded_registry["00-0038134"]
    n_players = db_session.scalar(select(func.count()).select_from(Player))
    rep = resolve_many(
        db_session,
        [ResolveInput("sleeper", "X1", "Kenneth Walker", None, None, gsis_id="00-0038134")],
    )
    assert rep.resolved[("sleeper", "X1")] == Resolution(walker, "gsis", 1.0)
    rep2 = resolve_many(db_session, [ResolveInput("sleeper", "X2", "Kenneth Walker", None, None)])
    assert rep2.unmatched == [("sleeper", "X2")]
    (u,) = [u for u in _unmatched(db_session) if u.external_id == "X2"]
    assert u.raw_position is None
    # No players row invented for the unresolvable id.
    assert db_session.scalar(select(func.count()).select_from(Player)) == n_players


def test_resolve_many_report(db_session, seeded_registry):
    rows = [
        ResolveInput("sleeper", "4046", "Patrick Mahomes", "QB", "KC"),  # exact
        ResolveInput("sleeper", "7564", "Ja'Marr Chase", "WR", "CIN"),  # exact
        ResolveInput("sleeper", "4881", "Lamarr Jackson", "QB", "BAL"),  # fuzzy pending
        ResolveInput("sleeper", "99999", "Nobody Nowhere", "QB", "FA"),  # unmatched
        ResolveInput("gsis", "00-0038134"),  # gsis
    ]
    rep = resolve_many(db_session, rows)
    assert set(rep.resolved) == {("sleeper", "4046"), ("sleeper", "7564"), ("gsis", "00-0038134")}
    assert rep.pending_review == [("sleeper", "4881")]
    assert rep.unmatched == [("sleeper", "99999")]
    assert rep.by_method == {"exact_name": 2, "gsis": 1, "fuzzy_pending": 1, "unmatched": 1}
    assert db_session.scalar(select(func.count()).select_from(CrosswalkUnmatched)) == 1
