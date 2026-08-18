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


def test_rung1_upgrade_conflict_routes_to_unmatched_not_stale_mapping(db_session, seeded_registry):
    # (sleeper, E1) → Mahomes @ exact_name 0.95 (stale guess); (sleeper, E2) → Chase @ 1.0.
    # A sync now says E1's gsis is Chase — the guess is proven wrong, but Chase already
    # holds a sleeper id. The contradicted mapping must NOT be returned.
    mahomes, chase = seeded_registry["00-0033873"], seeded_registry["00-0036900"]
    db_session.add(
        PlayerExternalId(
            player_id=mahomes,
            source="sleeper",
            external_id="E1",
            confidence=0.95,
            match_method="exact_name",
        )
    )
    db_session.add(
        PlayerExternalId(
            player_id=chase,
            source="sleeper",
            external_id="E2",
            confidence=1.0,
            match_method="dynastyprocess",
        )
    )
    db_session.flush()
    with structlog.testing.capture_logs() as logs:
        res = resolve(db_session, "sleeper", "E1", gsis_id="00-0036900")
    assert res is None  # never Resolution(mahomes, ...) — that mapping was just contradicted
    row = db_session.get(PlayerExternalId, ("sleeper", "E1"))
    # Disputed row stays in place for `ffh crosswalk verify --reject`.
    assert row.player_id == mahomes and row.match_method == "exact_name"
    assert [(u.source, u.external_id) for u in _unmatched(db_session)] == [("sleeper", "E1")]
    (conflict,) = [e for e in logs if e["event"] == "crosswalk.resolve.upgrade_conflict"]
    assert conflict["log_level"] == "warning"
    assert conflict["stored_player_id"] == str(mahomes)
    assert conflict["gsis_player_id"] == str(chase)


def test_rung1_verified_row_is_never_overwritten_by_gsis(db_session, seeded_registry):
    mahomes, chase = seeded_registry["00-0033873"], seeded_registry["00-0036900"]
    verified = datetime.now(UTC)
    db_session.add(
        PlayerExternalId(
            player_id=chase,
            source="sleeper",
            external_id="4046",
            confidence=0.89,
            match_method="fuzzy",
            verified_at=verified,
        )
    )
    db_session.flush()
    with structlog.testing.capture_logs() as logs:
        res = resolve(db_session, "sleeper", "4046", gsis_id="00-0033873")
    # The human-verified mapping stands, even against a contradicting gsis fact.
    assert res is not None and res.player_id == chase and res.method == "fuzzy"
    row = db_session.get(PlayerExternalId, ("sleeper", "4046"))
    assert row.player_id == chase and row.player_id != mahomes
    assert row.match_method == "fuzzy" and row.verified_at is not None
    assert any(e["event"] == "crosswalk.resolve.human_decision_conflict" for e in logs)
    assert not any(e["event"] == "crosswalk.resolve.upgraded" for e in logs)


def test_rung1_manual_row_is_never_overwritten_by_gsis(db_session, seeded_registry):
    mahomes, chase = seeded_registry["00-0033873"], seeded_registry["00-0036900"]
    db_session.add(
        PlayerExternalId(
            player_id=chase,
            source="sleeper",
            external_id="4046",
            confidence=0.95,
            match_method="manual",
        )
    )
    db_session.flush()
    with structlog.testing.capture_logs() as logs:
        res = resolve(db_session, "sleeper", "4046", gsis_id="00-0033873")
    assert res is not None and res.player_id == chase and res.method == "manual"
    row = db_session.get(PlayerExternalId, ("sleeper", "4046"))
    assert row.player_id == chase and row.player_id != mahomes and row.match_method == "manual"
    assert any(e["event"] == "crosswalk.resolve.human_decision_conflict" for e in logs)
    assert not any(e["event"] == "crosswalk.resolve.upgraded" for e in logs)


def test_rung4_contradicting_birth_date_eliminates_candidate(db_session, seeded_registry):
    # Lone fuzzy candidate (Lamar Jackson QB, born 1997-01-07) but the input says a
    # demonstrably different date → the candidate is ruled out, no fuzzy row persists,
    # and the id falls to rung 5 instead of becoming a rubber-stamp review suggestion.
    res = resolve(
        db_session, "sleeper", "4881", "Lamarr Jackson", "QB", "BAL", birth_date=date(1994, 1, 1)
    )
    assert res is None
    assert db_session.get(PlayerExternalId, ("sleeper", "4881")) is None
    assert [(u.source, u.external_id) for u in _unmatched(db_session)] == [("sleeper", "4881")]


def test_rung4_contradicting_college_eliminates_candidate(db_session, seeded_registry):
    # Same shape for the college leg: stored "Louisville" does not contain "Ohio State".
    res = resolve(
        db_session, "sleeper", "4881", "Lamarr Jackson", "QB", "BAL", college="Ohio State"
    )
    assert res is None
    assert db_session.get(PlayerExternalId, ("sleeper", "4881")) is None
    assert [(u.source, u.external_id) for u in _unmatched(db_session)] == [("sleeper", "4881")]


def test_rung4_whitespace_college_is_no_evidence(db_session, seeded_registry):
    # A whitespace-only college must not substring-confirm every non-NULL college (and
    # thereby drop the NULL-college fake): the Waddle tie must survive → unmatched.
    _add_fake_player(db_session, "Jaylon Waddle", "WR", "DEN", "FAKE-JW")
    res = resolve(db_session, "sleeper", "7526", "Jaylin Waddle", "WR", "DEN", college="   ")
    assert res is None
    assert _ids(db_session) == []
    (u,) = _unmatched(db_session)
    assert (u.source, u.external_id) == ("sleeper", "7526")


def test_resolve_many_processes_gsis_bearing_inputs_first(db_session, seeded_registry):
    # A rung-3 guess earlier in the batch must not steal the player from a rung-2
    # certainty later in the batch: gsis-bearing inputs are resolved first.
    allen = seeded_registry["00-0034857"]
    rows = [
        ResolveInput("sleeper", "A1", "Josh Allen", "QB", "BUF"),  # name guess, listed first
        ResolveInput("sleeper", "B1", "Joshua Allen", "QB", "BUF", gsis_id="00-0034857"),
    ]
    rep = resolve_many(db_session, rows)
    assert rep.resolved == {("sleeper", "B1"): Resolution(allen, "gsis", 1.0)}
    assert rep.unmatched == [("sleeper", "A1")]
    row = db_session.get(PlayerExternalId, ("sleeper", "B1"))
    assert row.player_id == allen and row.match_method == "gsis"
    assert db_session.get(PlayerExternalId, ("sleeper", "A1")) is None


# ---------------------------------------------------------------------------
# Fix wave: authority (a 1.0 fact beats an unverified incumbent), rejection
# tombstones, and diagnosable rung-5 reasons.
# ---------------------------------------------------------------------------


def test_rung2_gsis_fact_displaces_an_unverified_guess(db_session, seeded_registry):
    """A 1.0 gsis fact outranks an unverified 0.95 `exact_name` guess holding the slot.

    Before the fix the guess won by squatting: the gsis-certain id was routed to
    crosswalk_unmatched and the wrong guess stayed in use — the exact inversion of rung 1's
    upgrade path, which rules that a gsis fact beats a stale 0.95 row.
    """
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
    with structlog.testing.capture_logs() as logs:
        res = resolve(
            db_session, "sleeper", "4046", "Patrick Mahomes", "QB", "KC", gsis_id="00-0033873"
        )
    assert res == Resolution(mahomes, "gsis", 1.0)
    assert db_session.get(PlayerExternalId, ("sleeper", "GUESS")) is None
    row = db_session.get(PlayerExternalId, ("sleeper", "4046"))
    assert row.player_id == mahomes and row.match_method == "gsis"
    # The displaced id is genuinely unmapped now → it MUST be on the gate, not just in a log.
    assert [(u.source, u.external_id) for u in _unmatched(db_session)] == [("sleeper", "GUESS")]
    assert any(e["event"] == "crosswalk.resolve.incumbent_displaced" for e in logs)


@pytest.mark.parametrize(
    ("method", "confidence", "verified"),
    [("dynastyprocess", 1.0, False), ("manual", 1.0, False), ("fuzzy", 0.89, True)],
)
def test_rung2_gsis_fact_never_displaces_a_protected_incumbent(
    db_session, seeded_registry, method, confidence, verified
):
    """The other direction: human / DynastyProcess / verified rows hold their slot."""
    mahomes = seeded_registry["00-0033873"]
    db_session.add(
        PlayerExternalId(
            player_id=mahomes,
            source="sleeper",
            external_id="HOLDER",
            confidence=confidence,
            match_method=method,
            verified_at=datetime.now(UTC) if verified else None,
        )
    )
    db_session.flush()
    with structlog.testing.capture_logs() as logs:
        res = resolve(
            db_session, "sleeper", "4046", "Patrick Mahomes", "QB", "KC", gsis_id="00-0033873"
        )
    assert res is None
    holder = db_session.get(PlayerExternalId, ("sleeper", "HOLDER"))
    assert holder is not None and holder.match_method == method
    assert db_session.get(PlayerExternalId, ("sleeper", "4046")) is None
    assert [(u.source, u.external_id) for u in _unmatched(db_session)] == [("sleeper", "4046")]
    assert any(e["event"] == "crosswalk.resolve.duplicate_for_source" for e in logs)


def test_rung1_upgrade_displaces_an_unverified_incumbent(db_session, seeded_registry):
    """Same authority rule inside the rung-1 upgrade path: the correction proceeds by
    evicting the unverified guess instead of parking the corrected id as unmatched."""
    mahomes, chase = seeded_registry["00-0033873"], seeded_registry["00-0036900"]
    db_session.add(
        PlayerExternalId(
            player_id=mahomes,
            source="sleeper",
            external_id="E1",
            confidence=0.95,
            match_method="exact_name",
        )
    )
    db_session.add(
        PlayerExternalId(
            player_id=chase,
            source="sleeper",
            external_id="E2",
            confidence=0.89,
            match_method="fuzzy",
        )
    )
    db_session.flush()
    res = resolve(db_session, "sleeper", "E1", gsis_id="00-0036900")
    assert res == Resolution(chase, "gsis", 1.0)
    assert db_session.get(PlayerExternalId, ("sleeper", "E2")) is None
    assert db_session.get(PlayerExternalId, ("sleeper", "E1")).player_id == chase
    assert [(u.source, u.external_id) for u in _unmatched(db_session)] == [("sleeper", "E2")]


def test_human_decision_conflict_lands_on_the_gate(db_session, seeded_registry):
    """The lock stands (a sync never overwrites a human decision) — but the dispute is
    no longer log-only: the key is queued so `ffh crosswalk report` exits 1 on it."""
    from ffh.crosswalk.report import coverage_report

    mahomes, chase = seeded_registry["00-0033873"], seeded_registry["00-0036900"]
    db_session.add(
        PlayerExternalId(
            player_id=chase,
            source="sleeper",
            external_id="4046",
            confidence=0.95,
            match_method="manual",
        )
    )
    db_session.flush()
    with structlog.testing.capture_logs() as logs:
        res = resolve(db_session, "sleeper", "4046", gsis_id="00-0033873")
    assert res is not None and res.player_id == chase  # the human decision still stands
    assert db_session.get(PlayerExternalId, ("sleeper", "4046")).player_id == chase
    assert any(e["event"] == "crosswalk.resolve.human_decision_conflict" for e in logs)
    (u,) = _unmatched(db_session)
    assert (u.source, u.external_id) == ("sleeper", "4046") and u.resolved is False
    assert coverage_report(db_session).ok is False
    assert mahomes != chase


def test_confirming_gsis_on_a_human_row_is_not_a_conflict(db_session, seeded_registry):
    """A gsis id that AGREES with the human decision must not put it on the gate."""
    mahomes = seeded_registry["00-0033873"]
    db_session.add(
        PlayerExternalId(
            player_id=mahomes,
            source="sleeper",
            external_id="4046",
            confidence=0.95,
            match_method="manual",
        )
    )
    db_session.flush()
    res = resolve(db_session, "sleeper", "4046", gsis_id="00-0033873")
    assert res is not None and res.player_id == mahomes
    assert _unmatched(db_session) == []


def test_rejected_pairing_is_never_reminted_and_stays_on_the_gate(db_session, seeded_registry):
    """Finding 4, the full cycle: mint → reject → re-run the SAME resolve.

    Rung 3 would otherwise re-match the same wrong player at 0.95 and `close_unmatched`
    would turn the gate GREEN on a mapping a human explicitly rejected.
    """
    from ffh.crosswalk.report import coverage_report
    from ffh.crosswalk.review import reject_mapping

    # A wrong-but-exact guess: this sleeper id is minted against the elder Marvin Harrison.
    sr = seeded_registry["00-0007024"]
    minted = resolve(db_session, "sleeper", "MH", "Marvin Harrison", "WR", "IND")
    assert minted is not None and minted.player_id == sr and minted.method == "exact_name"

    assert reject_mapping(db_session, "sleeper", "MH") is True
    tomb = db_session.get(PlayerExternalId, ("sleeper", "MH"))
    assert tomb.match_method == "rejected" and tomb.player_id == sr

    with structlog.testing.capture_logs() as logs:
        again = resolve(db_session, "sleeper", "MH", "Marvin Harrison", "WR", "IND")
    assert again is None  # never re-minted, never returned
    row = db_session.get(PlayerExternalId, ("sleeper", "MH"))
    assert row.match_method == "rejected" and row.confidence == 0.0
    assert any(e.get("reason") == "rejected" for e in logs)
    (u,) = _unmatched(db_session)
    assert (u.source, u.external_id) == ("sleeper", "MH") and u.resolved is False
    assert coverage_report(db_session).ok is False


def test_tombstone_is_never_usable_even_if_verified(db_session, seeded_registry):
    """`is_usable` keys on the method first: a stamped `verified_at` must not resurrect
    a rejected pairing (and `verify_mapping` refuses to stamp one in the first place)."""
    from ffh.crosswalk.review import reject_mapping, verify_mapping

    assert is_usable(0.0, None, "rejected") is False
    assert is_usable(1.0, datetime.now(UTC), "rejected") is False
    assert is_usable(0.89, datetime.now(UTC), "fuzzy") is True

    resolve(db_session, "sleeper", "4046", "Patrick Mahomes", "QB", "KC")
    reject_mapping(db_session, "sleeper", "4046")
    assert verify_mapping(db_session, "sleeper", "4046") is False
    row = db_session.get(PlayerExternalId, ("sleeper", "4046"))
    row.verified_at = datetime.now(UTC)  # force the state verify_mapping refuses to create
    db_session.flush()
    assert resolve(db_session, "sleeper", "4046", "Patrick Mahomes", "QB", "KC") is None


def test_tombstone_does_not_block_the_correct_id_for_that_player(db_session, seeded_registry):
    """A tombstone is not a mapping: it must not squat the player's one slot for the
    source, or rejecting a wrong id would keep the RIGHT id unmappable forever."""
    from ffh.crosswalk.review import reject_mapping

    mahomes = seeded_registry["00-0033873"]
    resolve(db_session, "sleeper", "WRONG", "Patrick Mahomes", "QB", "KC")
    assert reject_mapping(db_session, "sleeper", "WRONG") is True
    res = resolve(db_session, "sleeper", "4046", "Patrick Mahomes", "QB", "KC")
    assert res is not None and res.player_id == mahomes and res.method == "exact_name"


def test_persist_refuses_to_remint_a_rejected_pairing_directly(db_session, seeded_registry):
    """Defence in depth for `_persist` itself (rung 1 already refuses tombstoned keys):
    the same pairing is refused, a DIFFERENT player is the correction we want."""
    from ffh.crosswalk.resolve import _persist
    from ffh.crosswalk.review import reject_mapping

    mahomes, chase = seeded_registry["00-0033873"], seeded_registry["00-0036900"]
    resolve(db_session, "sleeper", "X", "Patrick Mahomes", "QB", "KC")
    reject_mapping(db_session, "sleeper", "X")
    inp = ResolveInput("sleeper", "X", "Patrick Mahomes", "QB", "KC")
    assert _persist(db_session, inp, mahomes, "gsis", 1.0) is False
    assert db_session.get(PlayerExternalId, ("sleeper", "X")).match_method == "rejected"
    assert _persist(db_session, inp, chase, "gsis", 1.0) is True
    row = db_session.get(PlayerExternalId, ("sleeper", "X"))
    assert row.player_id == chase and row.match_method == "gsis" and row.verified_at is None


def test_college_elimination_is_symmetric(db_session, seeded_registry):
    """`"Ohio St."` vs stored `"Ohio State"` is agreement, not contradiction — the old
    asymmetric `needle in stored` test eliminated the correct candidate."""
    _add_fake_player(db_session, "Marvon Harrison", "WR", "ARI", "FAKE-MH")
    res = resolve(db_session, "sleeper", "11628", "Marvin Harrisom", "WR", None, college="Ohio St.")
    assert res is None  # rung-4 pending review …
    row = db_session.get(PlayerExternalId, ("sleeper", "11628"))
    assert row is not None and row.match_method == "fuzzy"
    assert row.player_id == seeded_registry["00-0039849"]  # … pointed at the Ohio State one


def test_college_agreement_rule_is_symmetric_but_not_permissive():
    """The symmetric rule must not become "everything agrees": two different schools
    sharing only a generic token still disagree."""
    from ffh.crosswalk.resolve import colleges_agree

    assert colleges_agree("Ohio St.", "Ohio State") is True
    assert colleges_agree("Ohio State", "Ohio St.") is True
    assert colleges_agree("Michigan State", "Michigan State; Wake Forest") is True
    assert colleges_agree("Michigan State", "Ohio State") is False
    assert colleges_agree("Louisville", "Ohio State") is False


def test_rung5_reasons_distinguish_elimination_from_no_match(db_session, seeded_registry):
    """A crosswalk miss caused by a contradicting birth date logged the same
    `reason="no_candidate"` as "no name matched at all" — undiagnosable."""
    with structlog.testing.capture_logs() as logs:
        assert resolve(db_session, "sleeper", "99999", "Nobody Nowhere", "QB", "FA") is None
    assert [e["reason"] for e in logs if e["event"] == "crosswalk.resolve.unmatched"] == [
        "no_candidate"
    ]
    with structlog.testing.capture_logs() as logs:
        assert (
            resolve(
                db_session,
                "sleeper",
                "4881",
                "Lamarr Jackson",
                "QB",
                "BAL",
                birth_date=date(1994, 1, 1),
            )
            is None
        )
    assert [e["reason"] for e in logs if e["event"] == "crosswalk.resolve.unmatched"] == [
        "fuzzy_eliminated"
    ]
    assert any(
        e["event"] == "crosswalk.resolve.fuzzy_eliminated" and e["eliminated_by"] == ["birth_date"]
        for e in logs
    )
    _add_fake_player(db_session, "Jaylon Waddle", "WR", "DEN", "FAKE-JW")
    with structlog.testing.capture_logs() as logs:
        assert resolve(db_session, "sleeper", "7526", "Jaylin Waddle", "WR", "DEN") is None
    assert [e["reason"] for e in logs if e["event"] == "crosswalk.resolve.unmatched"] == [
        "fuzzy_tie"
    ]
