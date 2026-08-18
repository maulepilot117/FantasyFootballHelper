"""Task 5 — the five-rung resolution ladder (DATABASE.md §3), strictly in order."""

import struct
from contextlib import contextmanager
from datetime import UTC, date, datetime

import pytest
import structlog.testing
from sqlalchemy import event, func, select

from ffh.crosswalk.resolve import (
    AUTHORITATIVE_METHODS,
    CONFIDENCE_EPSILON,
    DP_METHOD,
    EXACT_CONFIDENCE,
    EXACT_METHOD,
    FUZZY_CAP,
    FUZZY_METHOD,
    GSIS_METHOD,
    MANUAL_METHOD,
    MATCH_METHODS,
    PROTECTED_METHODS,
    REJECTED_METHOD,
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
    assert is_usable(1.0, None, GSIS_METHOD)
    assert is_usable(0.95, None, EXACT_METHOD)
    assert is_usable(0.9, None, MANUAL_METHOD)
    assert not is_usable(0.89, None, FUZZY_METHOD)
    assert is_usable(0.89, datetime.now(UTC), FUZZY_METHOD)


def test_is_usable_requires_the_match_method():
    """`player_external_ids.match_method` is NOT NULL, so no legitimate caller can be
    missing it — and a default made every 2-arg call skip the tombstone check silently,
    which is the pre-tombstone rule DATABASE.md §3 now explicitly calls wrong."""
    import inspect

    param = inspect.signature(is_usable).parameters["match_method"]
    assert param.default is inspect.Parameter.empty
    with pytest.raises(TypeError):
        is_usable(1.0, None)  # type: ignore[call-arg]


def test_is_usable_tolerates_float4_roundtrip():
    # player_external_ids.confidence is Postgres REAL: 0.9 comes back as float32(0.9).
    f32_09 = struct.unpack("f", struct.pack("f", 0.9))[0]
    assert f32_09 < USABLE_CONFIDENCE  # the raw comparison would wrongly fail …
    assert is_usable(f32_09, None, MANUAL_METHOD)  # … and the epsilon absorbs it
    assert not is_usable(USABLE_CONFIDENCE - 2 * CONFIDENCE_EPSILON, None, FUZZY_METHOD)


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


def test_rung4_college_does_not_break_a_tie_against_an_unknown_college(db_session, seeded_registry):
    """The college leg no longer *evicts* candidates whose stored college is NULL — NULL is
    no evidence against them. With two equally-similar names and only one college on file
    the evidence is genuinely inconclusive, so the tie stands and the id goes to the queue
    rather than becoming a fuzzy suggestion built on an eviction."""
    _add_fake_player(db_session, "Jaylon Waddle", "WR", "DEN", "FAKE-JW")  # college NULL
    res = resolve(db_session, "sleeper", "7526", "Jaylin Waddle", "WR", "DEN", college="Alabama")
    assert res is None
    assert db_session.get(PlayerExternalId, ("sleeper", "7526")) is None
    assert [(u.source, u.external_id) for u in _unmatched(db_session)] == [("sleeper", "7526")]


def test_rung4_college_confirmation_never_evicts_the_better_null_college_candidate(
    db_session, seeded_registry
):
    """`if confirmed: survivors = confirmed` handed the id to a *lower*-similarity
    candidate whose college happened to be on file, dropping the exact-name candidate
    purely because its college is unknown. NULL is not a contradiction."""
    fake = _add_fake_player(db_session, "Jaylin Waddel", "WR", "DEN", "FAKE-JW")  # college NULL
    assert db_session.get(Player, seeded_registry["00-0036613"]).college == "Alabama"
    # 'jaylin waddle' matches the fake at 0.9846 and the real Waddle at 0.9692 — outside
    # the 0.01 tie margin — but only the real one confirms the supplied college.
    res = resolve(db_session, "sleeper", "7526", "Jaylin Waddle", "WR", "DEN", college="Alabama")
    assert res is None  # rung 4 is always pending review …
    row = db_session.get(PlayerExternalId, ("sleeper", "7526"))
    assert row is not None and row.match_method == "fuzzy"
    assert row.player_id == fake.player_id  # … pointed at the better name match, not evicted
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
    no longer log-only: the key is queued so `ffh crosswalk report` exits 1 on it.

    The manual row is built with `map_mapping`, i.e. the ONLY thing that writes one:
    `manual`, confidence 1.0, verified. A hand-written 0.95 `manual` row is not a state
    the system can reach, and gating the contradiction check on `confidence < 1.0` made
    this gate unreachable for every real human decision.
    """
    from ffh.crosswalk.report import coverage_report
    from ffh.crosswalk.review import map_mapping

    mahomes, chase = seeded_registry["00-0033873"], seeded_registry["00-0036900"]
    assert map_mapping(db_session, "sleeper", "4046", chase).ok is True
    row = db_session.get(PlayerExternalId, ("sleeper", "4046"))
    assert float(row.confidence) == 1.0 and row.verified_at is not None
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
    from ffh.crosswalk.review import map_mapping

    mahomes = seeded_registry["00-0033873"]
    assert map_mapping(db_session, "sleeper", "4046", mahomes).ok is True
    res = resolve(db_session, "sleeper", "4046", gsis_id="00-0033873")
    assert res is not None and res.player_id == mahomes and res.method == "manual"
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


def test_displacement_preserves_the_queued_raw_context(db_session, seeded_registry):
    """`upsert_unmatched` refreshes raw_* by design, so the displacement path must carry
    the queued context forward — the same regression `reject_mapping` avoids
    (test_reject_preserves_queue_raw_fields). Blanking it leaves the operator with a bare
    `sleeper:GUESS` and no way to tell who it was."""
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
    # An earlier sync already described this id in the queue (then it got mapped).
    upsert_unmatched(
        db_session, "sleeper", "GUESS", raw_name="Pat Mahomes", raw_position="QB", raw_team="KC"
    )

    res = resolve(
        db_session, "sleeper", "4046", "Patrick Mahomes", "QB", "KC", gsis_id="00-0033873"
    )
    assert res == Resolution(mahomes, "gsis", 1.0)
    (u,) = [u for u in _unmatched(db_session) if u.external_id == "GUESS"]
    assert (u.raw_name, u.raw_position, u.raw_team) == ("Pat Mahomes", "QB", "KC")
    assert u.resolved is False


# ---------------------------------------------------------------------------
# Fix wave A: gate integrity, queue lifecycle, ladder ordering.
# ---------------------------------------------------------------------------


def test_source_gsis_rung2_hit_closes_its_queue_entry(db_session):
    """Rung 2 deliberately skips `_persist` for `source == "gsis"` (the row would
    duplicate `players.gsis_id`) — but `close_unmatched` lived inside `_persist`, so an id
    queued before the player was seeded resolved at 1.0 and stayed on the gate forever."""
    from ffh.crosswalk.report import coverage_report
    from tests.crosswalk.conftest import seed_fixture_registry

    # Rung 5 first: the player does not exist yet, so the gsis id is queued.
    assert resolve(db_session, "gsis", "00-0033873", "Patrick Mahomes", "QB", "KC") is None
    (u,) = _unmatched(db_session)
    assert (u.source, u.external_id) == ("gsis", "00-0033873") and u.resolved is False

    ids = seed_fixture_registry(db_session)
    # …and a second source so the crosswalk is not "empty" (report.CoverageReport.seeded).
    assert resolve(db_session, "sleeper", "4046", "Patrick Mahomes", "QB", "KC") is not None

    with structlog.testing.capture_logs() as logs:
        res = resolve(db_session, "gsis", "00-0033873", "Patrick Mahomes", "QB", "KC")
    assert res == Resolution(ids["00-0033873"], "gsis", 1.0)
    assert db_session.get(PlayerExternalId, ("gsis", "00-0033873")) is None  # still no row
    db_session.refresh(u)
    assert u.resolved is True
    assert any(e["event"] == "crosswalk.resolve.unmatched_closed" for e in logs)
    assert coverage_report(db_session).ok is True


def test_resolve_many_is_order_independent_a_fuzzy_never_steals_an_exact_players_slot(
    db_session, seeded_registry
):
    """Rungs 3-4 persist and both exclude players already mapped for the source, so a
    rung-4 *guess* listed first claimed the player a later rung-3 *exact* wanted — and the
    exact match was pushed onto the review queue. The ladder now runs in priority passes:
    every rung-1/2/3 claim settles batch-wide before any fuzzy guess is written."""
    lamar = seeded_registry["00-0034796"]
    rows = [
        ResolveInput("sleeper", "FUZZY", "Lamarr Jackson", "QB", "BAL"),  # JW ≈ 0.9857, first
        ResolveInput("sleeper", "EXACT", "Lamar Jackson", "QB", "BAL"),  # the real thing
    ]
    rep = resolve_many(db_session, rows)
    assert rep.resolved[("sleeper", "EXACT")] == Resolution(
        lamar, "exact_name", pytest.approx(EXACT_CONFIDENCE)
    )
    assert ("sleeper", "FUZZY") not in rep.resolved
    exact_row = db_session.get(PlayerExternalId, ("sleeper", "EXACT"))
    assert exact_row.player_id == lamar and exact_row.match_method == "exact_name"
    # The guess never took the slot, and its id is on the gate rather than silently gone.
    assert db_session.get(PlayerExternalId, ("sleeper", "FUZZY")) is None
    assert [(u.source, u.external_id) for u in _unmatched(db_session)] == [("sleeper", "FUZZY")]
    # Single-id resolve() is unchanged: one input has no batch to be ordered against.
    assert resolve(db_session, "espn", "F2", "Lamarr Jackson", "QB", "BAL") is None
    assert db_session.get(PlayerExternalId, ("espn", "F2")).match_method == "fuzzy"


def test_rung3_refuses_a_homonym_the_existing_mapping_exclusion_made_unique(
    db_session, seeded_registry
):
    """Two Marvin Harrisons, both WR. One already holds a sleeper id, so the "already
    mapped for this source" exclusion silently reduces the candidate set to one and mints
    a *usable* 0.95 on the other. The exclusion is a no-duplicate mechanism, not evidence:
    the id must go red instead."""
    jr, sr = seeded_registry["00-0039849"], seeded_registry["00-0007024"]
    db_session.add(
        PlayerExternalId(
            player_id=jr,
            source="sleeper",
            external_id="11628",
            confidence=1.0,
            match_method="dynastyprocess",
        )
    )
    db_session.flush()
    with structlog.testing.capture_logs() as logs:
        # No team supplied, so nothing distinguishes the two 'marvin harrison' WRs.
        res = resolve(db_session, "sleeper", "MH-2", "Marvin Harrison", "WR", None)
    assert res is None
    assert db_session.get(PlayerExternalId, ("sleeper", "MH-2")) is None  # no 0.95 on the elder
    assert [(u.source, u.external_id) for u in _unmatched(db_session)] == [("sleeper", "MH-2")]
    assert [e["reason"] for e in logs if e["event"] == "crosswalk.resolve.unmatched"] == [
        "homonym_blocked_by_existing_mapping"
    ]
    assert sr != jr
    # A team still disambiguates: that is real evidence, not an artefact of the exclusion.
    res_sr = resolve(db_session, "sleeper", "MH-3", "Marvin Harrison", "WR", "IND")
    assert res_sr is not None and res_sr.player_id == sr and res_sr.method == "exact_name"


def test_resolve_unmatched_refuses_while_a_contradicted_mapping_is_still_live(
    db_session, seeded_registry
):
    """The `upgrade_conflict` state parks the key in BOTH tables on purpose: the sub-1.0
    row stays live and usable while `resolve` refuses to return it. Closing the queue row
    greens the gate on exactly that — the command silences the only signal while every
    consumer keeps getting the contradicted mapping."""
    from ffh.crosswalk.report import coverage_report
    from ffh.crosswalk.review import mark_unmatched_resolved

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
    assert resolve(db_session, "sleeper", "E1", gsis_id="00-0036900") is None
    assert db_session.get(PlayerExternalId, ("sleeper", "E1")).player_id == mahomes

    with structlog.testing.capture_logs() as logs:
        assert mark_unmatched_resolved(db_session, "sleeper", "E1") is False
    assert any(e["event"] == "crosswalk.review.unmatched_resolve_refused" for e in logs)
    assert coverage_report(db_session).ok is False

    # `--force` is the escape hatch for the case the command is for; and rejecting the
    # disputed row (making the id genuinely unmapped) unblocks the normal path too.
    assert mark_unmatched_resolved(db_session, "sleeper", "E1", force=True) is True
    assert coverage_report(db_session).ok is True


def test_resolve_unmatched_still_closes_an_id_with_no_mapping_or_only_a_tombstone(
    db_session, seeded_registry
):
    """The guard must not break the command's actual job: a rung-5 id (no row at all) and
    a rejected id (a tombstone is not a mapping) both still close."""
    from ffh.crosswalk.review import mark_unmatched_resolved, reject_mapping

    assert resolve(db_session, "sleeper", "99999", "Nobody Nowhere", "QB", "FA") is None
    assert mark_unmatched_resolved(db_session, "sleeper", "99999") is True

    resolve(db_session, "sleeper", "4881", "Lamarr Jackson", "QB", "BAL")  # fuzzy pending
    assert reject_mapping(db_session, "sleeper", "4881") is True
    assert mark_unmatched_resolved(db_session, "sleeper", "4881") is True


# ---------------------------------------------------------------------------
# Fix wave B: rung 4's candidate pool is loaded once per batch, not once per input.
# ---------------------------------------------------------------------------

#: The rung-4 pool query and nothing else: `_exact` selects (player_id, team_abbr), and the
#: registry upserts name every column.
_FUZZY_POOL_SQL = "players.birth_date, players.college"


@contextmanager
def _count_statements(session, needle: str):
    seen: list[str] = []

    def _hook(conn, cursor, statement, parameters, context, executemany):
        if needle in " ".join(statement.split()):
            seen.append(statement)

    bind = session.get_bind()
    event.listen(bind, "before_cursor_execute", _hook)
    try:
        yield seen
    finally:
        event.remove(bind, "before_cursor_execute", _hook)


def test_resolve_many_loads_the_fuzzy_candidate_pool_once_per_source_and_position(
    db_session, seeded_registry
):
    """Rung 4 re-selected every unmapped same-position player and rebuilt the rapidfuzz
    choice map on EVERY call — and `resolve_many`'s two passes call it for every input that
    misses rungs 1-3, so a batch of a few hundred rostered players ran that query a few
    hundred times over a table that barely moves within the batch.

    Five QB inputs, one `(source, position)` key: one query.
    """
    rows = [
        ResolveInput("sleeper", "F1", "Lamarr Jackson", "QB", "BAL"),  # a real fuzzy hit
        ResolveInput("sleeper", "M1", "Zzz Aaa Oneee", "QB", "BAL"),
        ResolveInput("sleeper", "M2", "Zzz Bbb Twooo", "QB", "BAL"),
        ResolveInput("sleeper", "M3", "Zzz Ccc Threee", "QB", "BAL"),
        ResolveInput("sleeper", "M4", "Zzz Ddd Fourrr", "QB", "BAL"),
    ]
    with _count_statements(db_session, _FUZZY_POOL_SQL) as statements:
        rep = resolve_many(db_session, rows)

    assert len(statements) == 1, statements
    # …and the outcomes are exactly what the per-call query produced.
    assert rep.pending_review == [("sleeper", "F1")]
    assert sorted(rep.unmatched) == [("sleeper", f"M{i}") for i in range(1, 5)]
    assert db_session.get(PlayerExternalId, ("sleeper", "F1")).match_method == "fuzzy"


def test_the_cached_pool_drops_a_player_as_soon_as_the_batch_claims_him(
    db_session, seeded_registry
):
    """The cache must behave exactly like the reload it replaces: rung 4 excludes players
    already mapped for the source, so a player claimed earlier in the batch has to leave
    the pool. Two near-identical spellings of one QB — the second must NOT get him."""
    lamar = seeded_registry["00-0034796"]
    rows = [
        ResolveInput("sleeper", "F1", "Lamarr Jackson", "QB", "BAL"),
        ResolveInput("sleeper", "F2", "Lamar Jacksonn", "QB", "BAL"),
    ]
    rep = resolve_many(db_session, rows)

    claimed = [
        (r.source, r.external_id)
        for r in _ids(db_session)
        if r.player_id == lamar and r.match_method == "fuzzy"
    ]
    assert len(claimed) == 1, claimed
    assert len(rep.pending_review) == 1
    assert len(rep.unmatched) == 1
    # The loser is on the gate, not silently dropped.
    assert {(u.source, u.external_id) for u in _unmatched(db_session)} == set(rep.unmatched)


def test_single_resolve_still_queries_per_call(db_session, seeded_registry):
    """`resolve()` has no batch to amortize over and keeps today's path — the cache is a
    `resolve_many` concern and must not leak into the single-id API's behaviour."""
    with _count_statements(db_session, _FUZZY_POOL_SQL) as statements:
        assert resolve(db_session, "sleeper", "F1", "Lamarr Jackson", "QB", "BAL") is None
        assert resolve(db_session, "espn", "F2", "Lamarr Jackson", "QB", "BAL") is None
    assert len(statements) == 2, statements


# ---------------------------------------------------------------------------
# Fix wave C: the `match_method` vocabulary and the tombstone-exclusion clause each
# live in exactly ONE place. This component's recurring defect is a constant created
# to hold a rule and then bypassed by a re-typed literal three modules away.
# ---------------------------------------------------------------------------

#: The `ffh.crosswalk` modules that consume the vocabulary but must never define it.
_VOCABULARY_CONSUMERS = ("dynastyprocess", "review", "report")


def _module_source(name: str) -> str:
    import importlib
    import pathlib

    return pathlib.Path(importlib.import_module(f"ffh.crosswalk.{name}").__file__).read_text(
        encoding="utf-8"
    )


def test_match_method_vocabulary_values_are_pinned():
    """The six strings that may appear in `player_external_ids.match_method`.

    Pinned literally: these values are written to a table, compared in raw SQL (the partial
    unique index, report.py's queries, the DATABASE.md §3 consumer rule) and read back by
    every consumer, so a rename is a data migration — not a refactor. If this assertion
    fails, the change needs a migration and a docs pass, not a new expected value here.
    """
    assert (
        GSIS_METHOD,
        DP_METHOD,
        MANUAL_METHOD,
        EXACT_METHOD,
        FUZZY_METHOD,
        REJECTED_METHOD,
    ) == ("gsis", "dynastyprocess", "manual", "exact_name", "fuzzy", "rejected")
    assert MATCH_METHODS == {
        GSIS_METHOD,
        DP_METHOD,
        MANUAL_METHOD,
        EXACT_METHOD,
        FUZZY_METHOD,
        REJECTED_METHOD,
    }
    # The two derived sets are BUILT from those names, never re-typed as literals.
    assert AUTHORITATIVE_METHODS == {GSIS_METHOD, DP_METHOD, MANUAL_METHOD}
    assert PROTECTED_METHODS == {DP_METHOD, MANUAL_METHOD, REJECTED_METHOD}
    assert AUTHORITATIVE_METHODS < MATCH_METHODS and PROTECTED_METHODS < MATCH_METHODS
    # …and the rulings actually read them.
    from ffh.crosswalk.resolve import is_authoritative, is_displaceable

    for method in AUTHORITATIVE_METHODS:
        assert is_authoritative(method, 1.0) is True
    assert is_authoritative(EXACT_METHOD, 1.0) is False
    for method in PROTECTED_METHODS:
        assert is_displaceable(method, None) is False
    assert is_displaceable(EXACT_METHOD, None) is True


def test_no_crosswalk_module_redefines_a_match_method_literal():
    """`dynastyprocess.DP_METHOD` and `review.MANUAL_METHOD` used to be module-level
    literals in modules that already *import* `resolve` — three copies of one vocabulary.
    Python interns short strings, so an identity check proves nothing; read the source.

    Scoped to module-level bindings, so the DynastyProcess *ingest job*'s unrelated
    `source: ClassVar[str] = "dynastyprocess"` (a lake source name, not a match method)
    is not a false positive.
    """
    import ast

    offenders: list[tuple[str, str, str]] = []
    for name in _VOCABULARY_CONSUMERS:
        tree = ast.parse(_module_source(name))
        for node in tree.body:  # module level only
            targets = (
                node.targets
                if isinstance(node, ast.Assign)
                else [node.target]
                if isinstance(node, ast.AnnAssign)
                else []
            )
            value = getattr(node, "value", None)
            if not targets or not isinstance(value, ast.Constant):
                continue
            if isinstance(value.value, str) and value.value in MATCH_METHODS:
                bound = getattr(targets[0], "id", "?")
                offenders.append((name, bound, value.value))
    assert offenders == [], f"{offenders} — import the constant from ffh.crosswalk.resolve instead"


def test_the_tombstone_clause_is_written_once():
    """`match_method != REJECTED_METHOD` as a SQL clause was copy-pasted at seven query
    sites across four modules. It is now `resolve.LIVE_MAPPING`; the raw form may appear
    exactly once, on the line that defines it."""
    from ffh.crosswalk.resolve import LIVE_MAPPING

    raw = "PlayerExternalId.match_method != REJECTED_METHOD"
    assert _module_source("resolve").count(raw) == 1, "LIVE_MAPPING is not the only copy"
    for name in _VOCABULARY_CONSUMERS:
        assert raw not in _module_source(name), f"{name} re-typed the tombstone clause"
    # And it really is the predicate, not just a name.
    assert "match_method" in str(LIVE_MAPPING) and "!=" in str(LIVE_MAPPING)


def test_every_method_the_writers_persist_is_in_the_vocabulary(db_session, seeded_registry):
    """The end-to-end drift guard: run all five writers and assert the column only ever
    holds a value the vocabulary knows about."""
    from ffh.crosswalk.dynastyprocess import apply_playerids, read_playerids_csv
    from ffh.crosswalk.review import map_mapping, reject_mapping
    from tests.crosswalk.conftest import DP_SAMPLE_CSV

    apply_playerids(db_session, read_playerids_csv(DP_SAMPLE_CSV.read_bytes()))  # dynastyprocess
    resolve(db_session, "espn", "3916387", "Lamar Jackson", "QB", "BAL")  # exact_name
    resolve(db_session, "yahoo", "4881", "Lamarr Jackson", "QB", "BAL")  # fuzzy
    # A source DynastyProcess does not carry, so the player's slot for it is free.
    resolve(db_session, "custom", "PM", "Patrick Mahomes", "QB", "KC", gsis_id="00-0033873")
    assert map_mapping(db_session, "sleeper", "MANUAL", seeded_registry["00-0036963"]).ok
    # One of the many `dynastyprocess` rows — the others keep that rung represented.
    assert reject_mapping(db_session, "sleeper", "4046") is True  # rejected

    stored = set(db_session.scalars(select(PlayerExternalId.match_method)))
    assert stored <= MATCH_METHODS, stored - MATCH_METHODS
    # …and every rung is actually represented, so the assertion is not vacuous.
    assert stored == MATCH_METHODS, MATCH_METHODS - stored
