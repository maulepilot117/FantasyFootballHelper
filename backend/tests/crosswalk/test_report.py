import json
from pathlib import Path

import pytest
from sqlalchemy import select

from ffh.crosswalk.dynastyprocess import apply_playerids, read_playerids_csv
from ffh.crosswalk.report import CoverageReport, coverage_report
from ffh.crosswalk.resolve import USABLE_CONFIDENCE, is_usable, resolve, upsert_unmatched
from ffh.crosswalk.review import mark_unmatched_resolved, reject_mapping, verify_mapping
from ffh.db.models import CrosswalkUnmatched, PlayerExternalId

pytestmark = pytest.mark.db

FIXTURE = (
    Path(__file__).resolve().parents[1] / "fixtures" / "dynastyprocess" / "db_playerids_sample.csv"
)


def test_report_on_empty_db_is_ok(db_session):
    rep = coverage_report(db_session)
    assert isinstance(rep, CoverageReport)
    assert rep.ok and rep.players_total == 0 and rep.unmatched == ()
    assert rep.unverified_low_confidence == ()
    assert "unmatched: 0" in rep.render()
    json.dumps(rep.to_dict())  # serializable


def test_report_counts_and_flags(db_session, seeded_registry):
    apply_playerids(db_session, read_playerids_csv(FIXTURE.read_bytes()))
    resolve(db_session, "sleeper", "4881", "Lamarr Jackson", "QB", "BAL")  # fuzzy pending
    resolve(db_session, "espn", "3916387", "Lamar Jackson", "QB", "BAL")  # exact
    resolve(db_session, "sleeper", "99999", "Nobody Nowhere", "QB", "FA")  # unmatched
    rep = coverage_report(db_session)
    assert rep.players_total == 14 + 32 + 2
    assert rep.players_by_position["DST"] == 32 and rep.players_by_position["QB"] == 5
    # DP wrote 10 sleeper ids (Mahomes, Chase, DJ Moore, Walker, Butker, Juszczyk, MHJ, Pavia,
    # Fred 2295, Chiefs KC) and 10 espn ids; see the Task 4 oracle.
    assert rep.ids_by_source["sleeper"] == 11  # 10 DP + the pending fuzzy row
    assert rep.ids_by_source_method["sleeper"] == {"dynastyprocess": 10, "fuzzy": 1}
    assert rep.ids_by_source_method["espn"] == {"dynastyprocess": 10, "exact_name": 1}
    assert [(r.source, r.external_id, r.full_name) for r in rep.unverified_low_confidence] == [
        ("sleeper", "4881", "Lamar Jackson")
    ]
    assert rep.unverified_low_confidence[0].confidence == pytest.approx(0.89)
    assert [(r.source, r.external_id, r.raw_name) for r in rep.unmatched] == [
        ("sleeper", "99999", "Nobody Nowhere")
    ]
    assert rep.ok is False
    d = rep.to_dict()
    assert d["ok"] is False and d["unmatched"][0]["external_id"] == "99999"
    # `--json` determinism: the count maps are key-sorted, not in SQL GROUP BY order
    assert list(d["players_by_position"]) == sorted(d["players_by_position"])
    assert list(d["ids_by_source"]) == sorted(d["ids_by_source"])
    assert list(d["ids_by_source_method"]) == sorted(d["ids_by_source_method"])
    assert all(list(m) == sorted(m) for m in d["ids_by_source_method"].values())
    text = rep.render()
    assert "unverified low-confidence: 1" in text and "unmatched: 1" in text
    assert "Nobody Nowhere" in text


def test_verify_and_reject(db_session, seeded_registry):
    resolve(db_session, "sleeper", "4881", "Lamarr Jackson", "QB", "BAL")
    assert verify_mapping(db_session, "sleeper", "4881") is True
    row = db_session.get(PlayerExternalId, ("sleeper", "4881"))
    db_session.refresh(row)
    assert row.verified_at is not None and row.verified_at.tzinfo is not None
    assert resolve(db_session, "sleeper", "4881") is not None  # now usable
    assert coverage_report(db_session).ok
    assert verify_mapping(db_session, "sleeper", "nope") is False

    assert reject_mapping(db_session, "sleeper", "4881") is True
    assert db_session.get(PlayerExternalId, ("sleeper", "4881")) is None
    u = db_session.scalar(
        select(CrosswalkUnmatched).where(CrosswalkUnmatched.external_id == "4881")
    )
    assert u is not None and u.source == "sleeper" and u.resolved is False
    assert reject_mapping(db_session, "sleeper", "4881") is False


def test_reject_preserves_queue_raw_fields(db_session, seeded_registry):
    """The Task-5 conflict path leaves the same key in BOTH tables; rejecting the disputed
    mapping must not null out the raw_* context already parked in the review queue."""
    resolve(db_session, "sleeper", "4881", "Lamarr Jackson", "QB", "BAL")
    upsert_unmatched(
        db_session,
        "sleeper",
        "4881",
        raw_name="Lamarr Jackson",
        raw_position="QB",
        raw_team="BAL",
    )
    assert reject_mapping(db_session, "sleeper", "4881") is True
    u = db_session.scalar(
        select(CrosswalkUnmatched).where(CrosswalkUnmatched.external_id == "4881")
    )
    assert u is not None
    assert (u.raw_name, u.raw_position, u.raw_team) == ("Lamarr Jackson", "QB", "BAL")
    assert u.resolved is False


def test_unmatched_drops_off_when_mapping_appears(db_session, seeded_registry):
    """The ladder never flips `crosswalk_unmatched.resolved` when a queued id later maps
    (e.g. a newly-arrived gsis_id) — the report must reflect the tables, not the queue's
    bookkeeping, or the exit-1 gate latches red forever on a fully-mapped id."""
    resolve(db_session, "sleeper", "12345", "Nobody Yet", "QB", "FA")  # rung 5 -> queued
    rep = coverage_report(db_session)
    assert [(r.source, r.external_id) for r in rep.unmatched] == [("sleeper", "12345")]
    assert rep.ok is False

    assert resolve(db_session, "sleeper", "12345", gsis_id="00-0033873") is not None  # rung 2
    rep = coverage_report(db_session)
    assert rep.unmatched == () and rep.ok is True
    u = db_session.scalar(
        select(CrosswalkUnmatched).where(CrosswalkUnmatched.external_id == "12345")
    )
    assert u is not None and u.resolved is False  # bookkeeping untouched; report ignores it


def test_float4_boundary_confidence_is_usable_not_flagged(db_session, seeded_registry):
    """A stored confidence of exactly 0.9 round-trips through Postgres REAL as
    0.899999988… — the report's SQL must share resolve's epsilon or it flags usable rows."""
    pid = seeded_registry["00-0034857"]  # Josh Allen QB
    db_session.add(
        PlayerExternalId(
            player_id=pid,
            source="sleeper",
            external_id="4984",
            confidence=0.9,
            match_method="manual",
            verified_at=None,
        )
    )
    db_session.flush()
    # Server-side, REAL 0.9 promotes to float8 0.89999997… — strictly below the
    # threshold — so a hardcoded `confidence < 0.9` in the report's SQL would flag
    # this usable row. Only the shared epsilon keeps SQL and is_usable in agreement.
    below = db_session.scalar(
        select(PlayerExternalId.confidence < USABLE_CONFIDENCE).where(
            PlayerExternalId.source == "sleeper", PlayerExternalId.external_id == "4984"
        )
    )
    assert below is True
    row = db_session.get(PlayerExternalId, ("sleeper", "4984"))
    db_session.refresh(row)
    assert is_usable(float(row.confidence), None) is True
    assert is_usable(0.9, None) is True
    rep = coverage_report(db_session)
    assert rep.unverified_low_confidence == () and rep.ok is True


def test_mark_unmatched_resolved(db_session):
    upsert_unmatched(db_session, "sleeper", "424242", raw_name="Ghost Man")
    assert mark_unmatched_resolved(db_session, "sleeper", "424242") is True
    u = db_session.scalar(
        select(CrosswalkUnmatched).where(CrosswalkUnmatched.external_id == "424242")
    )
    assert u is not None and u.resolved is True
    assert mark_unmatched_resolved(db_session, "sleeper", "nope") is False
    # a resolved id that reappears unmatched re-opens for review
    upsert_unmatched(db_session, "sleeper", "424242", raw_name="Ghost Man")
    db_session.refresh(u)
    assert u.resolved is False
