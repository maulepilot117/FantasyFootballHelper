"""DATABASE.md §3 mandatory tests that do not need platform/ADP fixtures.

test_crosswalk_covers_all_rostered_players → PR ⑤ (needs the Sleeper league fixture)
test_crosswalk_covers_top_300_adp          → PR ⑥ (needs the ADP snapshot)
"""

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from ffh.crosswalk.dynastyprocess import apply_playerids, read_playerids_csv
from ffh.crosswalk.report import coverage_report
from ffh.crosswalk.resolve import (
    CONFIDENCE_EPSILON,
    USABLE_CONFIDENCE,
    ResolveInput,
    is_usable,
    resolve,
    resolve_many,
)
from ffh.crosswalk.review import verify_mapping
from ffh.db.models import PlayerExternalId
from tests.crosswalk.conftest import FIXTURE_DIR

pytestmark = pytest.mark.db

FIXTURE = FIXTURE_DIR / "dynastyprocess" / "db_playerids_sample.csv"

# The usability rule is `confidence >= 0.9 - epsilon OR verified_at IS NOT NULL`
# (resolve.is_usable). Never hardcode 0.9 in SQL here: confidence is Postgres REAL, so a
# stored 0.9 reads back as 0.899999988… and a bare `< 0.9` predicate would sweep usable
# rows into the "needs review" bucket. Report and ladder share the same epsilon.
LOW_CONFIDENCE_SQL = """
    SELECT source, external_id, player_id
    FROM player_external_ids
    WHERE confidence < :threshold AND verified_at IS NULL
    ORDER BY source, external_id
"""
LOW_CONFIDENCE_PARAMS = {"threshold": USABLE_CONFIDENCE - CONFIDENCE_EPSILON}


@pytest.fixture
def populated(db_session, seeded_registry):
    """Registry + DP ids + a spread of ladder outcomes (exact, fuzzy pending, unmatched)."""
    apply_playerids(db_session, read_playerids_csv(FIXTURE.read_bytes()))
    resolve(db_session, "sleeper", "4881", "Lamarr Jackson", "QB", "BAL")  # fuzzy → pending
    resolve(db_session, "espn", "3916387", "Lamar Jackson", "QB", "BAL")  # exact 0.95
    resolve(db_session, "sleeper", "KC", "Kansas City Chiefs", "DEF", "KC")  # rung 1 (from DP row)
    resolve(db_session, "sleeper", "99999", "Nobody Nowhere", "QB", "FA")  # unmatched
    return seeded_registry


def test_crosswalk_no_duplicate_player_ids(db_session, populated):
    """No two external ids from one source map to one player_id, and vice-versa."""
    # ── Direction A: one id per source per player. Enforced by the unique index added in
    # migration 0002 AND pre-checked by every writer (resolve._persist, apply_playerids).
    many_ids_per_player = db_session.execute(
        text(
            """
            SELECT source, player_id, count(*) AS n
            FROM player_external_ids
            GROUP BY source, player_id
            HAVING count(*) > 1
            """
        )
    ).all()
    assert many_ids_per_player == [], many_ids_per_player

    # ── Direction B: one player per (source, external_id). A `count(DISTINCT player_id)`
    # query can never fail here — those two columns ARE the primary key, so the assertion
    # would be vacuous. Assert the structural fact that makes it true instead: a future
    # migration that widened or re-keyed the PK (e.g. adding `season`, or moving to a
    # surrogate id) would let one (source, external_id) point at two players, and that
    # migration must fail this test.
    pk_columns = {c.name for c in PlayerExternalId.__table__.primary_key.columns}
    assert pk_columns == {"source", "external_id"}, pk_columns

    # ── The ladder cannot break direction A: a second sleeper id claiming an already
    # mapped player is refused (rungs 3/4 pre-check (source, player_id)) and routed to
    # crosswalk_unmatched rather than silently dropped.
    mahomes = populated["00-0033873"]
    assert resolve(db_session, "sleeper", "4046-dupe", "Patrick Mahomes", "QB", "KC") is None
    assert (
        db_session.execute(
            text(
                "SELECT count(*) FROM player_external_ids "
                "WHERE source='sleeper' AND player_id = :pid"
            ),
            {"pid": mahomes},
        ).scalar_one()
        == 1
    )
    assert (
        db_session.execute(
            text(
                "SELECT count(*) FROM crosswalk_unmatched "
                "WHERE source='sleeper' AND external_id='4046-dupe' AND resolved IS FALSE"
            )
        ).scalar_one()
        == 1
    )

    # ── And the database itself refuses one, even if a writer ever forgets to pre-check.
    # An IntegrityError poisons the enclosing transaction, so this runs inside its own
    # SAVEPOINT and is rolled back — the db_session fixture's transaction stays usable.
    savepoint = db_session.begin_nested()
    with pytest.raises(IntegrityError) as excinfo:
        db_session.add(
            PlayerExternalId(
                player_id=mahomes,
                source="sleeper",
                external_id="4046-dupe-forced",
                confidence=1.0,
                match_method="manual",
            )
        )
        db_session.flush()
    assert "player_external_ids_source_player_uidx" in str(excinfo.value)
    savepoint.rollback()

    # The session survived the rollback and the forced row never landed.
    assert (
        db_session.execute(
            text(
                "SELECT count(*) FROM player_external_ids "
                "WHERE source='sleeper' AND player_id = :pid"
            ),
            {"pid": mahomes},
        ).scalar_one()
        == 1
    )


def test_crosswalk_low_confidence_reviewed(db_session, populated):
    """No confidence < 0.9 row is used unverified: resolve never returns one."""
    unverified = db_session.execute(text(LOW_CONFIDENCE_SQL), LOW_CONFIDENCE_PARAMS).all()
    assert len(unverified) >= 1  # the fixture created a pending fuzzy row on purpose

    for source, external_id, _pid in unverified:
        assert resolve(db_session, source, external_id) is None

    rep = resolve_many(db_session, [ResolveInput(s, e) for s, e, _ in unverified])
    assert rep.resolved == {}
    assert sorted(rep.pending_review) == sorted((s, e) for s, e, _ in unverified)

    # Every Resolution the ladder hands out passes the consumer filter rule.
    all_rows = db_session.execute(
        text("SELECT source, external_id, confidence, verified_at FROM player_external_ids")
    ).all()
    for source, external_id, confidence, verified_at in all_rows:
        res = resolve(db_session, source, external_id)
        assert (res is not None) == is_usable(confidence, verified_at)

    # The report surfaces them and refuses to say "ok".
    report = coverage_report(db_session)
    assert {(r.source, r.external_id) for r in report.unverified_low_confidence} == {
        (s, e) for s, e, _ in unverified
    }
    assert report.ok is False

    # ── The other half of the invariant: human review is the ONLY way such a row becomes
    # usable, and it works. After verification the ladder returns it and it drops off the
    # report's review queue.
    for source, external_id, pid in unverified:
        assert verify_mapping(db_session, source, external_id) is True
        res = resolve(db_session, source, external_id)
        assert res is not None and res.player_id == pid
        assert res.confidence < USABLE_CONFIDENCE - CONFIDENCE_EPSILON  # still low, now trusted

    assert db_session.execute(text(LOW_CONFIDENCE_SQL), LOW_CONFIDENCE_PARAMS).all() == []
    assert coverage_report(db_session).unverified_low_confidence == ()
