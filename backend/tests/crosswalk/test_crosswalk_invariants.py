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
    REJECTED_METHOD,
    USABLE_CONFIDENCE,
    ResolveInput,
    is_usable,
    resolve,
    resolve_many,
)
from ffh.crosswalk.review import map_mapping, reject_mapping, verify_mapping
from ffh.db.models import PlayerExternalId
from tests.crosswalk.conftest import FIXTURE_DIR

pytestmark = pytest.mark.db

FIXTURE = FIXTURE_DIR / "dynastyprocess" / "db_playerids_sample.csv"

# The usability rule is
# `match_method <> 'rejected' AND (confidence >= 0.9 - epsilon OR verified_at IS NOT NULL)`
# (resolve.is_usable / DATABASE.md §3), so its complement — "rows awaiting review" — must
# carry the tombstone clause too, exactly as report.py does. Without it every
# `reject_mapping` (confidence 0.0, verified_at NULL by construction) shows up here as a
# row "awaiting review" and breaks this test for a reason that has nothing to do with the
# invariant. The sibling DUPLICATE_LIVE_IDS_SQL below already carries the same clause.
#
# Never hardcode 0.9 in SQL here either: confidence is Postgres REAL, so a stored 0.9 reads
# back as 0.899999976… and a bare `< 0.9` predicate would sweep usable rows into the
# "needs review" bucket. Report and ladder share the same epsilon.
LOW_CONFIDENCE_SQL = """
    SELECT source, external_id, player_id
    FROM player_external_ids
    WHERE match_method <> :rejected AND confidence < :threshold AND verified_at IS NULL
    ORDER BY source, external_id
"""
#: Same query without the tombstone clause — used once, to prove the clause is load-bearing.
LOW_CONFIDENCE_SQL_NO_TOMBSTONE_CLAUSE = """
    SELECT source, external_id
    FROM player_external_ids
    WHERE confidence < :threshold AND verified_at IS NULL
    ORDER BY source, external_id
"""
LOW_CONFIDENCE_PARAMS = {
    "threshold": USABLE_CONFIDENCE - CONFIDENCE_EPSILON,
    "rejected": REJECTED_METHOD,
}

# Direction A of test_crosswalk_no_duplicate_player_ids, mirroring the PARTIAL unique index
# `player_external_ids_source_player_uidx` exactly (see the comment at its call site).
DUPLICATE_LIVE_IDS_SQL = """
    SELECT source, player_id, count(*) AS n
    FROM player_external_ids
    WHERE match_method <> 'rejected'
    GROUP BY source, player_id
    HAVING count(*) > 1
"""


@pytest.fixture
def populated(db_session, seeded_registry):
    """Registry + DP ids + a spread of ladder outcomes (exact, fuzzy pending, unmatched)."""
    apply_playerids(db_session, read_playerids_csv(FIXTURE.read_bytes()))
    # rung 4 and rung 5 must withhold a Resolution *here*, at the moment the row is minted.
    # Asserting it only after re-resolving (below) would miss a regression that returns the
    # fuzzy Resolution on the first call: the second call goes through rung 1, which
    # re-applies is_usable independently and would still return None.
    assert resolve(db_session, "sleeper", "4881", "Lamarr Jackson", "QB", "BAL") is None  # fuzzy
    resolve(db_session, "espn", "3916387", "Lamar Jackson", "QB", "BAL")  # exact 0.95
    resolve(db_session, "sleeper", "KC", "Kansas City Chiefs", "DEF", "KC")  # rung 1 (from DP row)
    assert resolve(db_session, "sleeper", "99999", "Nobody Nowhere", "QB", "FA") is None  # rung 5
    return seeded_registry


def test_crosswalk_no_duplicate_player_ids(db_session, populated):
    """No two external ids from one source map to one player_id, and vice-versa."""
    # ── Direction A: one *live* id per source per player. Enforced by the PARTIAL unique
    # index added in migration 0002 (`WHERE match_method <> 'rejected'`) AND pre-checked by
    # every writer (resolve._persist, apply_playerids, review.map_mapping).
    #
    # The `match_method <> 'rejected'` filter is not cosmetic — it is the invariant. A
    # tombstone is a record of a pairing a human threw out, not a mapping, and deliberately
    # does NOT occupy the player's slot: `--reject` followed by `ffh crosswalk map` leaves a
    # rejected row and a live row for one (source, player_id) by design (see
    # tests/db/test_crosswalk_constraints.py). An unfiltered GROUP BY would fail on that
    # legal state — it would be stricter than the constraint it claims to mirror, and it
    # passes here only because this fixture happens to contain no rejection.
    many_ids_per_player = db_session.execute(text(DUPLICATE_LIVE_IDS_SQL)).all()
    assert many_ids_per_player == [], many_ids_per_player

    # ── Direction B: one player per (source, external_id). A `count(DISTINCT player_id)`
    # query can never fail here — those two columns ARE the primary key, so the assertion
    # would be vacuous. Assert the structural fact that makes it true instead: a future
    # migration that widened or re-keyed the PK (e.g. adding `season`, or moving to a
    # surrogate id) would let one (source, external_id) point at two players, and that
    # migration must fail this test.
    #
    # Read it off the LIVE database, which this fixture built with `alembic upgrade head`:
    # a metadata-only check duplicates tests/db/test_models_reference.py and can only fail
    # when the *model* changes, which is the one direction ruling 1 did not care about.
    live_pk = {
        r[0]
        for r in db_session.execute(
            text(
                """
                SELECT a.attname
                FROM pg_index i
                JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                WHERE i.indrelid = 'player_external_ids'::regclass AND i.indisprimary
                """
            )
        )
    }
    assert live_pk == {"source", "external_id"}, live_pk
    # …and the model agrees with it, so ORM-side writes cannot drift from the constraint.
    assert {c.name for c in PlayerExternalId.__table__.primary_key.columns} == live_pk

    # ── The ladder cannot break direction A. The gsis_id is mandatory in this call: without
    # it the name/position rungs are the only way in, and `_exact`/`_fuzzy` already exclude
    # players holding an id for the source (`Player.player_id.not_in(_mapped_for_source)`),
    # so the call would fall to rung 5 and never reach the guard under test. With it, rung 2
    # matches Mahomes directly and `resolve._persist`'s (source, player_id) pre-check is the
    # ONLY thing that can refuse the write — which it does, routing the id to
    # crosswalk_unmatched rather than silently dropping it.
    mahomes = populated["00-0033873"]
    assert (
        resolve(
            db_session,
            "sleeper",
            "4046-dupe",
            "Patrick Mahomes",
            "QB",
            "KC",
            gsis_id="00-0033873",
        )
        is None
    )
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

    # ── And the legal state the filter exists for: reject a mapping, then map the correct
    # id to the same player by hand. That leaves a tombstone AND a live row for one
    # (source, player_id) — permitted by the partial index, and the invariant must hold.
    chase = populated["00-0036900"]
    (chase_id,) = db_session.execute(
        text(
            "SELECT external_id FROM player_external_ids "
            "WHERE source='sleeper' AND player_id = :pid"
        ),
        {"pid": chase},
    ).one()
    assert reject_mapping(db_session, "sleeper", chase_id) is True
    assert map_mapping(db_session, "sleeper", "chase-corrected", chase).ok is True

    assert db_session.execute(text(DUPLICATE_LIVE_IDS_SQL)).all() == []
    # …and the filter is load-bearing: without it this legal state fails the invariant.
    unfiltered = db_session.execute(
        text(
            """
            SELECT source, player_id, count(*) AS n
            FROM player_external_ids
            GROUP BY source, player_id
            HAVING count(*) > 1
            """
        )
    ).all()
    assert [(s, p) for s, p, _n in unfiltered] == [("sleeper", chase)]


def test_crosswalk_low_confidence_reviewed(db_session, populated):
    """No confidence < 0.9 row is used unverified: resolve never returns one."""
    # A tombstone in the fixture makes the `match_method <> 'rejected'` clause load-bearing:
    # `reject_mapping` writes confidence 0.0 / verified_at NULL, so without the clause this
    # 1.0 DynastyProcess mapping a human just threw out would count as a row *awaiting
    # review* and the assertion below would fail for a reason unrelated to the invariant.
    assert reject_mapping(db_session, "sleeper", "4046") is True

    unverified = db_session.execute(text(LOW_CONFIDENCE_SQL), LOW_CONFIDENCE_PARAMS).all()
    assert ("sleeper", "4046") not in [(s, e) for s, e, _ in unverified]
    # …and it IS what the clause removes — not a row that was never selected anyway.
    assert ("sleeper", "4046") in db_session.execute(
        text(LOW_CONFIDENCE_SQL_NO_TOMBSTONE_CLAUSE), LOW_CONFIDENCE_PARAMS
    ).all()
    # A tombstone is not a mapping: the ladder returns nothing for it either.
    assert resolve(db_session, "sleeper", "4046") is None
    # Exactly one: the batch mints a single rung-4 row (sleeper:4881). `>= 1` would let a
    # regression that starts minting extra sub-threshold mappings slip through.
    assert len(unverified) == 1, unverified

    for source, external_id, _pid in unverified:
        assert resolve(db_session, source, external_id) is None

    rep = resolve_many(db_session, [ResolveInput(s, e) for s, e, _ in unverified])
    assert rep.resolved == {}
    assert sorted(rep.pending_review) == sorted((s, e) for s, e, _ in unverified)

    # Every Resolution the ladder hands out passes the consumer filter rule.
    all_rows = db_session.execute(
        text(
            "SELECT source, external_id, confidence, verified_at, match_method "
            "FROM player_external_ids"
        )
    ).all()
    for source, external_id, confidence, verified_at, match_method in all_rows:
        res = resolve(db_session, source, external_id)
        # match_method is part of the rule: a `rejected` tombstone is never usable, even
        # if something stamped verified_at on it.
        assert (res is not None) == is_usable(confidence, verified_at, match_method)

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
