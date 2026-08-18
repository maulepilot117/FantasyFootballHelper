"""Behavioural check that player_external_ids_source_player_uidx actually fires.

DATABASE.md §3 mandates test_crosswalk_no_duplicate_player_ids: no two external IDs
from the same source may map to one player_id. Migration 0002 adds a DB-level unique
index so later writers (resolve._persist, apply_playerids) cannot violate this by
construction alone.
"""

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from ffh.crosswalk.resolve import REJECTED_METHOD
from ffh.db.models import Player, PlayerExternalId

UIDX = "player_external_ids_source_player_uidx"
#: The one predicate that defines "a tombstone is not a mapping" at the DB level.
#: Interpolated from `resolve.REJECTED_METHOD` rather than re-typed: the model and the
#: migrations state this predicate as raw SQL, which no rename can reach. Pinning it here
#: means renaming the constant fails loudly instead of silently un-partialling the index
#: (every tombstone would then squat its player's one slot for that source).
UIDX_PREDICATE = f"match_method <> '{REJECTED_METHOD}'"

pytestmark = pytest.mark.db


def _player(name: str) -> Player:
    return Player(full_name=name, normalized_name=name.lower(), position="WR")


def test_second_external_id_same_source_and_player_is_rejected(db_session):
    p = _player("Test Player")
    db_session.add(p)
    db_session.flush()
    db_session.add(
        PlayerExternalId(
            player_id=p.player_id, source="sleeper", external_id="1", match_method="manual"
        )
    )
    db_session.flush()
    db_session.add(
        PlayerExternalId(
            player_id=p.player_id, source="sleeper", external_id="2", match_method="manual"
        )
    )
    with pytest.raises(IntegrityError, match="player_external_ids_source_player_uidx"):
        db_session.flush()


def test_same_source_different_players_succeeds(db_session):
    a, b = _player("Player A"), _player("Player B")
    db_session.add_all([a, b])
    db_session.flush()
    db_session.add_all(
        [
            PlayerExternalId(
                player_id=a.player_id, source="sleeper", external_id="1", match_method="manual"
            ),
            PlayerExternalId(
                player_id=b.player_id, source="sleeper", external_id="2", match_method="manual"
            ),
        ]
    )
    db_session.flush()


def test_different_source_same_player_succeeds(db_session):
    p = _player("Player C")
    db_session.add(p)
    db_session.flush()
    db_session.add_all(
        [
            PlayerExternalId(
                player_id=p.player_id, source="sleeper", external_id="1", match_method="manual"
            ),
            PlayerExternalId(
                player_id=p.player_id, source="espn", external_id="1", match_method="manual"
            ),
        ]
    )
    db_session.flush()


def test_rejected_tombstone_does_not_consume_the_source_player_slot(db_session):
    """The index is PARTIAL (`WHERE match_method <> 'rejected'`). A tombstone records a
    pairing a human threw out; it is not a mapping, so it must leave the player's one
    slot for that source free — otherwise rejecting a wrong id would make the *correct*
    id unmappable forever."""
    p = _player("Test Player Two")
    db_session.add(p)
    db_session.flush()
    db_session.add(
        PlayerExternalId(
            player_id=p.player_id,
            source="sleeper",
            external_id="wrong",
            match_method="rejected",
            confidence=0.0,
        )
    )
    db_session.flush()
    db_session.add(
        PlayerExternalId(
            player_id=p.player_id, source="sleeper", external_id="right", match_method="manual"
        )
    )
    db_session.flush()  # no IntegrityError
    # …and two tombstones for the same player/source coexist as well.
    db_session.add(
        PlayerExternalId(
            player_id=p.player_id,
            source="sleeper",
            external_id="wrong2",
            match_method="rejected",
            confidence=0.0,
        )
    )
    db_session.flush()


def test_source_player_uidx_predicate_matches_the_model_and_the_database(db_session):
    """The PARTIAL predicate is load-bearing and NOTHING else guards it.

    Verified empirically on this toolchain (alembic 1.19.0 / SQLAlchemy 2.0.51): deleting
    `postgresql_where` from the model still yields `compare_metadata(...) == []`, so
    `alembic check` — and therefore tests/db/test_migrations.py — is blind to exactly this
    drift. Assert both sides directly instead.

    If the predicate is ever dropped: a `rejected` tombstone starts occupying the player's
    one slot for that source, and rejecting a wrong id makes the CORRECT id unmappable
    forever. If it is ever *widened*: two live mappings for one (source, player_id) become
    legal and the crosswalk's headline invariant silently stops being enforced.
    """
    # ── The live index, built by `alembic upgrade head` (the db_session fixture).
    indexdef = db_session.execute(
        text("SELECT indexdef FROM pg_indexes WHERE indexname = :n"), {"n": UIDX}
    ).scalar_one()
    assert "UNIQUE INDEX" in indexdef, indexdef
    assert "(source, player_id)" in indexdef, indexdef
    predicate = db_session.execute(
        text(
            """
            SELECT pg_get_expr(i.indpred, i.indrelid)
            FROM pg_index i
            JOIN pg_class c ON c.oid = i.indexrelid
            WHERE c.relname = :n
            """
        ),
        {"n": UIDX},
    ).scalar_one()
    assert predicate is not None, f"{UIDX} is no longer partial: {indexdef}"
    # Postgres re-prints it with an explicit cast; compare on the normalized form.
    assert predicate.replace("(", "").replace(")", "").replace("::text", "") == UIDX_PREDICATE

    # ── …and the model declares the same predicate, so ORM-side metadata cannot drift
    # away from the migration without failing here.
    (model_index,) = [i for i in PlayerExternalId.__table__.indexes if i.name == UIDX]
    assert model_index.unique is True
    assert [c.name for c in model_index.columns] == ["source", "player_id"]
    where = model_index.dialect_options["postgresql"]["where"]
    assert where is not None, f"{UIDX} lost postgresql_where in the model"
    assert str(where) == UIDX_PREDICATE
