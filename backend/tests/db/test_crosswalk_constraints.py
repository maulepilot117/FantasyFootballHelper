"""Behavioural check that player_external_ids_source_player_uidx actually fires.

DATABASE.md §3 mandates test_crosswalk_no_duplicate_player_ids: no two external IDs
from the same source may map to one player_id. Migration 0002 adds a DB-level unique
index so later writers (resolve._persist, apply_playerids) cannot violate this by
construction alone.
"""

import pytest
from sqlalchemy.exc import IntegrityError

from ffh.db.models import Player, PlayerExternalId

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
