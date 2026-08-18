"""docs/DATABASE.md §3: test_crosswalk_covers_all_rostered_players.

Failing this means the app is wrong: a crosswalk gap presents as a MISSING PLAYER, which
looks exactly like a player who isn't rostered, and quietly moves every VORP baseline.

Coverage is proved through the LOAD REPORT (④'s `resolve_many` verdict), never by counting
`player_external_ids` rows: ④ deliberately persists no row for a `source == "gsis"`
resolution, so a row count would call a perfectly resolved player unmatched — and, worse,
call a `rejected` tombstone a mapping.

The headline test seeds a registry where **DynastyProcess lags**, because the fixture's own
id file is built from the same blob the rosters index: with every human carrying a
`sleeper_id`, "everyone resolves" is true by construction of the fixture and only rung 1 is
ever exercised. Production is not that world — DynastyProcess lags rookies and mid-season
signings, and 8,326 of Sleeper's 12,219 players have a null `gsis_id`. So two rostered
humans are seeded without their DynastyProcess mapping and must come back up the ladder on
their own, at a rung this test pins.
"""

import pytest
from sqlalchemy import func, select

from ffh.crosswalk.resolve import DP_METHOD, EXACT_METHOD, GSIS_METHOD, LIVE_MAPPING
from ffh.db.models import CrosswalkUnmatched, Player, PlayerExternalId, RosterSlot
from ffh.ingest.platform_sync import load_league
from tests.ingest._sleeper_seed import SEEDED_PLAYERS, seed_fixture_players

pytestmark = pytest.mark.db

LEAGUE = "1000000000000000001"
#: 13 on my roster + 10 on theirs: 21 humans and the 2 team defenses.
ROSTERED = 23

#: Rostered Sleeper ids seeded WITHOUT a DynastyProcess `sleeper_id` row, so rung 1 has
#: nothing to answer with. GSIS keeps its gsis_id (rung 2 must catch it); NAME has neither
#: id fact and can only be found by (normalized_name, position, team) — rung 3.
DP_GAP_GSIS = "3"
DP_GAP_NAME = "5"

#: Sleeper id -> the gsis_id that id actually BELONGS to (tests/fixtures/sleeper/
#: players_slice.json). Spelled out rather than re-read from the blob on purpose: a join
#: count only proves a roster slot points at *a* player, and re-deriving the expectation
#: from the same file the loader read would cancel a mis-mapping out.
IDENTITIES = {"1": "00-0090001", "13": "00-0090013", DP_GAP_GSIS: "00-0090003"}


def _live_mapping(session, external_id: str) -> PlayerExternalId:
    """The one usable sleeper mapping for an id. LIVE_MAPPING (④), never a hand-rolled
    predicate: a `rejected` tombstone is a row, not a mapping."""
    return session.scalars(
        select(PlayerExternalId).where(
            PlayerExternalId.source == "sleeper",
            PlayerExternalId.external_id == external_id,
            LIVE_MAPPING,
        )
    ).one()


def _assert_every_rostered_player_resolved(session, report) -> None:
    """The mandatory property, asserted the same way for every registry shape."""
    assert report.unmatched == [], "unmatched rostered players: " + ", ".join(
        f"{u.external_id}/{u.name}/{u.position}" for u in report.unmatched
    )
    assert report.pending_review == [], "rostered players awaiting crosswalk review: " + ", ".join(
        f"{u.external_id}/{u.name}/{u.position}" for u in report.pending_review
    )
    assert report.rostered == ROSTERED
    # The report's word and the database's word are the same word: one roster_slots row
    # per rostered player, every one of them pointing at a real players row.
    assert session.scalar(select(func.count()).select_from(RosterSlot)) == ROSTERED
    linked = session.scalar(
        select(func.count())
        .select_from(RosterSlot)
        .join(Player, Player.player_id == RosterSlot.player_id)
    )
    assert linked == ROSTERED
    # Nothing was queued for a human: an empty review queue is what greens `ffh crosswalk
    # report` and what makes `ffh league load` exit 0.
    assert session.scalar(select(func.count()).select_from(CrosswalkUnmatched)) == 0


def test_crosswalk_covers_all_rostered_players(db_session, catalog_adapter):
    """THE mandatory test. Every player on every roster resolves — including the two whose
    DynastyProcess mapping is missing, which is what production looks like."""
    seed_fixture_players(
        db_session,
        drop_sleeper_ids=(DP_GAP_GSIS, DP_GAP_NAME),
        drop_gsis_ids=(DP_GAP_NAME,),
    )
    # The gaps remove ID FACTS, not players: the registry still holds every human.
    assert db_session.scalar(select(func.count()).select_from(Player)) == SEEDED_PLAYERS

    report = load_league(db_session, catalog_adapter, LEAGUE, season=2026, week=1)
    _assert_every_rostered_player_resolved(db_session, report)

    # ...and each of the three rungs that carried a player is pinned, so the added coverage
    # cannot silently collapse back onto rung 1 the day the fixture id file changes.
    assert _live_mapping(db_session, "1").match_method == DP_METHOD
    assert _live_mapping(db_session, DP_GAP_GSIS).match_method == GSIS_METHOD
    assert _live_mapping(db_session, DP_GAP_NAME).match_method == EXACT_METHOD

    # Identity, not just linkage: each id resolved to the player that id belongs to, and
    # that player is the one holding the roster slot.
    rostered_player_ids = set(db_session.scalars(select(RosterSlot.player_id)))
    for sleeper_id, gsis_id in IDENTITIES.items():
        player = db_session.get(Player, _live_mapping(db_session, sleeper_id).player_id)
        assert player.gsis_id == gsis_id, f"sleeper:{sleeper_id} resolved to {player.full_name}"
        assert player.player_id in rostered_player_ids


def test_crosswalk_covers_all_rostered_players_from_id_only_refs(seeded, adapter):
    """The degraded shape: no PlayerCatalog, so a human reaches the crosswalk as a bare
    numeric id with no name, no team and no gsis. Only rung 1 can answer — which is exactly
    why `ffh league load` always attaches a catalog. Coverage must still hold when the
    DynastyProcess file is complete."""
    report = load_league(seeded, adapter, LEAGUE, season=2026, week=1)
    _assert_every_rostered_player_resolved(seeded, report)
    assert {_live_mapping(seeded, str(i)).match_method for i in range(1, 22)} == {DP_METHOD}


def test_defenses_resolve_through_the_dst_canonical_form(seeded, adapter):
    """A defense has no `full_name` and no `gsis_id` in Sleeper's blob — only the team
    abbreviation, which is also its player id. ④'s `normalize_dst` canonicalizes it to
    `<abbr> dst`, which is the form `seed_dst_players` wrote."""
    load_league(seeded, adapter, LEAGUE, season=2026, week=1)
    dst_slots = seeded.scalars(select(RosterSlot).where(RosterSlot.slot == "DST")).all()
    assert len(dst_slots) == 2
    players = [seeded.get(Player, s.player_id) for s in dst_slots]
    assert {p.position for p in players} == {"DST"}
    # Two DISTINCT defenses: both roster slots collapsing onto one team's row would still
    # satisfy every count above.
    assert {p.team_abbr for p in players} == {"KC", "SF"}
