"""docs/DATABASE.md §3: test_crosswalk_covers_all_rostered_players.

Failing this means the app is wrong: a crosswalk gap presents as a MISSING PLAYER, which
looks exactly like a player who isn't rostered, and quietly moves every VORP baseline.

Coverage is proved through the LOAD REPORT (④'s `resolve_many` verdict), never by counting
`player_external_ids` rows: ④ deliberately persists no row for a `source == "gsis"`
resolution, so a row count would call a perfectly resolved player unmatched — and, worse,
call a `rejected` tombstone a mapping.

The fixtures come from Task 7's `test_platform_sync` rather than being rebuilt here:
`seeded` is ④'s real seeding recipe (`apply_playerids` + `seed_dst_players`) and `adapter`
/ `catalog_adapter` close their SleeperClient in their own event loop, so no httpx client
leaks out of a test.
"""

import pytest
from sqlalchemy import func, select

from ffh.db.models import CrosswalkUnmatched, Player, RosterSlot
from ffh.ingest.platform_sync import load_league
from tests.ingest.test_platform_sync import (  # noqa: F401  (imported as fixtures)
    adapter,
    catalog_adapter,
    seeded,
)

pytestmark = pytest.mark.db

LEAGUE = "1000000000000000001"
#: 13 on my roster + 10 on theirs, 21 humans and the 2 team defenses.
ROSTERED = 23


def test_crosswalk_covers_all_rostered_players(seeded, adapter):  # noqa: F811
    """THE mandatory test. Every player on every roster in the league resolves."""
    report = load_league(seeded, adapter, LEAGUE, season=2026, week=1)

    assert report.unmatched == [], "unmatched rostered players: " + ", ".join(
        f"{u.external_id}/{u.name}/{u.position}" for u in report.unmatched
    )
    assert report.pending_review == [], "rostered players awaiting crosswalk review: " + ", ".join(
        f"{u.external_id}/{u.name}/{u.position}" for u in report.pending_review
    )
    assert report.rostered == ROSTERED
    # The report's word and the database's word are the same word: one roster_slots row
    # per rostered player, every one of them pointing at a real players row.
    assert seeded.scalar(select(func.count()).select_from(RosterSlot)) == ROSTERED
    linked = seeded.scalar(
        select(func.count())
        .select_from(RosterSlot)
        .join(Player, Player.player_id == RosterSlot.player_id)
    )
    assert linked == ROSTERED
    # Nothing was queued for a human: an empty review queue is what greens `ffh crosswalk
    # report` and what makes `ffh league load` exit 0.
    assert seeded.scalar(select(func.count()).select_from(CrosswalkUnmatched)) == 0


def test_crosswalk_covers_all_rostered_players_with_the_lake_catalog(seeded, catalog_adapter):  # noqa: F811
    """The production shape — `ffh league load` always attaches a PlayerCatalog, so every
    ref reaches ④ with a name, a position, a team AND a gsis_id. Coverage must hold with
    the richer input too: a name that reaches rung 3/4 instead of rung 1 would show up as
    `pending_review`, which is still a red gate."""
    report = load_league(seeded, catalog_adapter, LEAGUE, season=2026, week=1)
    assert report.unmatched == [] and report.pending_review == []
    assert report.rostered == ROSTERED
    assert seeded.scalar(select(func.count()).select_from(CrosswalkUnmatched)) == 0


def test_defenses_resolve_through_the_dst_canonical_form(seeded, adapter):  # noqa: F811
    """A defense has no `full_name` and no `gsis_id` in Sleeper's blob — only the team
    abbreviation, which is also its player id. ④'s `normalize_dst` canonicalizes it to
    `<abbr> dst`, which is the form `seed_dst_players` wrote."""
    load_league(seeded, adapter, LEAGUE, season=2026, week=1)
    dst_slots = seeded.scalars(select(RosterSlot).where(RosterSlot.slot == "DST")).all()
    assert len(dst_slots) == 2
    positions = {seeded.get(Player, s.player_id).position for s in dst_slots}
    assert positions == {"DST"}
