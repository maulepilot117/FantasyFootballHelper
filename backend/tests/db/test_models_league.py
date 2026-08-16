from sqlalchemy.dialects import postgresql

from ffh.db import models  # noqa: F401
from ffh.db.base import Base


def _uniques(table: str) -> set[tuple[str, ...]]:
    t = Base.metadata.tables[table]
    return {
        tuple(c.name for c in u.columns)
        for u in t.constraints
        if u.__class__.__name__ == "UniqueConstraint"
    }


def test_leagues_unique_and_jsonb_settings():
    assert ("platform", "external_id", "season") in _uniques("leagues")
    t = Base.metadata.tables["leagues"]
    assert isinstance(t.c.scoring_settings.type, postgresql.JSONB)
    assert not t.c.scoring_settings.nullable
    assert not t.c.roster_settings.nullable


def test_roster_slots_pk():
    t = Base.metadata.tables["roster_slots"]
    assert [c.name for c in t.primary_key.columns] == ["league_team_id", "week", "player_id"]


def test_draft_picks_pk_and_index():
    t = Base.metadata.tables["draft_picks"]
    assert [c.name for c in t.primary_key.columns] == ["draft_id", "pick_no"]
    assert any([c.name for c in i.columns] == ["player_id"] for i in t.indexes)


def test_adp_pk_and_stdev_present():
    t = Base.metadata.tables["adp"]
    assert [c.name for c in t.primary_key.columns] == [
        "source",
        "format",
        "num_teams",
        "scrape_date",
        "player_id",
    ]
    assert "adp_stdev" in t.c
