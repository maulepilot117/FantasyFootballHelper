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


def test_transactions_unique_treats_nulls_as_not_distinct():
    t = Base.metadata.tables["transactions"]
    uniques = [c for c in t.constraints if c.__class__.__name__ == "UniqueConstraint"]
    assert len(uniques) == 1
    assert uniques[0].dialect_options["postgresql"]["nulls_not_distinct"] is True


def _fks(table: str) -> dict[tuple[str, ...], tuple[str, tuple[str, ...]]]:
    """{local column names: (referred table, referred column names)}."""
    t = Base.metadata.tables[table]
    return {
        tuple(c.name for c in fk.columns): (
            fk.referred_table.name,
            tuple(e.column.name for e in fk.elements),
        )
        for fk in t.foreign_key_constraints
    }


def test_league_teams_unique_on_league_id_league_team_id():
    assert ("league_id", "league_team_id") in _uniques("league_teams")
    assert ("league_id", "external_id") in _uniques("league_teams")


def test_matchups_team_fks_are_composite_same_league():
    fks = _fks("matchups")
    target = ("league_teams", ("league_id", "league_team_id"))
    assert fks[("league_id", "home_team_id")] == target
    assert fks[("league_id", "away_team_id")] == target
    assert fks[("league_id",)] == ("leagues", ("league_id",))
    # No single-column FK on either team column may remain.
    assert ("home_team_id",) not in fks
    assert ("away_team_id",) not in fks


def test_leagues_my_team_fk_is_composite_and_deferred_via_use_alter():
    fks = _fks("leagues")
    assert fks[("league_id", "my_team_id")] == ("league_teams", ("league_id", "league_team_id"))
    fk = next(
        c
        for c in Base.metadata.tables["leagues"].foreign_key_constraints
        if c.name == "leagues_my_team_fkey"
    )
    assert fk.use_alter is True
