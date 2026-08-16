from ffh.db import models  # noqa: F401
from ffh.db.base import Base

EXPECTED_TABLES = {
    "players",
    "player_external_ids",
    "nfl_teams",
    "stadiums",
    "games",
    "game_weather_forecasts",
    "crosswalk_unmatched",
    "leagues",
    "league_teams",
    "roster_slots",
    "matchups",
    "transactions",
    "drafts",
    "draft_picks",
    "adp",
    "projections",
    "projection_correlations",
    "player_week_actuals",
    "player_injury_status",
    "recommendations",
    "ai_debates",
    "ingest_runs",
}


def test_all_database_md_tables_are_registered():
    assert set(Base.metadata.tables) == EXPECTED_TABLES


def test_projections_carry_gamma_params_not_null():
    t = Base.metadata.tables["projections"]
    for col in ("mean_points", "gamma_shape", "gamma_scale", "model_version", "inputs"):
        assert not t.c[col].nullable, col


def test_projections_unique_treats_nulls_as_not_distinct():
    t = Base.metadata.tables["projections"]
    uniques = [c for c in t.constraints if c.__class__.__name__ == "UniqueConstraint"]
    assert len(uniques) == 1
    assert uniques[0].name == "projections_scope_key"  # explicit: convention name > 63 chars
    assert uniques[0].dialect_options["postgresql"]["nulls_not_distinct"] is True


def test_projection_correlations_check_canonical_order():
    t = Base.metadata.tables["projection_correlations"]
    checks = [c for c in t.constraints if c.__class__.__name__ == "CheckConstraint"]
    assert any("player_a < player_b" in str(c.sqltext) for c in checks)


def test_ingest_runs_index():
    t = Base.metadata.tables["ingest_runs"]
    assert any(i.name == "ingest_runs_source_idx" for i in t.indexes)
