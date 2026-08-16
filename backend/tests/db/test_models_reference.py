from sqlalchemy.dialects import postgresql

from ffh.db import models  # noqa: F401  (registers tables)
from ffh.db.base import Base


def _cols(table: str) -> dict[str, object]:
    return {c.name: c for c in Base.metadata.tables[table].columns}


def test_players_table_shape():
    c = _cols("players")
    assert c["player_id"].primary_key
    assert c["gsis_id"].unique
    assert not c["full_name"].nullable
    assert not c["normalized_name"].nullable
    assert not c["position"].nullable
    assert "created_at" in c and "updated_at" in c


def test_player_external_ids_pk_is_source_external_id():
    t = Base.metadata.tables["player_external_ids"]
    assert [c.name for c in t.primary_key.columns] == ["source", "external_id"]
    assert not _cols("player_external_ids")["match_method"].nullable


def test_games_has_brin_index_on_kickoff():
    t = Base.metadata.tables["games"]
    brin = [i for i in t.indexes if i.dialect_options["postgresql"].get("using") == "brin"]
    assert brin and [c.name for c in brin[0].columns] == ["kickoff_at"]


def test_games_neutral_site_defaults_false():
    c = _cols("games")["neutral_site"]
    assert not c.nullable and c.server_default is not None


def test_crosswalk_unmatched_unique_source_external_id():
    t = Base.metadata.tables["crosswalk_unmatched"]
    uniques = [
        tuple(c.name for c in u.columns)
        for u in t.constraints
        if u.__class__.__name__ == "UniqueConstraint"
    ]
    assert ("source", "external_id") in uniques


def test_jsonb_and_uuid_types_used():
    assert isinstance(_cols("player_external_ids")["player_id"].type, postgresql.UUID)
