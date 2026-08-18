"""players.team_abbr (crosswalk rung-3 tie-breaker) + player_external_ids source/player uniqueness

Revision ID: 0002
Revises: 0001
Create Date: 2026-08-16

Two changes, landed together because both close gaps in the crosswalk (DATABASE.md §3):

1. `players.team_abbr` — nullable, no FK (DST rows and DynastyProcess `FA` would violate
   an `nfl_teams` FK). Rung 3 of the resolution ladder matches on
   `(normalized_name, position, team)`, but `players` had no team column; this is the
   tie-breaker, refreshed from nflverse `latest_team` by `seed_players`. It is a crosswalk
   input ONLY, never roster truth (rosters come from platforms).

2. `player_external_ids_source_player_uidx` — UNIQUE (source, player_id). DATABASE.md §3
   mandates a `test_crosswalk_no_duplicate_player_ids` invariant ("no two external IDs
   from the same source map to one player_id"), but nothing enforced it at the DB level:
   both `resolve._persist` and `apply_playerids` (later tasks in this PR) can each insert
   a second external id for a source against a player that already holds one. This index
   makes the invariant real. Those writers MUST pre-check `(source, player_id)` before
   insert and route the loser to `crosswalk_unmatched` / the ambiguity report — they must
   not rely on catching the resulting IntegrityError as their conflict policy.

   The index is PARTIAL (`WHERE match_method <> 'rejected'`). `ffh crosswalk verify
   --reject` keeps the row as a tombstone (`match_method='rejected'`, confidence 0.0)
   recording the pairing a human threw out, so no sync can re-mint it. A tombstone is not
   a mapping and must not occupy the player's single slot for that source: without the
   predicate, rejecting a wrong id would keep the *correct* id unmappable forever.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0002"
down_revision: str | Sequence[str] | None = "0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("players", sa.Column("team_abbr", sa.Text(), nullable=True))
    op.create_index(
        "player_external_ids_source_player_uidx",
        "player_external_ids",
        ["source", "player_id"],
        unique=True,
        postgresql_where=sa.text("match_method <> 'rejected'"),
    )


def downgrade() -> None:
    op.drop_index("player_external_ids_source_player_uidx", table_name="player_external_ids")
    op.drop_column("players", "team_abbr")
