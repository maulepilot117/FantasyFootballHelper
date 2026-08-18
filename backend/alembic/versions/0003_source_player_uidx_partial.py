"""player_external_ids_source_player_uidx: guarantee the PARTIAL predicate

Revision ID: 0003
Revises: 0002
Create Date: 2026-08-17

`0002` originally created `player_external_ids_source_player_uidx` with **no** predicate.
A later commit added `postgresql_where="match_method <> 'rejected'"` to that same revision
id — which fixes the index for a database created from scratch and does nothing at all for
one already stamped at `0002`. Alembic records revisions, not their content: `alembic
upgrade head` on such a database (the local dev DB, and any environment that ran the
earlier build) is a no-op, so it silently keeps the NON-partial index.

That is not cosmetic. Without the predicate a `rejected` tombstone occupies the player's one
slot for its source, so the legitimate tombstone+live pair that `ffh crosswalk verify
--reject` then `ffh crosswalk map` produces is rejected by the database: the *correct* id
becomes unmappable forever, and `apply_playerids`' displacement path hits an IntegrityError
it is explicitly designed never to hit. The behaviour also differs between two databases at
the same revision, which is the failure mode migrations exist to prevent.

This revision drops and recreates the index with the predicate. It is idempotent for a
database that already has the partial form (drop + identical create). `downgrade()` restores
the non-partial index, i.e. exactly what `0002`-as-originally-written left behind.

--- Operator pre-flight -----------------------------------------------------------------
`CREATE UNIQUE INDEX` fails outright on pre-existing duplicates and there is no fixup here,
so scan first and resolve any hits by hand (`ffh crosswalk verify <source> <id> --reject`
or `ffh crosswalk map`) BEFORE upgrading:

    SELECT source, player_id, count(*), array_agg(external_id)
    FROM player_external_ids
    WHERE match_method <> 'rejected'
    GROUP BY source, player_id
    HAVING count(*) > 1;

Zero rows = safe to upgrade. Note the `WHERE` clause: rows a human rejected are NOT
mappings and are expected to duplicate.

--- Post-deploy verification ------------------------------------------------------------
The predicate is load-bearing and `alembic check` is blind to it (verified on alembic
1.19.0 / SQLAlchemy 2.0.51: deleting `postgresql_where` from the model still yields
`compare_metadata(...) == []`). Assert it directly against the live catalog:

    SELECT indexdef FROM pg_indexes
    WHERE indexname = 'player_external_ids_source_player_uidx';

The output MUST contain `WHERE (match_method <> 'rejected'::text)`. If it does not, this
migration did not run — do not treat the deployment as complete.
(`tests/db/test_crosswalk_constraints.py::test_source_player_uidx_predicate_matches_the_model_and_the_database`
is the automated form of the same check.)
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0003"
down_revision: str | Sequence[str] | None = "0002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

INDEX_NAME = "player_external_ids_source_player_uidx"
TABLE_NAME = "player_external_ids"
PREDICATE = "match_method <> 'rejected'"


def upgrade() -> None:
    # if_exists: a database stamped at 0002 by the *original* migration has the index under
    # this name; one that somehow lacks it entirely must still be able to upgrade.
    op.drop_index(INDEX_NAME, table_name=TABLE_NAME, if_exists=True)
    op.create_index(
        INDEX_NAME,
        TABLE_NAME,
        ["source", "player_id"],
        unique=True,
        postgresql_where=sa.text(PREDICATE),
    )


def downgrade() -> None:
    op.drop_index(INDEX_NAME, table_name=TABLE_NAME, if_exists=True)
    op.create_index(INDEX_NAME, TABLE_NAME, ["source", "player_id"], unique=True)
