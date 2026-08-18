import subprocess
import sys
from pathlib import Path

import pytest
from sqlalchemy import inspect, text

from ffh.config import get_settings
from ffh.db.engine import make_engine
from tests.db._guard import assert_test_database

pytestmark = pytest.mark.db

BACKEND_DIR = Path(__file__).resolve().parents[2]


def _alembic(*args: str) -> subprocess.CompletedProcess[str]:
    url = get_settings().test_database_url
    return subprocess.run(
        [sys.executable, "-m", "alembic", "-x", f"url={url}", *args],
        cwd=BACKEND_DIR,
        capture_output=True,
        text=True,
        check=False,
    )


def _reset_schema() -> None:
    engine = make_engine(assert_test_database(get_settings().test_database_url))
    with engine.begin() as conn:
        conn.execute(text("DROP SCHEMA public CASCADE; CREATE SCHEMA public;"))
    engine.dispose()


@pytest.fixture(autouse=True)
def _leave_schema_at_head():
    """Other tests (db_session fixture) expect ffh_test at head after this module runs."""
    yield
    _reset_schema()
    assert _alembic("upgrade", "head").returncode == 0


def test_upgrade_head_creates_all_tables_and_check_reports_no_drift():
    _reset_schema()
    up = _alembic("upgrade", "head")
    assert up.returncode == 0, up.stderr
    engine = make_engine(get_settings().test_database_url)
    tables = set(inspect(engine).get_table_names())
    assert {
        "players",
        "player_external_ids",
        "games",
        "leagues",
        "draft_picks",
        "adp",
        "projections",
        "recommendations",
        "ai_debates",
        "ingest_runs",
    } <= tables
    check = _alembic("check")
    assert check.returncode == 0, check.stdout + check.stderr


def test_downgrade_base_removes_everything():
    _reset_schema()
    assert _alembic("upgrade", "head").returncode == 0
    down = _alembic("downgrade", "base")
    assert down.returncode == 0, down.stderr
    engine = make_engine(get_settings().test_database_url)
    assert set(inspect(engine).get_table_names()) == {"alembic_version"}


# ---------------------------------------------------------------------------
# Fix wave B: `0002` was mutated in place, so a database already stamped at it kept the
# NON-partial index. `0003` is what actually guarantees the predicate everywhere.
# ---------------------------------------------------------------------------

UIDX = "player_external_ids_source_player_uidx"
UIDX_PREDICATE = "match_method <> 'rejected'"
VERSIONS_DIR = BACKEND_DIR / "alembic" / "versions"


def _index_predicate(conn) -> str | None:
    """The normalized `WHERE` clause of the live index, or None if it is not partial."""
    raw = conn.execute(
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
    if raw is None:
        return None
    # Postgres re-prints the expression with parens and an explicit cast.
    return raw.replace("(", "").replace(")", "").replace("::text", "")


def test_a_database_stamped_at_0002_gets_the_partial_index_on_upgrade():
    """The exact state of the local dev DB and any environment that ran the earlier build.

    `0002` originally created the index with NO predicate; the predicate was added to that
    same revision id later. Alembic records revisions, not their content, so `upgrade head`
    was a no-op there and the non-partial index survived — silently forbidding the
    legitimate tombstone+live pair that `verify --reject` then `map` produces, and making
    two databases at the same revision behave differently.
    """
    _reset_schema()
    assert _alembic("upgrade", "0002").returncode == 0
    engine = make_engine(get_settings().test_database_url)
    with engine.begin() as conn:
        # Recreate exactly what 0002-as-originally-written left behind.
        conn.execute(text(f"DROP INDEX {UIDX}"))
        conn.execute(text(f"CREATE UNIQUE INDEX {UIDX} ON player_external_ids (source, player_id)"))
        assert _index_predicate(conn) is None, "precondition: the non-partial index"

    up = _alembic("upgrade", "head")
    assert up.returncode == 0, up.stderr

    with engine.connect() as conn:
        assert _index_predicate(conn) == UIDX_PREDICATE
    check = _alembic("check")
    assert check.returncode == 0, check.stdout + check.stderr
    engine.dispose()


def test_0003_downgrade_restores_the_non_partial_index_and_upgrade_re_applies_it():
    _reset_schema()
    assert _alembic("upgrade", "head").returncode == 0
    engine = make_engine(get_settings().test_database_url)
    with engine.connect() as conn:
        assert _index_predicate(conn) == UIDX_PREDICATE

    assert _alembic("downgrade", "0002").returncode == 0
    with engine.connect() as conn:
        assert _index_predicate(conn) is None

    assert _alembic("upgrade", "head").returncode == 0
    with engine.connect() as conn:
        assert _index_predicate(conn) == UIDX_PREDICATE
    engine.dispose()


def test_the_index_migrations_document_the_preflight_and_verification_queries():
    """A unique index is created with no pre-flight duplicate check and no fixup path: on
    pre-existing duplicates `CREATE UNIQUE INDEX` simply fails the deploy. The operator
    needs the scan that finds them and the query that proves the predicate landed — and the
    only place they will look is the migration that creates the index."""
    preflight = "FROM player_external_ids"
    having = "HAVING count(*) > 1"
    verification = f"WHERE indexname = '{UIDX}'"
    for name in ("0002_players_team_abbr.py", "0003_source_player_uidx_partial.py"):
        doc = (VERSIONS_DIR / name).read_text(encoding="utf-8")
        assert "SELECT source, player_id, count(*), array_agg(external_id)" in doc, name
        assert preflight in doc and having in doc, name
        assert f"WHERE {UIDX_PREDICATE}" in doc, name
        assert "SELECT indexdef FROM pg_indexes" in doc, name
        assert verification in doc, name
