"""`ffh league load` — the operator's entry point into ffh.ingest.platform_sync.

Nothing here touches Postgres or the network: `_session_scope` (③), `advisory_lock` and
both halves of the sync (`fetch_snapshot` / `persist_snapshot`) are patched out. The
database-backed proof that a real league loads lives in tests/ingest/test_platform_sync.py
and tests/ingest/test_crosswalk_coverage.py.
"""

import asyncio
import uuid

import pytest
from polars.exceptions import ComputeError
from sqlalchemy.exc import OperationalError
from typer.testing import CliRunner

from ffh.adapters.base import PlatformError
from ffh.cli import CROSSWALK_LOCK_KEY, app
from ffh.ingest.platform_sync import LeagueLoadReport, UnmatchedPlayer

runner = CliRunner()


def test_platforms_command_still_works():
    result = runner.invoke(app, ["league", "platforms"])
    assert result.exit_code == 0 and "sleeper" in result.stdout


def _report(**overrides):
    base = dict(
        league_id=uuid.UUID(int=1),
        teams=2,
        rostered=23,
        unmatched=[],
        pending_review=[],
        drafts=1,
        picks=4,
    )
    return LeagueLoadReport(**(base | overrides))


class _FakeLeague:
    def __init__(self, season: int = 2026):
        self.season = season


class _FakeSnapshot:
    """Only the attribute `league_load` reads off a snapshot before persisting it."""

    def __init__(self, season: int = 2026):
        self.league = _FakeLeague(season)


class _NullSession:
    """Stands in for ③'s `_session_scope()` context manager."""

    def __init__(self):
        self.commits = 0

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def commit(self):
        self.commits += 1


class _FakeUser:
    def __init__(self, user_id: str):
        self.user_id = user_id


class _FakeClient:
    """Records construction, closure, and WHICH RUNNING LOOP it was closed on.

    The loop matters: an `httpx.AsyncClient` keep-alive pool is owned by the loop that
    opened it, and `httpcore`'s pool close awaits per-connection closes wrapping anyio
    streams bound to that loop. A close driven from a second `asyncio.run` — the shape
    this CLI used to have, `aclose()` in a `finally` after the fetch's loop had already
    exited — fails with "Event loop is closed" on every real invocation and leaks the
    sockets to GC. respx replaces the transport, so no networked test can see that; this
    one can, by pinning the loop OBJECT (never `id()`, which a GC'd loop can hand back).
    """

    def __init__(self, *args, **kwargs):
        self.closed = False
        self.closed_on = None
        self.user_lookups: list[str] = []

    async def get_user(self, username_or_id: str):
        self.user_lookups.append(username_or_id)
        return _FakeUser(f"ID_OF_{username_or_id}")

    async def aclose(self):
        self.closed = True
        self.closed_on = asyncio.get_running_loop()


class _CliHarness:
    """The patched-out world one `ffh league load` invocation runs in."""

    def __init__(self):
        self.fetches: list[dict] = []
        self.persists: list[dict] = []
        self.sessions: list[_NullSession] = []
        self.clients: list[_FakeClient] = []
        self.locks: list[str] = []
        self.result = _report()
        self.snapshot = _FakeSnapshot()
        self.raises: Exception | None = None
        self.persist_raises: Exception | None = None
        #: The loop `fetch_snapshot` observed — i.e. the one that would own the pool.
        self.fetched_on = None

    async def fetch_snapshot(self, adapter, external_id, week=None):
        self.fetched_on = asyncio.get_running_loop()
        self.fetches.append({"adapter": adapter, "external_id": external_id, "week": week})
        if self.raises is not None:
            raise self.raises
        return self.snapshot

    def persist_snapshot(self, session, snapshot):
        self.persists.append({"session": session, "snapshot": snapshot})
        if self.persist_raises is not None:
            raise self.persist_raises
        return self.result

    def session_scope(self):
        self.sessions.append(_NullSession())
        return self.sessions[-1]

    def advisory_lock(self, session, key):
        self.locks.append(key)
        from contextlib import nullcontext

        return nullcontext()

    def client(self, *args, **kwargs):
        self.clients.append(_FakeClient(*args, **kwargs))
        return self.clients[-1]


@pytest.fixture
def harness(monkeypatch):
    import ffh.cli as cli

    h = _CliHarness()
    monkeypatch.setattr(cli, "fetch_snapshot", h.fetch_snapshot)
    monkeypatch.setattr(cli, "persist_snapshot", h.persist_snapshot)
    monkeypatch.setattr(cli, "_session_scope", h.session_scope)
    monkeypatch.setattr(cli, "advisory_lock", h.advisory_lock)
    monkeypatch.setattr(cli, "SleeperClient", h.client)
    monkeypatch.setenv("FFH_SLEEPER_USER_ID", "USER_ME")
    return h


def test_load_prints_the_report_and_exits_zero_when_everything_resolves(harness):
    result = runner.invoke(
        app, ["league", "load", "sleeper", "L1", "--season", "2026", "--week", "1"]
    )
    assert result.exit_code == 0, result.stdout + result.stderr
    fetch = harness.fetches[0]
    assert (fetch["external_id"], fetch["week"]) == ("L1", 1)
    assert "teams=2" in result.stdout and "rostered=23" in result.stdout
    assert "drafts=1" in result.stdout and "picks=4" in result.stdout
    assert "unmatched=0" in result.stdout and "pending_review=0" in result.stdout
    assert harness.sessions[0].commits == 1


def test_load_defaults_the_season_to_the_configured_one(harness, monkeypatch):
    monkeypatch.setenv("FFH_SEASON", "2031")
    harness.snapshot = _FakeSnapshot(season=2031)
    result = runner.invoke(app, ["league", "load", "sleeper", "L1"])
    assert result.exit_code == 0, result.stdout + result.stderr
    assert harness.fetches[0]["week"] is None


def test_a_season_mismatch_exits_three(harness):
    """The check `load_league` used to own: the CLI drives fetch/persist itself now, so
    loading a 2025 league into a 2026 run has to be refused HERE or nowhere."""
    harness.snapshot = _FakeSnapshot(season=2025)
    result = runner.invoke(app, ["league", "load", "sleeper", "L1", "--season", "2026"])
    assert result.exit_code == 3
    assert "season 2025, asked for 2026" in result.stderr
    assert harness.persists == []  # nothing was written


def test_load_exits_one_and_lists_unmatched(harness):
    harness.result = _report(unmatched=[UnmatchedPlayer("9999", "Mystery Person", "WR", "KC")])
    result = runner.invoke(app, ["league", "load", "sleeper", "L1"])
    assert result.exit_code == 1
    assert "UNMATCHED 9999 Mystery Person" in result.stdout
    assert "unmatched=1" in result.stdout
    # A gate-red run still commits: ④'s resolve_many queued every unmatched id in
    # crosswalk_unmatched, and rolling that back would empty the operator's review queue.
    assert harness.sessions[0].commits == 1


def test_load_exits_one_and_lists_pending_review(harness):
    """④ rung 4: a fuzzy hit is persisted unverified and is NOT in crosswalk_unmatched, so
    the CLI must surface it separately and still refuse a clean exit."""
    harness.result = _report(
        pending_review=[UnmatchedPlayer("4881", "Lamarr Jackson", "QB", "BAL")]
    )
    result = runner.invoke(app, ["league", "load", "sleeper", "L1"])
    assert result.exit_code == 1
    assert "PENDING_REVIEW 4881 Lamarr Jackson" in result.stdout
    assert "ffh crosswalk verify sleeper 4881" in result.stdout


def test_load_rejects_an_unknown_platform():
    result = runner.invoke(app, ["league", "load", "espn", "L1"])
    # Click's own exit code for a usage error, exactly like `ffh ingest run <unknown job>`.
    assert result.exit_code == 2
    assert "espn" in result.stderr


@pytest.mark.parametrize(
    "exc",
    [
        PlatformError("sleeper 500 for /league/L1"),
        OperationalError("SELECT 1", {}, Exception("connection refused")),
        ValueError("league L1 is season 2025, asked for 2026"),
        OSError("no Sleeper player partition"),
        # LakePlayerCatalog reads the players partition with pl.read_parquet INSIDE
        # fetch_snapshot; a truncated partition raises this, and it is neither an OSError
        # nor a ValueError, so before it was listed it escaped to Click and exited 1.
        ComputeError("parquet: File out of specification: The file must end with PAR1"),
    ],
)
def test_an_operational_failure_exits_three_never_one(harness, exc):
    """Exit 1 means "the crosswalk has a gap" — a data state a human resolves. A network,
    parse or database failure must never wear that code: a cron wrapper reading 1 would
    file a crosswalk gap for a run that never happened. ④'s vocabulary: 3 = operational."""
    harness.raises = exc
    result = runner.invoke(app, ["league", "load", "sleeper", "L1"])
    assert result.exit_code == 3
    assert type(exc).__name__ in result.stderr
    assert result.stdout == ""  # stdout stays the data channel; diagnostics go to stderr
    # Released on every path, including the failure path.
    assert harness.clients[0].closed is True


def test_each_invocation_builds_and_closes_its_own_client(harness):
    """An httpx pool cannot outlive the loop that opened it, so two runs must see two
    clients, both closed."""
    for _ in range(2):
        assert runner.invoke(app, ["league", "load", "sleeper", "L1"]).exit_code == 0
    assert len(harness.clients) == 2
    assert [c.closed for c in harness.clients] == [True, True]
    adapters = [f["adapter"] for f in harness.fetches]
    assert adapters[0] is not adapters[1]
    assert [a._client for a in adapters] == harness.clients


def test_the_client_is_closed_inside_the_loop_that_opened_its_pool(harness):
    """THE cross-loop-close regression.

    `aclose()` used to run in a second `asyncio.run` from the command's `finally`, after
    the fetch's loop was already closed. `httpcore` then awaits per-connection closes on
    anyio streams bound to a dead loop — "Event loop is closed" on stderr every real
    invocation, sockets leaked to GC. Pinning the loop object the close ran on is the only
    thing a mocked-transport test suite can see, and it is enough: under the old shape
    `closed_on` is a DIFFERENT, freshly created loop.
    """
    assert runner.invoke(app, ["league", "load", "sleeper", "L1"]).exit_code == 0
    client = harness.clients[0]
    assert client.closed is True
    assert client.closed_on is harness.fetched_on
    assert harness.fetched_on is not None


def test_the_client_is_closed_in_its_own_loop_on_the_failure_path_too(harness):
    harness.raises = PlatformError("sleeper 500 for /league/L1")
    assert runner.invoke(app, ["league", "load", "sleeper", "L1"]).exit_code == 3
    assert harness.clients[0].closed_on is harness.fetched_on


def test_the_adapter_carries_the_lake_catalog_and_my_user_id(harness, monkeypatch):
    """No silent degradation to id-only refs: the catalog is always attached, so a missing
    lake partition raises with the `ffh ingest run sleeper_players` remedy."""
    from ffh.adapters.sleeper.catalog import LakePlayerCatalog

    monkeypatch.setenv("FFH_SLEEPER_USER_ID", "USER_ME")
    assert runner.invoke(app, ["league", "load", "sleeper", "L1"]).exit_code == 0
    adapter = harness.fetches[0]["adapter"]
    assert adapter._my_user_id == "USER_ME"
    assert isinstance(adapter._catalog, LakePlayerCatalog)


def test_a_username_is_resolved_to_a_user_id_when_no_user_id_is_set(harness, monkeypatch):
    """`FFH_SLEEPER_USERNAME` was advertised in .env.example and config and read by
    NOTHING: an operator who set only the username silently got a load with no team
    marked as mine. GET /user/{username} is the resolver, and it runs inside the one
    event loop because it is a network call."""
    monkeypatch.delenv("FFH_SLEEPER_USER_ID", raising=False)
    monkeypatch.setenv("FFH_SLEEPER_USERNAME", "chris")
    assert runner.invoke(app, ["league", "load", "sleeper", "L1"]).exit_code == 0
    assert harness.clients[0].user_lookups == ["chris"]
    assert harness.fetches[0]["adapter"]._my_user_id == "ID_OF_chris"


def test_the_user_id_wins_and_costs_no_extra_request(harness, monkeypatch):
    monkeypatch.setenv("FFH_SLEEPER_USER_ID", "USER_ME")
    monkeypatch.setenv("FFH_SLEEPER_USERNAME", "chris")
    assert runner.invoke(app, ["league", "load", "sleeper", "L1"]).exit_code == 0
    assert harness.clients[0].user_lookups == []
    assert harness.fetches[0]["adapter"]._my_user_id == "USER_ME"


def test_a_run_with_no_identity_at_all_warns_loudly(harness, monkeypatch):
    """The failure this warns about is otherwise silent: with no identity no team can be
    marked as mine, and the CLI would print `teams=2` with no hint that
    `leagues.my_team_id` was not maintained on this run."""
    monkeypatch.delenv("FFH_SLEEPER_USER_ID", raising=False)
    monkeypatch.delenv("FFH_SLEEPER_USERNAME", raising=False)
    result = runner.invoke(app, ["league", "load", "sleeper", "L1"])
    assert result.exit_code == 0
    assert "FFH_SLEEPER_USER_ID" in result.stderr and "left unchanged" in result.stderr
    assert harness.fetches[0]["adapter"]._my_user_id is None
    assert result.stdout.startswith("league ")  # the warning stayed off stdout


def test_the_persist_half_runs_under_the_crosswalk_advisory_lock(harness):
    """`ffh league load` is the FIFTH crosswalk writer: persist_snapshot reaches ④'s
    resolve_many -> _persist, which pre-checks the unique (source, player_id) index and
    then writes. Without the lock an overlapping `ffh crosswalk seed` can race it into
    that index. The FETCH stays outside — it holds no session and touches no database."""
    assert runner.invoke(app, ["league", "load", "sleeper", "L1"]).exit_code == 0
    assert harness.locks == [CROSSWALK_LOCK_KEY]
    # ...and it is the same lock every `ffh crosswalk` write command takes.
    assert CROSSWALK_LOCK_KEY == "ffh.crosswalk/apply"


def test_a_failing_client_close_never_replaces_the_result(harness, monkeypatch):
    """A raise inside `finally` REPLACES the exception on its way out — it would swallow
    the operational error and its exit code, or the typer.Exit carrying the gate verdict.
    The close is therefore guarded, and its failure is a warning on stderr."""

    class _AngryClient(_FakeClient):
        async def aclose(self):
            raise RuntimeError("event loop is closed")

    monkeypatch.setattr("ffh.cli.SleeperClient", lambda *a, **k: _AngryClient())
    harness.result = _report(unmatched=[UnmatchedPlayer("9999", "Mystery Person", "WR", "KC")])
    result = runner.invoke(app, ["league", "load", "sleeper", "L1"])
    assert result.exit_code == 1  # the gate verdict survived the failed close
    assert "UNMATCHED 9999 Mystery Person" in result.stdout
    assert "closing the Sleeper client failed" in result.stderr
