"""`ffh league load` — the operator's entry point into ffh.ingest.platform_sync.

Nothing here touches Postgres or the network: `_session_scope` (③) and `load_league`
(Task 7) are both patched out. The database-backed proof that a real league loads lives in
tests/ingest/test_platform_sync.py and tests/ingest/test_crosswalk_coverage.py.
"""

import uuid

import pytest
from sqlalchemy.exc import OperationalError
from typer.testing import CliRunner

from ffh.adapters.base import PlatformError
from ffh.cli import app
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


class _FakeClient:
    """Records construction and closure so the per-invocation lifetime is testable.

    `load_league` runs and closes its own event loop, so a client that outlives one
    invocation holds a connection pool bound to a dead loop (see `load_league`'s docstring).
    respx would hide that in the DB tests, so the CLI's half of the contract — build one
    client per invocation, close it on every path — is asserted here instead.
    """

    def __init__(self, *args, **kwargs):
        self.closed = False

    async def aclose(self):
        self.closed = True


class _CliHarness:
    """The patched-out world one `ffh league load` invocation runs in."""

    def __init__(self):
        self.calls: list[dict] = []
        self.sessions: list[_NullSession] = []
        self.clients: list[_FakeClient] = []
        self.result = _report()
        self.raises: Exception | None = None

    def load_league(self, session, adapter, external_id, season, week=None):
        self.calls.append(
            {
                "session": session,
                "adapter": adapter,
                "external_id": external_id,
                "season": season,
                "week": week,
            }
        )
        if self.raises is not None:
            raise self.raises
        return self.result

    def session_scope(self):
        self.sessions.append(_NullSession())
        return self.sessions[-1]

    def client(self, *args, **kwargs):
        self.clients.append(_FakeClient(*args, **kwargs))
        return self.clients[-1]


@pytest.fixture
def harness(monkeypatch):
    import ffh.cli as cli

    h = _CliHarness()
    monkeypatch.setattr(cli, "load_league", h.load_league)
    monkeypatch.setattr(cli, "_session_scope", h.session_scope)
    monkeypatch.setattr(cli, "SleeperClient", h.client)
    return h


def test_load_prints_the_report_and_exits_zero_when_everything_resolves(harness):
    result = runner.invoke(
        app, ["league", "load", "sleeper", "L1", "--season", "2026", "--week", "1"]
    )
    assert result.exit_code == 0, result.stdout + result.stderr
    call = harness.calls[0]
    assert (call["external_id"], call["season"], call["week"]) == ("L1", 2026, 1)
    assert "teams=2" in result.stdout and "rostered=23" in result.stdout
    assert "drafts=1" in result.stdout and "picks=4" in result.stdout
    assert "unmatched=0" in result.stdout and "pending_review=0" in result.stdout
    assert harness.sessions[0].commits == 1


def test_load_defaults_the_season_to_the_configured_one(harness, monkeypatch):
    monkeypatch.setenv("FFH_SEASON", "2031")
    result = runner.invoke(app, ["league", "load", "sleeper", "L1"])
    assert result.exit_code == 0, result.stdout + result.stderr
    assert (harness.calls[0]["season"], harness.calls[0]["week"]) == (2031, None)


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
    """`load_league` owns an event loop per call, so an httpx pool cannot outlive one
    invocation. Two runs must therefore see two clients, both closed."""
    for _ in range(2):
        assert runner.invoke(app, ["league", "load", "sleeper", "L1"]).exit_code == 0
    assert len(harness.clients) == 2
    assert [c.closed for c in harness.clients] == [True, True]
    adapters = [call["adapter"] for call in harness.calls]
    assert adapters[0] is not adapters[1]
    assert [a._client for a in adapters] == harness.clients


def test_the_adapter_carries_the_lake_catalog_and_my_user_id(harness, monkeypatch):
    """No silent degradation to id-only refs: the catalog is always attached, so a missing
    lake partition raises with the `ffh ingest run sleeper_players` remedy."""
    from ffh.adapters.sleeper.catalog import LakePlayerCatalog

    monkeypatch.setenv("FFH_SLEEPER_USER_ID", "USER_ME")
    assert runner.invoke(app, ["league", "load", "sleeper", "L1"]).exit_code == 0
    adapter = harness.calls[0]["adapter"]
    assert adapter._my_user_id == "USER_ME"
    assert isinstance(adapter._catalog, LakePlayerCatalog)
