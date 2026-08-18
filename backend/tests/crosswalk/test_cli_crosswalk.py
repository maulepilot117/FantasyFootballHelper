import json
import uuid
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path

import polars as pl
import pytest
from sqlalchemy import select
from typer.testing import CliRunner

import ffh.cli as cli
from ffh.crosswalk.dynastyprocess import CrosswalkConflictError
from ffh.crosswalk.report import CoverageReport, UnmatchedRow, UnverifiedRow

runner = CliRunner()

FIXTURE = (
    Path(__file__).resolve().parents[1] / "fixtures" / "dynastyprocess" / "db_playerids_sample.csv"
)


def _fake_report(unmatched: int, *, seeded: bool = True) -> CoverageReport:
    rows = tuple(
        UnmatchedRow("sleeper", str(i), "Nobody", "QB", "FA", datetime.now(UTC), datetime.now(UTC))
        for i in range(unmatched)
    )
    return CoverageReport(
        players_total=1 if seeded else 0,
        players_by_position={"QB": 1} if seeded else {},
        # A report with no external ids at all is NOT seeded — see the emptiness floor in
        # report.CoverageReport.seeded; a "clean" fake must therefore carry one.
        ids_by_source={"sleeper": 1} if seeded else {},
        ids_by_source_method={"sleeper": {"dynastyprocess": 1}} if seeded else {},
        unverified_low_confidence=(),
        unmatched=rows,
    )


def test_report_exit_0_when_clean(monkeypatch):
    monkeypatch.setattr(cli, "_coverage_report_for_cli", lambda: _fake_report(0))
    result = runner.invoke(cli.app, ["crosswalk", "report"])
    assert result.exit_code == 0, result.output
    assert "unmatched: 0" in result.output


def test_report_exit_1_when_unmatched(monkeypatch):
    monkeypatch.setattr(cli, "_coverage_report_for_cli", lambda: _fake_report(2))
    result = runner.invoke(cli.app, ["crosswalk", "report"])
    assert result.exit_code == 1
    assert "unmatched: 2" in result.output


def test_report_json(monkeypatch):
    monkeypatch.setattr(cli, "_coverage_report_for_cli", lambda: _fake_report(1))
    result = runner.invoke(cli.app, ["crosswalk", "report", "--json"])
    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert payload["ok"] is False and len(payload["unmatched"]) == 1


def test_report_exit_1_when_unverified_only(monkeypatch):
    """`ok` must consider unverified low-confidence rows even with zero unmatched rows."""
    rep = CoverageReport(
        players_total=1,
        players_by_position={"QB": 1},
        ids_by_source={"sleeper": 1},
        ids_by_source_method={"sleeper": {"fuzzy": 1}},
        unverified_low_confidence=(
            UnverifiedRow(
                "sleeper", "4881", uuid.uuid4(), "Lamar Jackson", "QB", 0.89, datetime.now(UTC)
            ),
        ),
        unmatched=(),
    )
    monkeypatch.setattr(cli, "_coverage_report_for_cli", lambda: rep)
    result = runner.invoke(cli.app, ["crosswalk", "report"])
    assert result.exit_code == 1
    assert "unverified low-confidence: 1" in result.output


def test_report_exit_1_on_an_empty_crosswalk_and_0_with_allow_empty(monkeypatch):
    """Finding 19: nothing seeded = nothing unmatched = exit 0, on the exact database
    state where every downstream lookup silently finds no player."""
    monkeypatch.setattr(cli, "_coverage_report_for_cli", lambda: _fake_report(0, seeded=False))
    result = runner.invoke(cli.app, ["crosswalk", "report"])
    assert result.exit_code == 1, result.output
    assert "NOT SEEDED" in result.output

    allowed = runner.invoke(cli.app, ["crosswalk", "report", "--allow-empty"])
    assert allowed.exit_code == 0, allowed.output
    assert "NOT SEEDED" in allowed.output


def test_report_operational_failure_exits_3(monkeypatch):
    """A database outage must not look like a crosswalk gap: exit 1 is the gate signal,
    exit 3 says the report could not be produced at all."""
    from sqlalchemy.exc import OperationalError

    def boom():
        raise OperationalError("select 1", {}, Exception("connection refused"))

    monkeypatch.setattr(cli, "_coverage_report_for_cli", boom)
    result = runner.invoke(cli.app, ["crosswalk", "report"])
    assert result.exit_code == cli.EXIT_OPERATIONAL
    assert "crosswalk report failed" in result.output


def test_crosswalk_help_lists_commands():
    result = runner.invoke(cli.app, ["crosswalk", "--help"])
    assert result.exit_code == 0
    for cmd in ("report", "seed", "verify", "map", "resolve-unmatched"):
        assert cmd in result.output


def test_seed_without_players_and_empty_lake_exits_3(monkeypatch, tmp_path):
    """Operational, not a gate signal: the data never arrived (exit 3, not 1)."""
    monkeypatch.setenv("FFH_LAKE_ROOT", str(tmp_path))
    result = runner.invoke(cli.app, ["crosswalk", "seed"])
    assert result.exit_code == cli.EXIT_OPERATIONAL
    assert "ffh ingest run nflverse_players" in result.output


class _FakeSession:
    def __init__(self):
        self.commits = 0

    def commit(self):
        self.commits += 1


def _fake_scope(session):
    @contextmanager
    def scope():
        yield session

    return scope


def _players_parquet(tmp_path: Path) -> Path:
    path = tmp_path / "players.parquet"
    pl.DataFrame({"gsis_id": ["00-0000001"]}).write_parquet(path)
    return path


def test_seed_with_players_commits(monkeypatch, tmp_path):
    session = _FakeSession()
    monkeypatch.setattr(cli, "_session_scope", _fake_scope(session))
    monkeypatch.setattr("ffh.crosswalk.registry.seed_players", lambda s, frame: 46)
    players = _players_parquet(tmp_path)
    result = runner.invoke(cli.app, ["crosswalk", "seed", "--players", str(players)])
    assert result.exit_code == 0, result.output
    assert "46" in result.output
    assert session.commits == 1


def test_seed_conflict_exits_2_without_commit(monkeypatch, tmp_path):
    session = _FakeSession()
    monkeypatch.setattr(cli, "_session_scope", _fake_scope(session))
    monkeypatch.setattr("ffh.crosswalk.registry.seed_players", lambda s, frame: 46)

    def boom(s, frame):
        raise CrosswalkConflictError([("sleeper", "4046", uuid.uuid4(), uuid.uuid4())])

    monkeypatch.setattr("ffh.crosswalk.dynastyprocess.apply_playerids", boom)
    players = _players_parquet(tmp_path)
    result = runner.invoke(
        cli.app,
        ["crosswalk", "seed", "--players", str(players), "--playerids", str(FIXTURE)],
    )
    assert result.exit_code == 2, result.output
    assert "sleeper:4046" in result.output
    assert session.commits == 0


def test_seed_malformed_playerids_csv_exits_3(monkeypatch, tmp_path):
    """A malformed CSV is an operational failure, not "the crosswalk has a gap": before
    the fix `DynastyProcessError` escaped the guarded region and exited 1, so a cron
    wrapper could not tell an outage from a red gate."""
    session = _FakeSession()
    monkeypatch.setattr(cli, "_session_scope", _fake_scope(session))
    monkeypatch.setattr("ffh.crosswalk.registry.seed_players", lambda s, frame: 46)
    bad = tmp_path / "playerids.csv"
    bad.write_text("mfl_id,gsis_id,name,position,team\n1,NA,x,QB,FA\n", encoding="utf-8")
    result = runner.invoke(
        cli.app,
        [
            "crosswalk",
            "seed",
            "--players",
            str(_players_parquet(tmp_path)),
            "--playerids",
            str(bad),
        ],
    )
    assert result.exit_code == cli.EXIT_OPERATIONAL, result.output
    assert "crosswalk seed failed: DynastyProcessError" in result.output
    assert session.commits == 0


def test_seed_unreadable_players_parquet_exits_3(monkeypatch, tmp_path):
    """Same for a truncated/garbage partition: the read is inside the guarded region."""
    session = _FakeSession()
    monkeypatch.setattr(cli, "_session_scope", _fake_scope(session))
    truncated = tmp_path / "players.parquet"
    truncated.write_bytes(b"PAR1-not-really-a-parquet-file")
    result = runner.invoke(cli.app, ["crosswalk", "seed", "--players", str(truncated)])
    assert result.exit_code == cli.EXIT_OPERATIONAL, result.output
    assert "crosswalk seed failed" in result.output
    assert session.commits == 0


def test_verify_unknown_row_exits_1(monkeypatch):
    monkeypatch.setattr(cli, "_session_scope", _fake_scope(_FakeSession()))
    monkeypatch.setattr("ffh.crosswalk.review.verify_mapping", lambda s, src, ext: False)
    result = runner.invoke(cli.app, ["crosswalk", "verify", "sleeper", "nope"])
    assert result.exit_code == 1
    assert "no crosswalk row for sleeper:nope" in result.output


@pytest.mark.db
def test_cli_verify_marks_queue_entry_resolved(monkeypatch, db_session, seeded_registry):
    from ffh.crosswalk.resolve import resolve, upsert_unmatched
    from ffh.db.models import CrosswalkUnmatched, PlayerExternalId

    resolve(db_session, "sleeper", "4881", "Lamarr Jackson", "QB", "BAL")  # fuzzy pending
    upsert_unmatched(db_session, "sleeper", "4881", raw_name="Lamarr Jackson")
    monkeypatch.setattr(cli, "_session_scope", _fake_scope(db_session))

    result = runner.invoke(cli.app, ["crosswalk", "verify", "sleeper", "4881"])
    assert result.exit_code == 0, result.output
    assert "verified sleeper:4881" in result.output
    row = db_session.get(PlayerExternalId, ("sleeper", "4881"))
    assert row is not None and row.verified_at is not None
    u = db_session.scalar(
        select(CrosswalkUnmatched).where(CrosswalkUnmatched.external_id == "4881")
    )
    assert u is not None and u.resolved is True


@pytest.mark.db
def test_cli_verify_reject_round_trip(monkeypatch, db_session, seeded_registry):
    """`ffh crosswalk verify --reject` tombstones the disputed mapping and leaves the
    review-queue entry OPEN — the id is now unmapped and still needs attention
    (fix-round-1 ruling; the positive `verify` path is what closes the entry)."""
    from ffh.crosswalk.resolve import resolve, upsert_unmatched
    from ffh.db.models import CrosswalkUnmatched, PlayerExternalId

    resolve(db_session, "sleeper", "4881", "Lamarr Jackson", "QB", "BAL")  # fuzzy pending
    upsert_unmatched(db_session, "sleeper", "4881", raw_name="Lamarr Jackson")
    monkeypatch.setattr(cli, "_session_scope", _fake_scope(db_session))

    result = runner.invoke(cli.app, ["crosswalk", "verify", "sleeper", "4881", "--reject"])
    assert result.exit_code == 0, result.output
    assert "rejected sleeper:4881" in result.output
    tomb = db_session.get(PlayerExternalId, ("sleeper", "4881"))
    assert tomb is not None and tomb.match_method == "rejected" and tomb.confidence == 0.0
    u = db_session.scalar(
        select(CrosswalkUnmatched).where(CrosswalkUnmatched.external_id == "4881")
    )
    assert u is not None and u.raw_name == "Lamarr Jackson" and u.resolved is False


def test_resolve_unmatched_unknown_entry_exits_1(monkeypatch):
    monkeypatch.setattr(cli, "_session_scope", _fake_scope(_FakeSession()))
    monkeypatch.setattr(
        "ffh.crosswalk.review.mark_unmatched_resolved", lambda s, src, ext, force=False: False
    )
    monkeypatch.setattr("ffh.crosswalk.review.live_mapping", lambda s, src, ext: None)
    result = runner.invoke(cli.app, ["crosswalk", "resolve-unmatched", "sleeper", "nope"])
    assert result.exit_code == 1
    assert "no crosswalk_unmatched row for sleeper:nope" in result.output


@pytest.mark.db
def test_cli_resolve_unmatched_closes_queue_entry(monkeypatch, db_session, seeded_registry):
    """Rung-5 ids with no mapping row (retired / non-NFL) need an operator path back to
    exit 0: `ffh crosswalk resolve-unmatched` closes the queue entry directly."""
    from ffh.crosswalk.report import coverage_report
    from ffh.crosswalk.resolve import resolve, upsert_unmatched
    from ffh.db.models import CrosswalkUnmatched

    resolve(db_session, "sleeper", "4046", "Patrick Mahomes", "QB", "KC")  # a seeded crosswalk
    upsert_unmatched(db_session, "sleeper", "99999", raw_name="Nobody Nowhere")
    assert coverage_report(db_session).ok is False
    monkeypatch.setattr(cli, "_session_scope", _fake_scope(db_session))

    result = runner.invoke(cli.app, ["crosswalk", "resolve-unmatched", "sleeper", "99999"])
    assert result.exit_code == 0, result.output
    assert "resolved unmatched sleeper:99999" in result.output
    u = db_session.scalar(
        select(CrosswalkUnmatched).where(CrosswalkUnmatched.external_id == "99999")
    )
    assert u is not None and u.resolved is True
    assert coverage_report(db_session).ok is True


# ---------------------------------------------------------------------------
# `ffh crosswalk map` — the operator path from crosswalk_unmatched to *mapped*.
# Without it an id in the queue could only be silenced (`resolve-unmatched`),
# never mapped, and a `--reject` tombstone had no escape at all.
# ---------------------------------------------------------------------------


@pytest.mark.db
def test_cli_map_creates_a_manual_mapping_and_closes_the_queue(
    monkeypatch, db_session, seeded_registry
):
    from ffh.crosswalk.report import coverage_report
    from ffh.crosswalk.resolve import Resolution, resolve, upsert_unmatched
    from ffh.db.models import CrosswalkUnmatched, PlayerExternalId

    mahomes = seeded_registry["00-0033873"]
    upsert_unmatched(db_session, "sleeper", "4046", raw_name="P. Mahomes")
    assert coverage_report(db_session).ok is False
    monkeypatch.setattr(cli, "_session_scope", _fake_scope(db_session))

    result = runner.invoke(cli.app, ["crosswalk", "map", "sleeper", "4046", str(mahomes)])
    assert result.exit_code == 0, result.output
    assert "mapped sleeper:4046" in result.output
    row = db_session.get(PlayerExternalId, ("sleeper", "4046"))
    assert row.player_id == mahomes and row.match_method == "manual"
    assert row.confidence == 1.0 and row.verified_at is not None
    u = db_session.scalar(
        select(CrosswalkUnmatched).where(CrosswalkUnmatched.external_id == "4046")
    )
    assert u.resolved is True
    assert coverage_report(db_session).ok is True
    assert resolve(db_session, "sleeper", "4046") == Resolution(mahomes, "manual", 1.0)


@pytest.mark.db
def test_cli_map_is_the_escape_from_a_rejection_tombstone(monkeypatch, db_session, seeded_registry):
    """The rung-4 red loop from finding 4: a rejected fuzzy row is re-minted by every
    sync unless the rejection is durable — and once it is, `map` is how green happens."""
    from ffh.crosswalk.report import coverage_report
    from ffh.crosswalk.resolve import resolve
    from ffh.db.models import PlayerExternalId

    lamar = seeded_registry["00-0034796"]
    resolve(db_session, "sleeper", "4881", "Lamarr Jackson", "QB", "BAL")  # fuzzy pending
    monkeypatch.setattr(cli, "_session_scope", _fake_scope(db_session))
    assert (
        runner.invoke(cli.app, ["crosswalk", "verify", "sleeper", "4881", "--reject"]).exit_code
        == 0
    )
    # Re-running the same sync does NOT re-mint the rejected row, and the gate stays red.
    assert resolve(db_session, "sleeper", "4881", "Lamarr Jackson", "QB", "BAL") is None
    assert db_session.get(PlayerExternalId, ("sleeper", "4881")).match_method == "rejected"
    assert coverage_report(db_session).ok is False

    result = runner.invoke(cli.app, ["crosswalk", "map", "sleeper", "4881", str(lamar)])
    assert result.exit_code == 0, result.output
    assert "(replaced)" in result.output
    row = db_session.get(PlayerExternalId, ("sleeper", "4881"))
    assert row.match_method == "manual" and row.player_id == lamar
    assert coverage_report(db_session).ok is True


@pytest.mark.db
def test_cli_map_refuses_an_unknown_player(monkeypatch, db_session):
    monkeypatch.setattr(cli, "_session_scope", _fake_scope(db_session))
    ghost = uuid.uuid4()
    result = runner.invoke(cli.app, ["crosswalk", "map", "sleeper", "1", str(ghost)])
    assert result.exit_code == 1
    assert "no players row" in result.output


@pytest.mark.db
def test_cli_map_reports_the_source_player_clash_instead_of_raising(
    monkeypatch, db_session, seeded_registry
):
    """`player_external_ids_source_player_uidx` is pre-checked, never caught as an
    IntegrityError (DATABASE.md §2) — the operator gets a message naming the holder."""
    from ffh.db.models import PlayerExternalId

    mahomes = seeded_registry["00-0033873"]
    db_session.add(
        PlayerExternalId(
            player_id=mahomes,
            source="sleeper",
            external_id="4046",
            confidence=1.0,
            match_method="dynastyprocess",
        )
    )
    db_session.flush()
    monkeypatch.setattr(cli, "_session_scope", _fake_scope(db_session))
    result = runner.invoke(cli.app, ["crosswalk", "map", "sleeper", "9999", str(mahomes)])
    assert result.exit_code == 1
    assert "sleeper:4046 already maps to" in result.output
    assert db_session.get(PlayerExternalId, ("sleeper", "9999")) is None


def test_cli_map_rejects_a_non_uuid_player_id(monkeypatch):
    monkeypatch.setattr(cli, "_session_scope", _fake_scope(_FakeSession()))
    result = runner.invoke(cli.app, ["crosswalk", "map", "sleeper", "1", "not-a-uuid"])
    assert result.exit_code != 0


@pytest.mark.db
def test_cli_resolve_unmatched_refuses_a_contradicted_live_mapping(
    monkeypatch, db_session, seeded_registry
):
    """The command's docstring promises "a queue entry that has no mapping row" and never
    checked. In the `upgrade_conflict` state the contradicted sub-1.0 row is still live, so
    closing the entry greened the gate while `resolve` kept returning the wrong player."""
    from ffh.crosswalk.report import coverage_report
    from ffh.crosswalk.resolve import resolve
    from ffh.db.models import CrosswalkUnmatched, PlayerExternalId

    mahomes, chase = seeded_registry["00-0033873"], seeded_registry["00-0036900"]
    db_session.add(
        PlayerExternalId(
            player_id=mahomes,
            source="sleeper",
            external_id="E1",
            confidence=0.95,
            match_method="exact_name",
        )
    )
    db_session.add(
        PlayerExternalId(
            player_id=chase,
            source="sleeper",
            external_id="E2",
            confidence=1.0,
            match_method="dynastyprocess",
        )
    )
    db_session.flush()
    assert resolve(db_session, "sleeper", "E1", gsis_id="00-0036900") is None  # queued
    monkeypatch.setattr(cli, "_session_scope", _fake_scope(db_session))

    result = runner.invoke(cli.app, ["crosswalk", "resolve-unmatched", "sleeper", "E1"])
    assert result.exit_code == 1, result.output
    assert "still maps to" in result.output and "--reject" in result.output
    u = db_session.scalar(select(CrosswalkUnmatched).where(CrosswalkUnmatched.external_id == "E1"))
    assert u.resolved is False
    assert coverage_report(db_session).ok is False

    forced = runner.invoke(cli.app, ["crosswalk", "resolve-unmatched", "sleeper", "E1", "--force"])
    assert forced.exit_code == 0, forced.output
    db_session.refresh(u)
    assert u.resolved is True
