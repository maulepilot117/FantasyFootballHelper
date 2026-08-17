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
from ffh.crosswalk.report import CoverageReport, UnmatchedRow

runner = CliRunner()

FIXTURE = (
    Path(__file__).resolve().parents[1] / "fixtures" / "dynastyprocess" / "db_playerids_sample.csv"
)


def _fake_report(unmatched: int) -> CoverageReport:
    rows = tuple(
        UnmatchedRow("sleeper", str(i), "Nobody", "QB", "FA", datetime.now(UTC), datetime.now(UTC))
        for i in range(unmatched)
    )
    return CoverageReport(
        players_total=1,
        players_by_position={"QB": 1},
        ids_by_source={},
        ids_by_source_method={},
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


def test_crosswalk_help_lists_commands():
    result = runner.invoke(cli.app, ["crosswalk", "--help"])
    assert result.exit_code == 0
    for cmd in ("report", "seed", "verify"):
        assert cmd in result.output


def test_seed_without_players_and_empty_lake_exits_1(monkeypatch, tmp_path):
    monkeypatch.setenv("FFH_LAKE_ROOT", str(tmp_path))
    result = runner.invoke(cli.app, ["crosswalk", "seed"])
    assert result.exit_code == 1
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
    """Controller ruling: the Task-5 conflict path leaves one key in BOTH tables.
    `ffh crosswalk verify --reject` must delete the disputed mapping AND close the
    review-queue entry, or the two tables drift permanently."""
    from ffh.crosswalk.resolve import resolve, upsert_unmatched
    from ffh.db.models import CrosswalkUnmatched, PlayerExternalId

    resolve(db_session, "sleeper", "4881", "Lamarr Jackson", "QB", "BAL")  # fuzzy pending
    upsert_unmatched(db_session, "sleeper", "4881", raw_name="Lamarr Jackson")
    monkeypatch.setattr(cli, "_session_scope", _fake_scope(db_session))

    result = runner.invoke(cli.app, ["crosswalk", "verify", "sleeper", "4881", "--reject"])
    assert result.exit_code == 0, result.output
    assert "rejected sleeper:4881" in result.output
    assert db_session.get(PlayerExternalId, ("sleeper", "4881")) is None
    u = db_session.scalar(
        select(CrosswalkUnmatched).where(CrosswalkUnmatched.external_id == "4881")
    )
    assert u is not None and u.raw_name == "Lamarr Jackson" and u.resolved is True
