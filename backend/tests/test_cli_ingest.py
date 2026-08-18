import json

import pytest
from typer.testing import CliRunner

from ffh.cli import app
from ffh.ingest.base import IngestRunResult

runner = CliRunner()


def test_ingest_list_shows_every_registered_job():
    result = runner.invoke(app, ["ingest", "list"])
    assert result.exit_code == 0, result.stdout
    for name in (
        "nflverse_players",
        "nflverse_stats_player_week",
        "nflverse_snap_counts",
        "nflverse_depth_charts",
        "nflverse_injuries",
        "nflverse_pbp",
        "nfldata_games",
        "stadiums",
        "dynastyprocess_playerids",
    ):
        assert name in result.stdout
    assert "no ingest jobs registered" not in result.stdout


def test_ingest_run_rejects_an_unknown_job():
    result = runner.invoke(app, ["ingest", "run", "not_a_job"])
    assert result.exit_code != 0
    assert "not_a_job" in result.output  # BadParameter goes to stderr; .output merges both


def test_ingest_run_prints_json_and_exits_zero_on_success(monkeypatch):
    captured = {}

    def fake_run(self, session, lake_root):
        captured["season"] = self.season
        captured["lake_root"] = lake_root
        return IngestRunResult(status="success", rows_written=272, output_path="/lake/x.parquet")

    monkeypatch.setattr("ffh.ingest.games.NfldataGamesJob.run", fake_run)
    monkeypatch.setattr("ffh.cli._session_scope", _fake_session_scope)

    result = runner.invoke(app, ["ingest", "run", "nfldata_games", "--season", "2026"])
    assert result.exit_code == 0, result.stdout
    # The WHOLE of stdout, not `.splitlines()[-1]`: `ffh ingest run` writes one JSON object
    # to stdout and nothing else. The last-line workaround existed because structlog's
    # unconfigured sink dumped log lines onto the same stream (ffh.log).
    payload = json.loads(result.stdout)
    assert payload == {
        "job": "nfldata_games",
        "status": "success",
        "rows_written": 272,
        "output_path": "/lake/x.parquet",
        "error": None,
    }
    assert captured["season"] == 2026


def test_ingest_run_exits_one_on_failed(monkeypatch):
    monkeypatch.setattr(
        "ffh.ingest.games.NfldataGamesJob.run",
        lambda self, session, lake_root: IngestRunResult(status="failed", error="boom"),
    )
    monkeypatch.setattr("ffh.cli._session_scope", _fake_session_scope)
    result = runner.invoke(app, ["ingest", "run", "nfldata_games"])
    assert result.exit_code == 1
    assert "boom" in result.stdout


def test_ingest_run_exits_zero_on_skipped(monkeypatch):
    monkeypatch.setattr(
        "ffh.ingest.nflverse.NflversePbpJob.run",
        lambda self, session, lake_root: IngestRunResult(status="skipped", error="404"),
    )
    monkeypatch.setattr("ffh.cli._session_scope", _fake_session_scope)
    result = runner.invoke(app, ["ingest", "run", "nflverse_pbp", "--season", "2026"])
    assert result.exit_code == 0
    assert "skipped" in result.stdout


def test_ingest_run_defaults_season_to_settings(monkeypatch):
    seen = {}

    def fake_run(self, session, lake_root):
        seen["season"] = self.season
        return IngestRunResult(status="success", rows_written=1)

    monkeypatch.setattr("ffh.ingest.nflverse.NflverseInjuriesJob.run", fake_run)
    monkeypatch.setattr("ffh.cli._session_scope", _fake_session_scope)
    runner.invoke(app, ["ingest", "run", "nflverse_injuries"])
    assert seen["season"] == 2026  # Settings.season default


@pytest.mark.db
def test_ingest_seed_creates_teams_stadiums_and_generic_league(monkeypatch, migrated_engine):
    from sqlalchemy import select

    from ffh.config import get_settings
    from ffh.db.models import GENERIC_LEAGUE_ID, League, NflTeam

    # `ffh ingest seed` opens a real session on FFH_DATABASE_URL; point it at the migrated
    # test database rather than the compose default `ffh`.
    monkeypatch.setenv("FFH_DATABASE_URL", get_settings().test_database_url)
    get_settings.cache_clear()

    calls = []
    monkeypatch.setattr(
        "ffh.ingest.reference.StadiumsJob.run",
        lambda self, session, lake_root: (
            calls.append("stadiums") or IngestRunResult(status="success", rows_written=62)
        ),
    )
    result = runner.invoke(app, ["ingest", "seed"])
    assert result.exit_code == 0, result.stdout
    assert calls == ["stadiums"]

    from ffh.db.engine import make_engine, make_session_factory

    with make_session_factory(make_engine(get_settings().test_database_url))() as session:
        assert len(list(session.scalars(select(NflTeam)))) == 32
        assert session.get(League, GENERIC_LEAGUE_ID) is not None


class _FakeSession:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def commit(self):
        return None

    def rollback(self):
        return None


def _fake_session_scope():
    return _FakeSession()
