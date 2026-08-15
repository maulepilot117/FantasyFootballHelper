from fastapi.testclient import TestClient

from ffh import __version__
from ffh.api.app import app


def test_health_reports_ok_version_and_season(monkeypatch):
    monkeypatch.setenv("FFH_SEASON", "2026")
    with TestClient(app) as client:
        r = client.get("/healthz")
    assert r.status_code == 200
    assert r.json() == {"status": "ok", "version": __version__, "season": 2026}
