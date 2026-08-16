from fastapi import FastAPI

from ffh import __version__
from ffh.config import get_settings

app = FastAPI(title="FantasyFootballHelper", version=__version__)


def health() -> dict[str, object]:
    return {"status": "ok", "version": __version__, "season": get_settings().season}


# /healthz is the k8s liveness probe; /api/v1/healthz is the same payload reachable through the
# /api Gateway route (the UI cannot reach /healthz in production — it goes to nginx). API.md.
app.add_api_route("/healthz", health, methods=["GET"])
app.add_api_route("/api/v1/healthz", health, methods=["GET"])
