from fastapi import FastAPI

from ffh import __version__
from ffh.config import get_settings

app = FastAPI(title="FantasyFootballHelper", version=__version__)


@app.get("/health")
def health() -> dict[str, object]:
    return {"status": "ok", "version": __version__, "season": get_settings().season}
