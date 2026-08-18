from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime configuration. Every value comes from the environment (prefix FFH_).

    Secrets are injected by ESO in-cluster; locally they come from backend/.env (gitignored).
    """

    model_config = SettingsConfigDict(env_prefix="FFH_", env_file=".env", extra="ignore")

    database_url: str = "postgresql+psycopg://ffh:ffh@localhost:5432/ffh"
    test_database_url: str = "postgresql+psycopg://ffh:ffh@localhost:5432/ffh_test"
    redis_url: str = "redis://localhost:6379/0"
    lake_root: Path = Path("data/lake")
    sleeper_base_url: str = "https://api.sleeper.app/v1"
    # Chris's mock-draft league; used by the fixture recorder and manual smoke runs.
    # Not a secret, but it is personal — .env only, never committed.
    sleeper_mock_league_id: str | None = None
    # Identifies "my" team on a Sleeper league (`leagues.my_team_id`, `league_teams.is_me`).
    # Either is enough and user_id wins: `ffh league load` resolves the username to a user
    # id through GET /user/{username} only when user_id is unset — one extra request, and
    # the id is what every roster's `owner_id`/`co_owners` actually carries. With NEITHER
    # set, no team can be marked as mine and the load leaves the stored pointer alone.
    sleeper_user_id: str | None = None
    sleeper_username: str | None = None
    log_level: str = "INFO"
    season: int = 2026


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
