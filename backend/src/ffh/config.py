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
    log_level: str = "INFO"
    season: int = 2026


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
