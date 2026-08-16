from sqlalchemy import Engine, create_engine
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.orm import Session, sessionmaker

from ffh.config import get_settings


def make_engine(url: str | None = None) -> Engine:
    """Sync engine (Alembic, CLI ingest jobs, tests)."""
    return create_engine(url or get_settings().database_url, pool_pre_ping=True)


def make_async_engine(url: str | None = None) -> AsyncEngine:
    """Async engine for FastAPI request paths. Same psycopg driver, async mode."""
    return create_async_engine(url or get_settings().database_url, pool_pre_ping=True)


def make_session_factory(engine: Engine) -> sessionmaker[Session]:
    return sessionmaker(bind=engine, expire_on_commit=False)
