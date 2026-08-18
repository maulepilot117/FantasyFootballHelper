"""Postgres advisory locks — the shared serialization primitive.

Lives in ``ffh.db`` rather than ``ffh.ingest`` because two unrelated subsystems need it:
the ingest lifecycle (one run per ``(source, asset, season)``) and the crosswalk write
commands (``ffh crosswalk seed`` / ``map`` / ``verify`` / ``resolve-unmatched``, whose
read-then-write plans are TOCTOU without it). ``ffh.ingest.base`` re-exports both names
for its existing callers.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

import structlog
from sqlalchemy import text
from sqlalchemy.orm import Session

__all__ = ["advisory_lock", "try_lock"]

log = structlog.get_logger(__name__)


@contextmanager
def advisory_lock(session: Session, key: str) -> Iterator[None]:
    """Hold the Postgres session-level advisory lock for ``key`` on a DEDICATED connection.

    Session-level advisory locks are owned by a physical backend connection. A Session bound
    to an Engine returns its connection to the pool on ``commit()``, so taking the lock
    through the Session would let (a) the rest of the lifecycle run on another backend and
    (b) another Session re-acquire the "same" lock on the recycled backend. Instead we check
    out one connection from the Session's engine, lock on it, keep it open for the whole
    block, and unlock on that same connection — asserting the unlock actually released it.

    ``hashtext`` maps the key to an int4; a collision between unrelated keys only costs
    unnecessary serialization, never correctness.
    """
    engine = session.get_bind().engine
    conn = engine.connect()
    try:
        conn.execute(text("SELECT pg_advisory_lock(hashtext(:key))"), {"key": key})
        conn.commit()
        log.debug("db.lock.acquired", key=key)
        try:
            yield
        finally:
            released = conn.execute(
                text("SELECT pg_advisory_unlock(hashtext(:key))"), {"key": key}
            ).scalar()
            conn.commit()
            if not released:  # pragma: no cover - would mean the lock was never ours
                log.error("db.lock.release_returned_false", key=key)
            else:
                log.debug("db.lock.released", key=key)
    finally:
        conn.close()  # returns the connection to the pool; a dead connection drops the lock


def try_lock(session: Session, key: str) -> bool:
    """Non-blocking probe on the SESSION's current connection: True if it now holds the lock.

    Test helper; production code uses ``advisory_lock``.
    """
    return bool(session.scalar(text("SELECT pg_try_advisory_lock(hashtext(:key))"), {"key": key}))
