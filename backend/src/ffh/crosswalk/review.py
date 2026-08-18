"""Human review actions for rung-4 (fuzzy) rows and bad mappings."""

from __future__ import annotations

import structlog
from sqlalchemy import func, select
from sqlalchemy.orm import Session

# Single writer for crosswalk_unmatched (controller ruling): reuse resolve's upsert so
# the review flow and rung 5 can never drift apart on the conflict-set payload.
from ffh.crosswalk.resolve import upsert_unmatched
from ffh.db.models import CrosswalkUnmatched, PlayerExternalId

log = structlog.get_logger(__name__)


def verify_mapping(session: Session, source: str, external_id: str) -> bool:
    """Mark a mapping as human-verified (usable regardless of confidence)."""
    row = session.get(PlayerExternalId, (source, external_id))
    if row is None:
        return False
    row.verified_at = func.now()
    session.flush()
    log.info(
        "crosswalk.review.verified",
        source=source,
        external_id=external_id,
        player_id=str(row.player_id),
    )
    return True


def reject_mapping(session: Session, source: str, external_id: str) -> bool:
    """Delete a wrong mapping and park the id in crosswalk_unmatched so it is not forgotten."""
    row = session.get(PlayerExternalId, (source, external_id))
    if row is None:
        return False
    session.delete(row)
    # The Task-5 conflict path can leave this key in BOTH tables. The shared upsert
    # refreshes raw_* from its arguments, so carry any queued context forward rather
    # than nulling it out.
    existing = session.scalar(
        select(CrosswalkUnmatched).where(
            CrosswalkUnmatched.source == source, CrosswalkUnmatched.external_id == external_id
        )
    )
    upsert_unmatched(
        session,
        source,
        external_id,
        raw_name=existing.raw_name if existing else None,
        raw_position=existing.raw_position if existing else None,
        raw_team=existing.raw_team if existing else None,
    )
    log.info("crosswalk.review.rejected", source=source, external_id=external_id)
    return True


def mark_unmatched_resolved(session: Session, source: str, external_id: str) -> bool:
    """Flip crosswalk_unmatched.resolved after a human decision handled the queue entry."""
    u = session.scalar(
        select(CrosswalkUnmatched).where(
            CrosswalkUnmatched.source == source, CrosswalkUnmatched.external_id == external_id
        )
    )
    if u is None:
        return False
    u.resolved = True
    session.flush()
    log.info("crosswalk.review.unmatched_resolved", source=source, external_id=external_id)
    return True
