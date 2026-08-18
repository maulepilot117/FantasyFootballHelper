"""Human review actions for rung-4 (fuzzy) rows and bad mappings."""

from __future__ import annotations

import uuid
from dataclasses import dataclass

import structlog
from sqlalchemy import func, select
from sqlalchemy.orm import Session

# Single writer for crosswalk_unmatched (controller ruling): reuse resolve's upsert so
# the review flow and rung 5 can never drift apart on the conflict-set payload.
from ffh.crosswalk.resolve import (
    LIVE_MAPPING,
    MANUAL_METHOD,
    REJECTED_METHOD,
    close_unmatched,
    queued_raw_context,
    upsert_unmatched,
)
from ffh.db.models import CrosswalkUnmatched, Player, PlayerExternalId

#: `MANUAL_METHOD` comes from `resolve`, which owns the whole `match_method` vocabulary.
#: Re-typing `"manual"` here is what let the ladder's authority rule and this module's
#: writer drift — `resolve` locks `manual` rows against a gsis upgrade by name.
MANUAL_CONFIDENCE = 1.0

log = structlog.get_logger(__name__)


def verify_mapping(session: Session, source: str, external_id: str) -> bool:
    """Mark a mapping as human-verified (usable regardless of confidence)."""
    row = session.get(PlayerExternalId, (source, external_id))
    if row is None:
        return False
    if row.match_method == REJECTED_METHOD:
        # Verifying a tombstone would resurrect the pairing a human threw out — and
        # `verified_at` is exactly what makes a sub-0.9 row usable. Refuse; the operator
        # path back is `ffh crosswalk map` (a new, explicit decision).
        log.warning(
            "crosswalk.review.verify_refused_rejected",
            source=source,
            external_id=external_id,
            player_id=str(row.player_id),
        )
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
    """Tombstone a wrong mapping and park the id in crosswalk_unmatched.

    The row is NOT deleted: a deletion is forgotten by the next sync, which happily
    re-mints the identical wrong mapping (and `close_unmatched` then turns the gate GREEN
    on a pairing a human explicitly rejected). Instead the row survives as
    `match_method='rejected'`, `confidence=0.0`, still recording the rejected player_id —
    a durable "never this pairing again" that `resolve` rung 1 and `apply_playerids` both
    honour. It stops being a mapping: it is not returned, not usable, not counted in the
    review queue, and does not occupy the player's one slot for this source.
    Escape hatch: `ffh crosswalk map <source> <id> <player_id>`.
    """
    row = session.get(PlayerExternalId, (source, external_id))
    if row is None:
        return False
    if row.match_method != REJECTED_METHOD:
        row.match_method = REJECTED_METHOD
        row.confidence = 0.0
        row.verified_at = None
        session.flush()
    # The Task-5 conflict path can leave this key in BOTH tables. The shared upsert
    # refreshes raw_* from its arguments, so carry any queued context forward rather
    # than nulling it out (same helper the displacement paths use).
    upsert_unmatched(
        session, source, external_id, **queued_raw_context(session, source, external_id)
    )
    log.info("crosswalk.review.rejected", source=source, external_id=external_id)
    return True


def live_mapping(session: Session, source: str, external_id: str) -> PlayerExternalId | None:
    """The key's mapping row, or ``None`` when it has none. A ``rejected`` tombstone is not
    a mapping (the id is unmapped by definition), so it reads as ``None`` here."""
    row = session.get(PlayerExternalId, (source, external_id))
    return row if row is not None and row.match_method != REJECTED_METHOD else None


def mark_unmatched_resolved(
    session: Session, source: str, external_id: str, *, force: bool = False
) -> bool:
    """Flip crosswalk_unmatched.resolved after a human decision handled the queue entry.

    Refuses while the key still has a **live mapping row**. Three ladder states park an id
    in *both* tables (`upgrade_conflict`, `human_decision_conflict`, and a mapping a human
    is disputing): there the queue row is the gate signal for a mapping that `resolve`
    keeps returning, so closing it would green the gate on a contradicted mapping while
    every consumer still gets the wrong player. The operator paths for that state are
    `ffh crosswalk verify --reject` (tombstone it) or `ffh crosswalk map` (re-point it).

    ``force`` restores the unconditional close for the case the command is *for*: an id
    that will never map, plus the internal `ffh crosswalk verify` call, where accepting the
    mapping is precisely the decision that closes the entry.
    """
    u = session.scalar(
        select(CrosswalkUnmatched).where(
            CrosswalkUnmatched.source == source, CrosswalkUnmatched.external_id == external_id
        )
    )
    if u is None:
        return False
    if not force and (row := live_mapping(session, source, external_id)) is not None:
        log.warning(
            "crosswalk.review.unmatched_resolve_refused",
            source=source,
            external_id=external_id,
            match_method=row.match_method,
            player_id=str(row.player_id),
        )
        return False
    u.resolved = True
    session.flush()
    log.info("crosswalk.review.unmatched_resolved", source=source, external_id=external_id)
    return True


@dataclass(frozen=True)
class MapResult:
    """Outcome of ``ffh crosswalk map``. ``status`` is one of ``created`` / ``replaced`` /
    ``no_such_player`` / ``source_player_clash``."""

    status: str
    detail: str = ""

    @property
    def ok(self) -> bool:
        return self.status in {"created", "replaced"}


def map_mapping(session: Session, source: str, external_id: str, player_id: uuid.UUID) -> MapResult:
    """Create the human decision `(source, external_id) -> player_id` at `manual` 1.0.

    This is the only way an id sitting in `crosswalk_unmatched` becomes *mapped* rather
    than merely silenced (`resolve-unmatched`), and the only escape from a `rejected`
    tombstone. `manual` is first-class in the ladder: rung 1 returns it, the rung-1 gsis
    upgrade path is locked against it, and `apply_playerids` never displaces it.

    Guarded: the player must exist, and the `(source, player_id)` unique index is
    pre-checked (DATABASE.md §2 — pre-check, never catch the IntegrityError).
    """
    if session.get(Player, player_id) is None:
        return MapResult("no_such_player", f"no players row {player_id}")
    holder = session.scalar(
        select(PlayerExternalId).where(
            PlayerExternalId.source == source,
            PlayerExternalId.player_id == player_id,
            LIVE_MAPPING,
            PlayerExternalId.external_id != external_id,
        )
    )
    if holder is not None:
        return MapResult(
            "source_player_clash",
            f"{source}:{holder.external_id} already maps to {player_id} "
            f"({holder.match_method}); reject or re-map that id first",
        )
    row = session.get(PlayerExternalId, (source, external_id))
    status = "created"
    if row is None:
        row = PlayerExternalId(source=source, external_id=external_id, player_id=player_id)
        session.add(row)
    else:
        status = "replaced"
    previous_method, previous_player = row.match_method, row.player_id
    row.player_id = player_id
    row.match_method = MANUAL_METHOD
    row.confidence = MANUAL_CONFIDENCE
    # A manual mapping IS the human decision, so it is verified by construction.
    row.verified_at = func.now()
    session.flush()
    close_unmatched(session, source, external_id)
    log.info(
        "crosswalk.review.mapped",
        source=source,
        external_id=external_id,
        player_id=str(player_id),
        previous_method=previous_method if status == "replaced" else None,
        previous_player_id=str(previous_player) if status == "replaced" else None,
    )
    return MapResult(status)
