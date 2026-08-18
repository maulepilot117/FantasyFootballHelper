"""The resolution ladder (DATABASE.md §3) — strictly in order, records which rung won.

Consumers (platform_sync in PR ⑤, ADP ingest in PR ⑥) call ``resolve`` / ``resolve_many``
and never touch ``player_external_ids`` directly. If you must query the table in SQL, apply
``confidence >= 0.9 - epsilon OR verified_at IS NOT NULL`` (see ``is_usable`` /
``CONFIDENCE_EPSILON`` — the column is float4, so 0.9 round-trips as 0.899999988…).
"""

from __future__ import annotations

import uuid
from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Literal

import structlog
from rapidfuzz import process
from rapidfuzz.distance import JaroWinkler
from sqlalchemy import Select, func, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from ffh.crosswalk.normalize import (
    normalize_dst,
    normalize_name,
    normalize_position,
    normalize_team,
)
from ffh.db.models import CrosswalkUnmatched, Player, PlayerExternalId

log = structlog.get_logger(__name__)

EXACT_CONFIDENCE = 0.95
FUZZY_THRESHOLD = 0.92
FUZZY_CAP = 0.89
FUZZY_TIE_MARGIN = 0.01
USABLE_CONFIDENCE = 0.9
# player_external_ids.confidence is Postgres REAL (float4): a stored 0.9 reads back as
# 0.899999988…, so every threshold comparison allows this slack. Task 7's SQL variant of
# the usability rule must use the identical epsilon.
CONFIDENCE_EPSILON = 1e-6

Outcome = Literal["resolved", "pending_review", "unmatched"]


@dataclass(frozen=True)
class Resolution:
    player_id: uuid.UUID
    method: str
    confidence: float


@dataclass(frozen=True)
class ResolveInput:
    source: str
    external_id: str
    raw_name: str | None = None
    raw_position: str | None = None
    raw_team: str | None = None
    gsis_id: str | None = None
    birth_date: date | None = None
    college: str | None = None

    @property
    def key(self) -> tuple[str, str]:
        return (self.source, self.external_id)


@dataclass
class ResolveManyReport:
    resolved: dict[tuple[str, str], Resolution] = field(default_factory=dict)
    unmatched: list[tuple[str, str]] = field(default_factory=list)
    pending_review: list[tuple[str, str]] = field(default_factory=list)
    by_method: Counter[str] = field(default_factory=Counter)


def is_usable(confidence: float, verified_at: datetime | None) -> bool:
    """DATABASE.md §3: confidence < 0.9 rows require human review before use."""
    return confidence >= USABLE_CONFIDENCE - CONFIDENCE_EPSILON or verified_at is not None


def resolve(
    session: Session,
    source: str,
    external_id: str,
    raw_name: str | None = None,
    raw_position: str | None = None,
    raw_team: str | None = None,
    *,
    gsis_id: str | None = None,
    birth_date: date | None = None,
    college: str | None = None,
) -> Resolution | None:
    """Walk the ladder for one id. ``None`` means unmatched OR pending human review — both
    are recorded (``crosswalk_unmatched`` / unverified ``player_external_ids`` row)."""
    res, _outcome = _resolve(
        session,
        ResolveInput(
            source, external_id, raw_name, raw_position, raw_team, gsis_id, birth_date, college
        ),
    )
    return res


def resolve_many(session: Session, rows: Iterable[ResolveInput]) -> ResolveManyReport:
    report = ResolveManyReport()
    # gsis-bearing inputs first (order within each group is immaterial): rungs 3-4
    # persist, so a name-based guess earlier in the batch would otherwise claim a player
    # and force a later rung-2 certainty for that same player into crosswalk_unmatched.
    batch = list(rows)
    ordered = [r for r in batch if r.gsis_id is not None or r.source == "gsis"]
    ordered += [r for r in batch if r.gsis_id is None and r.source != "gsis"]
    for inp in ordered:
        res, outcome = _resolve(session, inp)
        if res is not None:
            report.resolved[inp.key] = res
            report.by_method[res.method] += 1
        elif outcome == "pending_review":
            report.pending_review.append(inp.key)
            report.by_method["fuzzy_pending"] += 1
        else:
            report.unmatched.append(inp.key)
            report.by_method["unmatched"] += 1
    log.info(
        "crosswalk.resolve_many",
        resolved=len(report.resolved),
        pending_review=len(report.pending_review),
        unmatched=len(report.unmatched),
        by_method=dict(report.by_method),
    )
    return report


def upsert_unmatched(
    session: Session,
    source: str,
    external_id: str,
    *,
    raw_name: str | None = None,
    raw_position: str | None = None,
    raw_team: str | None = None,
) -> None:
    """The single writer for ``crosswalk_unmatched`` (rung 5 here; Task 7's review flow).

    ``first_seen`` defaults on insert; on conflict the raw fields refresh, ``last_seen``
    advances with ``clock_timestamp()`` (``now()`` is frozen per transaction) and
    ``resolved`` flips back to false — a reappearing id needs another look.
    """
    stmt = insert(CrosswalkUnmatched).values(
        source=source,
        external_id=external_id,
        raw_name=raw_name,
        raw_position=raw_position,
        raw_team=raw_team,
    )
    stmt = stmt.on_conflict_do_update(
        index_elements=["source", "external_id"],
        set_={
            "raw_name": stmt.excluded.raw_name,
            "raw_position": stmt.excluded.raw_position,
            "raw_team": stmt.excluded.raw_team,
            "last_seen": func.clock_timestamp(),
            "resolved": False,
        },
    )
    session.execute(stmt)
    session.flush()


def close_unmatched(session: Session, source: str, external_id: str) -> bool:
    """Flip an open ``crosswalk_unmatched`` row to resolved once its key gains a mapping.

    Called at every point a mapping row is *created* for the key (``_persist`` here,
    ``apply_playerids`` in dynastyprocess): the queue entry means "this id has no
    mapping", so creating one closes it. Deliberately NOT called on rung-1 hits (no new
    mapping) or in ``_upgrade_from_gsis``'s conflict branch (no mapping is created there
    — that entry must stay open so ``ffh crosswalk report`` exits 1 on it).
    """
    result = session.execute(
        update(CrosswalkUnmatched)
        .where(
            CrosswalkUnmatched.source == source,
            CrosswalkUnmatched.external_id == external_id,
            CrosswalkUnmatched.resolved.is_(False),
        )
        .values(resolved=True)
    )
    closed = bool(result.rowcount)
    if closed:
        log.info("crosswalk.resolve.unmatched_closed", source=source, external_id=external_id)
    return closed


# ---------------------------------------------------------------------------


def _resolve(session: Session, inp: ResolveInput) -> tuple[Resolution | None, Outcome]:
    # Rung 1 — an existing crosswalk row (dynastyprocess, gsis, exact_name, verified fuzzy,
    # manual). Sub-1.0 rows are re-checked against a caller-supplied gsis_id so a stale
    # low-confidence guess is never enshrined over a 1.0 gsis fact.
    row = session.get(PlayerExternalId, (inp.source, inp.external_id))
    if row is not None:
        if inp.gsis_id is not None and float(row.confidence) < 1.0 - CONFIDENCE_EPSILON:
            handled = _upgrade_from_gsis(session, inp, row)
            if handled is not None:
                return handled
        if is_usable(float(row.confidence), row.verified_at):
            return Resolution(row.player_id, row.match_method, float(row.confidence)), "resolved"
        log.info("crosswalk.resolve.pending_review", source=inp.source, external_id=inp.external_id)
        return None, "pending_review"

    # Rung 2 — gsis direct (persisted unless source == "gsis": a gsis id under source
    # gsis would be redundant — the players table already holds it)
    gsis = inp.gsis_id or (inp.external_id if inp.source == "gsis" else None)
    if gsis:
        pid = session.scalar(select(Player.player_id).where(Player.gsis_id == gsis))
        if pid is not None:
            if inp.source != "gsis" and not _persist(session, inp, pid, "gsis", 1.0):
                return None, "unmatched"
            log.info("crosswalk.resolve.gsis", source=inp.source, external_id=inp.external_id)
            return Resolution(pid, "gsis", 1.0), "resolved"

    position = normalize_position(inp.raw_position)
    name = _canonical_name(inp, position)
    if not name or not position:
        _record_unmatched(session, inp, reason="no_name_or_position")
        return None, "unmatched"

    # Rung 3 — exact (normalized_name, position[, team]), persisted
    pid = _exact(session, inp, name, position)
    if pid is not None:
        if not _persist(session, inp, pid, "exact_name", EXACT_CONFIDENCE):
            return None, "unmatched"
        log.info(
            "crosswalk.resolve.exact", source=inp.source, external_id=inp.external_id, name=name
        )
        return Resolution(pid, "exact_name", EXACT_CONFIDENCE), "resolved"

    # Rung 4 — Jaro-Winkler ≥ 0.92, persisted for review, never returned unverified
    fuzzy = _fuzzy(session, inp, name, position)
    if fuzzy is not None:
        pid, similarity = fuzzy
        if not _persist(session, inp, pid, "fuzzy", min(similarity, FUZZY_CAP)):
            return None, "unmatched"
        log.info(
            "crosswalk.resolve.fuzzy_pending",
            source=inp.source,
            external_id=inp.external_id,
            name=name,
            similarity=round(similarity, 4),
        )
        return None, "pending_review"

    # Rung 5 — unmatched, never silently dropped
    _record_unmatched(session, inp, reason="no_candidate")
    return None, "unmatched"


def _upgrade_from_gsis(
    session: Session, inp: ResolveInput, row: PlayerExternalId
) -> tuple[Resolution | None, Outcome] | None:
    """Rung-1 upgrade path: the caller now supplies a gsis_id worth 1.0 for a stored
    sub-1.0 row. Same player → upgrade method/confidence; different player → the gsis
    fact wins and the stored row is corrected. Human decisions (verified rows, manual
    mappings) are never overwritten. A bare ``None`` means "no ruling" — the caller
    falls through to normal rung-1 handling."""
    pid = session.scalar(select(Player.player_id).where(Player.gsis_id == inp.gsis_id))
    if pid is None:
        return None
    if row.verified_at is not None or row.match_method == "manual":
        # A human owns this mapping; a sync's gsis_id must not rewrite it.
        if pid != row.player_id:
            log.warning(
                "crosswalk.resolve.human_decision_conflict",
                source=inp.source,
                external_id=inp.external_id,
                gsis_id=inp.gsis_id,
                match_method=row.match_method,
                stored_player_id=str(row.player_id),
                gsis_player_id=str(pid),
            )
        return None
    if pid != row.player_id:
        incumbent = session.scalar(
            select(PlayerExternalId.external_id).where(
                PlayerExternalId.source == inp.source,
                PlayerExternalId.player_id == pid,
            )
        )
        if incumbent is not None:
            # Correcting would give the player two ids for this source (unique index
            # player_external_ids_source_player_uidx) — but returning the stored row
            # would hand the caller a mapping the 1.0 gsis fact just contradicted.
            # Route this id to crosswalk_unmatched; the disputed row stays in place
            # for `ffh crosswalk verify --reject` (the report exits 1 on unmatched).
            log.warning(
                "crosswalk.resolve.upgrade_conflict",
                source=inp.source,
                external_id=inp.external_id,
                gsis_id=inp.gsis_id,
                incumbent_external_id=incumbent,
                stored_player_id=str(row.player_id),
                gsis_player_id=str(pid),
            )
            _record_unmatched(session, inp, reason="upgrade_conflict")
            return None, "unmatched"
    old_pid, old_method, old_confidence = row.player_id, row.match_method, float(row.confidence)
    row.player_id = pid
    row.match_method = "gsis"
    row.confidence = 1.0
    # A repoint invalidates any prior verification. Defensive: verified rows are locked
    # above and never reach here, so this only pins the invariant for future edits.
    row.verified_at = None
    session.flush()
    log.info(
        "crosswalk.resolve.upgraded",
        source=inp.source,
        external_id=inp.external_id,
        old_method=old_method,
        old_confidence=round(old_confidence, 4),
        corrected=old_pid != pid,
    )
    return Resolution(pid, "gsis", 1.0), "resolved"


def _canonical_name(inp: ResolveInput, position: str | None) -> str | None:
    if position == "DST":
        return (
            normalize_dst(inp.raw_name)
            or normalize_dst(inp.raw_team)
            or normalize_dst(inp.external_id)
        )
    return normalize_name(inp.raw_name) if inp.raw_name else None


def _mapped_for_source(source: str) -> Select[tuple[uuid.UUID]]:
    return select(PlayerExternalId.player_id).where(PlayerExternalId.source == source)


def _exact(session: Session, inp: ResolveInput, name: str, position: str) -> uuid.UUID | None:
    cands = session.execute(
        select(Player.player_id, Player.team_abbr).where(
            Player.normalized_name == name,
            Player.position == position,
            Player.player_id.not_in(_mapped_for_source(inp.source)),
        )
    ).all()
    if not cands:
        return None
    team = normalize_team(inp.raw_team)
    if team is None:
        return cands[0].player_id if len(cands) == 1 else None
    compatible = [c for c in cands if c.team_abbr in (team, None)]
    return compatible[0].player_id if len(compatible) == 1 else None


def _fuzzy(
    session: Session, inp: ResolveInput, name: str, position: str
) -> tuple[uuid.UUID, float] | None:
    rows = session.execute(
        select(Player.player_id, Player.normalized_name, Player.birth_date, Player.college).where(
            Player.position == position,
            Player.player_id.not_in(_mapped_for_source(inp.source)),
        )
    ).all()
    if not rows:
        return None
    meta = {r.player_id: (r.birth_date, r.college) for r in rows}
    choices = {r.player_id: r.normalized_name for r in rows}
    hits = process.extract(
        name,
        choices,
        scorer=JaroWinkler.normalized_similarity,
        score_cutoff=FUZZY_THRESHOLD,
        limit=None,
    )
    survivors = [(pid, float(score)) for _choice, score, pid in hits]
    if not survivors:
        return None
    # DATABASE.md §3: "disambiguated by birth date or college where available" — two legs:
    # NEGATIVE first (a stored non-NULL value that contradicts the input rules the
    # candidate out), then POSITIVE (if the input confirms at least one survivor, keep
    # only those). A candidate set contradicted by everything falls to rung 5 rather
    # than persisting a fuzzy guess at a demonstrably different player.
    if inp.birth_date is not None:
        survivors = [
            (p, s) for p, s in survivors if meta[p][0] is None or meta[p][0] == inp.birth_date
        ]
        confirmed = [(p, s) for p, s in survivors if meta[p][0] == inp.birth_date]
        if confirmed:
            survivors = confirmed
    if inp.college and (needle := inp.college.strip().lower()):
        # Substring, case-insensitive: nflverse stores "Michigan State; Wake Forest".
        # The walrus guard matters: a whitespace-only college must be "no evidence",
        # not an empty needle that substring-confirms every non-NULL college.
        survivors = [
            (p, s) for p, s in survivors if meta[p][1] is None or needle in meta[p][1].lower()
        ]
        confirmed = [
            (p, s) for p, s in survivors if meta[p][1] is not None and needle in meta[p][1].lower()
        ]
        if confirmed:
            survivors = confirmed
    if not survivors:
        return None
    survivors.sort(key=lambda t: t[1], reverse=True)
    if len(survivors) > 1 and survivors[0][1] - survivors[1][1] < FUZZY_TIE_MARGIN:
        log.info(
            "crosswalk.resolve.fuzzy_tie",
            source=inp.source,
            external_id=inp.external_id,
            name=name,
            top=[(str(p), round(s, 4)) for p, s in survivors[:3]],
        )
        return None
    return survivors[0]


def _persist(
    session: Session, inp: ResolveInput, pid: uuid.UUID, method: str, confidence: float
) -> bool:
    """Insert the ``(source, external_id) → player`` row for rungs 2-4.

    Pre-checks the unique ``(source, player_id)`` index (DATABASE.md §2 note: pre-check,
    never catch the IntegrityError): when the player already holds an id for this source,
    the incumbent wins and this id is routed to ``crosswalk_unmatched``. Returns False then.
    """
    incumbent = session.scalar(
        select(PlayerExternalId.external_id).where(
            PlayerExternalId.source == inp.source,
            PlayerExternalId.player_id == pid,
        )
    )
    if incumbent is not None:
        log.warning(
            "crosswalk.resolve.duplicate_for_source",
            source=inp.source,
            external_id=inp.external_id,
            player_id=str(pid),
            incumbent_external_id=incumbent,
            method=method,
        )
        upsert_unmatched(
            session,
            inp.source,
            inp.external_id,
            raw_name=inp.raw_name,
            raw_position=inp.raw_position,
            raw_team=inp.raw_team,
        )
        return False
    session.add(
        PlayerExternalId(
            player_id=pid,
            source=inp.source,
            external_id=inp.external_id,
            confidence=confidence,
            match_method=method,
            verified_at=None,
        )
    )
    session.flush()
    # A mapping row now exists for this key: close any open review-queue entry (e.g. an
    # id that hit rung 5 on an earlier sync and resolves now that new data arrived).
    close_unmatched(session, inp.source, inp.external_id)
    return True


def _record_unmatched(session: Session, inp: ResolveInput, *, reason: str) -> None:
    upsert_unmatched(
        session,
        inp.source,
        inp.external_id,
        raw_name=inp.raw_name,
        raw_position=inp.raw_position,
        raw_team=inp.raw_team,
    )
    log.warning(
        "crosswalk.resolve.unmatched",
        source=inp.source,
        external_id=inp.external_id,
        raw_name=inp.raw_name,
        raw_position=inp.raw_position,
        raw_team=inp.raw_team,
        reason=reason,
    )
