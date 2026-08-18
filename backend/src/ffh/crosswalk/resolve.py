"""The resolution ladder (DATABASE.md §3) — strictly in order, records which rung won.

Consumers (platform_sync in PR ⑤, ADP ingest in PR ⑥) call ``resolve`` / ``resolve_many``
and never touch ``player_external_ids`` directly. If you must query the table in SQL, apply
``confidence >= 0.9 - epsilon OR verified_at IS NOT NULL`` (see ``is_usable`` /
``CONFIDENCE_EPSILON`` — the column is float4, so 0.9 round-trips as 0.899999976…).
"""

from __future__ import annotations

import re
import uuid
from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Literal

import structlog
from rapidfuzz import process
from rapidfuzz.distance import JaroWinkler
from sqlalchemy import Select, case, func, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from ffh.crosswalk.normalize import (
    fold_accents,
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
# `ffh crosswalk verify --reject` does not delete the row: it leaves a TOMBSTONE
# (match_method='rejected', confidence 0.0) recording the pairing a human threw out, so
# the next sync cannot re-mint it. A tombstone is NOT a mapping — rung 1 refuses to
# return one, `is_usable` refuses it even if something stamped `verified_at`, and every
# (source, player_id) incumbency check skips it (the unique index is partial for the same
# reason: a tombstone must not squat the player's one slot for that source).
REJECTED_METHOD = "rejected"
# Rows a higher-authority (1.0) fact must NOT displace: a human decision, a
# DynastyProcess row (itself 1.0), or a tombstone. Everything else — an unverified
# `exact_name` 0.95 or `fuzzy` 0.89 guess — loses to a 1.0 fact.
PROTECTED_METHODS: frozenset[str] = frozenset({"dynastyprocess", "manual", REJECTED_METHOD})
# player_external_ids.confidence is Postgres REAL (float4): a stored 0.9 reads back as
# 0.899999976…, so every threshold comparison allows this slack. Task 7's SQL variant of
# the usability rule must use the identical epsilon.
CONFIDENCE_EPSILON = 1e-6

# Words carrying no school identity — without them "Michigan State" and "Ohio State"
# would "agree" on the token `state`.
_COLLEGE_STOPWORDS: frozenset[str] = frozenset(
    {"state", "st", "university", "univ", "college", "u", "of", "the", "and", "a", "am"}
)

#: ``deferred`` is internal to ``resolve_many``'s two-pass ordering (rung 4 held back so a
#: later rung-1/2/3 claim on the same player wins) and never escapes to a caller.
Outcome = Literal["resolved", "pending_review", "unmatched", "deferred"]


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


def is_usable(
    confidence: float, verified_at: datetime | None, match_method: str | None = None
) -> bool:
    """DATABASE.md §3: confidence < 0.9 rows require human review before use.

    A ``rejected`` tombstone is never usable — not even with a ``verified_at`` stamp,
    which is why the method is checked *before* the confidence/verification rule.
    """
    if match_method == REJECTED_METHOD:
        return False
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
    """Resolve a batch in **priority passes**, so the outcome does not depend on input order.

    Rungs 2-4 persist, and rungs 3/4 exclude players already mapped for the source — so a
    single ordered walk lets a rung-4 *guess* early in the batch claim the very player a
    later rung-3 exact (or rung-2 gsis) match wants, pushing the better evidence onto the
    review queue. Two passes fix that: pass 1 runs the whole batch with ``defer_fuzzy``
    (settling every rung-1/2/3 claim batch-wide, gsis-bearing inputs first), pass 2 re-runs
    only the deferred inputs with rung 4 enabled. ``resolve()`` — one id, no batch to be
    ordered against — is unaffected.
    """
    report = ResolveManyReport()

    def _record(inp: ResolveInput, res: Resolution | None, outcome: Outcome) -> None:
        if res is not None:
            report.resolved[inp.key] = res
            report.by_method[res.method] += 1
        elif outcome == "pending_review":
            report.pending_review.append(inp.key)
            report.by_method["fuzzy_pending"] += 1
        else:
            report.unmatched.append(inp.key)
            report.by_method["unmatched"] += 1

    # gsis-bearing inputs first (order within each group is immaterial): rung 2 persists,
    # so a rung-3 exact earlier in the batch would otherwise claim a player and force a
    # later rung-2 certainty for that same player into crosswalk_unmatched.
    batch = list(rows)
    ordered = [r for r in batch if r.gsis_id is not None or r.source == "gsis"]
    ordered += [r for r in batch if r.gsis_id is None and r.source != "gsis"]
    deferred: list[ResolveInput] = []
    for inp in ordered:
        res, outcome = _resolve(session, inp, defer_fuzzy=True)
        if outcome == "deferred":
            deferred.append(inp)
            continue
        _record(inp, res, outcome)
    for inp in deferred:
        _record(inp, *_resolve(session, inp))
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

    ``first_seen`` defaults on insert; on conflict the raw fields refresh and ``last_seen``
    advances with ``clock_timestamp()`` (``now()`` is frozen per transaction).

    ``resolved`` re-opens **only when a ``raw_*`` field actually changed** — i.e. the id
    reappeared *described differently* and deserves another look. An unconditional
    ``resolved = False`` here means every weekly seed silently undoes the operator's
    ``ffh crosswalk resolve-unmatched``: the permanently-glitched DynastyProcess ids
    (DATA_SOURCES.md §5) are re-asserted verbatim by every snapshot, so the gate could
    never reach green. The ``IS DISTINCT FROM`` comparison is NULL-safe in both directions.
    """
    stmt = insert(CrosswalkUnmatched).values(
        source=source,
        external_id=external_id,
        raw_name=raw_name,
        raw_position=raw_position,
        raw_team=raw_team,
    )
    raw_changed = (
        CrosswalkUnmatched.raw_name.is_distinct_from(stmt.excluded.raw_name)
        | CrosswalkUnmatched.raw_position.is_distinct_from(stmt.excluded.raw_position)
        | CrosswalkUnmatched.raw_team.is_distinct_from(stmt.excluded.raw_team)
    )
    stmt = stmt.on_conflict_do_update(
        index_elements=["source", "external_id"],
        set_={
            "raw_name": stmt.excluded.raw_name,
            "raw_position": stmt.excluded.raw_position,
            "raw_team": stmt.excluded.raw_team,
            "last_seen": func.clock_timestamp(),
            # In an ON CONFLICT SET clause the bare table name is the row already stored,
            # `excluded` the row we tried to insert.
            "resolved": case((raw_changed, False), else_=CrosswalkUnmatched.resolved),
        },
    )
    session.execute(stmt)
    session.flush()


def queued_raw_context(session: Session, source: str, external_id: str) -> dict[str, str | None]:
    """The ``raw_*`` fields already parked in ``crosswalk_unmatched`` for this key.

    ``upsert_unmatched`` refreshes ``raw_*`` from its arguments by design, so a caller with
    no raw context of its own — a *displaced* incumbent, or an id absent from the
    DynastyProcess file — must carry the queued values forward or it silently blanks the
    operator's only description of the id. ``review.reject_mapping`` has always done this;
    the displacement paths use the same helper.
    """
    row = session.scalar(
        select(CrosswalkUnmatched).where(
            CrosswalkUnmatched.source == source,
            CrosswalkUnmatched.external_id == external_id,
        )
    )
    return {
        "raw_name": row.raw_name if row else None,
        "raw_position": row.raw_position if row else None,
        "raw_team": row.raw_team if row else None,
    }


def close_unmatched(session: Session, source: str, external_id: str) -> bool:
    """Flip an open ``crosswalk_unmatched`` row to resolved once its key **resolves**.

    Called at every point a mapping row is *created* for the key (``_persist`` here,
    ``apply_playerids`` in dynastyprocess, ``map_mapping`` in review): the queue entry
    means "this id has no mapping", so creating one closes it.

    One resolution creates no row and is closed here directly: a rung-2 hit with
    ``source == "gsis"``, which deliberately skips ``_persist`` because a gsis id filed
    under source ``gsis`` would duplicate ``players.gsis_id``. Without this the queue entry
    of an id that missed before the player was seeded would stay open forever and latch
    ``ffh crosswalk report`` red on an id that resolves at confidence 1.0.

    Deliberately NOT called on rung-1 hits (no new mapping) or in
    ``_upgrade_from_gsis``'s conflict branches (no mapping is created there — that entry
    must stay open so ``ffh crosswalk report`` exits 1 on it).
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


def _resolve(
    session: Session, inp: ResolveInput, *, defer_fuzzy: bool = False
) -> tuple[Resolution | None, Outcome]:
    """One id down the ladder. ``defer_fuzzy`` (``resolve_many`` pass 1 only) makes rung 4
    return ``(None, "deferred")`` and persist nothing, so the batch settles every
    higher-rung claim before any fuzzy guess takes a player's slot for the source."""
    # Rung 1 — an existing crosswalk row (dynastyprocess, gsis, exact_name, verified fuzzy,
    # manual). Every row is re-checked against a caller-supplied gsis_id: a stale sub-1.0
    # guess is corrected rather than enshrined, and a 1.0 row the gsis fact contradicts is
    # returned but put on the gate.
    row = session.get(PlayerExternalId, (inp.source, inp.external_id))
    if row is not None:
        if row.match_method == REJECTED_METHOD:
            # A human rejected this exact pairing. The tombstone is not a mapping: the id
            # is unmapped, stays on the gate, and only `ffh crosswalk map` can re-map it.
            _record_unmatched(session, inp, reason="rejected")
            return None, "unmatched"
        if inp.gsis_id is not None:
            # Every supplied gsis_id is checked against the stored row, 1.0 rows included.
            # Gating this on `confidence < 1.0` made the contradiction check unreachable
            # for the rows `map_mapping` actually writes (`manual`, confidence 1.0,
            # verified): a human decision the gsis fact disputes was silently returned.
            handled = _upgrade_from_gsis(session, inp, row)
            if handled is not None:
                return handled
        if is_usable(float(row.confidence), row.verified_at, row.match_method):
            return Resolution(row.player_id, row.match_method, float(row.confidence)), "resolved"
        log.info("crosswalk.resolve.pending_review", source=inp.source, external_id=inp.external_id)
        return None, "pending_review"

    # Rung 2 — gsis direct (persisted unless source == "gsis": a gsis id under source
    # gsis would be redundant — the players table already holds it)
    gsis = inp.gsis_id or (inp.external_id if inp.source == "gsis" else None)
    if gsis:
        pid = session.scalar(select(Player.player_id).where(Player.gsis_id == gsis))
        if pid is not None:
            if inp.source == "gsis":
                # No row to create (it would duplicate players.gsis_id) — but the id IS
                # resolved, so close any queue entry from a miss before the player existed.
                # `close_unmatched` lives inside `_persist`, which this branch skips.
                close_unmatched(session, inp.source, inp.external_id)
            elif not _persist(session, inp, pid, "gsis", 1.0):
                return None, "unmatched"
            log.info("crosswalk.resolve.gsis", source=inp.source, external_id=inp.external_id)
            return Resolution(pid, "gsis", 1.0), "resolved"

    position = normalize_position(inp.raw_position)
    name = _canonical_name(inp, position)
    if not name or not position:
        _record_unmatched(session, inp, reason="no_name_or_position")
        return None, "unmatched"

    # Rung 3 — exact (normalized_name, position[, team]), persisted
    pid, homonym_blocked = _exact(session, inp, name, position)
    if homonym_blocked:
        # Two same-name/same-position players and the *exclusion* of the one already mapped
        # for this source is the only reason the answer looks unique. That is an ambiguity,
        # not evidence: refuse rather than mint a usable 0.95 on the homonym, and send the
        # id to the gate so a human rules on it.
        _record_unmatched(session, inp, reason="homonym_blocked_by_existing_mapping")
        return None, "unmatched"
    if pid is not None:
        if not _persist(session, inp, pid, "exact_name", EXACT_CONFIDENCE):
            return None, "unmatched"
        log.info(
            "crosswalk.resolve.exact", source=inp.source, external_id=inp.external_id, name=name
        )
        return Resolution(pid, "exact_name", EXACT_CONFIDENCE), "resolved"

    # Rung 4 — Jaro-Winkler ≥ 0.92, persisted for review, never returned unverified
    fuzzy, fuzzy_reason = _fuzzy(session, inp, name, position)
    if fuzzy is not None:
        if defer_fuzzy:
            # Pass 1 of resolve_many: persist nothing, claim nothing — the same input is
            # re-run in pass 2 once every higher-rung claim in the batch has settled.
            return None, "deferred"
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

    # Rung 5 — unmatched, never silently dropped. The reason distinguishes "no name
    # matched at all" from "candidates existed but the evidence ruled them out" — without
    # it a crosswalk miss caused by a bad birth date/college is undiagnosable.
    _record_unmatched(session, inp, reason=fuzzy_reason)
    return None, "unmatched"


def _upgrade_from_gsis(
    session: Session, inp: ResolveInput, row: PlayerExternalId
) -> tuple[Resolution | None, Outcome] | None:
    """Rung-1 upgrade path: the caller supplies a gsis_id worth 1.0 for a stored row.

    Stored row at 1.0 → never rewritten (it is a fact, not a guess), but a *disagreeing*
    gsis id is a two-facts-conflict and goes on the gate. Stored row below 1.0: same
    player → upgrade method/confidence; different player → the gsis fact wins and the
    stored row is corrected. Human decisions (verified rows, manual mappings) are never
    overwritten. A bare ``None`` means "no ruling" — the caller falls through to normal
    rung-1 handling, which still returns the stored row."""
    pid = session.scalar(select(Player.player_id).where(Player.gsis_id == inp.gsis_id))
    if pid is None:
        return None
    if float(row.confidence) >= 1.0 - CONFIDENCE_EPSILON:
        # A confidence-1.0 row — `manual` (what `ffh crosswalk map` writes), `gsis` or
        # `dynastyprocess`. Nothing here outranks it, so the row is returned unchanged;
        # but when the supplied gsis id names a DIFFERENT player the two 1.0 facts
        # contradict each other and only a human can rule. Queue the key so
        # `ffh crosswalk report` exits 1 until they do.
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
            _record_unmatched(session, inp, reason="human_decision_conflict")
        return None
    if row.verified_at is not None or row.match_method == "manual":
        # A human owns this mapping; a sync's gsis_id must not rewrite it. But a dispute
        # is not a non-event: the key also goes on the gate (same precedent as
        # upgrade_conflict below — in BOTH tables), so `ffh crosswalk report` exits 1
        # until a human re-rules with `verify --reject` or `crosswalk map`.
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
            _record_unmatched(session, inp, reason="human_decision_conflict")
        return None
    if pid != row.player_id:
        incumbent = _slot_holder(session, inp.source, pid)
        if incumbent is not None and displaceable(incumbent):
            # The 1.0 gsis fact outranks an unverified guess: evict the guess (its id
            # goes back on the gate) and let the correction below proceed.
            _take_slot(session, inp.source, inp.external_id, incumbent)
            incumbent = None
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
                incumbent_external_id=incumbent.external_id,
                incumbent_method=incumbent.match_method,
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
    """Players that already hold a *mapping* for this source. Tombstones are excluded:
    a rejected pairing leaves the player free to be claimed by the correct id."""
    return select(PlayerExternalId.player_id).where(
        PlayerExternalId.source == source,
        PlayerExternalId.match_method != REJECTED_METHOD,
    )


def _exact(
    session: Session, inp: ResolveInput, name: str, position: str
) -> tuple[uuid.UUID | None, bool]:
    """``(player_id | None, homonym_blocked)`` for rung 3.

    The "already mapped for this source" exclusion is a no-duplicate mechanism, not
    evidence about identity. Applied naively it silently *creates* uniqueness: with two
    same-name/same-position players, one of whom already holds an id for the source, the
    remaining candidate is picked and minted at a usable 0.95 — a coin flip presented as a
    match. So the pick is run twice, and the 0.95 is accepted only when the pre-exclusion
    candidate set answers the same way. When the exclusion is what made it unique the
    second element is True and the caller sends the id to the gate instead.
    """
    cands_all = session.execute(
        select(Player.player_id, Player.team_abbr).where(
            Player.normalized_name == name,
            Player.position == position,
        )
    ).all()
    if not cands_all:
        return None, False
    mapped = set(
        session.scalars(
            select(PlayerExternalId.player_id).where(
                PlayerExternalId.source == inp.source,
                PlayerExternalId.match_method != REJECTED_METHOD,
                PlayerExternalId.player_id.in_([c.player_id for c in cands_all]),
            )
        )
    )
    cands = [c for c in cands_all if c.player_id not in mapped]
    team = normalize_team(inp.raw_team)

    def _pick(rows: list) -> uuid.UUID | None:
        if not rows:
            return None
        if team is None:
            return rows[0].player_id if len(rows) == 1 else None
        compatible = [c for c in rows if c.team_abbr in (team, None)]
        return compatible[0].player_id if len(compatible) == 1 else None

    pid = _pick(cands)
    if pid is None:
        return None, False
    if _pick(cands_all) != pid:
        log.warning(
            "crosswalk.resolve.homonym_blocked_by_existing_mapping",
            source=inp.source,
            external_id=inp.external_id,
            name=name,
            position=position,
            candidates=len(cands_all),
        )
        return None, True
    return pid, False


def _college_tokens(raw: str) -> set[str]:
    """Comparable tokens for a college string. Generic words are dropped so ``Ohio St.``
    and ``Ohio State`` agree while ``Michigan State`` and ``Ohio State`` do not."""
    tokens = {t for t in re.split(r"[^a-z0-9]+", fold_accents(raw).lower()) if t}
    return tokens - _COLLEGE_STOPWORDS


def colleges_agree(a: str, b: str) -> bool:
    """Symmetric college comparison — the elimination leg must never be direction-dependent.

    The old asymmetric ``needle in stored`` test eliminated a correct candidate whenever
    the caller's spelling was the longer one (``"Ohio St."`` vs stored ``"Ohio State"``).
    Agreement = a shared meaningful token, or either string containing the other
    (nflverse stores multi-college strings: ``"Michigan State; Wake Forest"``).
    """
    a_norm, b_norm = fold_accents(a).lower().strip(), fold_accents(b).lower().strip()
    if not a_norm or not b_norm:
        return False
    if a_norm in b_norm or b_norm in a_norm:
        return True
    ta, tb = _college_tokens(a_norm), _college_tokens(b_norm)
    return bool(ta & tb)


def _fuzzy(
    session: Session, inp: ResolveInput, name: str, position: str
) -> tuple[tuple[uuid.UUID, float] | None, str]:
    """``((player_id, similarity) | None, reason)`` — the reason names rung 5's outcome."""
    rows = session.execute(
        select(Player.player_id, Player.normalized_name, Player.birth_date, Player.college).where(
            Player.position == position,
            Player.player_id.not_in(_mapped_for_source(inp.source)),
        )
    ).all()
    if not rows:
        return None, "no_candidate"
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
        return None, "no_candidate"
    # DATABASE.md §3: "disambiguated by birth date or college where available" — two legs:
    # NEGATIVE first (a stored non-NULL value that contradicts the input rules the
    # candidate out), then POSITIVE (the input confirms a survivor). A candidate set
    # contradicted by everything falls to rung 5 rather than persisting a fuzzy guess at a
    # demonstrably different player.
    # The positive leg is NOT "keep only the confirmed" on the college side: a stored NULL
    # college is no evidence *against* a candidate, and dropping it hands the id to a
    # lower-similarity homonym that merely happens to have a college on file. Confirmed
    # and unknown-college candidates both survive; the tie margin below rules on them.
    eliminated_by: list[str] = []
    if inp.birth_date is not None:
        before = len(survivors)
        survivors = [
            (p, s) for p, s in survivors if meta[p][0] is None or meta[p][0] == inp.birth_date
        ]
        if len(survivors) < before:
            eliminated_by.append("birth_date")
        confirmed = [(p, s) for p, s in survivors if meta[p][0] == inp.birth_date]
        if confirmed:
            survivors = confirmed
    if inp.college and (needle := inp.college.strip()):
        # The walrus guard matters: a whitespace-only college must be "no evidence",
        # not an empty needle that confirms every non-NULL college.
        before = len(survivors)
        survivors = [
            (p, s) for p, s in survivors if meta[p][1] is None or colleges_agree(needle, meta[p][1])
        ]
        if len(survivors) < before:
            eliminated_by.append("college")
        confirmed = [
            (p, s)
            for p, s in survivors
            if meta[p][1] is not None and colleges_agree(needle, meta[p][1])
        ]
        if confirmed:
            survivors = confirmed + [(p, s) for p, s in survivors if meta[p][1] is None]
    if not survivors:
        log.info(
            "crosswalk.resolve.fuzzy_eliminated",
            source=inp.source,
            external_id=inp.external_id,
            name=name,
            eliminated_by=eliminated_by,
        )
        return None, "fuzzy_eliminated"
    survivors.sort(key=lambda t: t[1], reverse=True)
    if len(survivors) > 1 and survivors[0][1] - survivors[1][1] < FUZZY_TIE_MARGIN:
        log.info(
            "crosswalk.resolve.fuzzy_tie",
            source=inp.source,
            external_id=inp.external_id,
            name=name,
            top=[(str(p), round(s, 4)) for p, s in survivors[:3]],
        )
        return None, "fuzzy_tie"
    return survivors[0], "fuzzy_hit"


def is_authoritative(method: str, confidence: float) -> bool:
    """A 1.0 fact (``gsis``, ``dynastyprocess``, ``manual``) — not a name guess."""
    return method in {"gsis", "dynastyprocess", "manual"} and (
        confidence >= 1.0 - CONFIDENCE_EPSILON
    )


def displaceable(row: PlayerExternalId) -> bool:
    """True when a 1.0 fact may take this row's ``(source, player_id)`` slot.

    Only unverified, non-human, non-DP, non-tombstone rows — i.e. an ``exact_name`` 0.95
    or ``fuzzy`` 0.89 *guess*. Anything else holds its slot and the incoming id loses.
    """
    return row.match_method not in PROTECTED_METHODS and row.verified_at is None


def _take_slot(
    session: Session, source: str, incoming_external_id: str, incumbent: PlayerExternalId
) -> None:
    """Evict a displaced guess: delete the row and put its id back on the gate.

    The displaced id is genuinely unmapped afterwards, so it MUST land in
    ``crosswalk_unmatched`` (Global Constraint) rather than vanishing into a log line.
    """
    log.warning(
        "crosswalk.resolve.incumbent_displaced",
        source=source,
        external_id=incoming_external_id,
        displaced_external_id=incumbent.external_id,
        displaced_method=incumbent.match_method,
        displaced_confidence=round(float(incumbent.confidence), 4),
        player_id=str(incumbent.player_id),
    )
    displaced = incumbent.external_id
    # Read the queued context BEFORE the delete/upsert: the id may already have a review
    # row (from an earlier sync) whose raw_* fields are the only description we have of it.
    carried = queued_raw_context(session, source, displaced)
    session.delete(incumbent)
    session.flush()
    upsert_unmatched(session, source, displaced, **carried)


def _slot_holder(session: Session, source: str, pid: uuid.UUID) -> PlayerExternalId | None:
    """The row (if any) holding this player's one id for this source. Tombstones excluded
    — they do not occupy the slot (the unique index is partial on the same predicate)."""
    return session.scalar(
        select(PlayerExternalId).where(
            PlayerExternalId.source == source,
            PlayerExternalId.player_id == pid,
            PlayerExternalId.match_method != REJECTED_METHOD,
        )
    )


def _persist(
    session: Session, inp: ResolveInput, pid: uuid.UUID, method: str, confidence: float
) -> bool:
    """Insert the ``(source, external_id) → player`` row for rungs 2-4.

    Pre-checks the unique ``(source, player_id)`` index (DATABASE.md §2 note: pre-check,
    never catch the IntegrityError). When the player already holds an id for this source:
    a 1.0 fact displaces an unverified guess (the guess's id goes to ``crosswalk_unmatched``),
    otherwise the incumbent wins and *this* id is routed there. Returns False in that case.
    """
    # Defence in depth: rung 1 already refuses tombstoned keys, so this can only fire if a
    # future caller reaches rungs 2-4 with one. Re-minting the exact pairing a human
    # rejected is forbidden; pointing the id at a DIFFERENT player is the correction we want.
    tombstone = session.get(PlayerExternalId, (inp.source, inp.external_id))
    if tombstone is not None and tombstone.match_method == REJECTED_METHOD:
        if tombstone.player_id == pid:
            log.warning(
                "crosswalk.resolve.rejected_remint_refused",
                source=inp.source,
                external_id=inp.external_id,
                player_id=str(pid),
                method=method,
            )
            _record_unmatched(session, inp, reason="rejected")
            return False
    else:
        tombstone = None

    incumbent = _slot_holder(session, inp.source, pid)
    if incumbent is not None:
        if is_authoritative(method, confidence) and displaceable(incumbent):
            _take_slot(session, inp.source, inp.external_id, incumbent)
        else:
            log.warning(
                "crosswalk.resolve.duplicate_for_source",
                source=inp.source,
                external_id=inp.external_id,
                player_id=str(pid),
                incumbent_external_id=incumbent.external_id,
                incumbent_method=incumbent.match_method,
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
    if tombstone is not None:
        # Same key, different player: correct the tombstone in place (the PK is taken).
        tombstone.player_id = pid
        tombstone.match_method = method
        tombstone.confidence = confidence
        tombstone.verified_at = None
    else:
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
