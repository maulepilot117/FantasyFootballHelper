"""DynastyProcess ``db_playerids.csv`` → ``player_external_ids`` (rung 1 of the ladder).

``apply_playerids`` and everything above it is pure with respect to ``ffh.ingest``: it takes
a DataFrame, never a URL or a lake path. The one exception is ``DynastyProcessPlayerIdsJob``
at the bottom of the module — the ingest job that fetches the CSV into the lake.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import date
from typing import Any, ClassVar

import polars as pl
import structlog
from sqlalchemy import Row, bindparam, delete, select, update
from sqlalchemy.orm import Session

from ffh.crosswalk.normalize import (
    FANTASY_POSITIONS,
    canonical_dst_key,
    normalize_name,
    normalize_position,
    normalize_team,
)
from ffh.crosswalk.registry import iter_gsis_to_player_id
from ffh.crosswalk.resolve import (
    CONFIDENCE_EPSILON,
    REJECTED_METHOD,
    close_unmatched,
    is_displaceable,
    queued_raw_context,
    upsert_unmatched,
)
from ffh.db.models import CrosswalkUnmatched, Player, PlayerExternalId
from ffh.ingest.base import HttpIngestJob, IngestValidationError, register
from ffh.ingest.lake import scrape_date

log = structlog.get_logger(__name__)

# CSV column → crosswalk source (DATABASE.md §2 player_external_ids.source).
DP_ID_COLUMNS: dict[str, str] = {
    "sleeper_id": "sleeper",
    "espn_id": "espn",
    "yahoo_id": "yahoo",
    "pfr_id": "pfr",
    "fantasypros_id": "fantasypros",
    "sportradar_id": "sportradar",
    "rotowire_id": "rotowire",
}
DP_TEXT_COLUMNS: frozenset[str] = frozenset({"mfl_id", "gsis_id", *DP_ID_COLUMNS})
# mfl_id is required: rows without a gsis_id are keyed on "mfl:<mfl_id>" placeholders.
DP_REQUIRED_COLUMNS: frozenset[str] = frozenset(
    {
        "mfl_id",
        "gsis_id",
        "name",
        "position",
        "team",
        "birthdate",
        "draft_year",
        "college",
        *DP_ID_COLUMNS,
    }
)
DP_METHOD = "dynastyprocess"
DP_CONFIDENCE = 1.0

# A DP row with no registry player yet is carried through the pipeline under a synthetic
# *person key* instead of a UUID. The prefixes are named constants and every site that
# mints, tests or strips one derives from them: a third prefix added by hand to only two
# of the three sites leaves the odd one out feeding `uuid.UUID(player_key)` a string like
# "mfl:17471" — mid-apply, after placeholder `players` rows have already been flushed.
_GSIS_PREFIX = "gsis:"
_MFL_PREFIX = "mfl:"
_PLACEHOLDER_PREFIXES: tuple[str, ...] = (_GSIS_PREFIX, _MFL_PREFIX)


def _placeholder_key(gsis_id: str | None, mfl_id: str | None) -> str:
    """The synthetic person key for a DP row that matches no registry player.

    Callers must have established that at least one of the two ids is present
    (``_validate`` drops rows with neither — their key would be the literal ``"mfl:None"``,
    shared by every such row).
    """
    return f"{_GSIS_PREFIX}{gsis_id}" if gsis_id else f"{_MFL_PREFIX}{mfl_id}"


def _is_placeholder_key(key: str) -> bool:
    return key.startswith(_PLACEHOLDER_PREFIXES)


def _placeholder_expr(column: str) -> pl.Expr:
    """The Polars predicate for ``_is_placeholder_key``, derived from the same constant."""
    return pl.any_horizontal([pl.col(column).str.starts_with(p) for p in _PLACEHOLDER_PREFIXES])


#: The Core table, used for the batched write statements. `session.execute` with a parameter
#: LIST against an ORM *entity* is read as an ORM bulk-update-by-primary-key; the Table is a
#: plain Core executemany, matching the `inserts` call.
_PEI = PlayerExternalId.__table__

#: Exactly the columns `apply_playerids` reads off `player_external_ids`. Selected as Row
#: tuples: materializing the whole crosswalk as ORM entities costs an identity-map entry and
#: a full instance per row for a table that grows with every source, and nothing here needs
#: a persistent instance (every write below is a Core `insert`/`update`/`delete` by key).
_CROSSWALK_COLUMNS = (
    PlayerExternalId.source,
    PlayerExternalId.external_id,
    PlayerExternalId.player_id,
    PlayerExternalId.match_method,
    PlayerExternalId.confidence,
    PlayerExternalId.verified_at,
)


class DynastyProcessError(RuntimeError):
    """The DP frame is missing required columns."""


class CrosswalkConflictError(RuntimeError):
    """An existing (source, external_id) row that OUTRANKS DynastyProcess points at a
    different player than DP says — `manual`, `dynastyprocess`, verified, or a tombstone.

    An unverified `exact_name`/`fuzzy` guess pointing elsewhere is NOT this: DP's 1.0 fact
    outranks it (the authority rule), so it is re-pointed in place. Raising on it aborted
    the whole seed — `seed_players` included — over one stale ladder row."""

    def __init__(self, conflicts: list[tuple[str, str, uuid.UUID, uuid.UUID]]) -> None:
        self.conflicts = conflicts
        shown = ", ".join(f"{s}:{e} db={a} dp={b}" for s, e, a, b in conflicts[:10])
        super().__init__(
            f"{len(conflicts)} DynastyProcess id(s) conflict with existing crosswalk rows "
            f"(first: {shown}). Resolve by hand (ffh crosswalk verify --reject) and re-run."
        )


@dataclass(frozen=True)
class CrosswalkApplyReport:
    """Every id cell in the file ends in exactly one of these buckets — the four
    ``*ambiguous*``/``blocked*`` tuples are also **queued in ``crosswalk_unmatched``**, so a
    known-but-unmapped id can never leave ``ffh crosswalk report`` green. The one
    exception: an ``ambiguous_in_file`` key that already has a live mapping is reported but
    NOT queued — it is mapped, and queueing it re-opened the entry on every seed."""

    inserted: int
    updated: int
    unchanged: int
    created_players: int
    #: Placeholder `players` rows (gsis_id NULL, minted by an earlier apply) folded into the
    #: real registry player once nflverse published that person's gsis_id.
    merged_placeholders: int
    skipped_no_ids: int
    skipped_position: int
    skipped_dst: int
    skipped_no_person_key: int
    #: DynastyProcess contradicts itself (one id on two players, or one player with two
    #: ids for a source) — nothing was written for these keys.
    ambiguous_in_file: tuple[tuple[str, str], ...]
    #: A pre-existing DB row (human/DP/verified) holds the player's one slot for the source.
    blocked_by_existing: tuple[tuple[str, str], ...]
    #: A human rejected exactly this pairing (`match_method='rejected'` tombstone).
    blocked_by_rejection: tuple[tuple[str, str], ...]
    #: Unverified guesses evicted by DP's 1.0 fact; each is back on the review queue.
    displaced: tuple[tuple[str, str], ...]


def read_playerids_csv(raw: bytes) -> pl.DataFrame:
    """Parse the CSV with every id column as text and ``NA`` as null."""
    # utf-8-sig: a UTF-8 BOM would otherwise hide inside the first column name and
    # surface as a bogus "mfl_id missing" error.
    header = raw.split(b"\n", 1)[0].decode("utf-8-sig").strip().split(",")
    missing = DP_REQUIRED_COLUMNS - set(header)
    if missing:
        raise DynastyProcessError(f"db_playerids.csv missing columns: {sorted(missing)}")
    return pl.read_csv(
        raw,
        null_values=["NA", ""],
        schema_overrides={c: pl.Utf8 for c in DP_TEXT_COLUMNS},
        infer_schema_length=20000,
    )


def _validate(df: pl.DataFrame) -> tuple[pl.DataFrame, int]:
    """Type-check the id columns and drop rows with no person key. Returns
    ``(frame, skipped_no_person_key)``."""
    missing = DP_REQUIRED_COLUMNS - set(df.columns)
    if missing:
        raise DynastyProcessError(f"DynastyProcess frame missing columns: {sorted(missing)}")
    # Defensive: a Parquet round-trip could have typed ids numerically. A blind
    # Float -> Utf8 cast would mangle 4046.0 into "4046.0", so integral floats are
    # routed back through Int64; a non-integral (or non-finite) value means the ids
    # are corrupt and the frame is rejected outright.
    casts: list[pl.Expr] = []
    for c in sorted(DP_TEXT_COLUMNS):
        if df.schema[c].is_float():
            n_bad = df.filter(
                pl.col(c).is_not_null()
                & (~pl.col(c).is_finite() | (pl.col(c) != pl.col(c).round(0)))
            ).height
            if n_bad:
                raise DynastyProcessError(
                    f"id column {c!r} is {df.schema[c]} with {n_bad} non-integral value(s); "
                    "ids must never pass through floats"
                )
            casts.append(pl.col(c).cast(pl.Int64).cast(pl.Utf8))
        else:
            casts.append(pl.col(c).cast(pl.Utf8))
    df = df.with_columns(casts)
    # A row with neither gsis_id nor mfl_id has no person key at all: its placeholder
    # would be the literal string "mfl:None", which every such row shares — one invented
    # `players` row silently accumulating ids belonging to several different people. The
    # ambiguity pass only catches the overlapping case (two rows sharing an id), never the
    # disjoint one. Drop them: counted and logged, never silently merged.
    has_person_key = pl.col("gsis_id").is_not_null() | pl.col("mfl_id").is_not_null()
    kept = df.filter(has_person_key)
    skipped = df.height - kept.height
    if skipped:
        log.warning("crosswalk.dynastyprocess.skipped_no_person_key", count=skipped)
    return kept, skipped


def _split_name(name: str) -> tuple[str | None, str | None]:
    parts = name.strip().split(maxsplit=1)
    if len(parts) == 2:
        return parts[0], parts[1]
    return (parts[0], None) if parts else (None, None)


def _parse_date(raw: object) -> date | None:
    if raw is None or raw == "":
        return None
    if isinstance(raw, date):
        return raw
    try:
        return date.fromisoformat(str(raw))
    except ValueError:
        return None


def _load_crosswalk(session: Session) -> list[Row[Any]]:
    """The DP-owned slice of ``player_external_ids`` as Row tuples (see _CROSSWALK_COLUMNS)."""
    return list(
        session.execute(
            select(*_CROSSWALK_COLUMNS).where(
                PlayerExternalId.source.in_(sorted(DP_ID_COLUMNS.values()))
            )
        ).all()
    )


def _index_crosswalk(
    rows: list[Row[Any]],
) -> tuple[
    dict[tuple[str, str], Row[Any]],
    dict[tuple[str, str], Row[Any]],
    dict[tuple[str, uuid.UUID], Row[Any]],
]:
    """``(tombstones, existing, held_by_player)`` — the three lookups step 3-7 rule with.

    Tombstones (``match_method='rejected'``) are NOT mappings: they must not satisfy an id
    lookup, must not trip the conflict scan, and must not occupy the player's one slot for
    the source (the unique index is partial on the same predicate). What they DO is veto
    re-minting the exact pairing a human rejected.
    """
    tombstones = {(r.source, r.external_id): r for r in rows if r.match_method == REJECTED_METHOD}
    live = [r for r in rows if r.match_method != REJECTED_METHOD]
    existing = {(r.source, r.external_id): r for r in live}
    # (source, player_id) → the row already holding that slot: the DB enforces one id per
    # source per player (player_external_ids_source_player_uidx), so this map is well-defined.
    held_by_player = {(r.source, r.player_id): r for r in live}
    return tombstones, existing, held_by_player


def _merge_placeholder_player(
    session: Session, placeholder_id: uuid.UUID, keeper_id: uuid.UUID
) -> list[tuple[str, str]]:
    """Fold a placeholder ``players`` row into the registry player for the same human.

    Repoints every ``player_external_ids`` row (tombstones included — the rejection was
    about this person) and deletes the placeholder. Returns the keys that could NOT be
    carried over because the keeper already holds a live id for that source; they are
    deleted with the placeholder and the caller MUST queue them (Global Constraint: an id a
    writer refuses to map is never dropped).
    """
    keeper_sources = set(
        session.scalars(
            select(PlayerExternalId.source).where(
                PlayerExternalId.player_id == keeper_id,
                PlayerExternalId.match_method != REJECTED_METHOD,
            )
        )
    )
    orphaned: list[tuple[str, str]] = []
    rows = session.execute(
        select(
            PlayerExternalId.source, PlayerExternalId.external_id, PlayerExternalId.match_method
        ).where(PlayerExternalId.player_id == placeholder_id)
    ).all()
    for source, external_id, match_method in rows:
        key_filter = (
            PlayerExternalId.source == source,
            PlayerExternalId.external_id == external_id,
        )
        if match_method != REJECTED_METHOD and source in keeper_sources:
            # The keeper's one slot for this source is taken by a different id.
            orphaned.append((source, external_id))
            session.execute(delete(PlayerExternalId).where(*key_filter))
            continue
        if match_method != REJECTED_METHOD:
            keeper_sources.add(source)
        session.execute(update(PlayerExternalId).where(*key_filter).values(player_id=keeper_id))
    session.execute(delete(Player).where(Player.player_id == placeholder_id))
    session.flush()
    return orphaned


def _reconcile_placeholders(
    session: Session,
    rows: pl.DataFrame,
    gsis_to_pid: dict[str, uuid.UUID],
    id_hit: Callable[[dict[str, object]], str | None],
    queue: Callable[[str, str], None],
) -> int:
    """Fold yesterday's ``mfl:`` placeholders into today's real registry players.

    A DP row with no ``gsis_id`` mints a placeholder ``players`` row carrying
    ``gsis_id = NULL``. When nflverse later publishes that player, ``seed_players``' ON
    CONFLICT is on ``gsis_id`` — NULL conflicts with nothing — so it inserts a **second**
    ``players`` row for the same human. From then on the two writers disagree forever: the
    DP row now resolves by gsis to the new row while its ids still point at the placeholder,
    so the conflict scan raises on every run (the incumbent is `dynastyprocess`, which
    outranks DP and is therefore never re-pointed), and rung 3 sees two
    ``(normalized_name, position)`` candidates and returns None.

    This pre-pass is the repair: the placeholder's ids are repointed at the gsis player and
    the placeholder row is deleted. It is deliberately the FIRST write in ``apply_playerids``
    so the step-5 conflict scan (and everything after it) reads the repointed rows.
    """
    merged = 0
    seen: set[uuid.UUID] = set()
    for row in rows.iter_rows(named=True):
        gsis = row["gsis_id"]
        keeper = gsis_to_pid.get(gsis) if gsis else None
        if keeper is None:
            continue
        hit = id_hit(row)
        if hit is None:
            continue
        placeholder_id = uuid.UUID(hit)
        if placeholder_id == keeper or placeholder_id in seen:
            continue
        player = session.get(Player, placeholder_id)
        # `gsis_id IS NULL` is what identifies a placeholder. DST rows are excluded
        # explicitly: the 32 seeded defenses also carry a NULL gsis_id and must never be
        # merged into a person.
        if player is None or player.gsis_id is not None or player.position == "DST":
            continue
        seen.add(placeholder_id)
        log.warning(
            "crosswalk.dynastyprocess.placeholder_merged",
            placeholder_player_id=str(placeholder_id),
            keeper_player_id=str(keeper),
            gsis_id=gsis,
            name=row["name"],
        )
        for source, external_id in _merge_placeholder_player(session, placeholder_id, keeper):
            log.warning(
                "crosswalk.dynastyprocess.placeholder_merge_orphaned_id",
                source=source,
                external_id=external_id,
                keeper_player_id=str(keeper),
            )
            queue(source, external_id)
        merged += 1
    return merged


def apply_playerids(session: Session, df: pl.DataFrame) -> CrosswalkApplyReport:
    """Populate ``player_external_ids`` at confidence 1.0 / ``dynastyprocess`` from a DP frame.

    Policies 1-7 are in the plan (Task 4) and DATABASE.md §3. Runs inside the caller's
    transaction; a ``CrosswalkConflictError`` is raised before any write — placeholder
    ``players`` rows AND ``crosswalk_unmatched`` queueing both happen only after the
    conflict scan passes, so an aborted seed leaves nothing behind in either table.

    The one deliberate exception is step 3a, ``_reconcile_placeholders``: a duplicate
    ``players`` row for a human nflverse has since published makes the conflict scan raise
    on *every* run, so the repair has to run before the scan it unblocks. It touches only
    the two rows for that one person, and the CLI still commits nothing if the scan raises.
    """
    df, skipped_no_person_key = _validate(df)
    n_in = df.height

    # 1. positions
    df = df.with_columns(
        pl.col("position")
        .map_elements(normalize_position, return_dtype=pl.Utf8)
        .alias("position_norm")
    )
    keep_pos = pl.col("position_norm").is_in(sorted(FANTASY_POSITIONS)).fill_null(False)
    kept = df.filter(keep_pos)
    skipped_position = df.filter(~keep_pos).height
    assert kept.height + skipped_position == n_in, "row loss in position filter"

    # 2. rows without any id
    has_id = pl.any_horizontal([pl.col(c).is_not_null() for c in DP_ID_COLUMNS])
    with_ids = kept.filter(has_id)
    skipped_no_ids = kept.filter(~has_id).height
    assert with_ids.height + skipped_no_ids == kept.height, "row loss in id filter"

    # 3. player assignment
    gsis_to_pid: dict[str, uuid.UUID] = dict(iter_gsis_to_player_id(session))
    dst_to_pid: dict[str, uuid.UUID] = dict(
        session.execute(
            select(Player.normalized_name, Player.player_id).where(Player.position == "DST")
        ).all()
    )
    tombstones, existing, held_by_player = _index_crosswalk(_load_crosswalk(session))
    # Slots claimed during THIS apply (by an insert or a displacement).
    claimed: set[tuple[str, uuid.UUID]] = set()

    def _existing_id_hit(row: dict[str, object]) -> str | None:
        """Any of the row's ids already mapped → that player (idempotent for rookies).

        Tombstones count *for identity only*: the rejection ruled on one id↔player
        pairing, not on who this DP row is about. Ignoring them would send a gsis-less row
        down the placeholder path on the next seed and mint a DUPLICATE `players` row —
        exactly the silent wrongness the crosswalk exists to prevent. The rejected id
        itself is still refused below (`blocked_by_rejection`); only the row's *other* ids
        follow the identity.
        """
        for col, source in DP_ID_COLUMNS.items():
            ext = row[col]
            if ext:
                hit = existing.get((source, ext)) or tombstones.get((source, ext))
                if hit is not None:
                    return str(hit.player_id)
        return None

    # (source, external_id) → the DP row's raw name/position/team, for the review-queue
    # payload of every id this apply refuses to write. First occurrence wins (CSV order).
    raw_context: dict[tuple[str, str], tuple[str | None, str | None, str | None]] = {}
    for row in with_ids.iter_rows(named=True):
        for col, source in DP_ID_COLUMNS.items():
            ext = row[col]
            if ext:
                raw_context.setdefault((source, ext), (row["name"], row["position"], row["team"]))

    def _queue(source: str, external_id: str) -> None:
        """Park a known-but-unmapped DynastyProcess id on the review queue.

        Global Constraint / DATABASE.md §3 rung 5: every unresolved id lands in
        `crosswalk_unmatched`. A report field and a log line are not the gate — without
        this, `ffh crosswalk report` exits 0 while fantasy-relevant ids are unmapped.

        A *displaced* incumbent is usually absent from the DP file (it was minted by the
        ladder, not by DynastyProcess). `upsert_unmatched` refreshes raw_* from its
        arguments, so fall back to whatever context the queue already holds instead of
        blanking it.
        """
        raw = raw_context.get((source, external_id))
        fields = (
            {"raw_name": raw[0], "raw_position": raw[1], "raw_team": raw[2]}
            if raw is not None
            else queued_raw_context(session, source, external_id)
        )
        upsert_unmatched(session, source, external_id, **fields)

    # 3a. Reconcile duplicate `players` rows for one human BEFORE anything reads `existing`
    # for real. This is the one write that precedes the conflict scan, and it exists
    # precisely so the scan stops raising forever on a placeholder nflverse has since
    # published (see `_reconcile_placeholders`). The three lookups are re-derived after it.
    merged_placeholders = _reconcile_placeholders(
        session, with_ids, gsis_to_pid, _existing_id_hit, _queue
    )
    if merged_placeholders:
        tombstones, existing, held_by_player = _index_crosswalk(_load_crosswalk(session))

    player_key: list[str | None] = []  # str(uuid) known / "gsis:…"/"mfl:…" placeholders
    skipped_dst = 0
    for row in with_ids.iter_rows(named=True):
        key: str | None
        if row["position_norm"] == "DST":
            # Defenses come only from seed_dst_players. A DST row that maps to neither a
            # seeded DST player nor an already-crosswalked id is counted and skipped —
            # it must NEVER fall through to the placeholder path and create a player.
            # NAME first, then team — the same argument order `resolve._canonical_name`
            # passes. When the two disagree ("Kansas City Chiefs" listed at DEN) the two
            # writers used to canonicalize the row to different defenses depending on which
            # one saw it first (DATABASE.md §3, DST canonicalization precedence).
            nn = canonical_dst_key(row["name"], row["team"])
            pid = dst_to_pid.get(nn) if nn else None
            key = str(pid) if pid is not None else _existing_id_hit(row)
            if key is None:
                skipped_dst += 1
                log.warning(
                    "crosswalk.dynastyprocess.skipped_dst",
                    name=row["name"],
                    team=row["team"],
                    mfl_id=row["mfl_id"],
                )
        else:
            key = None
            if row["gsis_id"] and row["gsis_id"] in gsis_to_pid:
                key = str(gsis_to_pid[row["gsis_id"]])
            if key is None:
                key = _existing_id_hit(row)
            if key is None:
                key = _placeholder_key(row["gsis_id"], row["mfl_id"])
        player_key.append(key)
    with_ids = with_ids.with_columns(pl.Series("player_key", player_key, dtype=pl.Utf8))
    mapped = with_ids.filter(pl.col("player_key").is_not_null())
    assert mapped.height + skipped_dst == with_ids.height, "row loss in player assignment"

    # 4. unpivot + ambiguity (one id per (source, player); one player per (source, id))
    # n_cells is counted straight off the wide frame so the unpivot chain is tied back
    # to its input in id-cell space (kept + dropped == input, Global Constraints).
    n_cells = int(
        mapped.select(
            pl.sum_horizontal([pl.col(c).is_not_null() for c in DP_ID_COLUMNS]).sum().fill_null(0)
        ).item()
    )
    cells = (
        mapped.select(["mfl_id", "player_key", *DP_ID_COLUMNS])
        .unpivot(
            on=list(DP_ID_COLUMNS),
            index=["mfl_id", "player_key"],
            variable_name="col",
            value_name="external_id",
        )
        .drop_nulls("external_id")
        .with_columns(pl.col("col").replace_strict(DP_ID_COLUMNS).alias("source"))
        .select(["source", "external_id", "player_key"])
    )
    assert cells.height == n_cells, "row loss in unpivot"
    long = cells.unique()
    n_long = long.height
    n_deduped = n_cells - n_long
    assert n_long + n_deduped == n_cells, "row loss in dedupe"
    if n_deduped:
        # Identical (source, external_id, player_key) triples collapsing into one —
        # e.g. a glitch pair sharing every id. Not a drop, but counted and logged.
        log.info("crosswalk.dynastyprocess.deduped_id_cells", count=n_deduped)
    many_players = (
        long.group_by(["source", "external_id"])
        .agg(pl.col("player_key").n_unique().alias("n"))
        .filter(pl.col("n") > 1)
        .select(["source", "external_id"])
    )
    many_ids = (
        long.group_by(["source", "player_key"])
        .agg(pl.col("external_id").n_unique().alias("n"))
        .filter(pl.col("n") > 1)
        .select(["source", "player_key"])
    )
    bad = pl.concat(
        [
            long.join(many_players, on=["source", "external_id"], how="semi"),
            long.join(many_ids, on=["source", "player_key"], how="semi"),
        ]
    ).unique()
    bad_keys = bad.select(["source", "external_id"]).unique()
    n_bad_rows = long.join(bad_keys, on=["source", "external_id"], how="semi").height
    clean = long.join(bad_keys, on=["source", "external_id"], how="anti")
    assert clean.height + n_bad_rows == n_long, "row loss in ambiguity pass"
    ambiguous = {(r["source"], r["external_id"]) for r in bad_keys.iter_rows(named=True)}
    if ambiguous:
        log.warning(
            "crosswalk.dynastyprocess.ambiguous_ids",
            count=len(ambiguous),
            sample=sorted(ambiguous)[:10],
        )
    # Deterministic apply/report order regardless of CSV row order.
    clean = clean.sort(["source", "external_id"])

    # 5. conflict scan (vs the DB) — BEFORE any write, including placeholder players and
    # the ambiguity queueing below. Placeholder rows cannot conflict: step 3 assigned them
    # a placeholder precisely because none of their ids exist in player_external_ids.
    # `existing` excludes tombstones, so a rejected pairing does not raise here — step 7
    # rules on it (and a rejection would otherwise fail every subsequent seed run).
    #
    # A mismatch is only a CONFLICT when the incumbent outranks DP's 1.0 fact. An
    # unverified `exact_name`/`fuzzy` guess does not: it is exactly what rung 1's upgrade
    # path re-points without complaint, so raising on it would abort the whole seed —
    # `ffh crosswalk seed` exits before `session.commit()`, discarding `seed_players` too —
    # over one stale ladder guess. Those keys are re-pointed in place instead (step 7).
    conflicts: list[tuple[str, str, uuid.UUID, uuid.UUID]] = []
    stale_guesses: dict[tuple[str, str], Row[Any]] = {}
    for r in clean.iter_rows(named=True):
        if _is_placeholder_key(r["player_key"]):
            continue
        pid = uuid.UUID(r["player_key"])
        ex = existing.get((r["source"], r["external_id"]))
        if ex is not None and ex.player_id != pid:
            if is_displaceable(ex.match_method, ex.verified_at):
                stale_guesses[(r["source"], r["external_id"])] = ex
            else:
                conflicts.append((r["source"], r["external_id"], ex.player_id, pid))
    if conflicts:
        log.error("crosswalk.dynastyprocess.conflicts", count=len(conflicts))
        raise CrosswalkConflictError(conflicts)

    # --- past this point the function writes. Nothing above it may. ---
    for (source, ext), ex in sorted(stale_guesses.items()):
        log.warning(
            "crosswalk.dynastyprocess.stale_guess_repointed",
            source=source,
            external_id=ext,
            stored_method=ex.match_method,
            stored_player_id=str(ex.player_id),
        )
        # Hidden from step 7's `existing` lookup so the key takes the full write path:
        # the (source, player_id) slot check and the authority rule both apply to it.
        del existing[(source, ext)]
        held_by_player.pop((source, ex.player_id), None)

    # 5b. the ambiguous ids DP contradicts itself on go on the review queue — EXCEPT the
    # ones that already have a live mapping. Those are mapped: DynastyProcess being
    # self-contradictory about an id it previously agreed on is a reason to report it, not
    # to re-open a queue entry an operator already ruled on. The glitch ids are permanent
    # (DATA_SOURCES.md §5), so queueing them every seed made the gate unreachable.
    for source, ext in sorted(ambiguous):
        live = existing.get((source, ext))
        if live is not None:
            log.info(
                "crosswalk.dynastyprocess.ambiguous_but_mapped",
                source=source,
                external_id=ext,
                match_method=live.match_method,
                player_id=str(live.player_id),
            )
            continue
        _queue(source, ext)

    # 6. create players for placeholders that still hold ≥ 1 id
    is_placeholder = _placeholder_expr("player_key")
    needed = sorted(set(clean.filter(is_placeholder)["player_key"].to_list()))
    created: dict[str, uuid.UUID] = {}
    # keep="first" in CSV order: a glitch pair sharing one placeholder key is named
    # from its first row (deterministic — the file order is the stable key here).
    first_rows = mapped.filter(pl.col("player_key").is_in(needed)).unique(
        subset=["player_key"], keep="first", maintain_order=True
    )
    for row in first_rows.iter_rows(named=True):
        assert row["position_norm"] != "DST", "DST rows must never reach the placeholder path"
        first, last = _split_name(row["name"])
        player = Player(
            gsis_id=row["gsis_id"],
            full_name=row["name"],
            first_name=first,
            last_name=last,
            normalized_name=normalize_name(row["name"]),
            position=row["position_norm"],
            birth_date=_parse_date(row["birthdate"]),
            rookie_year=int(row["draft_year"]) if row["draft_year"] is not None else None,
            college=row["college"],
            team_abbr=normalize_team(row["team"]),
        )
        session.add(player)
        session.flush()
        created[row["player_key"]] = player.player_id
    log.info("crosswalk.dynastyprocess.created_players", count=len(created))

    # 7. partition against existing rows, guarding (source, player_id) uniqueness.
    inserts: list[dict[str, object]] = []
    updates: list[tuple[str, str]] = []
    repoints: list[tuple[str, str, uuid.UUID]] = []
    unchanged = 0
    blocked_by_existing: list[tuple[str, str]] = []
    blocked_by_rejection: list[tuple[str, str]] = []
    displaced: list[tuple[str, str]] = []
    for r in clean.iter_rows(named=True):
        source, ext = r["source"], r["external_id"]
        pid = created.get(r["player_key"]) or uuid.UUID(r["player_key"])
        tomb = tombstones.get((source, ext))
        if tomb is not None and tomb.player_id == pid:
            # A human rejected exactly this pairing. DP re-asserting it is not new
            # evidence — the tombstone stands and the id stays on the gate.
            blocked_by_rejection.append((source, ext))
            log.warning(
                "crosswalk.dynastyprocess.blocked_by_rejection",
                source=source,
                external_id=ext,
                player_id=str(pid),
            )
            _queue(source, ext)
            continue
        ex = existing.get((source, ext))
        if ex is not None:
            # ex.player_id == pid here: step 5 raised on any mismatch.
            if (
                ex.match_method == DP_METHOD
                and abs(float(ex.confidence) - DP_CONFIDENCE) <= CONFIDENCE_EPSILON
            ):
                unchanged += 1
            else:
                updates.append((source, ext))
            continue
        held = held_by_player.get((source, pid))
        if held is not None or (source, pid) in claimed:
            # The player already holds a different id from this source (unique index
            # player_external_ids_source_player_uidx forbids a second).
            if held is not None and is_displaceable(held.match_method, held.verified_at):
                # AUTHORITY RULE: DP's 1.0 fact outranks an unverified `exact_name` 0.95 /
                # `fuzzy` 0.89 *guess* — the same ruling rung 1's upgrade path already
                # makes. The guess is evicted and ITS id goes back on the review queue;
                # keeping it would enshrine a low-confidence guess over a 1.0 fact.
                log.warning(
                    "crosswalk.dynastyprocess.incumbent_displaced",
                    source=source,
                    external_id=ext,
                    displaced_external_id=held.external_id,
                    displaced_method=held.match_method,
                    player_id=str(pid),
                )
                displaced.append((source, held.external_id))
                session.execute(
                    delete(PlayerExternalId).where(
                        PlayerExternalId.source == source,
                        PlayerExternalId.external_id == held.external_id,
                    )
                )
                session.flush()
                del held_by_player[(source, pid)]
                existing.pop((source, held.external_id), None)
                _queue(source, held.external_id)
            else:
                # A human/DP/verified row — or a slot this batch already claimed (defence
                # in depth: the many_ids ambiguity pass should have removed those, and
                # clean's (source, external_id) sort keeps the winner deterministic).
                # The holder wins; the loser is queued, reported, never raised.
                blocked_by_existing.append((source, ext))
                log.warning(
                    "crosswalk.dynastyprocess.duplicate_source_for_player",
                    source=source,
                    external_id=ext,
                    held_external_id=held.external_id if held is not None else None,
                    held_method=held.match_method if held is not None else "claimed_in_batch",
                    player_id=str(pid),
                )
                _queue(source, ext)
                continue
        claimed.add((source, pid))
        if tomb is not None or (source, ext) in stale_guesses:
            # The key already has a row pointing at another player — a tombstone (the
            # rejection was about that other player, and this is the correction it asked
            # for) or a stale ladder guess DP contradicts. Either way the primary key is
            # taken, so repoint in place rather than inserting.
            repoints.append((source, ext, pid))
        else:
            inserts.append(
                {
                    "player_id": pid,
                    "source": source,
                    "external_id": ext,
                    "confidence": DP_CONFIDENCE,
                    "match_method": DP_METHOD,
                }
            )
    assert (
        len(inserts)
        + len(updates)
        + len(repoints)
        + unchanged
        + len(blocked_by_existing)
        + len(blocked_by_rejection)
        == clean.height
    ), "row loss in partition"

    # Repoints run BEFORE the inserts: a repointed row still occupies its old
    # (source, player_id) slot, and this batch may be inserting a different id for that
    # old player — the partial unique index would refuse it in the other order.
    # One statement per batch, not per row: the live file is ~12.5k rows and a first seed
    # against a partially-populated crosswalk can repoint/update thousands of them.
    key_match = (
        _PEI.c.source == bindparam("b_source"),
        _PEI.c.external_id == bindparam("b_external_id"),
    )
    if repoints:
        session.execute(
            update(_PEI)
            .where(*key_match)
            .values(
                player_id=bindparam("b_player_id"),
                match_method=DP_METHOD,
                confidence=DP_CONFIDENCE,
                verified_at=None,
            ),
            [
                {"b_source": source, "b_external_id": ext, "b_player_id": pid}
                for source, ext, pid in repoints
            ],
        )
        session.flush()
        for source, ext, _pid in repoints:
            close_unmatched(session, source, ext)
    if inserts:
        session.execute(_PEI.insert(), inserts)
        # Every inserted key now has a mapping row: close its review-queue entry if one
        # is open (a rung-5-queued id that DP later maps). Intersect against the open
        # queue rows first — the queue is small, the insert batch can be tens of
        # thousands of rows, and per-key UPDATEs for all of them would be wasteful.
        inserted_keys = {(str(i["source"]), str(i["external_id"])) for i in inserts}
        open_keys = {
            (s, e)
            for s, e in session.execute(
                select(CrosswalkUnmatched.source, CrosswalkUnmatched.external_id).where(
                    CrosswalkUnmatched.resolved.is_(False),
                    CrosswalkUnmatched.source.in_(sorted({s for s, _ in inserted_keys})),
                )
            )
        }
        for source, ext in sorted(open_keys & inserted_keys):
            close_unmatched(session, source, ext)
    if updates:
        session.execute(
            update(_PEI).where(*key_match).values(match_method=DP_METHOD, confidence=DP_CONFIDENCE),
            [{"b_source": source, "b_external_id": ext} for source, ext in updates],
        )
    session.flush()

    report = CrosswalkApplyReport(
        inserted=len(inserts),
        updated=len(updates) + len(repoints),
        unchanged=unchanged,
        created_players=len(created),
        merged_placeholders=merged_placeholders,
        skipped_no_ids=skipped_no_ids,
        skipped_position=skipped_position,
        skipped_dst=skipped_dst,
        skipped_no_person_key=skipped_no_person_key,
        ambiguous_in_file=tuple(sorted(ambiguous)),
        blocked_by_existing=tuple(sorted(blocked_by_existing)),
        blocked_by_rejection=tuple(sorted(blocked_by_rejection)),
        displaced=tuple(sorted(displaced)),
    )
    log.info("crosswalk.dynastyprocess.applied", **asdict(report))
    return report


# ---------------------------------------------------------------------------------------
# The ingest job — the only ``ffh.ingest`` dependency in ``ffh.crosswalk``.
# ---------------------------------------------------------------------------------------

#: DATA_SOURCES.md §5, verified live 2026-08-16 (12,472 rows, 35 cols, ``NA`` = null).
DP_URL = "https://raw.githubusercontent.com/dynastyprocess/data/master/files/db_playerids.csv"
#: Row-count floor for a landable snapshot. `validate` only ever rejected an EMPTY frame, so
#: a truncated upstream file — 200 rows of the usual ~12.5k — landed as a *successful*
#: partition; and because the lake never overwrites a partition (`PartitionExistsError` →
#: `skipped`), that day could not be cleanly re-landed once the file recovered. The floor is
#: well under the live count so normal shrinkage (a source dropping retired players) is not
#: an outage, but far above anything a truncation produces.
DP_MIN_ROWS = 8000


@register
class DynastyProcessPlayerIdsJob(HttpIngestJob):
    """Land ``db_playerids.csv`` in the lake as Parquet with every id column still text.

    Weekly full snapshot: no season, ETag-conditional via ③'s ``HttpIngestJob.fetch``, and a
    new ``scrape_date=`` partition per scrape (never an overwrite). No ``persist``: the CSV
    reaches Postgres only through ``ffh crosswalk seed --playerids``, which re-reads the
    landed Parquet and calls ``apply_playerids``.
    """

    name: ClassVar[str] = "dynastyprocess_playerids"
    source: ClassVar[str] = "dynastyprocess"
    asset: ClassVar[str] = "playerids"
    REQUIRED_COLUMNS: ClassVar[frozenset[str]] = DP_REQUIRED_COLUMNS

    def url(self) -> str:
        return DP_URL

    def partition(self) -> dict[str, str]:
        # ③'s UTC clock — the same key every other lake partition uses.
        return {"scrape_date": scrape_date()}

    def parse(self, content: bytes) -> pl.DataFrame:
        return read_playerids_csv(content)

    def validate(self, df: pl.DataFrame) -> None:
        # The base checks REQUIRED_COLUMNS and the empty frame (both IngestValidationError).
        super().validate(df)
        # …but "not empty" is not "not truncated": see DP_MIN_ROWS.
        if df.height < DP_MIN_ROWS:
            raise IngestValidationError(
                f"{type(self).name}: {df.height} rows is below the {DP_MIN_ROWS}-row floor; "
                "the upstream snapshot looks truncated and must not land"
            )
        # Ids must land as text so the Parquet round-trip that `ffh crosswalk seed` reads
        # back can never hand `_validate` a float: 4046.0 -> "4046.0" would be silent
        # corruption, and a UUID/alphanumeric id (sportradar_id, pfr_id) is not numeric at all.
        wrong = [c for c in sorted(DP_TEXT_COLUMNS) if df.schema[c] != pl.Utf8]
        if wrong:
            raise IngestValidationError(
                f"{type(self).name}: id columns must be text, got non-text: {wrong}"
            )
