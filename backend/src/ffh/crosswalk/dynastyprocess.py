"""DynastyProcess ``db_playerids.csv`` → ``player_external_ids`` (rung 1 of the ladder).

``apply_playerids`` and everything above it is pure with respect to ``ffh.ingest``: it takes
a DataFrame, never a URL or a lake path. The one exception is ``DynastyProcessPlayerIdsJob``
at the bottom of the module — the ingest job that fetches the CSV into the lake.
"""

from __future__ import annotations

import uuid
from dataclasses import asdict, dataclass
from datetime import date
from typing import ClassVar

import polars as pl
import structlog
from sqlalchemy import select, update
from sqlalchemy.orm import Session

from ffh.crosswalk.normalize import (
    FANTASY_POSITIONS,
    normalize_dst,
    normalize_name,
    normalize_position,
    normalize_team,
)
from ffh.crosswalk.registry import iter_gsis_to_player_id
from ffh.crosswalk.resolve import (
    REJECTED_METHOD,
    close_unmatched,
    displaceable,
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

_PLACEHOLDER_PREFIXES = ("gsis:", "mfl:")


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


def apply_playerids(session: Session, df: pl.DataFrame) -> CrosswalkApplyReport:
    """Populate ``player_external_ids`` at confidence 1.0 / ``dynastyprocess`` from a DP frame.

    Policies 1-7 are in the plan (Task 4) and DATABASE.md §3. Runs inside the caller's
    transaction; a ``CrosswalkConflictError`` is raised before any write — placeholder
    ``players`` rows AND ``crosswalk_unmatched`` queueing both happen only after the
    conflict scan passes, so an aborted seed leaves nothing behind in either table.
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
    existing_rows = session.scalars(
        select(PlayerExternalId).where(PlayerExternalId.source.in_(sorted(DP_ID_COLUMNS.values())))
    ).all()
    # Tombstones (`match_method='rejected'`) are NOT mappings: they must not satisfy an
    # id lookup, must not trip the conflict scan, and must not occupy the player's one
    # slot for the source (the unique index is partial on the same predicate). What they
    # DO is veto re-minting the exact pairing a human rejected.
    tombstones: dict[tuple[str, str], PlayerExternalId] = {
        (r.source, r.external_id): r for r in existing_rows if r.match_method == REJECTED_METHOD
    }
    existing: dict[tuple[str, str], PlayerExternalId] = {
        (r.source, r.external_id): r for r in existing_rows if r.match_method != REJECTED_METHOD
    }
    # (source, player_id) → the row already holding that slot: the DB enforces one id per
    # source per player (player_external_ids_source_player_uidx), so this map is well-defined.
    held_by_player: dict[tuple[str, uuid.UUID], PlayerExternalId] = {
        (r.source, r.player_id): r for r in existing_rows if r.match_method != REJECTED_METHOD
    }
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

    player_key: list[str | None] = []  # str(uuid) known / "gsis:…"/"mfl:…" placeholders
    skipped_dst = 0
    for row in with_ids.iter_rows(named=True):
        key: str | None
        if row["position_norm"] == "DST":
            # Defenses come only from seed_dst_players. A DST row that maps to neither a
            # seeded DST player nor an already-crosswalked id is counted and skipped —
            # it must NEVER fall through to the placeholder path and create a player.
            nn = normalize_dst(row["team"]) or normalize_dst(row["name"])
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
                key = f"gsis:{row['gsis_id']}" if row["gsis_id"] else f"mfl:{row['mfl_id']}"
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
    stale_guesses: dict[tuple[str, str], PlayerExternalId] = {}
    for r in clean.iter_rows(named=True):
        if r["player_key"].startswith(_PLACEHOLDER_PREFIXES):
            continue
        pid = uuid.UUID(r["player_key"])
        ex = existing.get((r["source"], r["external_id"]))
        if ex is not None and ex.player_id != pid:
            if displaceable(ex):
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
    is_placeholder = pl.col("player_key").str.starts_with("gsis:") | pl.col(
        "player_key"
    ).str.starts_with("mfl:")
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
            if ex.match_method == DP_METHOD and ex.confidence == DP_CONFIDENCE:
                unchanged += 1
            else:
                updates.append((source, ext))
            continue
        held = held_by_player.get((source, pid))
        if held is not None or (source, pid) in claimed:
            # The player already holds a different id from this source (unique index
            # player_external_ids_source_player_uidx forbids a second).
            if held is not None and displaceable(held):
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
                session.delete(held)
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
    for source, ext, pid in repoints:
        session.execute(
            update(PlayerExternalId)
            .where(PlayerExternalId.source == source, PlayerExternalId.external_id == ext)
            .values(
                player_id=pid,
                match_method=DP_METHOD,
                confidence=DP_CONFIDENCE,
                verified_at=None,
            )
        )
        close_unmatched(session, source, ext)
    if repoints:
        session.flush()
    if inserts:
        session.execute(PlayerExternalId.__table__.insert(), inserts)
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
    for source, ext in updates:
        session.execute(
            update(PlayerExternalId)
            .where(PlayerExternalId.source == source, PlayerExternalId.external_id == ext)
            .values(match_method=DP_METHOD, confidence=DP_CONFIDENCE)
        )
    session.flush()

    report = CrosswalkApplyReport(
        inserted=len(inserts),
        updated=len(updates) + len(repoints),
        unchanged=unchanged,
        created_players=len(created),
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
        # Ids must land as text so the Parquet round-trip that `ffh crosswalk seed` reads
        # back can never hand `_validate` a float: 4046.0 -> "4046.0" would be silent
        # corruption, and a UUID/alphanumeric id (sportradar_id, pfr_id) is not numeric at all.
        wrong = [c for c in sorted(DP_TEXT_COLUMNS) if df.schema[c] != pl.Utf8]
        if wrong:
            raise IngestValidationError(
                f"{type(self).name}: id columns must be text, got non-text: {wrong}"
            )
