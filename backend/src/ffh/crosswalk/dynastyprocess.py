"""DynastyProcess ``db_playerids.csv`` → ``player_external_ids`` (rung 1 of the ladder).

Pure with respect to ``ffh.ingest``: takes a DataFrame. The IngestJob that fetches the CSV
into the lake is appended to this module in Task 8 (requires PR ③).
"""

from __future__ import annotations

import uuid
from dataclasses import asdict, dataclass
from datetime import date

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
from ffh.db.models import Player, PlayerExternalId

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
    """An existing (source, external_id) row points at a different player than DP says."""

    def __init__(self, conflicts: list[tuple[str, str, uuid.UUID, uuid.UUID]]) -> None:
        self.conflicts = conflicts
        shown = ", ".join(f"{s}:{e} db={a} dp={b}" for s, e, a, b in conflicts[:10])
        super().__init__(
            f"{len(conflicts)} DynastyProcess id(s) conflict with existing crosswalk rows "
            f"(first: {shown}). Resolve by hand (ffh crosswalk verify --reject) and re-run."
        )


@dataclass(frozen=True)
class CrosswalkApplyReport:
    inserted: int
    updated: int
    unchanged: int
    created_players: int
    skipped_no_ids: int
    skipped_position: int
    skipped_dst: int
    ambiguous: tuple[tuple[str, str], ...]


def read_playerids_csv(raw: bytes) -> pl.DataFrame:
    """Parse the CSV with every id column as text and ``NA`` as null."""
    header = raw.split(b"\n", 1)[0].decode("utf-8").strip().split(",")
    missing = DP_REQUIRED_COLUMNS - set(header)
    if missing:
        raise DynastyProcessError(f"db_playerids.csv missing columns: {sorted(missing)}")
    return pl.read_csv(
        raw,
        null_values=["NA", ""],
        schema_overrides={c: pl.Utf8 for c in DP_TEXT_COLUMNS},
        infer_schema_length=20000,
    )


def _validate(df: pl.DataFrame) -> pl.DataFrame:
    missing = DP_REQUIRED_COLUMNS - set(df.columns)
    if missing:
        raise DynastyProcessError(f"DynastyProcess frame missing columns: {sorted(missing)}")
    # Defensive: a Parquet round-trip could have typed ids numerically. Store as text.
    return df.with_columns([pl.col(c).cast(pl.Utf8) for c in DP_TEXT_COLUMNS])


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
    transaction; a ``CrosswalkConflictError`` is raised before any write — including
    placeholder ``players`` rows, which are only created after the conflict scan passes.
    """
    df = _validate(df)
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
    existing: dict[tuple[str, str], PlayerExternalId] = {
        (r.source, r.external_id): r for r in existing_rows
    }
    # (source, player_id) → external_id already held: the DB enforces one id per source
    # per player (player_external_ids_source_player_uidx), so this map is well-defined.
    held_by_player: dict[tuple[str, uuid.UUID], str] = {
        (r.source, r.player_id): r.external_id for r in existing_rows
    }

    def _existing_id_hit(row: dict[str, object]) -> str | None:
        """Any of the row's ids already mapped → that player (idempotent for rookies)."""
        for col, source in DP_ID_COLUMNS.items():
            ext = row[col]
            if ext:
                hit = existing.get((source, ext))
                if hit is not None:
                    return str(hit.player_id)
        return None

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
    long = (
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
        .unique()
    )
    n_long = long.height
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

    # 5. conflict scan (vs the DB) — BEFORE any write, including placeholder players.
    # Placeholder rows cannot conflict: step 3 assigned them a placeholder precisely
    # because none of their ids exist in player_external_ids.
    conflicts: list[tuple[str, str, uuid.UUID, uuid.UUID]] = []
    for r in clean.iter_rows(named=True):
        if r["player_key"].startswith(_PLACEHOLDER_PREFIXES):
            continue
        pid = uuid.UUID(r["player_key"])
        ex = existing.get((r["source"], r["external_id"]))
        if ex is not None and ex.player_id != pid:
            conflicts.append((r["source"], r["external_id"], ex.player_id, pid))
    if conflicts:
        log.error("crosswalk.dynastyprocess.conflicts", count=len(conflicts))
        raise CrosswalkConflictError(conflicts)

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
    unchanged = 0
    db_ambiguous: list[tuple[str, str]] = []
    for r in clean.iter_rows(named=True):
        pid = created.get(r["player_key"]) or uuid.UUID(r["player_key"])
        ex = existing.get((r["source"], r["external_id"]))
        if ex is not None:
            # ex.player_id == pid here: step 5 raised on any mismatch.
            if ex.match_method == DP_METHOD and ex.confidence == DP_CONFIDENCE:
                unchanged += 1
            else:
                updates.append((r["source"], r["external_id"]))
            continue
        held = held_by_player.get((r["source"], pid))
        if held is not None:
            # The player already holds a different id from this source (unique index
            # player_external_ids_source_player_uidx forbids a second). The holder wins:
            # a DB row wins because DP must never displace an existing mapping, and
            # within the batch the lexicographically smallest external_id wins (clean is
            # sorted by (source, external_id)). The loser is reported, never raised.
            db_ambiguous.append((r["source"], r["external_id"]))
            log.warning(
                "crosswalk.dynastyprocess.duplicate_source_for_player",
                source=r["source"],
                external_id=r["external_id"],
                held_external_id=held,
                player_id=str(pid),
            )
            continue
        held_by_player[(r["source"], pid)] = r["external_id"]
        inserts.append(
            {
                "player_id": pid,
                "source": r["source"],
                "external_id": r["external_id"],
                "confidence": DP_CONFIDENCE,
                "match_method": DP_METHOD,
            }
        )
    assert len(inserts) + len(updates) + unchanged + len(db_ambiguous) == clean.height, (
        "row loss in partition"
    )
    ambiguous_all = tuple(sorted(ambiguous | set(db_ambiguous)))

    if inserts:
        session.execute(PlayerExternalId.__table__.insert(), inserts)
    for source, ext in updates:
        session.execute(
            update(PlayerExternalId)
            .where(PlayerExternalId.source == source, PlayerExternalId.external_id == ext)
            .values(match_method=DP_METHOD, confidence=DP_CONFIDENCE)
        )
    session.flush()

    report = CrosswalkApplyReport(
        inserted=len(inserts),
        updated=len(updates),
        unchanged=unchanged,
        created_players=len(created),
        skipped_no_ids=skipped_no_ids,
        skipped_position=skipped_position,
        skipped_dst=skipped_dst,
        ambiguous=ambiguous_all,
    )
    log.info("crosswalk.dynastyprocess.applied", **asdict(report))
    return report
