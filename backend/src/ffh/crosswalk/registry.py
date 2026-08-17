"""Seed the canonical ``players`` registry from the nflverse players frame (DATABASE.md §2).

Takes a DataFrame — never a URL or lake path — so it is testable without ``ffh.ingest``.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from typing import Any

import polars as pl
import structlog
from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from ffh.crosswalk.normalize import (
    FANTASY_POSITIONS,
    TEAMS,
    dst_full_name,
    normalize_dst,
    normalize_name,
    normalize_position,
    normalize_team,
)
from ffh.db.models import Player

log = structlog.get_logger(__name__)

# Live-verified nflverse column names (2026-08-16).
PLAYERS_REQUIRED_COLUMNS: frozenset[str] = frozenset(
    {
        "gsis_id",
        "display_name",
        "first_name",
        "last_name",
        "position",
        "birth_date",
        "rookie_season",
        "height",
        "weight",
        "college_name",
        "status",
        "latest_team",
    }
)

_UPSERT_CHUNK = 1000


class RegistryError(RuntimeError):
    """The players frame is not usable as-is (missing columns, null/duplicate gsis_id)."""


def _chunks(rows: list[dict[str, Any]], size: int) -> Iterator[list[dict[str, Any]]]:
    for i in range(0, len(rows), size):
        yield rows[i : i + size]


def prepare_players_frame(players_df: pl.DataFrame) -> tuple[pl.DataFrame, dict[str, int]]:
    """nflverse frame → frame in ``players`` column names + dropped-by-position counts.

    Never silently drops: kept + dropped == input, and the dropped breakdown is returned.
    """
    missing = PLAYERS_REQUIRED_COLUMNS - set(players_df.columns)
    if missing:
        raise RegistryError(f"players frame missing columns: {sorted(missing)}")

    n_in = players_df.height
    df = players_df.with_columns(
        pl.col("position")
        .map_elements(normalize_position, return_dtype=pl.Utf8)
        .alias("position_norm")
    )
    keep = pl.col("position_norm").is_in(sorted(FANTASY_POSITIONS)).fill_null(False)
    kept = df.filter(keep)
    dropped = df.filter(~keep)
    assert kept.height + dropped.height == n_in, "row loss in position filter"
    dropped_by_position = {
        str(row["position"]): int(row["len"])
        for row in dropped.group_by("position").len().sort("position").iter_rows(named=True)
    }

    if kept["gsis_id"].null_count():
        raise RegistryError(f"{kept['gsis_id'].null_count()} fantasy rows have null gsis_id")
    if kept["gsis_id"].n_unique() != kept.height:
        raise RegistryError("duplicate gsis_id in players frame")

    birth = pl.col("birth_date")
    birth_expr = (
        birth.str.to_date("%Y-%m-%d", strict=False)
        if kept.schema["birth_date"] == pl.Utf8
        else birth.cast(pl.Date)
    )
    out = kept.select(
        pl.col("gsis_id"),
        pl.col("display_name").alias("full_name"),
        pl.col("first_name"),
        pl.col("last_name"),
        pl.col("display_name")
        .map_elements(normalize_name, return_dtype=pl.Utf8)
        .alias("normalized_name"),
        pl.col("position_norm").alias("position"),
        birth_expr.alias("birth_date"),
        pl.col("rookie_season").cast(pl.Int32).alias("rookie_year"),
        pl.col("height").cast(pl.Int32).alias("height_in"),
        pl.col("weight").cast(pl.Int32).alias("weight_lb"),
        pl.col("college_name").alias("college"),
        pl.col("status"),
        pl.col("latest_team").map_elements(normalize_team, return_dtype=pl.Utf8).alias("team_abbr"),
    )
    empty_names = out.filter(pl.col("normalized_name") == "").height
    if empty_names:
        raise RegistryError(f"{empty_names} rows normalize to an empty name")
    unparsed = kept["birth_date"].drop_nulls().len() - out["birth_date"].drop_nulls().len()
    if unparsed:
        log.warning("crosswalk.seed_players.unparsed_birth_dates", count=unparsed)
    unknown_team = out.filter(
        pl.col("team_abbr").is_null() & pl.col("gsis_id").is_not_null()
    ).height
    log.info(
        "crosswalk.seed_players.prepared",
        input=n_in,
        kept=out.height,
        dropped_by_position=dropped_by_position,
        rows_without_team=unknown_team,
    )
    return out, dropped_by_position


_UPDATE_COLUMNS: tuple[str, ...] = (
    "full_name",
    "first_name",
    "last_name",
    "normalized_name",
    "position",
    "birth_date",
    "rookie_year",
    "height_in",
    "weight_lb",
    "college",
    "status",
    "team_abbr",
)


def seed_players(session: Session, players_df: pl.DataFrame) -> int:
    """Upsert ``players`` on ``gsis_id`` from the nflverse frame, then ensure 32 DST rows.

    Idempotent. Sets ``updated_at = now()`` explicitly on conflict (DATABASE.md §2).
    Returns rows upserted + 32.
    """
    frame, _dropped = prepare_players_frame(players_df)
    rows = frame.to_dicts()
    for chunk in _chunks(rows, _UPSERT_CHUNK):
        stmt = insert(Player).values(chunk)
        stmt = stmt.on_conflict_do_update(
            index_elements=[Player.gsis_id],
            # ORM onupdate does not fire for INSERT ... ON CONFLICT (DATABASE.md §2).
            set_={
                **{c: getattr(stmt.excluded, c) for c in _UPDATE_COLUMNS},
                "updated_at": func.now(),
            },
        )
        session.execute(stmt)
    created_dst = seed_dst_players(session)
    session.flush()
    log.info("crosswalk.seed_players.done", upserted=len(rows), dst_created=created_dst)
    return len(rows) + len(TEAMS)


def seed_dst_players(session: Session) -> int:
    """One ``players`` row per team DST: gsis NULL, position DST, normalized_name 'kc dst'."""
    existing = set(session.scalars(select(Player.normalized_name).where(Player.position == "DST")))
    new: list[Player] = []
    for abbr, city, nickname, _aliases in TEAMS:
        nn = normalize_dst(abbr)
        assert nn is not None
        if nn in existing:
            continue
        new.append(
            Player(
                gsis_id=None,
                full_name=dst_full_name(abbr),
                first_name=city,
                last_name=nickname,
                normalized_name=nn,
                position="DST",
                team_abbr=abbr,
            )
        )
    session.add_all(new)
    session.flush()
    return len(new)


def iter_gsis_to_player_id(session: Session) -> Iterable[tuple[str, Any]]:
    """(gsis_id, player_id) for every registry row that has a gsis_id. Used by Task 4."""
    return session.execute(
        select(Player.gsis_id, Player.player_id).where(Player.gsis_id.is_not(None))
    ).all()
