"""Seed the players registry for the Sleeper fixture league (Tasks 7 and 10).

Mirrors `ffh crosswalk seed`, from the fixture blob instead of the lake: ④'s
`seed_dst_players` creates the 32 team defenses that rung 3 resolves by `<abbr> dst`, and
④'s `apply_playerids` creates one `players` row + `player_external_ids(sleeper=...)` per
fixture human (its rookie path: a gsis_id not yet in the registry becomes a new player).
"""

import json
from pathlib import Path
from typing import Any

import polars as pl
from sqlalchemy.orm import Session

from ffh.crosswalk.dynastyprocess import DP_REQUIRED_COLUMNS, apply_playerids
from ffh.crosswalk.registry import seed_dst_players

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "sleeper"
FIXTURE_HUMANS = 23  # 21 rostered + 2 free agents in players_slice.json
DST_ROWS = 32
SEEDED_PLAYERS = FIXTURE_HUMANS + DST_ROWS

# ④'s DP_REQUIRED_COLUMNS, spelled out so a drift in either direction fails loudly below.
_PLAYERIDS_SCHEMA: dict[str, Any] = {
    "mfl_id": pl.Utf8,
    "gsis_id": pl.Utf8,
    "sleeper_id": pl.Utf8,
    "espn_id": pl.Utf8,
    "yahoo_id": pl.Utf8,
    "pfr_id": pl.Utf8,
    "fantasypros_id": pl.Utf8,
    "sportradar_id": pl.Utf8,
    "rotowire_id": pl.Utf8,
    "name": pl.Utf8,
    "position": pl.Utf8,
    "team": pl.Utf8,
    "birthdate": pl.Utf8,
    "draft_year": pl.Int64,
    "college": pl.Utf8,
}


def _text(value: Any) -> str | None:
    """None-preserving str(): `str(None)` would store the literal id "None"."""
    return None if value is None else str(value)


def playerids_frame() -> pl.DataFrame:
    """A DynastyProcess `db_playerids` slice covering every human in the fixture league.

    `mfl_id` must be non-null and UNIQUE per row: ④ keys gsis-less rows on `mfl:<mfl_id>`,
    so a null column would collapse every rookie into one placeholder. The Sleeper id doubles
    as the mfl id here. `pfr_id`, `fantasypros_id` and `draft_year` are required columns
    that may be null.
    """
    blob = json.loads((FIXTURES / "players_slice.json").read_text(encoding="utf-8"))
    humans = [p for p in blob.values() if p.get("position") != "DEF"]
    n = len(humans)
    frame = pl.DataFrame(
        {
            "mfl_id": [p["player_id"] for p in humans],
            "gsis_id": [p.get("gsis_id") for p in humans],
            "sleeper_id": [p["player_id"] for p in humans],
            "espn_id": [_text(p.get("espn_id")) for p in humans],
            "yahoo_id": [_text(p.get("yahoo_id")) for p in humans],
            "pfr_id": [None] * n,
            "fantasypros_id": [None] * n,
            "sportradar_id": [p.get("sportradar_id") for p in humans],
            "rotowire_id": [_text(p.get("rotowire_id")) for p in humans],
            "name": [p["full_name"] for p in humans],
            "position": [p["position"] for p in humans],
            "team": [p.get("team") for p in humans],
            "birthdate": [p.get("birth_date") for p in humans],
            "draft_year": [None] * n,
            "college": [p.get("college") for p in humans],
        },
        schema=_PLAYERIDS_SCHEMA,
    )
    missing = DP_REQUIRED_COLUMNS - set(frame.columns)
    assert not missing, f"fixture frame lags ④'s DP_REQUIRED_COLUMNS: {sorted(missing)}"
    assert frame.height == FIXTURE_HUMANS, frame.height
    assert frame["mfl_id"].n_unique() == frame.height, "mfl_id must be unique per row"
    return frame


def seed_fixture_players(session: Session) -> None:
    """32 DST rows + one player (and its sleeper id) per fixture human. Flushes only —
    the caller's transaction owns the commit/rollback."""
    created_dst = seed_dst_players(session)
    assert created_dst == DST_ROWS, created_dst
    report = apply_playerids(session, playerids_frame())
    assert report.created_players == FIXTURE_HUMANS, report
    # PR ④'s final review wave replaced `ambiguous` with four buckets; every one of them
    # also queues the id in crosswalk_unmatched, so a non-empty bucket here would make
    # the fixture's own `ffh crosswalk report` red.
    assert report.ambiguous_in_file == (), report
    assert report.blocked_by_existing == (), report
    assert report.blocked_by_rejection == (), report
    assert report.displaced == (), report
    assert report.skipped_no_person_key == 0, report
    session.flush()
