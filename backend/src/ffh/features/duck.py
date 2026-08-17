"""DuckDB over the Parquet lake — read-only, in-memory, no state of its own.

CLAUDE.md rule 5 and DATABASE.md §1: DuckDB owns no state and never writes a `.duckdb`
file. `duckdb.connect()` with no argument is an in-memory database; the views are plain
`read_parquet` over whatever the ingest jobs last landed.
"""

from pathlib import Path

import duckdb
import structlog

log = structlog.get_logger(__name__)

#: view name -> (lake source, lake asset)
VIEWS: dict[str, tuple[str, str]] = {
    "players": ("nflverse", "players"),
    "stats_player_week": ("nflverse", "stats_player_week"),
    "snap_counts": ("nflverse", "snap_counts"),
    "depth_charts": ("nflverse", "depth_charts"),
    "injuries": ("nflverse", "injuries"),
    "games": ("nfldata", "games"),
}

#: assets partitioned by `season=` as well as `scrape_date=`
SEASONAL_ASSETS = frozenset({"stats_player_week", "snap_counts", "depth_charts", "injuries", "pbp"})


def latest_partition(
    lake_root: Path, source: str, asset: str, season: int | None = None
) -> Path | None:
    """The newest landed Parquet for an asset, or None.

    "Newest" is defined without touching the filesystem clock: partition keys are emitted
    as `season=YYYY/scrape_date=YYYY-MM-DD`, both zero-padded ISO, so the lexicographic
    maximum of the POSIX path string is the chronological maximum. Deterministic and
    reproducible from the path alone.
    """
    root = Path(lake_root) / "raw" / source / asset
    if not root.is_dir():
        return None
    candidates = list(root.rglob(f"{asset}.parquet"))
    if season is not None:
        marker = f"/season={season}/"
        candidates = [p for p in candidates if marker in p.as_posix()]
    if not candidates:
        return None
    return max(candidates, key=lambda p: p.as_posix())


def connect(lake_root: Path, season: int) -> duckdb.DuckDBPyConnection:
    """In-memory DuckDB with a `read_parquet` view per landed asset for `season`."""
    con = duckdb.connect()
    for view, (source, asset) in VIEWS.items():
        path = latest_partition(
            lake_root, source, asset, season if asset in SEASONAL_ASSETS else None
        )
        if path is None:
            log.warning("features.duck.view_missing", view=view, source=source, asset=asset)
            continue
        literal = path.as_posix().replace("'", "''")
        con.execute(f"CREATE VIEW {view} AS SELECT * FROM read_parquet('{literal}')")
        log.info("features.duck.view_created", view=view, path=path.as_posix())
    return con
