"""The Sleeper player universe, read off the request path.

GET /players/nfl is 14.6 MB. It is landed to the Parquet lake at most once a day by the
`sleeper_players` IngestJob; this reads the newest partition. Polars + pathlib only —
no ffh.ingest import, so the adapter package stands alone.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
from pydantic import ValidationError

from ffh.adapters.base import PlatformError, PlayerRef

PLAYERS_LAKE_GLOB = "raw/sleeper/players/scrape_date=*"
# The ONE definition of the player-partition column contract. The `sleeper_players`
# IngestJob imports this to write exactly these columns.
REQUIRED_COLUMNS = ("player_id", "name", "position", "team")


class LakePlayerCatalog:
    """PlayerCatalog backed by the newest raw/sleeper/players Parquet partition."""

    def __init__(self, lake_root: Path) -> None:
        self._root = Path(lake_root)

    def _newest_partition(self) -> Path:
        parts = sorted(p for p in self._root.glob(PLAYERS_LAKE_GLOB) if p.is_dir())
        if not parts:
            raise PlatformError(
                f"no Sleeper player partition under {self._root / PLAYERS_LAKE_GLOB}; "
                "run `ffh ingest run sleeper_players` first"
            )
        return parts[-1]

    async def all_players(self) -> dict[str, PlayerRef]:
        partition = self._newest_partition()
        files = sorted(partition.glob("*.parquet"))
        if not files:
            raise PlatformError(f"partition {partition} contains no parquet files")
        df = pl.read_parquet(files)
        missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
        if missing:
            raise PlatformError(f"{partition} is missing columns {missing}")
        try:
            refs = {
                row["player_id"]: PlayerRef(
                    external_id=row["player_id"],
                    name=row["name"],
                    position=row["position"],
                    team=row["team"],
                )
                for row in df.select(REQUIRED_COLUMNS).iter_rows(named=True)
            }
        except ValidationError as exc:
            raise PlatformError(
                f"{partition} has a row that is not a valid PlayerRef: {exc}"
            ) from exc
        if len(refs) != df.height:
            raise PlatformError(
                f"{partition} has duplicate player_id rows ({df.height} rows, {len(refs)} ids)"
            )
        return refs
