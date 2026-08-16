"""Record the small committed test fixtures from the live sources. Run by hand:

    cd backend && uv run python scripts/record_nflverse_fixtures.py

Never run in CI. Re-run when `test_fixture_schemas.py` fails, then review the diff: a
column that disappeared upstream is a real incident, not a fixture to rubber-stamp.
"""

import io
import sys
from pathlib import Path

import polars as pl

from ffh.ingest.games import NfldataGamesJob
from ffh.ingest.http import Fetched, NotFound, get_bytes, make_client
from ffh.ingest.nflverse import (
    NflverseDepthChartsJob,
    NflverseInjuriesJob,
    NflversePbpJob,
    NflversePlayersJob,
    NflverseSnapCountsJob,
    NflverseStatsPlayerWeekJob,
)
from ffh.ingest.reference import StadiumsJob

FIXTURES = Path(__file__).resolve().parents[1] / "tests" / "fixtures"
ROWS = 50

#: Seasonal assets 404 before Week 1, so record from the last completed season.
NFLVERSE_JOBS = [
    (NflversePlayersJob, None),
    (NflverseStatsPlayerWeekJob, 2025),
    (NflverseSnapCountsJob, 2025),
    (NflverseDepthChartsJob, 2026),
    (NflverseInjuriesJob, 2025),
    (NflversePbpJob, 2025),
]


def _write_parquet_fixture(job, content: bytes) -> None:
    df = pl.read_parquet(io.BytesIO(content)).head(ROWS)
    out = FIXTURES / "nflverse" / f"{job.asset}.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(out, compression="zstd")
    print(f"  wrote {out.relative_to(FIXTURES.parent)} ({df.height} rows x {df.width} cols)")


def _write_games_fixture(content: bytes) -> None:
    """Slice the RAW TEXT — a Polars round trip would turn the quoted empty roof into NULL."""
    lines = content.decode().splitlines()
    header, body = lines[0], lines[1:]
    keep = [ln for ln in body if ln.split(",")[1] == "2026" and ln.split(",")[3] in {"1", "2"}]
    keep += [ln for ln in body if ln.split(",")[1] == "2025" and ln.split(",")[3] == "1"]
    out = FIXTURES / "nfldata" / "games_sample.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join([header, *keep]) + "\n", encoding="utf-8")
    assert any('"",' in ln for ln in keep), "expected at least one quoted-empty roof row"
    print(f"  wrote {out.relative_to(FIXTURES.parent)} ({len(keep)} rows)")


def main() -> int:
    with make_client() as client:
        for job_cls, season in NFLVERSE_JOBS:
            job = job_cls(season=season)
            print(f"{job_cls.name} -> {job.url()}")
            result = get_bytes(client, job.url())
            if isinstance(result, NotFound):
                print("  404 - skipped (expected for seasonal assets before Week 1)")
                continue
            assert isinstance(result, Fetched)
            _write_parquet_fixture(job_cls, result.content)

        games = get_bytes(client, NfldataGamesJob(season=2026).url())
        assert isinstance(games, Fetched)
        _write_games_fixture(games.content)

        stadiums = get_bytes(client, StadiumsJob().url())
        assert isinstance(stadiums, Fetched)
        out = FIXTURES / "stadiums" / "stadiums.csv"
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_bytes(stadiums.content)
        print(f"  wrote {out.relative_to(FIXTURES.parent)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
