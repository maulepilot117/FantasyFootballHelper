from datetime import UTC, datetime
from pathlib import Path

import polars as pl
import pytest

from ffh.ingest.lake import (
    PartitionExistsError,
    parquet_file,
    partition_path,
    scrape_date,
    write_parquet,
)


def test_scrape_date_is_utc_iso_day():
    assert scrape_date(datetime(2026, 8, 16, 23, 59, tzinfo=UTC)) == "2026-08-16"


def test_scrape_date_defaults_to_now_utc():
    assert len(scrape_date()) == 10 and scrape_date()[4] == "-"


def test_partition_path_uses_hive_keys_in_insertion_order(tmp_path: Path):
    p = partition_path(tmp_path, "nflverse", "injuries", season=2026, scrape_date="2026-08-16")
    assert (
        p == tmp_path / "raw" / "nflverse" / "injuries" / "season=2026" / "scrape_date=2026-08-16"
    )


def test_partition_path_with_no_keys_is_the_asset_dir(tmp_path: Path):
    assert partition_path(tmp_path, "nfldata", "games") == tmp_path / "raw" / "nfldata" / "games"


def test_parquet_file_appends_asset_filename(tmp_path: Path):
    p = parquet_file(tmp_path, "nfldata", "games", scrape_date="2026-08-16")
    assert p.name == "games.parquet"
    assert p.parent.name == "scrape_date=2026-08-16"


def test_write_parquet_creates_parents_and_returns_row_count(tmp_path: Path):
    df = pl.DataFrame({"a": [1, 2, 3]})
    path = parquet_file(tmp_path, "nflverse", "players", scrape_date="2026-08-16")
    assert write_parquet(df, path) == 3
    assert path.exists()
    assert pl.read_parquet(path).height == 3


def test_write_parquet_refuses_to_overwrite(tmp_path: Path):
    df = pl.DataFrame({"a": [1]})
    path = parquet_file(tmp_path, "nflverse", "players", scrape_date="2026-08-16")
    write_parquet(df, path)
    with pytest.raises(PartitionExistsError):
        write_parquet(pl.DataFrame({"a": [9]}), path)
    # the original content survives — DATABASE.md §1 "never overwrite a scrape partition"
    assert pl.read_parquet(path)["a"].to_list() == [1]


def test_write_parquet_leaves_no_temp_file_behind(tmp_path: Path):
    path = parquet_file(tmp_path, "nflverse", "players", scrape_date="2026-08-16")
    write_parquet(pl.DataFrame({"a": [1]}), path)
    assert sorted(p.name for p in path.parent.iterdir()) == ["players.parquet"]


def test_write_parquet_cleans_up_a_partial_tmp_when_serialization_fails(
    tmp_path: Path, monkeypatch
):
    """A failing ``DataFrame.write_parquet`` that already created its temp file must not
    leak it, and must not create the real partition file."""
    path = parquet_file(tmp_path, "nflverse", "players", scrape_date="2026-08-16")

    def failing_write(self, file, *args, **kwargs):
        Path(file).write_bytes(b"partial")  # the file exists before the failure
        raise OSError("disk full mid-write")

    monkeypatch.setattr(pl.DataFrame, "write_parquet", failing_write)
    with pytest.raises(OSError, match="disk full"):
        write_parquet(pl.DataFrame({"a": [1]}), path)
    assert not path.exists()
    assert list(path.parent.iterdir()) == [], "partial temp file leaked"


def test_write_parquet_uses_a_unique_tmp_per_call(tmp_path: Path, monkeypatch):
    """Two writers of the same partition must never share a temp inode.

    Writer A is caught mid-write (inside ``DataFrame.write_parquet``) while writer B lands
    first; A then loses the ``os.link`` race, raises, unlinks only its own temp file, and
    B's content survives untouched.
    """
    path = parquet_file(tmp_path, "nflverse", "players", scrape_date="2026-08-16")

    seen_tmps: list[Path] = []
    real_write = pl.DataFrame.write_parquet

    def spying_write(self, file, *args, **kwargs):
        seen_tmps.append(Path(file))
        if len(seen_tmps) == 1:  # writer B lands while writer A is still writing its temp
            write_parquet(pl.DataFrame({"a": [2]}), path)
        return real_write(self, file, *args, **kwargs)

    monkeypatch.setattr(pl.DataFrame, "write_parquet", spying_write)

    with pytest.raises(PartitionExistsError):
        write_parquet(pl.DataFrame({"a": [1]}), path)

    assert len(seen_tmps) == 2 and seen_tmps[0] != seen_tmps[1]
    assert all(t.name.startswith(".players.parquet.") and t.suffix == ".tmp" for t in seen_tmps)
    assert pl.read_parquet(path)["a"].to_list() == [2]  # the winner's frame survives
    assert sorted(p.name for p in path.parent.iterdir()) == ["players.parquet"]  # no tmp left
