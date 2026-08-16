"""Lake layout: partition paths and the never-overwrite Parquet writer (DATABASE.md §1)."""

import os
import uuid
from datetime import UTC, datetime
from pathlib import Path

import polars as pl


class PartitionExistsError(FileExistsError):
    """Raised when a lake partition file already exists. A new scrape is a NEW partition."""


def scrape_date(now: datetime | None = None) -> str:
    """Today's UTC date as ``YYYY-MM-DD`` — the ``scrape_date=`` partition key."""
    return (now or datetime.now(UTC)).strftime("%Y-%m-%d")


def partition_path(lake_root: Path, source: str, asset: str, **keys: str | int) -> Path:
    """``<lake_root>/raw/<source>/<asset>/<k>=<v>/...`` in the insertion order of ``keys``.

    Hive-style so DuckDB can read the tree with ``hive_partitioning=1`` later.
    """
    path = Path(lake_root) / "raw" / source / asset
    for key, value in keys.items():
        path = path / f"{key}={value}"
    return path


def parquet_file(lake_root: Path, source: str, asset: str, **keys: str | int) -> Path:
    """The single Parquet file inside a partition directory."""
    return partition_path(lake_root, source, asset, **keys) / f"{asset}.parquet"


def write_parquet(df: pl.DataFrame, path: Path) -> int:
    """Write ``df`` to ``path``, refusing to overwrite. Returns the row count.

    Writes to a per-call, uniquely named sibling ``.tmp`` first and then hard-links it into
    place: ``os.link`` fails atomically if the target already exists, so a crash mid-write
    can never leave a partial file at the real partition path, and two concurrent writers
    never share a temp inode — exactly one wins the link, the other gets
    ``PartitionExistsError``. Each writer unlinks only its own temp file.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    df.write_parquet(tmp, compression="zstd")
    try:
        os.link(tmp, path)
    except FileExistsError as exc:
        raise PartitionExistsError(f"lake partition already exists: {path}") from exc
    finally:
        tmp.unlink(missing_ok=True)
    return df.height
