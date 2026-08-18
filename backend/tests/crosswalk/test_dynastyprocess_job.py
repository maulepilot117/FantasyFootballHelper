"""The ``dynastyprocess_playerids`` IngestJob: fetch db_playerids.csv -> lake Parquet.

No network: every fetch is a respx-mocked response over the 13-row sample fixture. The
lifecycle assertions (success / rows / partition path / 304 / failed) are PR ③'s contract.
"""

import uuid
from pathlib import Path

import httpx
import polars as pl
import pytest
import respx
from sqlalchemy import select

from ffh.crosswalk.dynastyprocess import (
    DP_REQUIRED_COLUMNS,
    DP_TEXT_COLUMNS,
    DP_URL,
    DynastyProcessPlayerIdsJob,
    apply_playerids,
)
from ffh.db.models import IngestRun, PlayerExternalId
from ffh.ingest.base import IngestValidationError, get_job
from ffh.ingest.lake import scrape_date

FIXTURE = (
    Path(__file__).resolve().parents[1] / "fixtures" / "dynastyprocess" / "db_playerids_sample.csv"
)
SAMPLE_ROWS = 13


def _body() -> bytes:
    return FIXTURE.read_bytes()


def _job() -> DynastyProcessPlayerIdsJob:
    return DynastyProcessPlayerIdsJob()


# --- registration / identity ---------------------------------------------------------


def test_job_is_registered_under_its_name():
    assert DynastyProcessPlayerIdsJob.name == "dynastyprocess_playerids"
    assert get_job("dynastyprocess_playerids") is DynastyProcessPlayerIdsJob
    assert DynastyProcessPlayerIdsJob.source == "dynastyprocess"
    assert DynastyProcessPlayerIdsJob.asset == "playerids"
    # A weekly full snapshot: no season in the URL and none on ingest_runs.
    assert DynastyProcessPlayerIdsJob.seasonal is False
    assert DynastyProcessPlayerIdsJob.season_scoped is False
    assert DynastyProcessPlayerIdsJob.REQUIRED_COLUMNS == DP_REQUIRED_COLUMNS


def test_url_matches_data_sources_section_5_character_for_character():
    assert DP_URL == (
        "https://raw.githubusercontent.com/dynastyprocess/data/master/files/db_playerids.csv"
    )
    assert _job().url() == DP_URL


def test_partition_is_the_utc_scrape_date():
    assert _job().partition() == {"scrape_date": scrape_date()}


# --- parse / validate ----------------------------------------------------------------


def test_parse_reads_every_id_column_as_text():
    df = _job().parse(_body())
    assert df.height == SAMPLE_ROWS
    assert {c: df.schema[c] for c in sorted(DP_TEXT_COLUMNS)} == dict.fromkeys(
        sorted(DP_TEXT_COLUMNS), pl.Utf8
    )
    assert df.filter(pl.col("mfl_id") == "13116")["sleeper_id"].item() == "4046"


def test_validate_accepts_the_sample_and_rejects_short_or_retyped_frames():
    job = _job()
    df = job.parse(_body())
    job.validate(df)  # the happy path must not raise

    # ③'s contract: validate() raises IngestValidationError so run() maps it to `failed`.
    with pytest.raises(IngestValidationError, match="sleeper_id"):
        job.validate(df.drop("sleeper_id"))
    with pytest.raises(IngestValidationError, match="0 rows"):
        job.validate(df.head(0))
    with pytest.raises(IngestValidationError, match="non-text"):
        job.validate(df.with_columns(pl.col("espn_id").cast(pl.Int64, strict=False)))


# --- lifecycle -----------------------------------------------------------------------


@pytest.mark.db
@respx.mock
def test_run_lands_one_partition_then_304_is_skipped(db_session, tmp_path: Path):
    route = respx.get(DP_URL).mock(
        side_effect=[
            httpx.Response(200, content=_body(), headers={"ETag": '"dp-v1"'}),
            httpx.Response(304),
        ]
    )

    job = _job()
    result = job.run(db_session, tmp_path)
    assert result.status == "success"
    assert result.rows_written == SAMPLE_ROWS

    out = Path(result.output_path)
    assert out.exists()
    assert out == (
        tmp_path
        / "raw"
        / "dynastyprocess"
        / "playerids"
        / f"scrape_date={scrape_date()}"
        / "playerids.parquet"
    )
    assert out.parts[-4:-2] == ("dynastyprocess", "playerids")
    assert out.parts[-2] == f"scrape_date={scrape_date()}"

    landed = pl.read_parquet(out)
    assert landed.height == SAMPLE_ROWS
    # Ids survive the Parquet round-trip as text — a UUID and a numeric id both.
    assert {landed.schema[c] for c in DP_TEXT_COLUMNS} == {pl.Utf8}
    assert landed.filter(pl.col("mfl_id") == "13116")["sleeper_id"].item() == "4046"

    # The second run 304s against the stored ETag and writes nothing new.
    second = _job().run(db_session, tmp_path)
    assert second.status == "skipped_not_modified"
    assert second.rows_written is None and second.output_path is None
    assert route.calls[1].request.headers["if-none-match"] == '"dp-v1"'
    assert len(list((tmp_path / "raw" / "dynastyprocess").rglob("*.parquet"))) == 1
    assert [r.status for r in _runs(db_session)] == ["success", "skipped_not_modified"]


def _runs(session) -> list[IngestRun]:
    return list(
        session.scalars(
            select(IngestRun)
            .where(IngestRun.source == "dynastyprocess")
            # started_at is transaction_timestamp(): identical for BOTH rows under the
            # db_session fixture (one transaction), so ordering by it alone left the
            # ["success", "skipped_not_modified"] assertion resting on heap order.
            # Same tie-break tests/ingest/test_base.py already uses.
            .order_by(IngestRun.started_at, IngestRun.finished_at.nulls_last())
        )
    )


@pytest.mark.db
@respx.mock
def test_an_empty_fetch_fails_the_run_and_lands_nothing(db_session, tmp_path: Path):
    header = _body().split(b"\n", 1)[0] + b"\n"
    respx.get(DP_URL).mock(return_value=httpx.Response(200, content=header))

    result = _job().run(db_session, tmp_path)
    assert result.status == "failed"
    assert "0 rows" in result.error
    assert not list(tmp_path.rglob("*.parquet"))
    (run,) = _runs(db_session)
    assert run.status == "failed"
    assert run.rows_written is None and run.output_path is None


@pytest.mark.db
@respx.mock
def test_a_fetch_missing_a_required_column_fails_the_run(db_session, tmp_path: Path):
    truncated = pl.read_csv(_body(), infer_schema_length=None).drop("sleeper_id").write_csv()
    respx.get(DP_URL).mock(return_value=httpx.Response(200, content=truncated.encode()))

    result = _job().run(db_session, tmp_path)
    assert result.status == "failed"
    assert "sleeper_id" in result.error
    assert not list(tmp_path.rglob("*.parquet"))


@pytest.mark.db
@respx.mock
def test_the_landed_parquet_feeds_apply_playerids_without_float_mangled_ids(
    db_session, seeded_registry: dict[str, uuid.UUID], tmp_path: Path
):
    """`ffh crosswalk seed --playerids <landed>.parquet` reads Parquet, not CSV.

    That read path goes through ``apply_playerids`` -> ``_validate``, which is where the
    integral-float id guard lives; this pins the whole round-trip end to end.
    """
    respx.get(DP_URL).mock(return_value=httpx.Response(200, content=_body()))
    result = _job().run(db_session, tmp_path)
    assert result.status == "success"

    landed = pl.read_parquet(result.output_path)
    report = apply_playerids(db_session, landed)
    assert report.inserted > 0

    mahomes = seeded_registry["00-0033873"]
    sleeper_id = db_session.scalar(
        select(PlayerExternalId.external_id).where(
            PlayerExternalId.source == "sleeper", PlayerExternalId.player_id == mahomes
        )
    )
    # "4046", never "4046.0" — the id must never have passed through a float.
    assert sleeper_id == "4046"
    sportradar_id = db_session.scalar(
        select(PlayerExternalId.external_id).where(
            PlayerExternalId.source == "sportradar", PlayerExternalId.player_id == mahomes
        )
    )
    assert sportradar_id == "11cad59d-90dd-449c-a839-dddaba4fe16c"
