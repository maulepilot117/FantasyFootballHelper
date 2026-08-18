import json
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Annotated

import typer
from sqlalchemy.orm import Session

from ffh import __version__
from ffh.config import get_settings
from ffh.db.engine import make_engine, make_session_factory

# Importing the job modules registers every @register-decorated class in ffh.ingest.base.JOBS.
from ffh.ingest import games as _games  # noqa: F401
from ffh.ingest import nflverse as _nflverse  # noqa: F401
from ffh.ingest import reference as _reference  # noqa: F401
from ffh.ingest.base import JOBS, STATUS_FAILED, get_job
from ffh.ingest.reference import StadiumsJob, seed_generic_league, seed_nfl_teams

app = typer.Typer(no_args_is_help=True, help="FantasyFootballHelper CLI.")
ingest_app = typer.Typer(no_args_is_help=True, help="Run ingest jobs.")
league_app = typer.Typer(no_args_is_help=True, help="Load leagues from platforms.")
crosswalk_app = typer.Typer(no_args_is_help=True, help="Player ID crosswalk tools.")

app.add_typer(ingest_app, name="ingest")
app.add_typer(league_app, name="league")
app.add_typer(crosswalk_app, name="crosswalk")


@app.command()
def version() -> None:
    """Print the ffh version."""
    typer.echo(__version__)


@contextmanager
def _session_scope() -> Iterator[Session]:
    """One sync session per CLI invocation. Patched out in unit tests."""
    engine = make_engine()
    factory = make_session_factory(engine)
    session = factory()
    try:
        yield session
    finally:
        session.close()
        engine.dispose()


@ingest_app.command("list")
def ingest_list() -> None:
    """List registered ingest jobs."""
    for name in sorted(JOBS):
        cls = JOBS[name]
        scope = "seasonal" if cls.seasonal else "static"
        typer.echo(f"{name}\t{cls.source}/{cls.asset}\t{scope}")


@ingest_app.command("run")
def ingest_run(
    job: str = typer.Argument(..., help="Job name; see `ffh ingest list`."),
    season: int | None = typer.Option(None, "--season", help="Defaults to FFH_SEASON."),
) -> None:
    """Run one ingest job. Exits non-zero only when the run FAILED."""
    settings = get_settings()
    try:
        cls = get_job(job)
    except KeyError as exc:
        raise typer.BadParameter(str(exc)) from exc

    with _session_scope() as session:
        result = cls(season=season or settings.season).run(session, settings.lake_root)

    typer.echo(
        json.dumps(
            {
                "job": job,
                "status": result.status,
                "rows_written": result.rows_written,
                "output_path": result.output_path,
                "error": result.error,
            }
        )
    )
    if result.status == STATUS_FAILED:
        raise typer.Exit(1)


@ingest_app.command("seed")
def ingest_seed() -> None:
    """Seed nfl_teams, stadiums, and the sentinel generic league. Idempotent."""
    settings = get_settings()
    with _session_scope() as session:
        teams = seed_nfl_teams(session)
        session.commit()
        stadium_result = StadiumsJob().run(session, settings.lake_root)
        league_id = seed_generic_league(session)
        session.commit()

    typer.echo(
        json.dumps(
            {
                "nfl_teams": teams,
                "stadiums_status": stadium_result.status,
                "stadiums_rows": stadium_result.rows_written,
                "generic_league_id": str(league_id),
            }
        )
    )
    if stadium_result.status == STATUS_FAILED:
        raise typer.Exit(1)


# Placeholder commands so `--help` works on empty groups; later PRs replace these.
@league_app.command("platforms")
def league_platforms() -> None:
    """List supported platforms."""
    typer.echo("sleeper")


def _coverage_report_for_cli():
    """Indirection so tests can monkeypatch the report without a database."""
    from ffh.crosswalk.report import coverage_report

    with _session_scope() as session:
        return coverage_report(session)


@crosswalk_app.command("report")
def crosswalk_report(
    json_out: bool = typer.Option(False, "--json", help="Machine-readable output."),
) -> None:
    """Crosswalk coverage report. Exit 1 if anything is unmatched or awaiting review."""
    rep = _coverage_report_for_cli()
    typer.echo(json.dumps(rep.to_dict(), indent=2, default=str) if json_out else rep.render())
    raise typer.Exit(code=0 if rep.ok else 1)


@crosswalk_app.command("seed")
def crosswalk_seed(
    players: Annotated[
        Path | None,
        typer.Option(
            "--players",
            exists=True,
            help="nflverse players.parquet. Default: the newest raw/nflverse/players partition in "
            "FFH_LAKE_ROOT (landed by `ffh ingest run nflverse_players`, PR ③).",
        ),
    ] = None,
    playerids: Annotated[
        Path | None,
        typer.Option(
            "--playerids", exists=True, help="DynastyProcess db_playerids (.csv or .parquet)"
        ),
    ] = None,
) -> None:
    """Seed the players registry (and DSTs) from nflverse; optionally apply DynastyProcess ids."""
    # Lazy imports: keep CLI start-up fast (the draft pick clock is 90 seconds) — in
    # particular ffh.features.duck pulls in duckdb, which must not load on every command.
    import polars as pl

    from ffh.crosswalk.dynastyprocess import (
        CrosswalkConflictError,
        apply_playerids,
        read_playerids_csv,
    )
    from ffh.crosswalk.registry import seed_players

    if players is None:
        from ffh.features.duck import latest_partition

        # ③'s ffh.features.duck.latest_partition — the same "newest scrape_date" rule the
        # DuckDB views use; returns None when nothing has landed yet.
        players = latest_partition(get_settings().lake_root, "nflverse", "players")
        if players is None:
            typer.echo(
                "no nflverse players partition in the lake; run `ffh ingest run "
                "nflverse_players` first or pass --players",
                err=True,
            )
            raise typer.Exit(code=1)

    with _session_scope() as session:
        n = seed_players(session, pl.read_parquet(players))
        typer.echo(f"players upserted (incl. 32 DST): {n}")
        if playerids is not None:
            frame = (
                read_playerids_csv(playerids.read_bytes())
                if playerids.suffix == ".csv"
                else pl.read_parquet(playerids)
            )
            try:
                report = apply_playerids(session, frame)
            except CrosswalkConflictError as exc:
                # Exit 2: conflicts need a human (`ffh crosswalk verify --reject`); the
                # session is closed without commit so nothing partial lands. Exit 1 is
                # reserved for the report's "unmatched or unverified rows exist" signal.
                typer.echo(str(exc), err=True)
                raise typer.Exit(code=2) from exc
            typer.echo(json.dumps(report.__dict__, indent=2))
        session.commit()


@crosswalk_app.command("verify")
def crosswalk_verify(
    source: str = typer.Argument(
        ..., help="sleeper|espn|yahoo|pfr|fantasypros|sportradar|rotowire"
    ),
    external_id: str = typer.Argument(...),
    reject: bool = typer.Option(
        False, "--reject", help="Delete the mapping instead of verifying it."
    ),
) -> None:
    """Human review of a crosswalk row: mark verified (default) or reject."""
    from ffh.crosswalk.review import mark_unmatched_resolved, reject_mapping, verify_mapping

    with _session_scope() as session:
        ok = (reject_mapping if reject else verify_mapping)(session, source, external_id)
        if ok:
            if not reject:
                # The Task-5 conflict path leaves the same key in BOTH player_external_ids
                # and crosswalk_unmatched; accepting the mapping closes the queue entry.
                # `--reject` deliberately leaves it OPEN: the id is now unmapped and still
                # needs attention (reject_mapping parks it "so it is not forgotten").
                mark_unmatched_resolved(session, source, external_id)
            session.commit()
    if not ok:
        typer.echo(f"no crosswalk row for {source}:{external_id}", err=True)
        raise typer.Exit(code=1)
    typer.echo(("rejected " if reject else "verified ") + f"{source}:{external_id}")


@crosswalk_app.command("resolve-unmatched")
def crosswalk_resolve_unmatched(
    source: str = typer.Argument(
        ..., help="sleeper|espn|yahoo|pfr|fantasypros|sportradar|rotowire"
    ),
    external_id: str = typer.Argument(...),
) -> None:
    """Close a review-queue entry that has no mapping row (rung-5 ids that will never map)."""
    from ffh.crosswalk.review import mark_unmatched_resolved

    with _session_scope() as session:
        ok = mark_unmatched_resolved(session, source, external_id)
        if ok:
            session.commit()
    if not ok:
        typer.echo(f"no crosswalk_unmatched row for {source}:{external_id}", err=True)
        raise typer.Exit(code=1)
    typer.echo(f"resolved unmatched {source}:{external_id}")
