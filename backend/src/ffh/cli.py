import json
from collections.abc import Iterator
from contextlib import contextmanager

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


@crosswalk_app.command("report")
def crosswalk_report() -> None:
    """Crosswalk coverage report (implemented in PR ④)."""
    typer.echo("crosswalk not yet implemented")
