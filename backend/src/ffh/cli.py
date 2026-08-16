import typer

from ffh import __version__

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


# Placeholder commands so `--help` works on empty groups; later PRs replace these.
@ingest_app.command("list")
def ingest_list() -> None:
    """List registered ingest jobs (none yet)."""
    typer.echo("no ingest jobs registered")


@league_app.command("platforms")
def league_platforms() -> None:
    """List supported platforms."""
    typer.echo("sleeper")


@crosswalk_app.command("report")
def crosswalk_report() -> None:
    """Crosswalk coverage report (implemented in PR ④)."""
    typer.echo("crosswalk not yet implemented")
