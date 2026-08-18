import json
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Annotated

import typer
from sqlalchemy.orm import Session

from ffh import __version__
from ffh.config import get_settings

# Importing the job modules registers every @register-decorated class in ffh.ingest.base.JOBS.
# isort orders these by module path, so the one crosswalk-owned job sits above ffh.db here
# rather than with the ffh.ingest block below.
from ffh.crosswalk import dynastyprocess as _dynastyprocess  # noqa: F401
from ffh.db.engine import make_engine, make_session_factory
from ffh.ingest import games as _games  # noqa: F401
from ffh.ingest import nflverse as _nflverse  # noqa: F401
from ffh.ingest import reference as _reference  # noqa: F401
from ffh.ingest.base import JOBS, STATUS_FAILED, get_job
from ffh.ingest.reference import StadiumsJob, seed_generic_league, seed_nfl_teams

#: Crosswalk exit codes (DATABASE.md §3). 0 = gate green, 1 = gate red (something is
#: unmatched or awaiting review — a *data* state a human resolves), 2 = a crosswalk
#: conflict a human must rule on, 3 = an OPERATIONAL failure (unreadable file, malformed
#: CSV, database down). 3 exists so a cron wrapper can tell "the crosswalk has a gap" from
#: "the run never happened": both used to exit 1 and were indistinguishable.
EXIT_GATE_RED = 1
EXIT_CONFLICT = 2
EXIT_OPERATIONAL = 3

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
    allow_empty: bool = typer.Option(
        False,
        "--allow-empty",
        help="Do not fail on an empty (never-seeded) crosswalk. For pre-seed invocations.",
    ),
) -> None:
    """Crosswalk coverage report — the gate.

    Exit 0 = green. Exit 1 = red: something is unmatched, awaiting review, or the
    crosswalk was never seeded (`--allow-empty` opts out of the last one). Exit 3 = the
    report could not be produced at all (database down); a wrapper must not read that as
    a clean crosswalk.
    """
    from sqlalchemy.exc import SQLAlchemyError

    try:
        rep = _coverage_report_for_cli()
    except (SQLAlchemyError, OSError) as exc:
        typer.echo(f"crosswalk report failed: {type(exc).__name__}: {exc}", err=True)
        raise typer.Exit(code=EXIT_OPERATIONAL) from exc
    typer.echo(json.dumps(rep.to_dict(), indent=2, default=str) if json_out else rep.render())
    raise typer.Exit(code=0 if rep.gate_ok(allow_empty=allow_empty) else EXIT_GATE_RED)


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
    """Seed the players registry (and DSTs) from nflverse; optionally apply DynastyProcess ids.

    Exit 0 = seeded. Exit 2 = a `CrosswalkConflictError` a human must rule on (nothing is
    committed). Exit 3 = an operational failure — no players partition, an unreadable or
    malformed file, a truncated Parquet, a database error. Exit 1 is reserved for the
    `report` gate and is never returned here.
    """
    # Lazy imports: keep CLI start-up fast (the draft pick clock is 90 seconds) — in
    # particular ffh.features.duck pulls in duckdb, which must not load on every command.
    import polars as pl
    from polars.exceptions import PolarsError
    from sqlalchemy.exc import SQLAlchemyError

    from ffh.crosswalk.dynastyprocess import (
        CrosswalkConflictError,
        DynastyProcessError,
        apply_playerids,
        read_playerids_csv,
    )
    from ffh.crosswalk.registry import RegistryError, seed_players

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
            # Operational, not a gate signal: the data never arrived.
            raise typer.Exit(code=EXIT_OPERATIONAL)

    # The guarded region covers the frame reads and seed_players as well as the DP apply:
    # a truncated partition, a malformed CSV or a database outage are operational
    # failures, and letting them escape as exit 1 makes them indistinguishable from a
    # crosswalk gap to any cron wrapper reading the exit code.
    try:
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
                    # session is closed without commit so nothing partial lands.
                    typer.echo(str(exc), err=True)
                    raise typer.Exit(code=EXIT_CONFLICT) from exc
                typer.echo(json.dumps(report.__dict__, indent=2))
            session.commit()
    except (DynastyProcessError, RegistryError, OSError, PolarsError, SQLAlchemyError) as exc:
        typer.echo(f"crosswalk seed failed: {type(exc).__name__}: {exc}", err=True)
        raise typer.Exit(code=EXIT_OPERATIONAL) from exc


@crosswalk_app.command("verify")
def crosswalk_verify(
    source: str = typer.Argument(
        ..., help="sleeper|espn|yahoo|pfr|fantasypros|sportradar|rotowire"
    ),
    external_id: str = typer.Argument(...),
    reject: bool = typer.Option(
        False, "--reject", help="Tombstone the mapping instead of verifying it."
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
                # `--reject` deliberately leaves it OPEN: the row becomes a `rejected`
                # tombstone (so no sync can re-mint the pairing) and the id is now
                # unmapped, so it must stay on the gate until `ffh crosswalk map` or
                # `ffh crosswalk resolve-unmatched` rules on it.
                # force=True: the key HAS a live mapping here — accepting that mapping is
                # exactly the human decision that closes the entry, which is the one case
                # `mark_unmatched_resolved`'s live-mapping guard must not refuse.
                mark_unmatched_resolved(session, source, external_id, force=True)
            session.commit()
    if not ok:
        hint = (
            ""
            if reject
            # verify_mapping also refuses a `rejected` tombstone: stamping verified_at on
            # one would resurrect the pairing a human threw out.
            else " — missing, or already a `rejected` tombstone (`ffh crosswalk map` re-maps it)"
        )
        typer.echo(f"no crosswalk row for {source}:{external_id}{hint}", err=True)
        raise typer.Exit(code=1)
    typer.echo(("rejected " if reject else "verified ") + f"{source}:{external_id}")


@crosswalk_app.command("map")
def crosswalk_map(
    source: Annotated[
        str, typer.Argument(help="sleeper|espn|yahoo|pfr|fantasypros|sportradar|rotowire")
    ],
    external_id: Annotated[str, typer.Argument()],
    player_id: Annotated[uuid.UUID, typer.Argument(help="players.player_id (UUID).")],
) -> None:
    """Map an id to a player by hand: `manual`, confidence 1.0, verified.

    The operator path from `crosswalk_unmatched` to *mapped* (`resolve-unmatched` only
    silences a queue entry), and the only way out of a `--reject` tombstone.
    """
    from ffh.crosswalk.review import map_mapping

    with _session_scope() as session:
        result = map_mapping(session, source, external_id, player_id)
        if result.ok:
            session.commit()
    if not result.ok:
        typer.echo(f"{source}:{external_id} not mapped: {result.detail}", err=True)
        raise typer.Exit(code=1)
    typer.echo(f"mapped {source}:{external_id} -> {player_id} ({result.status})")


@crosswalk_app.command("resolve-unmatched")
def crosswalk_resolve_unmatched(
    source: str = typer.Argument(
        ..., help="sleeper|espn|yahoo|pfr|fantasypros|sportradar|rotowire"
    ),
    external_id: str = typer.Argument(...),
    force: bool = typer.Option(
        False,
        "--force",
        help="Close the entry even though the id still has a mapping row. Only for an id "
        "that will never map correctly and whose mapping you accept as-is.",
    ),
) -> None:
    """Close a review-queue entry that has no mapping row (rung-5 ids that will never map).

    Exit 0 = closed. Exit 1 = nothing to close, or the precondition failed: the id still
    has a live mapping row, so closing the entry would green the gate while `resolve`
    keeps handing consumers the contradicted mapping. Rule on that with
    `ffh crosswalk verify --reject` / `ffh crosswalk map` (or `--force`).
    """
    from ffh.crosswalk.review import live_mapping, mark_unmatched_resolved

    with _session_scope() as session:
        ok = mark_unmatched_resolved(session, source, external_id, force=force)
        mapping = None if ok else live_mapping(session, source, external_id)
        if ok:
            session.commit()
    if not ok:
        if mapping is not None:
            typer.echo(
                f"{source}:{external_id} still maps to {mapping.player_id} "
                f"({mapping.match_method}) — closing the queue entry would hide a mapping "
                "the ladder still returns. Rule on it with "
                f"`ffh crosswalk verify {source} {external_id} --reject` or "
                f"`ffh crosswalk map {source} {external_id} <player_id>`, or pass --force.",
                err=True,
            )
        else:
            typer.echo(f"no crosswalk_unmatched row for {source}:{external_id}", err=True)
        raise typer.Exit(code=EXIT_GATE_RED)
    typer.echo(f"resolved unmatched {source}:{external_id}")
