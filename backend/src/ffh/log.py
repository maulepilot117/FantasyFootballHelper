"""Process-wide structlog configuration: diagnostics on stderr, data on stdout.

structlog's *default* logger factory prints to **stdout**, and nothing in `ffh` ever
called `structlog.configure()`. Every log line a command emitted therefore landed on the
same stream as the command's own output, so `ffh crosswalk seed --playerids … | jq` read a
console-rendered log line as its first token and died — moving the one human progress line
to stderr fixed the symptom the author could see and left the sink itself pointing at
stdout.

The rule this module enforces: **stdout is the command's data channel** (the JSON reports
`ffh crosswalk seed`, `ffh crosswalk report --json` and `ffh ingest run` write), **stderr
carries everything else** — logs, progress, errors. Exit codes (DATABASE.md §3) are the
other half of that contract.

`ffh.cli` calls `configure_logging()` from the Typer root callback, so it runs once before
any subcommand. A future FastAPI entry point should call the same function.
"""

from __future__ import annotations

import sys

import structlog

_configured = False


class _StderrProxy:
    """A write target that resolves ``sys.stderr`` at write time, not at configure time.

    ``PrintLoggerFactory(file=sys.stderr)`` captures whatever object ``sys.stderr`` is
    bound to when `configure_logging` runs and writes to that object forever. Configuring
    exactly once per process is the point, so anything that swaps the stream afterwards —
    `typer.testing.CliRunner`, pytest's capture, `contextlib.redirect_stderr` — would be
    silently written past. One level of indirection makes the sink follow the stream.
    """

    def write(self, message: str) -> int:
        return sys.stderr.write(message)

    def flush(self) -> None:
        sys.stderr.flush()


def configure_logging() -> None:
    """Point structlog's sink at stderr. Idempotent; safe to call from every entry point.

    Only ``logger_factory`` is passed: `structlog.configure` applies just the keyword
    arguments it is given, so the processor chain is left exactly as structlog set it up.
    That is deliberate — `structlog.testing.capture_logs` works by swapping *processors*
    and restoring them, and a call to this function from inside a capture block must not
    disturb it. The sink was the defect; nothing else here needs to change.
    """
    global _configured
    if _configured:
        return
    structlog.configure(logger_factory=structlog.PrintLoggerFactory(file=_StderrProxy()))
    _configured = True
