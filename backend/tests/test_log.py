"""The stdout/stderr contract: stdout is a command's data channel, stderr carries logs."""

import contextlib
import io

import pytest
import structlog.testing
from typer.testing import CliRunner

import ffh.log as log_module
from ffh.cli import app
from ffh.log import configure_logging

runner = CliRunner()


@pytest.fixture
def unconfigured():
    """Put structlog back where a fresh `ffh` process finds it: unconfigured, sink = stdout.

    Without this the tests below would pass on whatever configuration an earlier test in
    the session happened to leave behind, and would no longer fail if the fix regressed.
    """
    structlog.reset_defaults()
    log_module._configured = False
    yield
    log_module._configured = False
    configure_logging()


def test_configure_logging_puts_structlog_on_stderr(unconfigured, capsys):
    """structlog's default sink is STDOUT. Nothing in `ffh` configured it, so every log
    line a command emitted was interleaved with the JSON that command wrote — the reason
    `ffh crosswalk seed --playerids … | jq` could not be piped."""
    configure_logging()
    structlog.get_logger("tests.log").info("ffh.test.event", answer=42)

    out, err = capsys.readouterr()
    assert "ffh.test.event" in err
    assert "ffh.test.event" not in out


def test_the_cli_configures_logging_before_running_a_command(unconfigured, capsys):
    """The root Typer callback is what makes the contract hold for every subcommand — not
    just the ones a test remembered to configure by hand."""
    result = runner.invoke(app, ["version"])
    assert result.exit_code == 0, result.output

    structlog.get_logger("tests.log").info("ffh.test.after_cli")
    out, err = capsys.readouterr()
    assert "ffh.test.after_cli" in err
    assert "ffh.test.after_cli" not in out


def test_the_stderr_sink_follows_a_swapped_stream(unconfigured):
    """`PrintLoggerFactory(file=sys.stderr)` binds the stream object it saw at configure
    time — one CliRunner's buffer, pytest's capture, a `redirect_stderr` target that is
    long gone. Configuring once per process is the point, so the sink resolves
    `sys.stderr` at write time instead."""
    configure_logging()
    buf = io.StringIO()
    with contextlib.redirect_stderr(buf):
        structlog.get_logger("tests.log").info("ffh.test.redirected")

    assert "ffh.test.redirected" in buf.getvalue()


def test_configure_logging_does_not_disturb_capture_logs(unconfigured):
    """`structlog.testing.capture_logs` swaps the PROCESSORS and restores them; a great
    many crosswalk tests read their assertions out of it. `configure_logging` passes only
    `logger_factory`, so calling it (as every CLI invocation does) cannot break a capture
    in flight."""
    with structlog.testing.capture_logs() as logs:
        configure_logging()
        structlog.get_logger("tests.log").info("ffh.test.captured")

    assert [e["event"] for e in logs] == ["ffh.test.captured"]
