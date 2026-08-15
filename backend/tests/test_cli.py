from typer.testing import CliRunner

from ffh import __version__
from ffh.cli import app

runner = CliRunner()


def test_version_prints_package_version():
    result = runner.invoke(app, ["version"])
    assert result.exit_code == 0
    assert __version__ in result.stdout


def test_subcommand_groups_exist():
    for group in ("ingest", "league", "crosswalk"):
        result = runner.invoke(app, [group, "--help"])
        assert result.exit_code == 0, result.stdout
