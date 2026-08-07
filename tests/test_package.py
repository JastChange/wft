from typer.testing import CliRunner

from wft import __version__
from wft.cli.app import app


def test_version_and_help() -> None:
    assert __version__ == "1.0.0"
    result = CliRunner().invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "WFT node diagnostics" in result.stdout
