from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

from typer.testing import CliRunner

from mediaflow.cli import app

runner = CliRunner()


def test_help_renders_without_typer_path_union_error() -> None:
    result = runner.invoke(app, ["--help"])

    assert result.exit_code == 0
    assert "Usage:" in result.stdout
    assert "--source" in result.stdout
    assert "--library" in result.stdout


def test_cli_forwards_optional_paths_to_launch(tmp_path: Path) -> None:
    source = tmp_path / "source"
    library = tmp_path / "library"

    with patch("mediaflow.app.launch") as mock_launch:
        result = runner.invoke(
            app,
            ["--source", str(source), "--library", str(library)],
        )

    assert result.exit_code == 0
    mock_launch.assert_called_once_with(source=source, library=library)


def test_doctor_reports_success_when_runtime_compatibility_passes() -> None:
    with patch("mediaflow.cli.check_runtime_compatibility", return_value=[]):
        result = runner.invoke(app, ["doctor"])

    assert result.exit_code == 0
    assert "Runtime compatibility check passed." in result.stdout


def test_doctor_reports_failures_when_runtime_compatibility_fails() -> None:
    issues = [object()]
    with patch("mediaflow.cli.check_runtime_compatibility", return_value=issues), patch(
        "mediaflow.cli.compatibility_error_text",
        return_value="Runtime compatibility check failed:\n- plexify: mismatch",
    ):
        result = runner.invoke(app, ["doctor"])

    assert result.exit_code == 1
    assert "plexify: mismatch" in result.stdout
