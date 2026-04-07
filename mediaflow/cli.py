from __future__ import annotations

from pathlib import Path
from typing import Optional

import typer

from .compat import check_runtime_compatibility, compatibility_error_text

app = typer.Typer(add_completion=False, help="Launch the mediaflow desktop GUI.")


@app.callback(invoke_without_command=True)
def main(
    ctx: typer.Context,
    source: Optional[Path] = typer.Option(None, "--source", help="Default source folder."),
    library: Optional[Path] = typer.Option(None, "--library", help="Default library folder."),
) -> None:
    if ctx.invoked_subcommand is not None:
        return

    from .app import launch

    launch(source=source, library=library)


@app.command()
def doctor() -> None:
    """Check that mediaflow can import the expected plexify and mediashrink integration APIs."""
    issues = check_runtime_compatibility()
    if issues:
        typer.echo(compatibility_error_text(issues))
        raise typer.Exit(code=1)
    typer.echo("Runtime compatibility check passed.")


if __name__ == "__main__":
    app()
