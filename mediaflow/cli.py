from __future__ import annotations

from pathlib import Path

import typer

app = typer.Typer(add_completion=False, help="Launch the mediaflow desktop GUI.")


@app.command()
def main(
    source: Path | None = typer.Option(None, "--source", help="Default source folder."),
    library: Path | None = typer.Option(None, "--library", help="Default library folder."),
) -> None:
    from .app import launch

    launch(source=source, library=library)


if __name__ == "__main__":
    app()
