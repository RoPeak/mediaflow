from pathlib import Path
from typing import Optional

import typer

app = typer.Typer(add_completion=False, help="Launch the mediaflow desktop GUI.")


@app.command()
def main(
    source: Optional[Path] = typer.Option(None, "--source", help="Default source folder."),
    library: Optional[Path] = typer.Option(None, "--library", help="Default library folder."),
) -> None:
    from .app import launch

    launch(source=source, library=library)


if __name__ == "__main__":
    app()
