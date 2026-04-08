from __future__ import annotations

from pathlib import Path
from typing import Optional

import typer

from .compat import check_runtime_compatibility, compatibility_error_text
from .mediashrink_adapter import prepare_tools

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
def doctor(
    source: Optional[Path] = typer.Option(None, "--source", help="Optional source folder to verify."),
    library: Optional[Path] = typer.Option(
        None, "--library", help="Optional library/output folder to verify."
    ),
    compression_root: Optional[Path] = typer.Option(
        None, "--compression-root", help="Optional compression root to verify."
    ),
) -> None:
    """Check that mediaflow can import the expected plexify and mediashrink integration APIs."""
    failures: list[str] = []
    issues = check_runtime_compatibility()
    if issues:
        failures.append(compatibility_error_text(issues))

    try:
        ffmpeg, ffprobe = prepare_tools()
    except Exception as exc:
        failures.append(f"FFmpeg tools are unavailable: {exc}")
    else:
        typer.echo(f"FFmpeg: {ffmpeg}")
        typer.echo(f"FFprobe: {ffprobe}")

    for label, path in (
        ("Source", source),
        ("Library / Output Folder", library),
        ("Compression Root", compression_root),
    ):
        if path is None:
            continue
        if not path.exists():
            failures.append(f"{label} does not exist: {path}")
        elif not path.is_dir():
            failures.append(f"{label} is not a directory: {path}")
        else:
            typer.echo(f"{label}: {path}")

    if failures:
        typer.echo("\n".join(failures))
        raise typer.Exit(code=1)

    typer.echo("Runtime compatibility check passed.")


if __name__ == "__main__":
    app()
