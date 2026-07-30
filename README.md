# mediaflow

Practical desktop GUI for running `plexify` and `mediashrink` as one video workflow.

## Current scope

- configure source, library/output, and compression root folders
- review and apply plexify organisation
- prepare and run mediashrink compression
- inspect plain progress and summary output
- remember the last-used UI configuration in user-level config storage

This project is intentionally utility-first. The goal is a clear operator-facing desktop app, not a decorative interface.

## Install for users

Download the latest release artifact for your operating system from GitHub
Releases, extract it, and run the `mediaflow` executable inside the extracted
folder.

Release builds are portable PyInstaller bundles. They include `mediaflow`,
`plexify`, `mediashrink`, and the Python GUI runtime, so you do not need to
clone the project, create a virtual environment, or install Python packages
manually.

FFmpeg and ffprobe are still runtime prerequisites. Install FFmpeg separately
and make sure both commands are available on `PATH` before running compression.

macOS release bundles are currently unsigned. Until signing and notarization are
added, macOS may require you to approve the app in Privacy & Security settings
the first time you launch it.

## Install for development

`mediaflow` expects local editable installs of `plexify` and `mediashrink`, plus a GUI runtime:

```bash
pip install -e ../plexify
pip install -e ../mediashrink
pip install -e .
```

## Run

```bash
mediaflow
```

Optional defaults:

```bash
mediaflow --source /path/to/incoming --library /path/to/library
```

Runtime diagnostics:

```bash
mediaflow doctor --source /path/to/incoming --library /path/to/library --compression-root /path/to/compress
```

## Packaging

Release builds use `PyInstaller` through `mediaflow.spec`. PyInstaller builds
must run on the target OS; a WSL build produces the Linux bundle, while Windows
and macOS release artifacts should come from native runners or GitHub Actions:

```bash
pip install -e .[dev]
pyinstaller --noconfirm --clean mediaflow.spec
```

See `docs/releasing.md` for the full release process, including pinned
`plexify` and `mediashrink` refs, CI artifacts, and smoke-test steps.

The current GUI also records startup, plan-preparation, and first-progress
timings so packaged builds can be compared against editable runs.

## Notes

- `PySide6` is required to launch the desktop window.
- FFmpeg and ffprobe must be available for the compression stage.
- `Library / Output Folder` is where organised files are written by `plexify`.
- `Compression Root` defaults to the library/output folder, but you can point compression at a different folder if needed.
- `Start Guided Pipeline` scans, auto-accepts high-confidence matches, previews the organisation stage, then continues into compression once the organise stage is applied.
