# mediaflow

Practical desktop GUI for running `plexify` and `mediashrink` as one video workflow.

## Current scope

- configure source and library folders
- review and apply plexify organisation
- prepare and run mediashrink compression
- inspect plain progress and summary output
- remember the last-used UI configuration in user-level config storage

This project is intentionally utility-first. The goal is a clear operator-facing desktop app, not a decorative interface.

## Install

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

## Notes

- `PySide6` is required to launch the desktop window.
- FFmpeg and ffprobe must be available for the compression stage.
- `Run Full Pipeline` scans, auto-accepts high-confidence matches, previews the organisation stage, then continues into compression once the organise stage is applied.
- Project-local `AGENTS.md` and `CLAUDE.md` are intentionally excluded from git and must remain untracked.
