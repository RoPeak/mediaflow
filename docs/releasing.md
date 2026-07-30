# Releasing mediaflow

`mediaflow` releases are portable PyInstaller bundles built on each target OS.
Users should not need to clone the repository, activate a virtual environment, or
install `plexify` and `mediashrink` manually.

## Release inputs

- Release builds install `plexify` and `mediashrink` from `requirements-release.txt`.
- Before cutting a release, update those refs to the exact tags or commit SHAs
  that were tested with the current `mediaflow` version.
- FFmpeg and ffprobe are external runtime prerequisites. They must be available
  on `PATH` for compression features to work.
- macOS bundles are unsigned at this stage. Users may need to approve the app in
  macOS Privacy & Security settings until signing and notarization are added.

## Build locally

PyInstaller builds are OS-specific. A build run inside WSL produces a Linux
bundle, not a Windows `.exe`. Use GitHub Actions or a native Windows shell for
Windows artifacts, and a macOS runner for macOS artifacts.

From a clean checkout on the target OS:

```bash
python -m pip install --upgrade pip
python -m pip install -r requirements-release.txt
python -m pip install -e ".[dev]"
ruff check .
pytest -x -q
python -m PyInstaller --noconfirm --clean mediaflow.spec
```

The portable app is written to `dist/mediaflow/`.

Smoke-test the built command:

```bash
dist/mediaflow/mediaflow --help
dist/mediaflow/mediaflow doctor
```

On Windows, use `dist\mediaflow\mediaflow.exe`. When developing on WSL, run the
Linux smoke-test commands inside WSL and use the CI workflow for the Windows
bundle.

## CI release flow

The release workflow runs on:

- manual `workflow_dispatch`
- tags matching `v*`, such as `v0.1.0`

For each release:

1. Update `mediaflow.__version__` and `pyproject.toml` to the same version.
2. Update `requirements-release.txt` to exact `plexify` and `mediashrink` tags
   or commits.
3. Run the local validation and PyInstaller build for the OS available to you.
4. Commit the release changes.
5. Tag the commit, for example `git tag v0.1.0`.
6. Push the branch and tag.
7. Download and smoke-test the GitHub Actions artifacts.

Expected artifact names:

- `mediaflow-windows-x64.zip`
- `mediaflow-linux-x64.tar.gz`
- `mediaflow-macos-arm64.zip`

## Smoke-test checklist

- Launch the app without activating a virtual environment.
- Run `mediaflow doctor`.
- Confirm FFmpeg and ffprobe status is clear.
- Confirm missing source, library, or compression paths produce clear errors.
- Run a small organise-and-compress workflow before publishing the artifact.

## Follow-up hardening

Portable bundles are the first release step. Native installers, auto-updates,
bundled FFmpeg, Windows signing, and macOS signing/notarization should be added
after portable releases are working reliably.
