from __future__ import annotations

import json
import os
from pathlib import Path


def get_settings_path() -> Path:
    appdata = os.getenv("APPDATA")
    if appdata:
        return Path(appdata) / "mediaflow" / "ui-state.json"

    xdg_config = os.getenv("XDG_CONFIG_HOME")
    if xdg_config:
        return Path(xdg_config) / "mediaflow" / "ui-state.json"

    return Path.home() / ".config" / "mediaflow" / "ui-state.json"


def load_ui_state(path: Path | None = None) -> dict[str, object]:
    state_path = path or get_settings_path()
    if not state_path.exists():
        return {}
    try:
        raw = json.loads(state_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return raw if isinstance(raw, dict) else {}


def save_ui_state(payload: dict[str, object], path: Path | None = None) -> Path:
    state_path = path or get_settings_path()
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return state_path
