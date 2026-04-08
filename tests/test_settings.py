from __future__ import annotations

from pathlib import Path

from mediaflow.settings import load_ui_state, save_ui_state


def test_save_and_load_ui_state_round_trip(tmp_path: Path) -> None:
    state_path = tmp_path / "ui-state.json"
    payload = {
        "source": "/tmp/source",
        "library": "/tmp/library",
        "compression_root": "/tmp/compress",
        "compression_root_linked": False,
        "organise_enabled": True,
        "min_confidence": 0.9,
    }

    saved_path = save_ui_state(payload, path=state_path)
    loaded = load_ui_state(path=state_path)

    assert saved_path == state_path
    assert loaded == payload
