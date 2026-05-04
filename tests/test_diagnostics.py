from __future__ import annotations

import json
from pathlib import Path

from mediaflow.diagnostics import DiagnosticsRecorder, select_diagnostics_dir


def test_diagnostics_recorder_writes_structured_run_file(tmp_path: Path) -> None:
    recorder = DiagnosticsRecorder()
    recorder.set_config({"source": "/tmp/source"})
    recorder.set_provenance({"app_version": "0.1.0"})
    recorder.record_event("compression_started", jobs=2)
    recorder.record_warning("Overwrite is enabled.")

    path = recorder.write(base_dir=tmp_path, summary={"encoded": 1})

    payload = json.loads(path.read_text(encoding="utf-8"))
    assert path.parent == tmp_path
    assert payload["effective_config"]["source"] == "/tmp/source"
    assert payload["provenance"]["app_version"] == "0.1.0"
    assert payload["warnings"] == ["Overwrite is enabled."]
    assert payload["summary"]["encoded"] == 1
    assert path.with_suffix(".log").exists()
    assert "compression_started" in path.with_suffix(".log").read_text(encoding="utf-8")


def test_diagnostics_recorder_deduplicates_identical_events() -> None:
    recorder = DiagnosticsRecorder()

    recorder.record_event("encode_progress", current_file="movie.mkv", current_file_progress=0.1)
    recorder.record_event("encode_progress", current_file="movie.mkv", current_file_progress=0.1)
    recorder.record_event("encode_progress", current_file="movie.mkv", current_file_progress=0.2)

    assert [event["current_file_progress"] for event in recorder.events] == [0.1, 0.2]


def test_select_diagnostics_dir_uses_verified_fallback(tmp_path: Path) -> None:
    blocked_file = tmp_path / "not-a-dir"
    blocked_file.write_text("blocked", encoding="utf-8")
    fallback = tmp_path / "fallback"

    selected, warning = select_diagnostics_dir([blocked_file, fallback])

    assert selected == fallback
    assert warning is not None
    assert fallback.exists()
