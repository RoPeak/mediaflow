from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

pytest.importorskip("PySide6")

from PySide6.QtWidgets import QApplication

from mediaflow.main_window import MainWindow
from mediaflow.workflow import WorkflowState
from mediashrink.gui_api import EncodePreparation


def _app() -> QApplication:
    return QApplication.instance() or QApplication([])


def test_initial_window_state_guides_user_through_setup() -> None:
    _app()
    window = MainWindow()

    assert window.workflow_state == WorkflowState.SETUP
    assert window.tabs.currentIndex() == 0
    assert window.tabs.isTabEnabled(1) is False
    assert window.tabs.isTabEnabled(2) is False
    assert window.scan_button.isEnabled() is True
    assert window.guided_button.isEnabled() is True
    assert window.preview_button.isEnabled() is False
    assert window.apply_button.isEnabled() is False
    assert window.start_compress_button.isEnabled() is False
    assert "guided pipeline" in window.setup_hint_label.text().lower()


def test_config_edits_only_mark_runtime_data_as_stale_when_runtime_exists() -> None:
    _app()
    window = MainWindow()

    window._on_config_edited()
    assert window._config_dirty is False

    window.controller = SimpleNamespace(items=[])
    window._on_config_edited()

    assert window._config_dirty is True
    assert "settings have changed" in window.guidance_label.text().lower()


def test_refresh_pipeline_summary_surfaces_existing_stage_results() -> None:
    _app()
    window = MainWindow()

    class ResultState:
        planned = 2
        errors = ["failure"]

    class ApplyState:
        result = ResultState()
        report_path = "/tmp/report.json"
        apply_report_path = None
        summary_lines = ["Applied 2 planned changes."]

    window.apply_result = ApplyState()
    window._refresh_pipeline_summary()

    overview = window.summary_overview_label.text()
    details = window.summary_log.toPlainText()
    assert "Organised plans: 2" in overview
    assert "Organise errors: 1" in overview
    assert "Organise report: /tmp/report.json" in overview
    assert "Applied 2 planned changes." in details


def test_compression_prepared_enables_encode_step_and_populates_plan(tmp_path: Path) -> None:
    _app()
    window = MainWindow()
    item_source = tmp_path / "movie.mkv"
    analysis_item = SimpleNamespace(
        source=item_source,
        codec="h264",
        recommendation="recommended",
        reason_text="Large AVC file",
        estimated_output_bytes=400,
        estimated_savings_bytes=600,
    )
    profile = SimpleNamespace(
        name="Fast",
        encoder_key="faster",
        crf=22,
    )
    prep = EncodePreparation(
        directory=tmp_path,
        ffmpeg=tmp_path / "ffmpeg",
        ffprobe=tmp_path / "ffprobe",
        items=[analysis_item],
        duplicate_warnings=["example duplicate warning"],
        profile=profile,
        jobs=[SimpleNamespace(source=item_source)],
        recommended_count=1,
        maybe_count=0,
        skip_count=0,
        selected_count=1,
        total_input_bytes=1000,
        selected_input_bytes=1000,
        selected_estimated_output_bytes=400,
        estimated_total_seconds=120.0,
        on_file_failure="retry",
        use_calibration=True,
        size_confidence="High",
        time_confidence="Medium",
        grouped_incompatibilities={"subtitle stream incompatibility": 1},
        recommendation_reason="Fast profile covers the selected file.",
        stage_messages=["Benchmarking complete."],
    )

    window._compression_prepared(prep)

    assert window.workflow_state == WorkflowState.READY_TO_COMPRESS
    assert window.start_compress_button.isEnabled() is True
    assert window.compression_table.rowCount() == 1
    assert "Selected: 1" in window.compress_summary_label.text()
    assert "Fast" in window.compress_summary_label.text()
    assert "Fast profile covers the selected file." in window.summary_log.toPlainText()
    assert "Benchmarking complete." in window.compress_status_log.toPlainText()


def test_traceback_errors_are_summarised_for_users() -> None:
    _app()
    window = MainWindow()
    traceback_text = "\n".join(
        [
            "Traceback (most recent call last):",
            '  File "example.py", line 1, in <module>',
            "    run()",
            "TypeError: scan_controller() got an unexpected keyword argument 'progress_callback'",
        ]
    )

    summary, details = window._summarise_error(traceback_text)

    assert "unexpected keyword argument" in summary
    assert details == traceback_text
