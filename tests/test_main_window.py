from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

pytest.importorskip("PySide6")

from PySide6.QtWidgets import QApplication

from mediaflow.main_window import MainWindow
from mediaflow.callback_types import PreparationProgress, PreparationStageUpdate
from mediaflow.workflow import WorkflowState
from mediashrink.gui_api import EncodePreparation, EncodeProgress


def _app() -> QApplication:
    return QApplication.instance() or QApplication([])


def test_initial_window_state_guides_user_through_setup() -> None:
    _app()
    window = MainWindow()

    assert window.workflow_state == WorkflowState.SETUP
    assert window.tabs.currentIndex() == 0
    assert window.scan_button.isEnabled() is True
    assert window.guided_button.isEnabled() is True
    assert window.preview_button.isEnabled() is False
    assert window.apply_button.isEnabled() is False
    assert window.start_compress_button.isEnabled() is False
    assert "guided pipeline" in window.setup_hint_label.text().lower()
    assert "compression root" in window.setup_summary_label.text().lower()


def test_library_path_updates_compression_root_while_linked() -> None:
    _app()
    window = MainWindow()

    window.library_input.setText("/tmp/library")

    assert window.compression_root_input.text() == "/tmp/library"
    assert window.link_compression_root.isChecked() is True


def test_manual_compression_root_edit_breaks_link() -> None:
    _app()
    window = MainWindow()
    window.library_input.setText("/tmp/library")

    window._compression_root_manually_edited("/tmp/custom-compress")

    assert window.link_compression_root.isChecked() is False


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
        moved = [object(), object()]
        skipped = []
        errors = ["failure"]

    class ApplyState:
        result = ResultState()
        report_path = "/tmp/report.json"
        apply_report_path = None
        summary_lines = ["Applied 2 planned changes."]

    window.apply_result = ApplyState()
    window.compression_root_input.setText("/tmp/compress")
    window._refresh_pipeline_summary()

    overview = window.summary_overview_label.text()
    details = window.summary_log.toPlainText()
    assert "Organised:        2 file(s)" in overview
    assert "Organise report: /tmp/report.json" in overview
    assert "Compression root: /tmp/compress" in overview
    assert "Applied 2 planned changes." in details


def test_compression_prepared_enables_encode_step_and_populates_plan(tmp_path: Path) -> None:
    _app()
    window = MainWindow()
    item_source = tmp_path / "movie.mkv"
    item_source.write_bytes(b"x")
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
    assert "1 file(s)" in window.compress_summary_label.text()
    assert "Fast" in window.compress_summary_label.text()
    assert "Fast profile covers the selected file." in window.compress_summary_label.text()
    assert "Benchmarking complete." in window.compress_status_log.toPlainText()


def test_compression_plan_defers_risky_jobs_by_default(tmp_path: Path) -> None:
    _app()
    window = MainWindow()
    safe_source = tmp_path / "safe.mkv"
    risky_source = tmp_path / "risky.mp4"
    safe_source.write_bytes(b"x")
    risky_source.write_bytes(b"x")
    items = [
        SimpleNamespace(
            source=safe_source,
            codec="mpeg2video",
            recommendation="recommended",
            reason_text="legacy codec with strong projected space savings",
            estimated_output_bytes=400,
            estimated_savings_bytes=600,
        ),
        SimpleNamespace(
            source=risky_source,
            codec="h264",
            recommendation="recommended",
            reason_text="output header failure: 6",
            estimated_output_bytes=500,
            estimated_savings_bytes=500,
        ),
    ]
    prep = EncodePreparation(
        directory=tmp_path,
        ffmpeg=tmp_path / "ffmpeg",
        ffprobe=tmp_path / "ffprobe",
        items=items,
        duplicate_warnings=[],
        profile=SimpleNamespace(name="Fast", encoder_key="faster", crf=22),
        jobs=[SimpleNamespace(source=safe_source), SimpleNamespace(source=risky_source)],
        recommended_count=2,
        maybe_count=0,
        skip_count=0,
        selected_count=2,
        total_input_bytes=2000,
        selected_input_bytes=2000,
        selected_estimated_output_bytes=900,
        estimated_total_seconds=120.0,
        on_file_failure="retry",
        use_calibration=True,
    )

    window._compression_prepared(prep)

    assert "deferred" in {window.compression_table.item(row, 7).text() for row in range(window.compression_table.rowCount())}
    assert "risky file(s) are deferred" in window.compress_summary_label.text()


def test_review_placeholder_shows_loading_state_during_scan() -> None:
    _app()
    window = MainWindow()

    window._set_state(WorkflowState.SCANNING)

    assert "Scanning source with plexify" in window.review_placeholder_label.text()


def test_progress_model_does_not_lose_current_file_progress_between_ticks() -> None:
    _app()
    window = MainWindow()
    progress = EncodeProgress(
        current_file="movie.mkv",
        current_file_progress=0.36,
        overall_progress=0.25,
        completed_files=1,
        remaining_files=3,
        bytes_processed=360,
        total_bytes=1000,
        heartbeat_state="active",
    )

    window._compression_start = 1.0
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr("mediaflow.main_window.time.monotonic", lambda: 10.0)
        window._encode_progress(progress)
        window._tick_compression()

    assert window.file_progress.value() >= 36
    assert window.overall_progress.value() == 25


def test_encode_progress_cleans_display_name_and_shows_eta_settling() -> None:
    _app()
    window = MainWindow()
    progress = EncodeProgress(
        current_file="In progress: Ghost (1990) (Unknown Year).mkv",
        current_file_progress=0.36,
        overall_progress=0.02,
        completed_files=0,
        remaining_files=3,
        bytes_processed=36,
        total_bytes=1000,
        heartbeat_state="active",
    )

    window._compression_start = 1.0
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr("mediaflow.main_window.time.monotonic", lambda: 10.0)
        window._encode_progress(progress)
        window._tick_compression()

    assert "In progress:" not in window.encode_filename_label.text()
    assert "Unknown Year" not in window.encode_filename_label.text()
    assert "settling" in window.eta_label.text().lower()


def test_preparing_compression_uses_preparing_view() -> None:
    _app()
    window = MainWindow()

    window._set_state(WorkflowState.PREPARING_COMPRESSION)

    assert window.compress_stack.currentIndex() == 1
    assert "compression plan" in window.compress_hint_label.text().lower()


def test_preparation_progress_updates_stage_dashboard(tmp_path: Path) -> None:
    _app()
    window = MainWindow()
    sample = tmp_path / "movie.mkv"
    sample.write_bytes(b"x" * 64)

    window._preparation_progress(
        PreparationStageUpdate(stage="benchmarking", message="Benchmarking profiles...", completed=1, total=3)
    )
    window._preparation_progress(PreparationProgress(1, 1, str(sample)))

    assert "Benchmarking" in window.prepare_log.toPlainText()
    assert "1 file(s)" in window.prepare_counts_label.text()
    assert "Analysing files" in window.prepare_stage_label.text()


def test_summary_export_includes_headline_and_mode(tmp_path: Path) -> None:
    _app()
    window = MainWindow()
    window.summary_headline_label.setText("Compression-only run completed")
    window.summary_mode_label.setText("Compression output mode: in-place")
    window.diagnostics_path_label.setText("Diagnostics: /tmp/run.json")
    window.summary_overview_label.setText("Encoded: 1 file(s)")
    window.summary_log.setPlainText("Compression results\n- movie.mkv")

    exported = window._build_summary_export_text()

    assert "Compression-only run completed" in exported
    assert "Compression output mode: in-place" in exported
    assert "Diagnostics: /tmp/run.json" in exported
    assert "Compression results" in exported


def test_encode_dashboard_toggle_hides_live_view() -> None:
    _app()
    window = MainWindow()
    window.encode_preparation = SimpleNamespace(
        jobs=[SimpleNamespace(source=Path("/tmp/movie.mkv"))],
        selected_input_bytes=0,
        selected_estimated_output_bytes=0,
        selected_count=1,
        recommended_count=0,
        maybe_count=0,
        skip_count=0,
        directory=Path("/tmp"),
        profile=None,
        followup_manifest_path=None,
        recommendation_reason=None,
        size_confidence=None,
        time_confidence=None,
        compatible_count=0,
        incompatible_count=0,
        grouped_incompatibilities={},
    )
    window.compress_stack.setCurrentIndex(2)
    window.show()
    _app().processEvents()
    window._set_state(WorkflowState.READY_TO_COMPRESS)

    window.encode_card.setVisible(True)
    window._on_toggle_encode_card(True)

    assert window.encode_card.isVisible() is False


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


def test_missing_file_error_is_translated_for_users() -> None:
    _app()
    window = MainWindow()

    summary = window._translate_common_error(
        "[WinError 2] The system cannot find the file specified: 'D:\\\\Done\\\\Point Break (1991).mp4'"
    )

    assert "planned compression file is missing" in summary.lower()
    assert "Point Break" in summary
