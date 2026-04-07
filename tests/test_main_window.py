from __future__ import annotations

import pytest

pytest.importorskip("PySide6")

from PySide6.QtWidgets import QApplication

from mediaflow.main_window import MainWindow
from mediashrink.gui_api import EncodePreparation
from mediashrink.wizard import EncoderProfile


def test_stage_toggles_enable_and_disable_group_boxes() -> None:
    app = QApplication.instance() or QApplication([])
    window = MainWindow()

    window.organise_enabled.setChecked(False)
    window.compress_enabled.setChecked(True)
    window._update_stage_controls()

    assert window.organise_group.isEnabled() is False
    assert window.compress_group.isEnabled() is True

    window.compress_enabled.setChecked(False)
    window._update_stage_controls()

    assert window.compress_group.isEnabled() is False


def test_initial_action_state_is_practical() -> None:
    app = QApplication.instance() or QApplication([])
    window = MainWindow()

    assert window.scan_button.isEnabled() is True
    assert window.run_pipeline_button.isEnabled() is True
    assert window.preview_button.isEnabled() is False
    assert window.apply_button.isEnabled() is False
    assert window.accept_button.isEnabled() is False
    assert window.compress_button.isEnabled() is True
    assert window.auto_accept_button.isEnabled() is False


def test_disabling_compress_stage_disables_run_button() -> None:
    app = QApplication.instance() or QApplication([])
    window = MainWindow()

    window.compress_enabled.setChecked(False)
    window._update_stage_controls()

    assert window.compress_button.isEnabled() is False


def test_cancel_clears_pending_pipeline_flags() -> None:
    app = QApplication.instance() or QApplication([])
    window = MainWindow()
    window._pipeline_requested = True
    window._pipeline_should_compress_after_apply = True
    window.status_log.setPlainText("status")
    window.summary_log.setPlainText("summary")

    window._cancel_requested()

    assert window._pipeline_requested is False
    assert window._pipeline_should_compress_after_apply is False
    assert window.status_log.toPlainText() == ""
    assert window.summary_log.toPlainText() == ""


def test_refresh_pipeline_summary_includes_existing_summary_text() -> None:
    app = QApplication.instance() or QApplication([])
    window = MainWindow()

    class ResultState:
        planned = 2
        errors = []

    class ApplyState:
        result = ResultState()
        report_path = "/tmp/report.json"
        apply_report_path = None

    window.apply_result = ApplyState()
    window.summary_log.setPlainText("Existing details")

    window._refresh_pipeline_summary()

    text = window.summary_log.toPlainText()
    assert "Pipeline Summary" in text
    assert "Organised plans: 2" in text
    assert "Existing details" in text


def test_compression_prepared_surfaces_compatibility_summary(tmp_path) -> None:
    app = QApplication.instance() or QApplication([])
    window = MainWindow()
    prep = EncodePreparation(
        directory=tmp_path,
        ffmpeg=tmp_path / "ffmpeg",
        ffprobe=tmp_path / "ffprobe",
        items=[object()],
        duplicate_warnings=[],
        profile=EncoderProfile(
            index=1,
            intent_label="Fast",
            name="Fast",
            encoder_key="faster",
            crf=22,
            sw_preset="faster",
            estimated_output_bytes=100,
            estimated_encode_seconds=20.0,
            quality_label="Very good",
            is_recommended=True,
            compatible_count=7,
            incompatible_count=4,
            grouped_incompatibilities={"attachment stream incompatibility": 4},
            why_choose="Fastest wait: AMF, but Fast covers all 7 files while AMF likely leaves 4 for follow-up.",
        ),
        jobs=[object()],
        recommended_count=7,
        maybe_count=0,
        skip_count=2,
        selected_count=7,
        total_input_bytes=1000,
        selected_input_bytes=800,
        selected_estimated_output_bytes=300,
        estimated_total_seconds=120.0,
        on_file_failure="retry",
        use_calibration=True,
        size_confidence="High",
        time_confidence="Medium",
        compatible_count=7,
        incompatible_count=4,
        grouped_incompatibilities={"attachment stream incompatibility": 4},
        recommendation_reason="Fastest wait: AMF, but Fast covers all 7 files while AMF likely leaves 4 for follow-up.",
        stage_messages=["Benchmarking profiles... 3/3"],
    )

    with pytest.MonkeyPatch.context() as mp:
        from PySide6.QtWidgets import QMessageBox

        mp.setattr(QMessageBox, "question", lambda *args, **kwargs: QMessageBox.No)
        window._compression_prepared(prep)

    summary = window.summary_log.toPlainText()
    status = window.status_log.toPlainText()
    assert "Compatibility coverage: 7 compatible / 4 likely follow-up" in summary
    assert "Likely incompatibilities: attachment stream incompatibility (4)" in summary
    assert "Benchmarking profiles... 3/3" in status
