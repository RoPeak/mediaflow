from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

pytest.importorskip("PySide6")

from PySide6.QtWidgets import QApplication, QMessageBox

from mediaflow.main_window import MainWindow
from mediaflow.callback_types import ApplyProgress, PreparationProgress, PreparationStageUpdate
from mediaflow.workflow import WorkflowState
from mediashrink.gui_api import EncodePreparation, EncodeProgress


def _app() -> QApplication:
    return QApplication.instance() or QApplication([])


@pytest.fixture(autouse=True)
def _isolate_persisted_state(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr("mediaflow.main_window.load_ui_state", lambda: {})
    monkeypatch.setattr("mediaflow.main_window.save_ui_state", lambda _payload: None)

    def _fake_write(self, *, base_dir=None, summary=None, failure=None):
        self.written_path = tmp_path / "run.json"
        return self.written_path

    monkeypatch.setattr("mediaflow.diagnostics.DiagnosticsRecorder.write", _fake_write)


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
    assert "Diagnostics:" in window.diagnostics_path_label.text()
    assert window.font().pointSizeF() > 0


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


def test_scan_progress_updates_placeholder_counts() -> None:
    _app()
    window = MainWindow()
    window._scan_started_at = 0.0
    window._set_state(WorkflowState.SCANNING)

    window._scan_progress({"kind": "scan_progress", "discovered": 3, "path": "/tmp/Doctor.Who.S07E05.mkv"})

    assert "Discovered so far: 3" in window.review_placeholder_label.text()
    assert "Doctor.Who.S07E05.mkv" in window.review_placeholder_label.text()


def test_review_placeholder_distinguishes_empty_scan_from_no_scan() -> None:
    _app()
    window = MainWindow()

    window.controller = SimpleNamespace(items=[])
    window._set_state(WorkflowState.REVIEW)

    assert "No organise candidates were discovered" in window.review_placeholder_label.text()
    assert window.review_summary_label.text() == "No organise candidates found in the last scan."


def test_review_summary_counts_blocked_items_as_unresolved() -> None:
    _app()
    window = MainWindow()
    blocked = SimpleNamespace(
        item=SimpleNamespace(path=Path("/tmp/blocked.mkv"), media_type="tv", title="Blocked", season=None, episode=None),
        manual_candidate=None,
        selected_candidate_index=None,
        candidates=[],
        decision_status="accepted",
        preview_block_reason="Missing season or episode.",
        unresolved_reason=None,
        warning=None,
        cache_context="search result",
        auto_selectable=False,
        status_label="blocked",
        has_more=False,
        candidate_states=[],
        skipped=False,
    )
    accepted = SimpleNamespace(
        item=SimpleNamespace(path=Path("/tmp/ok.mkv"), media_type="movie", title="Movie", season=None, episode=None),
        manual_candidate=None,
        selected_candidate_index=None,
        candidates=[],
        decision_status="accepted",
        preview_block_reason=None,
        unresolved_reason=None,
        warning=None,
        cache_context="search result",
        auto_selectable=False,
        status_label="accepted",
        has_more=False,
        candidate_states=[],
        skipped=False,
    )
    window.controller = SimpleNamespace(items=[blocked, accepted])

    window._update_review_summary()

    assert "Accepted: 1" in window.review_summary_label.text()
    assert "Unresolved: 1" in window.review_summary_label.text()
    assert "Why apply is blocked" in window.review_blocked_label.text()


def test_review_filter_can_focus_blocked_items() -> None:
    _app()
    window = MainWindow()
    items = [
        SimpleNamespace(
            item=SimpleNamespace(path=Path("/tmp/blocked.mkv"), media_type="tv", title="Blocked", season=None, episode=None),
            manual_candidate=None,
            selected_candidate_index=None,
            candidates=[],
            decision_status="accepted",
            preview_block_reason="Missing season or episode.",
            unresolved_reason=None,
            warning=None,
            cache_context="search result",
            auto_selectable=False,
            status_label="blocked",
            has_more=False,
            candidate_states=[],
            skipped=False,
        ),
        SimpleNamespace(
            item=SimpleNamespace(path=Path("/tmp/ok.mkv"), media_type="movie", title="Movie", season=None, episode=None),
            manual_candidate=None,
            selected_candidate_index=None,
            candidates=[],
            decision_status="accepted",
            preview_block_reason=None,
            unresolved_reason=None,
            warning=None,
            cache_context="search result",
            auto_selectable=False,
            status_label="accepted",
            has_more=False,
            candidate_states=[],
            skipped=False,
        ),
    ]
    window.controller = SimpleNamespace(items=items)

    window._populate_review_table()
    window._set_combo_value(window.review_filter_combo, "Blocked only")
    window._apply_review_filter()

    assert window.review_table.isRowHidden(0) is False
    assert window.review_table.isRowHidden(1) is True


def test_guided_pipeline_resets_filters_to_defaults(monkeypatch, tmp_path: Path) -> None:
    _app()
    window = MainWindow()
    source = tmp_path / "incoming"
    library = tmp_path / "library"
    source.mkdir()
    library.mkdir()
    window.source_input.setText(str(source))
    window.library_input.setText(str(library))
    window.compression_root_input.setText(str(library))
    window._set_combo_value(window.review_filter_combo, "Blocked only")
    window._set_combo_value(window.compression_filter_combo, "Missing items")

    monkeypatch.setattr(window, "_ensure_compatibility", lambda: True)
    monkeypatch.setattr("mediaflow.main_window.QMessageBox.question", lambda *_args, **_kwargs: QMessageBox.Yes)
    monkeypatch.setattr(window, "_start_worker", lambda *_args, **_kwargs: None)

    window._start_guided_pipeline()

    assert window.review_filter_combo.currentText() == "All items"
    assert window.compression_filter_combo.currentText() == "All plan items"


def test_review_filter_banner_explains_hidden_rows() -> None:
    _app()
    window = MainWindow()
    items = [
        SimpleNamespace(
            item=SimpleNamespace(path=Path("/tmp/ok.mkv"), media_type="movie", title="Movie", season=None, episode=None),
            manual_candidate=None,
            selected_candidate_index=None,
            candidates=[],
            decision_status="accepted",
            preview_block_reason=None,
            preview_valid=True,
            unresolved_reason=None,
            warning=None,
            cache_context="search result",
            auto_selectable=False,
            status_label="accepted",
            has_more=False,
            candidate_states=[],
            skipped=False,
        ),
    ]
    window.controller = SimpleNamespace(items=items)

    window._populate_review_table()
    window._set_combo_value(window.review_filter_combo, "Blocked only")
    window._apply_review_filter()

    assert "Filtered view" in window.review_filter_status_label.text()
    assert "All items" in window.review_filter_status_label.text()


def test_compression_filter_banner_explains_hidden_rows(tmp_path: Path) -> None:
    _app()
    window = MainWindow()
    item_source = tmp_path / "movie.mkv"
    item_source.write_bytes(b"x")
    prep = EncodePreparation(
        directory=tmp_path,
        ffmpeg=tmp_path / "ffmpeg",
        ffprobe=tmp_path / "ffprobe",
        items=[
            SimpleNamespace(
                source=item_source,
                codec="h264",
                recommendation="recommended",
                reason_text="Large AVC file",
                estimated_output_bytes=400,
                estimated_savings_bytes=600,
            )
        ],
        duplicate_warnings=[],
        profile=SimpleNamespace(name="Fast", encoder_key="faster", crf=22),
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
    )

    window._compression_prepared(prep)
    window._set_combo_value(window.compression_filter_combo, "Missing items")
    window._apply_compression_filter()

    assert "Filtered view" in window.compression_filter_status_label.text()
    assert "All plan items" in window.compression_filter_status_label.text()


def test_summary_headline_tracks_non_completed_state() -> None:
    _app()
    window = MainWindow()
    window.organise_enabled.setChecked(True)
    window.compress_enabled.setChecked(True)
    window._set_state(WorkflowState.READY_TO_APPLY)
    window._refresh_pipeline_summary()

    assert window.summary_headline_label.text() == "Organisation preview ready"
    assert "Current workflow state: ready_to_apply" in window.summary_overview_label.text()


def test_compression_prepared_flushes_matching_diagnostics_summary(monkeypatch, tmp_path: Path) -> None:
    _app()
    window = MainWindow()
    item_source = tmp_path / "movie.mkv"
    item_source.write_bytes(b"x")
    prep = EncodePreparation(
        directory=tmp_path,
        ffmpeg=tmp_path / "ffmpeg",
        ffprobe=tmp_path / "ffprobe",
        items=[
            SimpleNamespace(
                source=item_source,
                codec="h264",
                recommendation="recommended",
                reason_text="Large AVC file",
                estimated_output_bytes=400,
                estimated_savings_bytes=600,
            )
        ],
        duplicate_warnings=[],
        profile=SimpleNamespace(name="Fast", encoder_key="faster", crf=22),
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
    )
    captured: dict[str, object] = {}

    def _fake_write(*, summary, failure, base_dir=None):
        captured["summary"] = summary
        captured["failure"] = failure
        return tmp_path / "run.json"

    monkeypatch.setattr(window._diagnostics, "write", _fake_write)

    window._compression_prepared(prep)

    assert window._diagnostics.events[-1]["kind"] == "compression_prepared"
    assert captured["summary"]["workflow_state"] == "ready_to_compress"
    assert captured["summary"]["summary_headline"] == "Compression plan ready"


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


def test_manual_movie_match_uses_explicit_year(monkeypatch) -> None:
    _app()
    window = MainWindow()
    captured: dict[str, object] = {}
    item = SimpleNamespace(
        item=SimpleNamespace(path=Path("/tmp/movie.mp4"), media_type="movie", title="Movie", season=None, episode=None),
        manual_candidate=None,
        selected_candidate_index=None,
        candidates=[],
        decision_status="pending",
        preview_block_reason=None,
        unresolved_reason=None,
        warning=None,
        cache_context="search result",
        auto_selectable=False,
        status_label="pending",
        has_more=False,
        candidate_states=[],
        skipped=False,
    )
    window.controller = SimpleNamespace(
        items=[item],
        manual_select=lambda index, **payload: captured.update({"index": index, **payload}),
    )
    window._populate_review_table()
    monkeypatch.setattr(window, "_prompt_manual_movie_selection", lambda _item: {"title": "Teen Titans", "year": 2019})
    monkeypatch.setattr(window, "_refresh_review", lambda: None)

    window._manual_select_current_item()

    assert captured["index"] == 0
    assert captured["title"] == "Teen Titans"
    assert captured["year"] == 2019


def test_apply_progress_updates_current_action_and_log() -> None:
    _app()
    window = MainWindow()

    window._apply_progress_update(
        ApplyProgress(
            phase="copying",
            current_source="/tmp/source.mp4",
            current_destination="/tmp/dest.mp4",
            completed=0,
            total=3,
            message="Copying source.mp4",
        )
    )

    assert "Copying organisation (item 1 of 3)" in window.current_action_label.text()
    assert "Destination: /tmp/dest.mp4" in window.current_action_label.text()
    assert "source.mp4" in window.current_action_label.text()
    assert window._diagnostics.events[-1]["kind"] == "organisation_apply_progress"


def test_apply_heartbeat_mentions_still_working_on_current_file(monkeypatch) -> None:
    _app()
    window = MainWindow()
    window._apply_started_at = 10.0
    window._apply_last_update_at = 15.0
    window._set_state(WorkflowState.APPLYING)
    window._apply_progress = ApplyProgress(
        phase="copying",
        current_source="/tmp/source.mp4",
        current_destination="/tmp/dest.mp4",
        completed=0,
        total=2,
        message="Copying source.mp4",
    )

    monkeypatch.setattr("mediaflow.main_window.time.monotonic", lambda: 30.0)
    window._tick_apply()

    assert "Still working on the last reported file." in window.current_action_label.text()


def test_diagnostics_write_failure_becomes_visible_warning(monkeypatch) -> None:
    _app()
    window = MainWindow()

    def _boom(*, summary, failure, base_dir=None):
        raise OSError("disk full")

    monkeypatch.setattr(window._diagnostics, "write", _boom)

    window._flush_runtime_diagnostics()

    assert "Unable to write diagnostics" in window.diagnostics_path_label.text()
    assert any("Unable to write diagnostics" in warning for warning in window._custom_warnings)


def test_open_diagnostics_folder_creates_target_before_opening(monkeypatch, tmp_path: Path) -> None:
    _app()
    window = MainWindow()
    target = tmp_path / "missing" / "runs" / "run.json"
    opened: list[str] = []
    monkeypatch.setattr(window, "_last_diagnostics_path", target)
    monkeypatch.setattr(window, "_open_path", opened.append)

    window._open_diagnostics_folder()

    assert target.parent.exists() is True
    assert opened == [str(target.parent)]


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


def test_zero_job_compression_plan_explains_disabled_start(tmp_path: Path) -> None:
    _app()
    window = MainWindow()
    item_source = tmp_path / "movie.mkv"
    item_source.write_bytes(b"x")
    prep = EncodePreparation(
        directory=tmp_path,
        ffmpeg=tmp_path / "ffmpeg",
        ffprobe=tmp_path / "ffprobe",
        items=[
            SimpleNamespace(
                source=item_source,
                codec="h264",
                recommendation="recommended",
                reason_text="Large AVC file",
                estimated_output_bytes=400,
                estimated_savings_bytes=600,
            )
        ],
        duplicate_warnings=[],
        profile=SimpleNamespace(name="Fast", encoder_key="faster", crf=22),
        jobs=[],
        recommended_count=1,
        maybe_count=0,
        skip_count=0,
        selected_count=1,
        total_input_bytes=1000,
        selected_input_bytes=0,
        selected_estimated_output_bytes=0,
        estimated_total_seconds=120.0,
        on_file_failure="retry",
        use_calibration=True,
    )

    window._compression_prepared(prep)

    assert window.start_compress_button.isEnabled() is False
    assert "cannot start yet" in window.current_action_label.text().lower()
    assert "no encode jobs were auto-selected" in window.compress_summary_label.text().lower()
    assert "no runnable jobs were selected" in window.start_compress_button.toolTip().lower()


def test_zero_compatible_plan_enters_attention_state_and_offers_safer_rebuild(tmp_path: Path) -> None:
    _app()
    window = MainWindow()
    item_source = tmp_path / "movie.mkv"
    item_source.write_bytes(b"x")
    prep = EncodePreparation(
        directory=tmp_path,
        ffmpeg=tmp_path / "ffmpeg",
        ffprobe=tmp_path / "ffprobe",
        items=[
            SimpleNamespace(
                source=item_source,
                codec="h264",
                recommendation="recommended",
                reason_text="Large AVC file",
                estimated_output_bytes=400,
                estimated_savings_bytes=600,
            )
        ],
        duplicate_warnings=[],
        profile=SimpleNamespace(name="Fastest", encoder_key="amf", crf=20),
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
        compatible_count=0,
        incompatible_count=1,
        grouped_incompatibilities={"hardware encoder startup failure": 1},
        recommendation_reason="Likely works for 0 file(s); try a safer fallback.",
    )

    window._compression_prepared(prep)

    assert window.summary_headline_label.text() == "Compression plan needs attention"
    assert "not safe for this batch" in window.current_action_label.text().lower()
    assert window.rebuild_safer_button.isVisible() is True
    assert window.rebuild_safer_button.isEnabled() is True
    assert "predicted to work for 0 files" in window.start_compress_button.toolTip().lower()


def test_completed_summary_marks_all_skipped_compression_as_degraded(tmp_path: Path) -> None:
    _app()
    window = MainWindow()
    source = tmp_path / "movie.mkv"
    source.write_bytes(b"x")
    window.compress_enabled.setChecked(True)
    window.encode_results = [
        SimpleNamespace(
            job=SimpleNamespace(source=source),
            skipped=True,
            success=False,
            skip_reason="incompatible: hardware encoder startup failure",
            input_size_bytes=100,
            output_size_bytes=0,
            error_message=None,
        )
    ]
    window._retry_sources = {source}
    window._set_state(WorkflowState.COMPLETED)
    window._refresh_pipeline_summary()

    assert "follow-up needed" in window.summary_headline_label.text().lower()
    assert "compression produced no successful encodes" in window.summary_log.toPlainText().lower()
    assert "compatibility checks" in window.summary_log.toPlainText().lower()


def test_missing_file_error_is_translated_for_users() -> None:
    _app()
    window = MainWindow()

    summary = window._translate_common_error(
        "[WinError 2] The system cannot find the file specified: 'D:\\\\Done\\\\Point Break (1991).mp4'"
    )

    assert "planned compression file is missing" in summary.lower()
    assert "Point Break" in summary
