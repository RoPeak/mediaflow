from __future__ import annotations

import json
import zipfile
from pathlib import Path
from types import SimpleNamespace
from datetime import datetime

import pytest

pytest.importorskip("PySide6")

from PySide6.QtWidgets import QApplication, QMessageBox, QHeaderView

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
    monkeypatch.setattr("mediaflow.main_window.MainWindow._diagnostics_directory_path", lambda _self: tmp_path)

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
    assert window.title_group_button.text() == "Apply To Filename Show Group"


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
    assert window.compression_table.item(0, 7).text() == "runnable"
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


def test_guided_preflight_surfaces_scoped_compression_capability_warning(monkeypatch, tmp_path: Path) -> None:
    _app()
    window = MainWindow(default_source=tmp_path / "source", default_library=tmp_path / "library")
    (tmp_path / "source").mkdir()
    (tmp_path / "library").mkdir()
    window.compression_root_input.setText(str(tmp_path / "library"))
    monkeypatch.setattr("mediaflow.main_window.supports_prepare_source_paths", lambda: False)
    monkeypatch.setattr("mediaflow.main_window.supports_encode_run_results", lambda: False)
    config = window._current_config()

    text = window._guided_preflight_text(config)

    assert "prepare_encode_run(source_paths=...)" in text
    assert "EncodeRunResults" in text


def test_review_action_timestamps_are_timezone_aware_utc() -> None:
    _app()
    window = MainWindow()

    window._record_review_action("example")

    timestamp = window._review_action_timeline[-1]["timestamp"]
    parsed = datetime.fromisoformat(timestamp)
    assert parsed.tzinfo is not None
    assert timestamp.endswith("+00:00")


def test_source_paths_type_error_has_dependency_guidance() -> None:
    _app()
    window = MainWindow()

    summary, technical = window._summarise_error(
        "TypeError: prepare_encode_run() got an unexpected keyword argument 'source_paths'"
    )

    assert technical is None
    assert "mediashrink is too old" in summary
    assert "prepare_encode_run(source_paths=...)" in summary


def test_copy_space_preflight_includes_compression_quarantine_headroom(tmp_path: Path) -> None:
    _app()
    window = MainWindow()
    source = tmp_path / "movie.mkv"
    source.write_bytes(b"x" * 1024)
    window.copy_mode.setChecked(True)
    window.compress_enabled.setChecked(True)
    window.overwrite.setChecked(True)
    window.quarantine_originals.setChecked(True)
    window.preview_state = SimpleNamespace(plans=[SimpleNamespace(source=source, destination=tmp_path / "out" / "movie.mkv")])

    lines = window._organisation_preflight_lines()

    assert any("Compression/quarantine headroom estimate" in line for line in lines)
    assert any("Estimated total temporary space risk" in line for line in lines)


def test_guided_apply_prepares_compression_for_current_organised_batch(monkeypatch, tmp_path: Path) -> None:
    _app()
    window = MainWindow()
    destination = tmp_path / "library" / "Movie (2020).mkv"
    destination.parent.mkdir()
    destination.write_bytes(b"x")
    window.compression_root_input.setText(str(tmp_path / "library"))
    window._guided_mode = True
    window._continue_to_compress = True
    captured: dict[str, object] = {}
    monkeypatch.setattr(window, "_guided_compression_can_continue", lambda: True)
    monkeypatch.setattr(
        window,
        "_start_compression_preparation",
        lambda status, **kwargs: captured.update({"status": status, **kwargs}),
    )
    result = SimpleNamespace(
        result=SimpleNamespace(moved=[SimpleNamespace(destination=destination)], skipped=[], errors=[]),
        warnings=[],
        summary_lines=[],
    )

    window._apply_complete(result)

    assert captured["scope"] == "guided-organised-batch"
    assert captured["source_paths"] == [destination]
    assert destination.resolve(strict=False) in window._guided_batch_paths


def test_guided_apply_falls_back_to_root_when_batch_destinations_missing(monkeypatch, tmp_path: Path) -> None:
    _app()
    window = MainWindow()
    window.compression_root_input.setText(str(tmp_path))
    window._guided_mode = True
    window._continue_to_compress = True
    captured: dict[str, object] = {}
    monkeypatch.setattr(window, "_guided_compression_can_continue", lambda: True)
    monkeypatch.setattr(
        window,
        "_start_compression_preparation",
        lambda status, **kwargs: captured.update({"status": status, **kwargs}),
    )
    result = SimpleNamespace(
        result=SimpleNamespace(moved=[], skipped=[], errors=[]),
        warnings=[],
        summary_lines=[],
    )

    window._apply_complete(result)

    assert captured["scope"] == "guided-root-fallback"
    assert captured["source_paths"] is None
    assert "falling back" in window._compression_scope_warning


def test_title_group_uses_tv_filename_group_when_inferred_titles_are_generic() -> None:
    _app()
    window = MainWindow()

    def review_item(filename: str) -> SimpleNamespace:
        return SimpleNamespace(
            item=SimpleNamespace(
                path=Path("/tmp/iPlayer Recordings") / filename,
                media_type="tv",
                title="iPlayer Recordings",
                season=None,
                episode=None,
            )
        )

    half_man = review_item("Half_Man_Series_1_-_02._Episode_2_m002w06w_editorial.mp4")
    scot_squad = review_item("Scot_Squad_Series_2_-_06._Episode_6_b06pdw8k_original.mp4")
    young_1 = review_item("The_Young_Offenders_Series_5_-_02._Episode_2_m002tfsl_editorial.mp4")
    young_2 = review_item("The_Young_Offenders_Series_5_-_03._Episode_3_m002tfsm_editorial.mp4")
    window.controller = SimpleNamespace(items=[half_man, scot_squad, young_1, young_2])

    affected = window._title_group_items(2)

    assert affected == [young_1, young_2]


def test_review_table_shows_filename_group_for_iplayer_rows() -> None:
    _app()
    window = MainWindow()
    item = SimpleNamespace(
        item=SimpleNamespace(
            path=Path("/tmp/iPlayer Recordings/Half_Man_Series_1_-_02._Episode_2_m002w06w_editorial.mp4"),
            media_type="tv",
            title="iPlayer Recordings",
            season=1,
            episode=2,
        ),
        manual_candidate=None,
        selected_candidate=None,
        selected_candidate_index=None,
        candidates=[],
        decision_status="unresolved",
        preview_block_reason=None,
        unresolved_reason="No candidates available.",
        warning=None,
        cache_context="search result",
        auto_selectable=False,
        status_label="unresolved",
        has_more=False,
        candidate_states=[],
        skipped=False,
    )
    window.controller = SimpleNamespace(items=[item])

    window._populate_review_table()

    assert window.review_table.item(0, 1).text() == "half man"
    assert "Filename group: half man" in window.review_table.item(0, 1).toolTip()


def test_unresolved_groups_filter_surfaces_grouped_tv_rows() -> None:
    _app()
    window = MainWindow()
    grouped = []
    for filename in [
        "Half_Man_Series_1_-_02._Episode_2_m002w06w_editorial.mp4",
        "Half_Man_Series_1_-_03._Episode_3_m002w8zg_editorial.mp4",
    ]:
        grouped.append(
            SimpleNamespace(
                item=SimpleNamespace(path=Path("/tmp/iPlayer Recordings") / filename, media_type="tv", title="iPlayer Recordings", season=None, episode=None),
                manual_candidate=None,
                selected_candidate=None,
                selected_candidate_index=None,
                candidates=[],
                decision_status="unresolved",
                preview_block_reason=None,
                unresolved_reason="No candidates available.",
                warning=None,
                cache_context="search result",
                auto_selectable=False,
                status_label="unresolved",
                has_more=False,
                candidate_states=[],
                skipped=False,
            )
        )
    grouped.append(
        SimpleNamespace(
            item=SimpleNamespace(path=Path("/tmp/movie.mkv"), media_type="movie", title="Movie", season=None, episode=None),
            manual_candidate=None,
            selected_candidate=None,
            selected_candidate_index=None,
            candidates=[],
            decision_status="unresolved",
            preview_block_reason=None,
            unresolved_reason="No candidates available.",
            warning=None,
            cache_context="search result",
            auto_selectable=False,
            status_label="unresolved",
            has_more=False,
            candidate_states=[],
            skipped=False,
        )
    )
    window.controller = SimpleNamespace(items=grouped)

    window._populate_review_table()
    window._set_combo_value(window.review_filter_combo, "Unresolved groups")
    window._apply_review_filter()

    assert window.review_table.isRowHidden(0) is False
    assert window.review_table.isRowHidden(1) is False
    assert window.review_table.isRowHidden(2) is True
    assert "half man: 2" in window.review_filter_status_label.text()


def test_bulk_confirmation_table_includes_affected_row_details() -> None:
    _app()
    window = MainWindow()
    item = SimpleNamespace(
        item=SimpleNamespace(path=Path("/tmp/Half_Man_Series_1_-_02.mp4"), media_type="tv", title="Half Man", season=1, episode=2),
        manual_candidate=None,
        selected_candidate=SimpleNamespace(title="Half Man"),
        selected_candidate_index=0,
        candidates=[SimpleNamespace(title="Half Man")],
        decision_status="accepted",
        preview_block_reason=None,
        unresolved_reason=None,
        warning=None,
        cache_context="search result",
        auto_selectable=True,
        status_label="accepted",
        has_more=False,
        candidate_states=[],
        skipped=False,
    )
    window.controller = SimpleNamespace(items=[item])

    table = window._bulk_confirmation_table([0], "Half Man", "half man")

    assert table.item(0, 1).text() == "Half_Man_Series_1_-_02.mp4"
    assert table.item(0, 2).text() == "half man"
    assert table.item(0, 4).text() == "Half Man"


def test_suspicious_bulk_apply_is_blocked_without_changing_review_state(monkeypatch) -> None:
    _app()
    window = MainWindow()
    warnings: list[str] = []
    monkeypatch.setattr("mediaflow.main_window.QMessageBox.warning", lambda *_args, **_kwargs: warnings.append("shown"))
    source = SimpleNamespace(
        item=SimpleNamespace(path=Path("/tmp/Half_Man_Series_1_-_02.mp4"), media_type="tv", title="iPlayer Recordings", season=1, episode=2),
        selected_candidate=SimpleNamespace(title="The Young Offenders"),
        decision_status="accepted",
        status_label="accepted",
        preview_block_reason=None,
        unresolved_reason=None,
        warning=None,
    )
    other = SimpleNamespace(
        item=SimpleNamespace(path=Path("/tmp/Scot_Squad_Series_2_-_06.mp4"), media_type="tv", title="iPlayer Recordings", season=2, episode=6),
        selected_candidate=None,
        decision_status="unresolved",
        status_label="unresolved",
        preview_block_reason=None,
        unresolved_reason=None,
        warning=None,
    )
    window.controller = SimpleNamespace(items=[source, other])

    assert window._confirm_bulk_action(0, "filename show group", [0, 1]) is False

    assert warnings == ["shown"]
    assert any(event["kind"] == "bulk_apply_blocked" for event in window._diagnostics.events)


def test_bulk_undo_restores_affected_review_rows() -> None:
    _app()
    window = MainWindow()
    item = SimpleNamespace(
        item=SimpleNamespace(path=Path("/tmp/Half_Man_Series_1_-_02.mp4"), media_type="tv", title="Half Man", season=1, episode=2),
        selected_candidate=SimpleNamespace(title="Half Man"),
        selected_candidate_index=0,
        candidates=[SimpleNamespace(title="Half Man")],
        manual_candidate=None,
        decision_status="accepted",
        status_label="accepted",
        preview_block_reason=None,
        unresolved_reason=None,
        warning=None,
        candidate_states=[],
        has_more=False,
        auto_selectable=True,
        skipped=False,
    )
    window.controller = SimpleNamespace(items=[item])
    window._populate_review_table()
    window._store_bulk_undo([0], "filename show group")
    window.controller.items[0].decision_status = "manual"
    window.controller.items[0].selected_candidate = SimpleNamespace(title="Wrong")

    window._undo_last_bulk_action()

    assert window.controller.items[0].decision_status == "accepted"
    assert window.controller.items[0].selected_candidate.title == "Half Man"
    assert window._last_bulk_undo is None


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


def test_encode_progress_uses_normalized_counts_when_raw_counts_stall() -> None:
    _app()
    window = MainWindow()
    first = SimpleNamespace(source=Path("/tmp/first.mkv"))
    second = SimpleNamespace(source=Path("/tmp/second.mkv"))
    window._active_encode_jobs = [first, second]
    window._compression_start = 1.0

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr("mediaflow.main_window.time.monotonic", lambda: 10.0)
        window._encode_progress(
            EncodeProgress(
                current_file="Current file (Primary batch): second.mkv",
                current_file_progress=0.10,
                overall_progress=0.55,
                completed_files=0,
                remaining_files=2,
                bytes_processed=550,
                total_bytes=1000,
                heartbeat_state="active",
            )
        )

    assert "Files: 1 done / 2 total" in window.run_stats_label.text()
    assert "Completed: 1" in window.current_action_label.text()
    assert "Remaining: 1" in window.current_action_label.text()


def test_encode_progress_diagnostics_are_compacted() -> None:
    _app()
    window = MainWindow()
    window._active_encode_jobs = [SimpleNamespace(source=Path("/tmp/movie.mkv"))]
    window._compression_start = 1.0
    progress = EncodeProgress(
        current_file="movie.mkv",
        current_file_progress=0.10,
        overall_progress=0.10,
        completed_files=0,
        remaining_files=1,
        bytes_processed=100,
        total_bytes=1000,
        heartbeat_state="active",
    )

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr("mediaflow.main_window.time.monotonic", lambda: 10.0)
        window._encode_progress(progress)
        window._encode_progress(progress)

    progress_events = [event for event in window._diagnostics.events if event["kind"] == "encode_progress"]
    assert len(progress_events) == 1
    assert window._suppressed_encode_progress_events == 1


def test_encode_progress_cleans_display_name_and_shows_eta_collecting_samples() -> None:
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
    assert "collecting samples" in window.eta_label.text().lower()


def test_compression_tick_throttles_live_counts_detail_text() -> None:
    _app()
    window = MainWindow()
    window._active_encode_jobs = [SimpleNamespace(source=Path("/tmp/movie.mkv"))]
    window._compression_start = 0.0
    progress = EncodeProgress(
        current_file="movie.mkv",
        current_file_progress=0.10,
        overall_progress=0.10,
        completed_files=0,
        remaining_files=1,
        bytes_processed=100,
        total_bytes=1000,
        heartbeat_state="active",
    )

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr("mediaflow.main_window.time.monotonic", lambda: 10.0)
        window._encode_progress(progress)
        mp.setattr("mediaflow.main_window.time.monotonic", lambda: 13.0)
        window._tick_compression()
        first_detail = window.encode_counts_label.text()
        mp.setattr("mediaflow.main_window.time.monotonic", lambda: 14.0)
        window._tick_compression()
        throttled_detail = window.encode_counts_label.text()
        mp.setattr("mediaflow.main_window.time.monotonic", lambda: 34.0)
        window._tick_compression()

    assert "Elapsed 13s" in first_detail
    assert throttled_detail == first_detail
    assert "Elapsed 34s" in window.encode_counts_label.text()


def test_start_compression_records_session_before_worker(monkeypatch, tmp_path: Path) -> None:
    _app()
    window = MainWindow()
    item_source = tmp_path / "movie.mkv"
    item_source.write_bytes(b"x")
    job = SimpleNamespace(source=item_source, preset="faster", crf=22)
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
        jobs=[job],
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
    monkeypatch.setattr(window, "_confirm_compression_start", lambda **_kwargs: True)
    monkeypatch.setattr(window, "_start_worker", lambda worker, *_args: captured.update({"worker": worker}))

    window._compression_prepared(prep)
    window._start_compression()

    worker = captured["worker"]
    assert window._compression_session_path == tmp_path / ".mediashrink-session.json"
    assert worker.kwargs["session_path"] == window._compression_session_path
    assert worker.kwargs["cancel_callback"]() is False
    assert window._diagnostics.events[-1]["kind"] == "compression_started"
    assert window._diagnostics.events[-1]["session_path"] == str(window._compression_session_path)


def test_stop_compression_requests_graceful_cancel(monkeypatch, tmp_path: Path) -> None:
    _app()
    window = MainWindow()
    window._compression_session_path = tmp_path / ".mediashrink-session.json"
    window._set_state(WorkflowState.COMPRESSING)
    monkeypatch.setattr("mediaflow.main_window.QMessageBox.question", lambda *_args, **_kwargs: QMessageBox.Yes)

    window._request_compression_stop()

    assert window._compression_stop_requested is True
    assert window._compression_cancel_requested() is True
    assert "Stopping after current FFmpeg process" in window.current_action_label.text()
    assert window._diagnostics.events[-1]["kind"] == "compression_stop_requested"


def test_compression_complete_records_session_and_file_metrics(tmp_path: Path) -> None:
    _app()
    window = MainWindow()
    source = tmp_path / "movie.mkv"
    source.write_bytes(b"x")
    job = SimpleNamespace(source=source, output=source, estimated_output_bytes=40)
    result = SimpleNamespace(
        job=job,
        skipped=False,
        success=True,
        input_size_bytes=100,
        output_size_bytes=50,
        duration_seconds=5.0,
        error_message=None,
    )
    from mediashrink.gui_api import EncodeRunResults

    results = EncodeRunResults(
        [result],
        session_path=tmp_path / ".mediashrink-session.json",
        resumed_from_session=True,
        session_status={"success": 1},
    )
    window.encode_preparation = EncodePreparation(
        directory=tmp_path,
        ffmpeg=tmp_path / "ffmpeg",
        ffprobe=tmp_path / "ffprobe",
        items=[],
        duplicate_warnings=[],
        profile=SimpleNamespace(name="Fast", encoder_key="faster", crf=22),
        jobs=[job],
        recommended_count=1,
        maybe_count=0,
        skip_count=0,
        selected_count=1,
        total_input_bytes=100,
        selected_input_bytes=100,
        selected_estimated_output_bytes=40,
        estimated_total_seconds=5.0,
        on_file_failure="retry",
        use_calibration=True,
    )

    window._compression_complete(results)

    assert window._compression_resumed_from_session is True
    assert window._compression_session_status == {"success": 1}
    assert window._encode_progress_model.completed_files == 1
    assert window._encode_progress_model.remaining_files == 0
    assert window._encode_progress_model.overall_progress == 1.0
    assert window._encode_file_metrics["movie.mkv"]["average_mbps"] > 0
    assert window._overwrite_audit_manifest_path is not None
    assert window._overwrite_audit_manifest_path.exists()
    assert "Compression session:" in window.summary_overview_label.text()
    assert "Overwrite audit manifest:" in window.summary_log.toPlainText()
    assert "time:" in window.summary_log.toPlainText()


def test_interrupted_compression_uses_attention_summary(tmp_path: Path) -> None:
    _app()
    window = MainWindow()
    source = tmp_path / "movie.mkv"
    source.write_bytes(b"x")
    job = SimpleNamespace(source=source, output=source, estimated_output_bytes=40)
    result = SimpleNamespace(
        job=job,
        skipped=False,
        success=False,
        input_size_bytes=100,
        output_size_bytes=0,
        duration_seconds=2.0,
        error_message="Stopped by user",
    )
    from mediashrink.gui_api import EncodeRunResults

    results = EncodeRunResults(
        [result],
        session_path=tmp_path / ".mediashrink-session.json",
        session_status={"pending": 1},
        stopped_early=True,
        interrupted=True,
    )
    window.encode_preparation = EncodePreparation(
        directory=tmp_path,
        ffmpeg=tmp_path / "ffmpeg",
        ffprobe=tmp_path / "ffprobe",
        items=[],
        duplicate_warnings=[],
        profile=SimpleNamespace(name="Fast", encoder_key="faster", crf=22),
        jobs=[job],
        recommended_count=1,
        maybe_count=0,
        skip_count=0,
        selected_count=1,
        total_input_bytes=100,
        selected_input_bytes=100,
        selected_estimated_output_bytes=40,
        estimated_total_seconds=5.0,
        on_file_failure="retry",
        use_calibration=True,
    )

    window._compression_complete(results)

    assert window.summary_headline_label.text() == "Compression interrupted"
    assert "remaining jobs can be resumed" in window.current_action_label.text().lower()
    assert "Completed files remain replaced" in window.summary_log.toPlainText()
    assert any(event["kind"] == "compression_interrupted" for event in window._diagnostics.events)


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


def test_review_details_show_lookup_diagnostics() -> None:
    _app()
    window = MainWindow()
    item = SimpleNamespace(
        item=SimpleNamespace(path=Path("/tmp/movie.mp4"), media_type="movie", title="Movie", year=None, season=None, episode=None),
        lookup_title="Custom Movie",
        search_query="custom movie",
        manual_candidate=None,
        selected_candidate_index=None,
        candidates=[],
        decision_status="unresolved",
        preview_block_reason="No candidates available.",
        unresolved_reason="No candidates available.",
        warning=None,
        cache_context="search result",
        auto_selectable=False,
        preview_valid=False,
        status_label="unresolved",
        has_more=False,
        candidate_states=[],
        skipped=False,
        provider="Wikidata",
        lookup_status="network_error",
        lookup_reason="Wikidata lookups are unavailable (network error).",
        attempted_queries=["custom movie", "movie"],
        raw_result_count=0,
        candidate_count=0,
        filtered_count=0,
        search_time=0.25,
        fetch_time=None,
        total_time=0.25,
    )
    window.controller = SimpleNamespace(items=[item])

    window._populate_review_table()

    details = window.details_log.toPlainText()
    assert "Provider: Wikidata" in details
    assert "Lookup status: network_error" in details
    assert "Attempted queries: custom movie | movie" in details
    assert window.manual_button.text() == "Manual Match Recommended"


def test_search_again_runs_with_review_search_state(monkeypatch) -> None:
    _app()
    window = MainWindow()
    item = SimpleNamespace(
        item=SimpleNamespace(path=Path("/tmp/movie.mp4"), media_type="movie", title="Movie", year=None, season=None, episode=None),
        lookup_title=None,
        search_query="movie",
        manual_candidate=None,
        selected_candidate_index=None,
        candidates=[],
        decision_status="unresolved",
        preview_block_reason="No candidates available.",
        unresolved_reason="No candidates available.",
        warning=None,
        cache_context="search result",
        auto_selectable=False,
        preview_valid=False,
        status_label="unresolved",
        has_more=False,
        candidate_states=[],
        skipped=False,
        provider="Wikidata",
        lookup_status="no_results",
        lookup_reason=None,
        attempted_queries=[],
        raw_result_count=0,
        candidate_count=0,
        filtered_count=0,
    )

    def refine(index, query):
        item.lookup_title = query
        item.search_query = query.lower()
        item.lookup_status = "ok"
        item.candidate_count = 1

    window.controller = SimpleNamespace(items=[item], refine_search=refine, build_preview=lambda: None)
    window._populate_review_table()
    window.search_input.setText("Better Movie")
    monkeypatch.setattr(window, "_refresh_review", lambda: None)
    monkeypatch.setattr(window.thread_pool, "start", lambda worker: worker.run())

    window._search_current_item()

    assert item.lookup_title == "Better Movie"
    assert window._searching_review_index is None
    assert window._diagnostics.events[-1]["kind"] in {"status", "review_search_finished"}


def test_review_diagnostics_snapshot_includes_lookup_fields() -> None:
    _app()
    window = MainWindow()
    item = SimpleNamespace(
        item=SimpleNamespace(path=Path("/tmp/movie.mp4"), media_type="movie", title="Movie", year=2020, season=None, episode=None),
        lookup_title="Movie Search",
        search_query="movie search",
        selected_candidate=None,
        manual_candidate=None,
        decision_status="unresolved",
        status_label="unresolved",
        preview_block_reason="No candidates available.",
        unresolved_reason="No candidates available.",
        warning=None,
        provider="Wikidata",
        lookup_status="no_results",
        lookup_reason=None,
        attempted_queries=["movie search"],
        raw_result_count=0,
        candidate_count=0,
        filtered_count=0,
        cache_context="search result",
        auto_selectable=False,
        candidate_states=[],
    )
    window.controller = SimpleNamespace(items=[item])

    snapshot = window._review_diagnostics_snapshot()

    assert snapshot[0]["lookup_title"] == "Movie Search"
    assert snapshot[0]["provider"] == "Wikidata"
    assert snapshot[0]["attempted_queries"] == ["movie search"]


def test_create_diagnostics_bundle_writes_light_archive(monkeypatch, tmp_path: Path) -> None:
    _app()
    window = MainWindow()
    monkeypatch.setattr(window, "_diagnostics_directory_path", lambda: tmp_path)
    monkeypatch.setattr(window, "_flush_runtime_diagnostics", lambda: None)
    monkeypatch.setattr("mediaflow.main_window.QMessageBox.question", lambda *_args, **_kwargs: QMessageBox.No)
    session_path = tmp_path / ".mediashrink-session.json"
    session_path.write_text('{"entries": []}', encoding="utf-8")
    window._compression_session_path = session_path
    batch_manifest = window._last_batch_manifest_path()
    batch_manifest.write_text('{"paths": []}', encoding="utf-8")

    window._create_diagnostics_bundle()

    bundles = list(tmp_path.glob("mediaflow-diagnostics-bundle-*.zip"))
    assert len(bundles) == 1
    with zipfile.ZipFile(bundles[0]) as archive:
        names = archive.namelist()
    assert any(name.startswith("mediaflow-review-snapshot-") for name in names)
    assert ".mediashrink-session.json" in names
    assert "mediaflow-last-organised-batch.json" in names
    assert "mediaflow-dependency-capabilities.json" in names
    assert window._diagnostics.events[-1]["redacted"] is False


def test_bypass_blocked_organisation_prepares_compression_from_source_when_linked(monkeypatch, tmp_path: Path) -> None:
    _app()
    window = MainWindow()
    source = tmp_path / "source"
    library = tmp_path / "library"
    source.mkdir()
    library.mkdir()
    window.source_input.setText(str(source))
    window.library_input.setText(str(library))
    window.compression_root_input.setText(str(library))
    window.link_compression_root.setChecked(True)
    window.compress_enabled.setChecked(True)
    window._guided_mode = True
    window.preview_state = SimpleNamespace(unresolved_count=2)
    window._set_state(WorkflowState.REVIEW_BLOCKED)
    started: list[str] = []
    monkeypatch.setattr("mediaflow.main_window.QMessageBox.question", lambda *_args, **_kwargs: QMessageBox.Yes)
    monkeypatch.setattr(window, "_start_compression_preparation", started.append)

    window._bypass_blocked_organisation()

    assert window.compression_root_input.text() == str(source)
    assert started
    assert window._diagnostics.events[-1]["kind"] == "organisation_bypassed_for_compression"


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

    assert "Copying organisation (current item 1 of 3" in window.current_action_label.text()
    assert "completed 0 of 3" in window.current_action_label.text()
    assert "Destination: /tmp/dest.mp4" in window.current_action_label.text()
    assert "source.mp4" in window.current_action_label.text()
    assert window._diagnostics.events[-1]["kind"] == "organisation_apply_progress"
    assert "Current item: 1 of 3" in window.apply_counts_label.text()


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

    assert "Still working on the last reported file" in window.current_action_label.text()
    assert "No new apply update" in window.current_action_label.text()


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
    assert window.compression_table.item(0, 7).text() == "recommended only"


def test_guided_zero_job_plan_prompts_for_safer_rebuild(monkeypatch, tmp_path: Path) -> None:
    _app()
    window = MainWindow()
    item_source = tmp_path / "movie.mkv"
    item_source.write_bytes(b"x")
    window._compression_scope = "guided-organised-batch"
    window._compression_source_paths = {item_source}
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
        profile=None,
        jobs=[],
        recommended_count=1,
        maybe_count=0,
        skip_count=0,
        selected_count=1,
        total_input_bytes=1000,
        selected_input_bytes=0,
        selected_estimated_output_bytes=0,
        estimated_total_seconds=0.0,
        on_file_failure="retry",
        use_calibration=True,
    )
    prompted: list[str] = []
    monkeypatch.setattr("mediaflow.main_window.QMessageBox.question", lambda *_args, **_kwargs: QMessageBox.Yes)
    monkeypatch.setattr(window, "_prepare_safer_plan", lambda: prompted.append("safer"))

    window._compression_prepared(prep)

    assert prompted == ["safer"]
    assert window.compression_table.item(0, 7).text() == "blocked by profile"
    assert any(event["kind"] == "zero_runnable_safer_rebuild_prompted" for event in window._diagnostics.events)


def test_prepare_safer_plan_preserves_guided_source_scope(monkeypatch, tmp_path: Path) -> None:
    _app()
    window = MainWindow()
    source = tmp_path / "source"
    library = tmp_path / "library"
    source.mkdir()
    library.mkdir()
    item_source = library / "movie.mkv"
    item_source.write_bytes(b"x")
    window.source_input.setText(str(source))
    window.library_input.setText(str(library))
    window.compression_root_input.setText(str(library))
    window.compress_enabled.setChecked(True)
    window.encode_preparation = SimpleNamespace(profile=None)
    window._compression_scope = "guided-organised-batch"
    window._compression_source_paths = {item_source}
    captured: dict[str, object] = {}

    def fake_start_worker(worker, *_args):
        captured["fn"] = worker.fn
        captured["kwargs"] = worker.kwargs

    monkeypatch.setattr(window, "_start_worker", fake_start_worker)

    window._prepare_safer_plan()

    assert captured["fn"].__name__ == "prepare_safer_compression"
    assert captured["kwargs"]["source_paths"] == {item_source}
    assert window._diagnostics.events[-1]["kind"] == "safer_preparation_started"
    assert window._diagnostics.events[-1]["scope"] == "guided-organised-batch"


def test_guided_batch_paths_write_previous_batch_manifest(tmp_path: Path) -> None:
    _app()
    window = MainWindow()
    first = tmp_path / "movie-one.mkv"
    second = tmp_path / "movie-two.mkv"
    first.write_bytes(b"x")
    second.write_bytes(b"x")
    window.compression_root_input.setText(str(tmp_path))
    window.library_input.setText(str(tmp_path))

    window._set_guided_batch_paths([first, second])

    manifest = window._last_batch_manifest_path()
    assert manifest.exists()
    payload = json.loads(manifest.read_text(encoding="utf-8"))
    assert payload["compression_root"] == str(tmp_path)
    assert payload["paths"] == [str(first), str(second)]


def test_prepare_previous_batch_uses_saved_manifest(monkeypatch, tmp_path: Path) -> None:
    _app()
    window = MainWindow()
    first = tmp_path / "movie-one.mkv"
    missing = tmp_path / "missing.mkv"
    first.write_bytes(b"x")
    window.compression_root_input.setText(str(tmp_path / "old-root"))
    manifest = window._last_batch_manifest_path()
    manifest.write_text(
        json.dumps(
            {
                "compression_root": str(tmp_path),
                "library": str(tmp_path),
                "paths": [str(first), str(missing)],
            }
        ),
        encoding="utf-8",
    )
    captured: dict[str, object] = {}

    def fake_start(message, *, source_paths=None, scope="whole-root"):
        captured["message"] = message
        captured["source_paths"] = source_paths
        captured["scope"] = scope

    monkeypatch.setattr(window, "_start_compression_preparation", fake_start)

    window._prepare_previous_batch_compression()

    assert captured["source_paths"] == [first]
    assert captured["scope"] == "guided-organised-batch"
    assert window.compression_root_input.text() == str(tmp_path)


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
    assert window.rebuild_safer_button.isHidden() is False
    assert window.rebuild_safer_button.isEnabled() is True
    assert "predicted to work for 0 files" in window.start_compress_button.toolTip().lower()


def test_risky_compression_plan_blocks_start_until_safer_rebuild(tmp_path: Path) -> None:
    _app()
    window = MainWindow()
    item_source = tmp_path / "movie.mp4"
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
                reason_text="hardware encoder startup failure",
                estimated_output_bytes=400,
                estimated_savings_bytes=600,
            )
        ],
        duplicate_warnings=[],
        profile=SimpleNamespace(name="GPU", encoder_key="amf", crf=22),
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
        compatible_count=1,
        incompatible_count=1,
        grouped_incompatibilities={"hardware encoder startup failure": 1},
        recommendation_reason="Hardware profile has startup risk.",
    )

    window._compression_prepared(prep)

    assert window.start_compress_button.isEnabled() is False
    assert "compatibility risk" in window.start_compress_button.toolTip().lower()
    assert "start blocked" in window.compress_summary_label.text().lower()


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


def test_major_tables_use_interactive_resizable_headers() -> None:
    _app()
    window = MainWindow()

    for table in (window.review_table, window.candidate_table, window.compression_table, window.summary_table):
        header = table.horizontalHeader()
        assert header.sectionResizeMode(0) == QHeaderView.Interactive
        assert header.sectionsMovable() is True


def test_persisted_table_widths_restore(monkeypatch) -> None:
    saved_state = {
        "table_layouts": {
            "version": 1,
            "tables": {
                "review": {
                    "widths": [321, 150, 70, 180, 115, 190, 90, 240],
                    "visual_order": [0, 1, 2, 3, 4, 5, 6, 7],
                }
            },
        }
    }
    monkeypatch.setattr("mediaflow.main_window.load_ui_state", lambda: saved_state)

    _app()
    window = MainWindow()

    assert window.review_table.columnWidth(0) == 321


def test_review_cells_expose_full_value_tooltips() -> None:
    _app()
    window = MainWindow()
    item = SimpleNamespace(
        item=SimpleNamespace(
            path=Path("/tmp/very-long-source-name-that-would-be-truncated.mkv"),
            media_type="movie",
            title="Very Long Movie Title That Needs A Tooltip",
            season=None,
            episode=None,
        ),
        manual_candidate=None,
        selected_candidate=SimpleNamespace(title="Very Long Movie Title That Needs A Tooltip", year=2020),
        selected_candidate_index=0,
        candidates=[],
        decision_status="accepted",
        preview_block_reason=None,
        unresolved_reason=None,
        warning="subtitle missing from candidate",
        cache_context="search result",
        auto_selectable=False,
        status_label="accepted",
        has_more=False,
        candidate_states=[],
        skipped=False,
    )
    window.controller = SimpleNamespace(items=[item])

    window._populate_review_table()

    assert "very-long-source-name" in window.review_table.item(0, 0).toolTip()
    assert "subtitle missing" in window.review_table.item(0, 7).toolTip()


def test_compression_ordering_can_sort_runnable_jobs_by_savings(tmp_path: Path) -> None:
    _app()
    window = MainWindow()
    small = tmp_path / "small.mkv"
    large = tmp_path / "large.mkv"
    small.write_bytes(b"x")
    large.write_bytes(b"x")
    small_job = SimpleNamespace(source=small)
    large_job = SimpleNamespace(source=large)
    window._compression_plan_rows = [
        SimpleNamespace(source=small, selected=True, exists=True, classification="recommended", estimated_output_bytes=100, estimated_savings_bytes=10),
        SimpleNamespace(source=large, selected=True, exists=True, classification="recommended", estimated_output_bytes=100, estimated_savings_bytes=500),
    ]
    prep = SimpleNamespace(jobs=[small_job, large_job])

    window._set_combo_value(window.compression_order_combo, "Largest savings first")

    assert window._runnable_jobs(prep) == [large_job, small_job]


def test_review_duration_stops_when_pipeline_leaves_review(monkeypatch) -> None:
    _app()
    window = MainWindow()
    item = SimpleNamespace(
        item=SimpleNamespace(path=Path("/tmp/movie.mkv"), title="Movie"),
        status_label="accepted",
        decision_status="accepted",
        preview_block_reason=None,
        warning=None,
    )
    window.controller = SimpleNamespace(items=[item])

    monkeypatch.setattr("mediaflow.main_window.time.monotonic", lambda: 10.0)
    window._start_review_item_timer(0)
    monkeypatch.setattr("mediaflow.main_window.time.monotonic", lambda: 25.0)
    window._set_state(WorkflowState.PREPARING_COMPRESSION)
    snapshot = window._review_duration_snapshot()

    assert snapshot[0]["duration_seconds"] == 15.0


def test_quarantine_restore_moves_current_output_aside_and_restores_original(monkeypatch, tmp_path: Path) -> None:
    _app()
    window = MainWindow()
    output = tmp_path / "movie.mkv"
    quarantine = tmp_path / "quarantine" / "movie.original.mkv"
    quarantine.parent.mkdir()
    output.write_bytes(b"encoded")
    quarantine.write_bytes(b"original")
    audit = tmp_path / "audit.json"
    monkeypatch.setattr("mediaflow.main_window.QMessageBox.question", lambda *_args, **_kwargs: QMessageBox.Yes)
    monkeypatch.setattr("mediaflow.main_window.QMessageBox.information", lambda *_args, **_kwargs: None)

    window._perform_quarantine_restore(
        {
            "output_path": str(output),
            "quarantine_path": str(quarantine),
        },
        audit_path=audit,
    )

    assert output.read_bytes() == b"original"
    recovery_files = list(tmp_path.glob("movie.mkv.mediaflow-replaced-*"))
    assert len(recovery_files) == 1
    assert recovery_files[0].read_bytes() == b"encoded"
    assert any(event["kind"] == "quarantine_restore_completed" for event in window._diagnostics.events)
