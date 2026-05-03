from __future__ import annotations

from collections import Counter
from dataclasses import is_dataclass, replace
from datetime import datetime
import importlib
import re
import sys
import time
from pathlib import Path

from PySide6.QtCore import Qt, QThreadPool, QTimer
from PySide6.QtGui import QCloseEvent, QFont, QKeySequence, QShortcut
from PySide6.QtWidgets import QSystemTrayIcon
from PySide6.QtWidgets import (
    QApplication,
    QCheckBox,
    QComboBox,
    QDialog,
    QDialogButtonBox,
    QDoubleSpinBox,
    QFileDialog,
    QFormLayout,
    QFrame,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMainWindow,
    QMessageBox,
    QPlainTextEdit,
    QProgressBar,
    QPushButton,
    QHeaderView,
    QScrollArea,
    QSplitter,
    QSpinBox,
    QStackedWidget,
    QTableWidget,
    QTableWidgetItem,
    QTabWidget,
    QVBoxLayout,
    QWidget,
)

from mediashrink.gui_api import EncodePreparation, EncodeProgress
from plexify.ui_controller import ApplyResultState, PreviewState, VideoUIController

from . import __version__
from .callback_types import ApplyProgress, PreparationProgress, PreparationStageUpdate
from .compat import check_runtime_compatibility, compatibility_error_text
from .config import PipelineConfig, PlexifySettings, ShrinkSettings, build_pipeline_config
from .diagnostics import DiagnosticsRecorder, diagnostics_dir
from .integrations import (
    build_compression_plan_rows,
    build_encode_result_rows,
    classify_compression_plan,
    collect_retry_sources,
    display_name_for_ui,
    group_failure_rows,
    recommended_headroom_bytes,
    summarise_apply_result,
    translate_result_reason,
)
from .mediashrink_adapter import (
    missing_job_sources,
    prepare_compression,
    prepare_safer_compression,
    prepare_retry_compression,
    run_compression,
)
from .pipeline import build_pipeline_summary
from .plexify_adapter import (
    apply_preview_controller,
    build_preview,
    build_video_controller,
    scan_controller,
)
from .progress import (
    ApplyProgressModel,
    EncodeProgressModel,
    PreparationProgressModel,
    preparation_stage_title,
    preparation_timeline_text,
)
from .settings import get_config_dir, load_ui_state, save_ui_state
from .workers import FunctionWorker
from .workflow import WorkflowState, describe_workflow_state


class MainWindow(QMainWindow):
    def __init__(self, *, default_source: Path | None = None, default_library: Path | None = None) -> None:
        super().__init__()
        self.setWindowTitle("mediaflow")

        self.thread_pool = QThreadPool.globalInstance()
        self.workflow_state = WorkflowState.SETUP
        self.controller: VideoUIController | None = None
        self.preview_state: PreviewState | None = None
        self.apply_result: ApplyResultState | None = None
        self.encode_preparation: EncodePreparation | None = None
        self.encode_results: list = []

        self._active_worker_count = 0
        self._worker_refs: set[FunctionWorker] = set()
        self._loading_state = True
        self._guided_mode = False
        self._continue_to_compress = False
        self._config_dirty = False
        self._shutting_down = False
        self._compatibility_checked = False
        self._compression_root_linked = True
        self._current_action = "Not started"
        self._last_completed_action = "Nothing completed yet"
        self._custom_warnings: list[str] = []
        self._last_diagnostics_path: Path | None = None
        self._last_diagnostics_error: str | None = None
        self._retry_sources: set[Path] = set()
        self._compression_plan_rows: list = []
        self._summary_rows: list = []
        self._plan_classification = classify_compression_plan(())
        self._diagnostics = DiagnosticsRecorder()
        self._encode_progress_model = EncodeProgressModel()
        self._preparation_model = PreparationProgressModel()
        self._apply_progress_model = ApplyProgressModel()

        self._compression_start: float = 0.0
        self._preparation_start: float = 0.0
        self._spinner_idx: int = 0
        self._last_encode_log_key: tuple[int, str] = (-1, "")
        self._last_encode_bucket: int = -1
        self._last_encode_file: str = ""
        self._last_status_text: str = ""
        self._scan_discovered_count: int = 0
        self._scan_last_path: str = ""
        self._syncing_diagnostics: bool = False
        self._startup_duration: float | None = None
        self._preparation_duration: float | None = None
        self._first_progress_delay: float | None = None
        self._restored_state_warnings: list[str] = []
        self._scan_started_at: float | None = None
        self._scan_last_update_at: float | None = None
        self._preparation_last_update_at: float | None = None
        self._apply_started_at: float | None = None
        self._apply_last_update_at: float | None = None
        self._apply_progress: ApplyProgress | None = None
        self._apply_cancel_requested: bool = False
        self._last_apply_log_key: tuple[str, int, int, str] | None = None
        self._last_diagnostics_flush_at: float = 0.0
        self._activity_spinner_idx: int = 0

        self._build_widgets(default_source=default_source, default_library=default_library)
        self._build_ui()
        self._apply_styles()
        self._connect_signals()
        self._restore_ui_state(default_source=default_source, default_library=default_library)
        self._loading_state = False
        self._apply_initial_geometry()
        self._refresh_compression_link_label()
        if self._compression_root_linked:
            target = self.library_input.text() if self.organise_enabled.isChecked() else self.source_input.text()
            self.compression_root_input.setText(target)
        self._set_state(WorkflowState.SETUP)
        self._set_diagnostics_provenance()
        self._flush_runtime_diagnostics()

        self._tray: QSystemTrayIcon | None = None

    def _build_widgets(self, *, default_source: Path | None, default_library: Path | None) -> None:
        self.step_label = QLabel()
        self.headline_label = QLabel()
        self.headline_label.setWordWrap(True)
        self.guidance_label = QLabel()
        self.guidance_label.setWordWrap(True)
        self.warning_label = QLabel()
        self.warning_label.setWordWrap(True)
        self.activity_indicator_label = QLabel("Idle")
        self.step_checklist_label = QLabel()
        self.step_checklist_label.setWordWrap(True)
        self.activity_label = QLabel()
        self.activity_label.setWordWrap(True)
        self.activity_label.setObjectName("muted-label")
        self.active_diagnostics_label = QLabel()
        self.active_diagnostics_label.setWordWrap(True)
        self.active_diagnostics_label.setObjectName("muted-label")
        self.active_open_diagnostics_button = QPushButton("Open diagnostics folder")
        self.active_copy_diagnostics_button = QPushButton("Copy diagnostics path")

        self.tabs = QTabWidget()

        self.source_input = QLineEdit(str(default_source) if default_source else "")
        self.library_input = QLineEdit(str(default_library) if default_library else "")
        self.library_label = QLabel("Library / Output Folder")
        self.library_browse = QPushButton("Browse")
        self.library_help = QLabel("Organised output is written here by the organise stage.")
        self.library_help.setObjectName("helper-label")
        self.source_help_label = QLabel("Incoming folder scanned by plexify for new media.")
        self.source_help_label.setObjectName("helper-label")
        self.compression_root_input = QLineEdit(str(default_library) if default_library else "")
        self.link_compression_root = QCheckBox("Use the library / output folder as the compression root")
        self.link_compression_root.setChecked(True)

        self.organise_enabled = QCheckBox("Enable organise stage")
        self.organise_enabled.setChecked(True)
        self.compress_enabled = QCheckBox("Enable compress stage")
        self.compress_enabled.setChecked(True)

        self.apply_mode = QCheckBox("Apply organisation to disk")
        self.apply_mode.setChecked(True)
        self.copy_mode = QCheckBox("Copy files instead of move")
        self.copy_mode.setChecked(True)
        self.use_cache = QCheckBox("Use plexify cache")
        self.use_cache.setChecked(True)
        self.offline = QCheckBox("Offline lookup")
        self.min_confidence = QDoubleSpinBox()
        self.min_confidence.setRange(0.0, 1.0)
        self.min_confidence.setSingleStep(0.05)
        self.min_confidence.setValue(0.90)
        self.extensions_input = QLineEdit(".mkv,.mp4,.avi,.m4v,.mov,.ts")
        self.conflict_mode = QComboBox()
        self.conflict_mode.addItems(["rename", "skip", "overwrite"])

        self.overwrite = QCheckBox("Overwrite originals after successful encode")
        self.overwrite.setChecked(True)
        self.recursive = QCheckBox("Scan compression root recursively")
        self.recursive.setChecked(True)
        self.no_skip = QCheckBox("Encode files even if already HEVC")
        self.policy = QComboBox()
        self.policy.addItems(
            ["fastest-wall-clock", "lowest-cpu", "best-compression", "highest-confidence"]
        )
        self.on_file_failure = QComboBox()
        self.on_file_failure.addItems(["retry", "skip", "stop"])
        self.use_calibration = QCheckBox("Use mediashrink calibration data")
        self.use_calibration.setChecked(True)
        self.duplicate_policy = QComboBox()
        self.duplicate_policy.addItems(["prefer-mkv", "all", "skip-title"])

        self.show_organise_advanced_button = QPushButton("Show organise options")
        self.show_organise_advanced_button.setCheckable(True)
        self.show_compress_advanced_button = QPushButton("Show compression options")
        self.show_compress_advanced_button.setCheckable(True)
        self.organise_advanced_panel = self._build_organise_settings_page()
        self.organise_advanced_panel.setVisible(False)
        self.compress_advanced_panel = self._build_compress_settings_page()
        self.compress_advanced_panel.setVisible(False)

        self.setup_summary_label = QLabel()
        self.setup_summary_label.setWordWrap(True)
        self.setup_hint_label = QLabel()
        self.setup_hint_label.setWordWrap(True)
        self.overwrite_warning_label = QLabel()
        self.overwrite_warning_label.setWordWrap(True)
        self.next_action_label = QLabel()
        self.next_action_label.setWordWrap(True)

        self.guided_button = QPushButton("Start Guided Pipeline")
        self.scan_button = QPushButton("Review Organise Matches")
        self.prepare_compress_button = QPushButton("Prepare Compression Plan")
        self.reset_button = QPushButton("Reset Runtime State")

        self.review_summary_label = QLabel()
        self.review_summary_label.setWordWrap(True)
        self.review_hint_label = QLabel()
        self.review_hint_label.setWordWrap(True)
        self.review_placeholder_label = QLabel()
        self.review_placeholder_label.setWordWrap(True)
        self.review_filter_combo = QComboBox()
        self.review_filter_combo.addItems(
            [
                "All items",
                "Blocked only",
                "Unresolved only",
                "Accepted/manual only",
                "TV only",
            ]
        )
        self.review_filter_status_label = QLabel("")
        self.review_filter_status_label.setWordWrap(True)
        self.review_filter_status_label.setObjectName("muted-label")
        self.review_blocked_label = QLabel("")
        self.review_blocked_label.setWordWrap(True)
        self.review_blocked_label.setObjectName("muted-label")
        self.review_stack = QStackedWidget()
        self.apply_dashboard_label = QLabel("Organisation apply has not started.")
        self.apply_dashboard_label.setWordWrap(True)
        self.apply_counts_label = QLabel("")
        self.apply_counts_label.setWordWrap(True)
        self.apply_current_label = QLabel("")
        self.apply_current_label.setWordWrap(True)
        self.apply_destination_label = QLabel("")
        self.apply_destination_label.setWordWrap(True)
        self.apply_elapsed_label = QLabel("")
        self.apply_elapsed_label.setWordWrap(True)
        self.apply_progress_bar = QProgressBar()
        self.apply_progress_bar.setRange(0, 100)
        self.apply_log = QPlainTextEdit()
        self.apply_log.setReadOnly(True)
        self.apply_log.document().setMaximumBlockCount(300)
        self.cancel_apply_button = QPushButton("Cancel after current file")
        self.review_table = QTableWidget(0, 7)
        self.review_table.setHorizontalHeaderLabels(
            ["Source", "Type", "Title", "Season/Episode", "Selected", "Status", "Warning"]
        )
        self.review_table.setSelectionBehavior(QTableWidget.SelectRows)
        self.review_table.setSelectionMode(QTableWidget.SingleSelection)
        self.review_table.setEditTriggers(QTableWidget.NoEditTriggers)
        self.review_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.candidate_table = QTableWidget(0, 4)
        self.candidate_table.setHorizontalHeaderLabels(["Title", "Year", "Source", "Confidence"])
        self.candidate_table.setSelectionBehavior(QTableWidget.SelectRows)
        self.candidate_table.setSelectionMode(QTableWidget.SingleSelection)
        self.candidate_table.setEditTriggers(QTableWidget.NoEditTriggers)
        self.candidate_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.details_log = QPlainTextEdit()
        self.details_log.setReadOnly(True)
        self.preview_log = QPlainTextEdit()
        self.preview_log.setReadOnly(True)
        self.search_input = QLineEdit()
        self.search_input.setPlaceholderText("Search query or manual title")

        self.prev_item_button = QPushButton("Previous")
        self.next_item_button = QPushButton("Next")
        self.next_blocked_button = QPushButton("Next Blocked")
        self.accept_button = QPushButton("Accept")
        self.skip_button = QPushButton("Skip")
        self.search_button = QPushButton("Search Again")
        self.manual_button = QPushButton("Manual Match")
        self.next_page_button = QPushButton("More Candidates")
        self.auto_accept_button = QPushButton("Auto-Accept Safe Matches")
        self.switch_button = QPushButton("Switch TV/Movie")
        self.folder_button = QPushButton("Apply To Folder")
        self.title_group_button = QPushButton("Apply To Title Group")
        self.preview_button = QPushButton("Build Preview")
        self.apply_button = QPushButton("Apply Organisation")

        self.compress_summary_label = QLabel()
        self.compress_summary_label.setWordWrap(True)
        self.compress_hint_label = QLabel()
        self.compress_hint_label.setWordWrap(True)
        self.compress_stack = QStackedWidget()
        self.compress_empty_label = QLabel()
        self.compress_empty_label.setWordWrap(True)
        self.compress_preparing_label = QLabel("Preparing a compression plan...")
        self.compress_preparing_label.setWordWrap(True)
        self.prepare_counts_label = QLabel("0 file(s) discovered • 0.0 B")
        self.prepare_counts_label.setObjectName("muted-label")
        self.prepare_timeline_label = QLabel("")
        self.prepare_timeline_label.setWordWrap(True)
        self.prepare_timeline_label.setObjectName("muted-label")
        self.prepare_progress = QProgressBar()
        self.file_progress = QProgressBar()
        self.overall_progress = QProgressBar()
        self.start_compress_button = QPushButton("Start Compression")
        self.include_risky_jobs = QCheckBox("Include risky follow-up jobs in the first run")
        self.include_risky_jobs.setChecked(False)
        self.rebuild_safer_button = QPushButton("Rebuild Safer Plan")
        self.rebuild_safer_button.setVisible(False)
        self.prepare_followup_button = QPushButton("Prepare Follow-up Plan")
        self.prepare_followup_button.setVisible(False)
        self.retry_failed_button = QPushButton("Prepare Retry Plan")
        self.retry_failed_button.setVisible(False)
        self.retry_summary_button = QPushButton("Prepare Retry Plan")
        self.retry_summary_button.setVisible(False)
        self.current_action_label = QLabel()
        self.current_action_label.setWordWrap(True)
        self.last_completed_label = QLabel()
        self.last_completed_label.setWordWrap(True)
        self.runtime_warnings_label = QLabel()
        self.runtime_warnings_label.setWordWrap(True)
        self.toggle_details_button = QPushButton("Show Details")
        self.toggle_details_button.setCheckable(True)
        self.compression_filter_combo = QComboBox()
        self.compression_filter_combo.addItems(
            [
                "All plan items",
                "Runnable now",
                "Follow-up / incompatible",
                "Informational skips",
                "Selected only",
                "Recommended only",
                "Problem items",
                "Missing items",
            ]
        )
        self.compression_filter_status_label = QLabel("")
        self.compression_filter_status_label.setWordWrap(True)
        self.compression_filter_status_label.setObjectName("muted-label")
        self.compression_table = QTableWidget(0, 8)
        self.compression_table.setHorizontalHeaderLabels(
            ["File", "Codec", "Recommendation", "Reason", "Issue", "Est. Output", "Est. Saving", "Selected"]
        )
        self.compression_table.setSelectionBehavior(QTableWidget.SelectRows)
        self.compression_table.setSelectionMode(QTableWidget.NoSelection)
        self.compression_table.setEditTriggers(QTableWidget.NoEditTriggers)
        self.compression_table.setSortingEnabled(True)
        self.compression_table.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive)
        self.compression_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.Stretch)   # File
        self.compression_table.horizontalHeader().setSectionResizeMode(3, QHeaderView.Stretch)   # Reason
        self.compression_table.horizontalHeader().setSectionResizeMode(4, QHeaderView.Stretch)   # Issue
        self.compression_table.horizontalHeader().setStretchLastSection(False)
        self.compress_status_log = QPlainTextEdit()
        self.compress_status_log.setReadOnly(True)
        self.compress_status_log.document().setMaximumBlockCount(300)
        self.compress_status_log.setVisible(False)

        self.elapsed_label = QLabel("Elapsed: —")
        self.eta_label = QLabel("ETA: —")
        self.run_stats_label = QLabel("Files: —")

        # Encode card widgets
        self.spinner_label = QLabel("⠋")
        self.spinner_label.setObjectName("encode-spinner")
        self.encode_filename_label = QLabel("")
        self.encode_filename_label.setObjectName("encode-filename")
        self.encode_filename_label.setWordWrap(True)
        self.encode_speed_label = QLabel("")
        self.encode_phase_label = QLabel("")
        self.encode_phase_label.setObjectName("muted-label")
        self.encode_counts_label = QLabel("")
        self.encode_counts_label.setObjectName("muted-label")
        self.encode_projection_label = QLabel("")
        self.encode_projection_label.setWordWrap(True)
        self.encode_projection_label.setObjectName("muted-label")
        self.encode_visual_bar = QProgressBar()
        self.encode_visual_bar.setObjectName("encode-visual")
        self.encode_visual_bar.setRange(0, 100)
        self.encode_visual_bar.setTextVisible(True)
        self.encode_visual_bar.setFormat("%p%")
        self.encode_projection_bar = QProgressBar()
        self.encode_projection_bar.setObjectName("savings-bar")
        self.encode_projection_bar.setRange(0, 100)
        self.encode_projection_bar.setTextVisible(True)
        self.toggle_encode_card_button = QPushButton("Hide live view")
        self.toggle_encode_card_button.setCheckable(True)

        self._compression_timer = QTimer(self)
        self._compression_timer.setInterval(250)
        self._compression_timer.timeout.connect(self._tick_compression)

        self.prepare_elapsed_label = QLabel("")
        self.prepare_stage_label = QLabel("Analysing files...")
        self.prepare_stage_label.setObjectName("muted-label")
        self.prepare_log = QPlainTextEdit()
        self.prepare_log.setReadOnly(True)
        self.prepare_log.document().setMaximumBlockCount(200)
        self._preparation_timer = QTimer(self)
        self._preparation_timer.setInterval(1000)
        self._preparation_timer.timeout.connect(self._tick_preparation)
        self._apply_timer = QTimer(self)
        self._apply_timer.setInterval(1000)
        self._apply_timer.timeout.connect(self._tick_apply)
        self._activity_timer = QTimer(self)
        self._activity_timer.setInterval(250)
        self._activity_timer.timeout.connect(self._tick_activity_indicator)
        self._scan_timer = QTimer(self)
        self._scan_timer.setInterval(1000)
        self._scan_timer.timeout.connect(self._tick_scan)

        self.stat_files_label = QLabel("—")
        self.stat_files_label.setObjectName("stat-tile")
        self.stat_files_label.setAlignment(Qt.AlignCenter)
        self.stat_saved_label = QLabel("—")
        self.stat_saved_label.setObjectName("stat-tile")
        self.stat_saved_label.setAlignment(Qt.AlignCenter)
        self.stat_pct_label = QLabel("—")
        self.stat_pct_label.setObjectName("stat-tile")
        self.stat_pct_label.setAlignment(Qt.AlignCenter)
        self.summary_overview_label = QLabel()
        self.summary_overview_label.setWordWrap(True)
        self.summary_headline_label = QLabel()
        self.summary_headline_label.setWordWrap(True)
        self.summary_headline_label.setObjectName("headline-label")
        self.summary_mode_label = QLabel()
        self.summary_mode_label.setWordWrap(True)
        self.summary_mode_label.setObjectName("muted-label")
        self.summary_failure_label = QLabel()
        self.summary_failure_label.setWordWrap(True)
        self.summary_failure_label.setObjectName("muted-label")
        self.summary_timeline_label = QLabel()
        self.summary_timeline_label.setWordWrap(True)
        self.summary_timeline_label.setObjectName("muted-label")
        self.summary_filter_combo = QComboBox()
        self.summary_filter_combo.addItems(["All results", "Encoded only", "Failed only", "Skipped only", "Retry-ready"])
        self.summary_filter_status_label = QLabel("")
        self.summary_filter_status_label.setWordWrap(True)
        self.summary_filter_status_label.setObjectName("muted-label")
        self.summary_table = QTableWidget(0, 7)
        self.summary_table.setHorizontalHeaderLabels(
            ["File", "Status", "Original", "Final", "Saved", "Reason", "Location"]
        )
        self.summary_table.horizontalHeader().setStretchLastSection(True)
        self.summary_table.setEditTriggers(QTableWidget.NoEditTriggers)
        self.summary_table.setSelectionBehavior(QTableWidget.SelectRows)
        self.summary_table.setSortingEnabled(True)
        self.summary_table.verticalHeader().setVisible(False)
        self.summary_table.setVisible(False)
        self.savings_bar = QProgressBar()
        self.savings_bar.setObjectName("savings-bar")
        self.savings_bar.setRange(0, 100)
        self.savings_bar.setTextVisible(True)
        self.savings_bar.setVisible(False)
        self.summary_log = QPlainTextEdit()
        self.summary_log.setReadOnly(True)
        self.open_output_button = QPushButton("Open output folder")
        self.open_output_button.setVisible(False)
        self.open_diagnostics_button = QPushButton("Open diagnostics folder")
        self.open_diagnostics_button.setVisible(False)
        self.copy_diagnostics_button = QPushButton("Copy diagnostics path")
        self.copy_diagnostics_button.setVisible(False)
        self.save_summary_button = QPushButton("Save run summary...")
        self.save_summary_button.setVisible(False)
        self.diagnostics_path_label = QLabel()
        self.diagnostics_path_label.setWordWrap(True)
        self.diagnostics_path_label.setObjectName("muted-label")

    def _build_ui(self) -> None:
        root = QWidget()
        self.setCentralWidget(root)
        layout = QVBoxLayout(root)
        layout.setContentsMargins(16, 16, 16, 16)
        layout.setSpacing(8)

        banner = QGroupBox("Current Step")
        banner_layout = QVBoxLayout(banner)
        banner_layout.setSpacing(4)
        banner_layout.addWidget(self.step_label)
        banner_layout.addWidget(self.headline_label)
        banner_layout.addWidget(self.guidance_label)
        banner_layout.addWidget(self.warning_label)
        banner_layout.addWidget(self.step_checklist_label)
        banner_activity_row = QHBoxLayout()
        banner_activity_row.addWidget(self.activity_indicator_label)
        banner_activity_row.addWidget(self.activity_label, stretch=1)
        banner_layout.addLayout(banner_activity_row)
        diagnostics_row = QHBoxLayout()
        diagnostics_row.addWidget(self.active_diagnostics_label, stretch=1)
        diagnostics_row.addWidget(self.active_open_diagnostics_button)
        diagnostics_row.addWidget(self.active_copy_diagnostics_button)
        banner_layout.addLayout(diagnostics_row)
        layout.addWidget(banner)

        self.tabs.addTab(self._build_setup_tab(), "Setup")
        self.tabs.addTab(self._build_review_tab(), "Review")
        self.tabs.addTab(self._build_compress_tab(), "Compress")
        self.tabs.addTab(self._build_summary_tab(), "Summary")
        layout.addWidget(self.tabs, stretch=1)

    def _apply_styles(self) -> None:
        base_font = QFont(self.font())
        if base_font.pointSizeF() <= 0:
            base_font.setPointSize(10)
        self.setFont(base_font)
        self.setStyleSheet(
            """
            QMainWindow, QWidget { background: #1e1f22; color: #f2f2f2; }
            QGroupBox {
                border: 1px solid #555;
                border-radius: 8px;
                margin-top: 10px;
                padding: 12px;
                font-weight: 600;
            }
            QGroupBox::title {
                subcontrol-origin: margin;
                left: 10px;
                padding: 0 4px;
                color: #f5f5f5;
            }
            QLabel#helper-label {
                color: #b7bcc4;
                font-size: 12px;
            }
            QLabel#step-label {
                font-size: 18px;
                font-weight: 700;
                color: #ffffff;
            }
            QLabel#headline-label {
                font-size: 15px;
                font-weight: 600;
                color: #f5f5f5;
            }
            QLabel#muted-label {
                color: #c5cad1;
            }
            QLabel#activity-indicator {
                color: #4f9cf5;
                font-weight: 700;
                min-width: 58px;
            }
            QLabel#warning-label {
                color: #ffcf99;
            }
            QLineEdit, QComboBox, QPlainTextEdit, QTableWidget, QDoubleSpinBox {
                background: #2a2d31;
                border: 1px solid #4c5158;
                border-radius: 6px;
                padding: 6px;
            }
            QPushButton {
                background: #343840;
                border: 1px solid #555c66;
                border-radius: 6px;
                padding: 8px 12px;
            }
            QPushButton:hover { background: #3c424c; }
            QPushButton:disabled { color: #838993; background: #2a2d31; }
            QPushButton#primary-button {
                background: #3267c8;
                border-color: #4f86ea;
                font-weight: 700;
            }
            QPushButton#primary-button:hover { background: #3d73d5; }
            QCheckBox, QRadioButton { spacing: 8px; }
            QTabBar::tab {
                background: #2a2d31;
                padding: 8px 14px;
                border-top-left-radius: 6px;
                border-top-right-radius: 6px;
                margin-right: 4px;
            }
            QTabBar::tab:selected { background: #353940; }
            QLabel#encode-spinner {
                font-size: 26px;
                color: #4f9cf5;
                min-width: 36px;
            }
            QLabel#encode-filename {
                font-size: 14px;
                font-weight: 600;
                color: #f5f5f5;
            }
            QProgressBar#encode-visual {
                min-height: 26px;
                border-radius: 6px;
                background: #2a2d31;
                border: 1px solid #4c5158;
                color: #ffffff;
                font-weight: 700;
            }
            QProgressBar#encode-visual::chunk {
                background: qlineargradient(x1:0, y1:0, x2:1, y2:0,
                    stop:0 #1e5fa8, stop:1 #4f9cf5);
                border-radius: 5px;
            }
            QProgressBar#savings-bar {
                min-height: 22px;
                border-radius: 6px;
                background: #2a2d31;
                border: 1px solid #4c5158;
                color: #ffffff;
                font-weight: 600;
            }
            QProgressBar#savings-bar::chunk {
                background: qlineargradient(x1:0, y1:0, x2:1, y2:0,
                    stop:0 #1e7a4a, stop:1 #3cbf77);
                border-radius: 5px;
            }
            QLabel#stat-tile {
                font-size: 22px;
                font-weight: 700;
                color: #f5f5f5;
                background: #2a2d31;
                border: 1px solid #4c5158;
                border-radius: 8px;
                padding: 14px 24px;
                min-width: 120px;
            }
            """
        )
        self.step_label.setObjectName("step-label")
        self.headline_label.setObjectName("headline-label")
        self.guidance_label.setObjectName("muted-label")
        self.warning_label.setObjectName("warning-label")
        self.activity_indicator_label.setObjectName("activity-indicator")
        self.next_action_label.setObjectName("muted-label")
        self.setup_hint_label.setObjectName("muted-label")
        self.review_hint_label.setObjectName("muted-label")
        self.compress_hint_label.setObjectName("muted-label")
        self.guided_button.setObjectName("primary-button")

    def _build_setup_tab(self) -> QWidget:
        panel = QWidget()
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.NoFrame)

        content = QWidget()
        layout = QVBoxLayout(content)
        layout.setSpacing(12)

        path_group = QGroupBox("Folders")
        path_layout = QGridLayout(path_group)
        path_layout.setVerticalSpacing(8)
        source_browse = QPushButton("Browse")
        source_browse.clicked.connect(lambda: self._browse_into(self.source_input))
        self.library_browse.clicked.connect(lambda: self._browse_into(self.library_input))
        compression_browse = QPushButton("Browse")
        compression_browse.clicked.connect(lambda: self._browse_into(self.compression_root_input))
        compression_help = QLabel("Mediashrink scans and encodes files from this folder.")
        compression_help.setObjectName("helper-label")
        path_layout.addWidget(QLabel("Source"), 0, 0)
        path_layout.addWidget(self.source_input, 0, 1)
        path_layout.addWidget(source_browse, 0, 2)
        path_layout.addWidget(self.source_help_label, 1, 1, 1, 2)
        path_layout.addWidget(self.library_label, 2, 0)
        path_layout.addWidget(self.library_input, 2, 1)
        path_layout.addWidget(self.library_browse, 2, 2)
        path_layout.addWidget(self.library_help, 3, 1, 1, 2)
        path_layout.addWidget(QLabel("Compression Root"), 4, 0)
        path_layout.addWidget(self.compression_root_input, 4, 1)
        path_layout.addWidget(compression_browse, 4, 2)
        path_layout.addWidget(self.link_compression_root, 5, 1, 1, 2)
        path_layout.addWidget(compression_help, 6, 1, 1, 2)
        layout.addWidget(path_group)

        stage_group = QGroupBox("Pipeline Mode")
        stage_layout = QVBoxLayout(stage_group)
        stage_layout.addWidget(self.organise_enabled)
        stage_layout.addWidget(self.compress_enabled)
        stage_layout.addWidget(self.next_action_label)
        layout.addWidget(stage_group)

        self.organise_options_group = QGroupBox("Organise Options")
        organise_layout = QVBoxLayout(self.organise_options_group)
        organise_layout.addWidget(self.show_organise_advanced_button)
        organise_layout.addWidget(self.organise_advanced_panel)
        layout.addWidget(self.organise_options_group)

        compress_group = QGroupBox("Compression Options")
        compress_layout = QVBoxLayout(compress_group)
        compress_layout.addWidget(self.show_compress_advanced_button)
        compress_layout.addWidget(self.compress_advanced_panel)
        layout.addWidget(compress_group)

        action_group = QGroupBox("Actions")
        action_layout = QVBoxLayout(action_group)
        row = QHBoxLayout()
        row.addWidget(self.guided_button)
        row.addWidget(self.scan_button)
        row.addWidget(self.prepare_compress_button)
        row.addWidget(self.reset_button)
        action_layout.addLayout(row)
        action_layout.addWidget(self.setup_summary_label)
        action_layout.addWidget(self.setup_hint_label)
        action_layout.addWidget(self.overwrite_warning_label)
        layout.addWidget(action_group)
        layout.addStretch(1)

        scroll.setWidget(content)
        outer = QVBoxLayout(panel)
        outer.setContentsMargins(0, 0, 0, 0)
        outer.addWidget(scroll)
        return panel

    def _build_organise_settings_page(self) -> QWidget:
        page = QWidget()
        form = QFormLayout(page)
        form.addRow(self.apply_mode)
        form.addRow(self.copy_mode)
        form.addRow(self.use_cache)
        form.addRow(self.offline)
        form.addRow("Minimum confidence", self.min_confidence)
        form.addRow("Extensions", self.extensions_input)
        form.addRow("Conflict handling", self.conflict_mode)
        return page

    def _build_compress_settings_page(self) -> QWidget:
        page = QWidget()
        form = QFormLayout(page)
        form.addRow(self.overwrite)
        form.addRow(self.recursive)
        form.addRow(self.no_skip)
        form.addRow("Recommendation policy", self.policy)
        form.addRow("On file failure", self.on_file_failure)
        form.addRow(self.use_calibration)
        form.addRow("Duplicate policy", self.duplicate_policy)
        return page

    def _build_review_tab(self) -> QWidget:
        panel = QWidget()
        layout = QVBoxLayout(panel)
        layout.setSpacing(10)
        layout.addWidget(self.review_summary_label)
        layout.addWidget(self.review_hint_label)

        placeholder = QWidget()
        placeholder_layout = QVBoxLayout(placeholder)
        placeholder_layout.addStretch(1)
        placeholder_layout.addWidget(self.review_placeholder_label)
        placeholder_layout.addStretch(1)

        # Left pane: discovered items list + all action controls
        left_pane = QWidget()
        left_layout = QVBoxLayout(left_pane)
        left_layout.setSpacing(8)
        left_layout.setContentsMargins(0, 0, 0, 0)
        left_layout.addWidget(QLabel("Discovered Items"))
        left_layout.addWidget(self.review_filter_combo)
        left_layout.addWidget(self.review_filter_status_label)
        left_layout.addWidget(self.review_table, stretch=1)

        nav_row = QHBoxLayout()
        nav_row.addWidget(self.prev_item_button)
        nav_row.addWidget(self.next_item_button)
        nav_row.addWidget(self.next_blocked_button)
        nav_row.addWidget(self.accept_button)
        nav_row.addWidget(self.skip_button)
        nav_row.addWidget(self.auto_accept_button)
        left_layout.addLayout(nav_row)

        extra_row = QHBoxLayout()
        extra_row.addWidget(self.switch_button)
        extra_row.addWidget(self.folder_button)
        extra_row.addWidget(self.title_group_button)
        extra_row.addWidget(self.next_page_button)
        left_layout.addLayout(extra_row)

        search_row = QHBoxLayout()
        search_row.addWidget(self.search_input)
        search_row.addWidget(self.search_button)
        search_row.addWidget(self.manual_button)
        left_layout.addLayout(search_row)

        footer_row = QHBoxLayout()
        footer_row.addWidget(self.preview_button)
        footer_row.addWidget(self.apply_button)
        left_layout.addLayout(footer_row)
        left_layout.addWidget(self.review_blocked_label)

        # Right pane: candidates (top) + details/preview (bottom)
        detail_group = QGroupBox("Selected Item Details")
        detail_layout = QVBoxLayout(detail_group)
        detail_layout.addWidget(self.details_log)
        preview_group = QGroupBox("Organisation Preview")
        preview_layout = QVBoxLayout(preview_group)
        preview_layout.addWidget(self.preview_log)

        bottom_splitter = QSplitter(Qt.Horizontal)
        bottom_splitter.addWidget(detail_group)
        bottom_splitter.addWidget(preview_group)

        right_pane = QWidget()
        right_layout = QVBoxLayout(right_pane)
        right_layout.setSpacing(8)
        right_layout.setContentsMargins(0, 0, 0, 0)
        right_layout.addWidget(QLabel("Candidate Matches"))
        right_layout.addWidget(self.candidate_table, stretch=1)
        right_layout.addWidget(bottom_splitter, stretch=1)

        main_splitter = QSplitter(Qt.Horizontal)
        main_splitter.addWidget(left_pane)
        main_splitter.addWidget(right_pane)
        main_splitter.setStretchFactor(0, 1)
        main_splitter.setStretchFactor(1, 1)

        content = QWidget()
        content_layout = QVBoxLayout(content)
        content_layout.setContentsMargins(0, 0, 0, 0)
        content_layout.addWidget(main_splitter)

        self.review_stack.addWidget(placeholder)
        self.review_stack.addWidget(content)
        self.review_stack.addWidget(self._build_apply_dashboard())
        layout.addWidget(self.review_stack, stretch=1)
        return panel

    def _build_apply_dashboard(self) -> QWidget:
        panel = QWidget()
        layout = QVBoxLayout(panel)
        layout.setSpacing(10)
        dashboard = QGroupBox("Organisation Apply")
        dashboard_layout = QVBoxLayout(dashboard)
        dashboard_layout.addWidget(self.apply_dashboard_label)
        dashboard_layout.addWidget(self.apply_counts_label)
        dashboard_layout.addWidget(self.apply_current_label)
        dashboard_layout.addWidget(self.apply_destination_label)
        dashboard_layout.addWidget(self.apply_elapsed_label)
        dashboard_layout.addWidget(self.apply_progress_bar)
        action_row = QHBoxLayout()
        action_row.addStretch(1)
        action_row.addWidget(self.cancel_apply_button)
        dashboard_layout.addLayout(action_row)
        layout.addWidget(dashboard)
        layout.addWidget(self.apply_log, stretch=1)
        return panel

    def _build_compress_tab(self) -> QWidget:
        panel = QWidget()
        outer = QVBoxLayout(panel)
        outer.setContentsMargins(0, 0, 0, 0)
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.NoFrame)

        content = QWidget()
        layout = QVBoxLayout(content)
        layout.setSpacing(10)
        layout.addWidget(self.compress_summary_label)
        layout.addWidget(self.compress_hint_label)

        empty_page = QWidget()
        empty_layout = QVBoxLayout(empty_page)
        empty_layout.addStretch(1)
        empty_layout.addWidget(self.compress_empty_label)
        empty_layout.addStretch(1)

        preparing_page = QWidget()
        preparing_layout = QVBoxLayout(preparing_page)
        preparation_card = QGroupBox("Preparation Dashboard")
        card_layout = QVBoxLayout(preparation_card)
        card_layout.addWidget(self.compress_preparing_label)
        card_layout.addWidget(self.prepare_stage_label)
        card_layout.addWidget(self.prepare_counts_label)
        card_layout.addWidget(self.prepare_elapsed_label)
        card_layout.addWidget(self.prepare_progress)
        card_layout.addWidget(self.prepare_timeline_label)
        preparing_layout.addWidget(preparation_card)
        preparing_layout.addWidget(self.prepare_log, stretch=1)

        ready_page = QWidget()
        ready_layout = QVBoxLayout(ready_page)
        ready_layout.setContentsMargins(0, 0, 0, 0)

        # Left: status + controls + progress
        left_pane = QWidget()
        left_layout = QVBoxLayout(left_pane)
        left_layout.setContentsMargins(0, 0, 4, 0)
        status_group = QGroupBox("Run Status")
        status_layout = QVBoxLayout(status_group)
        status_layout.addWidget(QLabel("Current action"))
        status_layout.addWidget(self.current_action_label)
        status_layout.addWidget(QLabel("Last completed"))
        status_layout.addWidget(self.last_completed_label)
        status_layout.addWidget(QLabel("Warnings"))
        status_layout.addWidget(self.runtime_warnings_label)
        status_layout.addWidget(self.run_stats_label)
        left_layout.addWidget(status_group)
        action_row = QHBoxLayout()
        action_row.addWidget(self.start_compress_button)
        action_row.addWidget(self.rebuild_safer_button)
        action_row.addWidget(self.prepare_followup_button)
        action_row.addWidget(self.retry_failed_button)
        left_layout.addLayout(action_row)
        left_layout.addWidget(self.include_risky_jobs)
        left_layout.addWidget(QLabel("Current file progress"))
        left_layout.addWidget(self.file_progress)
        left_layout.addWidget(QLabel("Overall encode progress"))
        left_layout.addWidget(self.overall_progress)
        left_layout.addWidget(self.elapsed_label)
        left_layout.addWidget(self.eta_label)
        left_layout.addStretch(1)

        # Right: plan table + log
        right_pane = QWidget()
        right_layout = QVBoxLayout(right_pane)
        right_layout.setContentsMargins(4, 0, 0, 0)
        right_layout.addWidget(QLabel("Compression Plan"))
        right_layout.addWidget(self.compression_filter_combo)
        right_layout.addWidget(self.compression_filter_status_label)
        self.compression_table.setMinimumHeight(280)
        right_layout.addWidget(self.compression_table, stretch=1)
        right_layout.addWidget(self.toggle_details_button)
        right_layout.addWidget(self.compress_status_log, stretch=1)

        # Encode card (animated, shown during compression)
        self.encode_card = QGroupBox("Live Encode")
        card_layout = QVBoxLayout(self.encode_card)
        card_layout.setSpacing(8)
        header_row = QHBoxLayout()
        header_row.addWidget(self.spinner_label)
        text_col = QVBoxLayout()
        text_col.addWidget(self.encode_filename_label)
        text_col.addWidget(self.encode_phase_label)
        header_row.addLayout(text_col, stretch=1)
        header_row.addWidget(self.encode_speed_label)
        card_layout.addLayout(header_row)
        card_layout.addWidget(self.encode_visual_bar)
        card_layout.addWidget(self.encode_counts_label)
        card_layout.addWidget(self.encode_projection_label)
        card_layout.addWidget(self.encode_projection_bar)
        self.encode_card.setVisible(False)

        compress_splitter = QSplitter(Qt.Horizontal)
        compress_splitter.addWidget(left_pane)
        compress_splitter.addWidget(right_pane)
        compress_splitter.setStretchFactor(0, 1)
        compress_splitter.setStretchFactor(1, 2)

        ready_layout.addWidget(self.encode_card)
        toggle_row = QHBoxLayout()
        toggle_row.addStretch(1)
        toggle_row.addWidget(self.toggle_encode_card_button)
        ready_layout.addLayout(toggle_row)
        ready_layout.addWidget(compress_splitter, stretch=1)

        self.compress_stack.addWidget(empty_page)
        self.compress_stack.addWidget(preparing_page)
        self.compress_stack.addWidget(ready_page)
        layout.addWidget(self.compress_stack, stretch=1)
        layout.addStretch(1)
        scroll.setWidget(content)
        outer.addWidget(scroll)
        return panel

    def _build_summary_tab(self) -> QWidget:
        panel = QWidget()
        outer = QVBoxLayout(panel)
        outer.setContentsMargins(0, 0, 0, 0)
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.NoFrame)

        content = QWidget()
        layout = QVBoxLayout(content)
        layout.addWidget(self.summary_headline_label)
        layout.addWidget(self.summary_mode_label)
        layout.addWidget(self.diagnostics_path_label)
        layout.addWidget(self.summary_failure_label)
        layout.addWidget(self.summary_timeline_label)
        tile_row = QHBoxLayout()
        tile_row.addWidget(self.stat_files_label)
        tile_row.addWidget(self.stat_saved_label)
        tile_row.addWidget(self.stat_pct_label)
        tile_row.addStretch(1)
        layout.addLayout(tile_row)
        layout.addWidget(self.summary_overview_label)
        layout.addWidget(self.savings_bar)
        layout.addWidget(self.summary_filter_combo)
        layout.addWidget(self.summary_filter_status_label)
        self.summary_table.setMinimumHeight(240)
        layout.addWidget(self.summary_table, stretch=1)
        self.summary_log.setMinimumHeight(220)
        layout.addWidget(self.summary_log, stretch=1)
        btn_row = QHBoxLayout()
        btn_row.addWidget(self.open_output_button)
        btn_row.addWidget(self.open_diagnostics_button)
        btn_row.addWidget(self.copy_diagnostics_button)
        btn_row.addWidget(self.save_summary_button)
        btn_row.addWidget(self.retry_summary_button)
        btn_row.addStretch(1)
        layout.addLayout(btn_row)
        layout.addStretch(1)
        scroll.setWidget(content)
        outer.addWidget(scroll)
        return panel

    def _connect_signals(self) -> None:
        for signal in [
            self.compress_enabled.toggled,
            self.apply_mode.toggled,
            self.copy_mode.toggled,
            self.use_cache.toggled,
            self.offline.toggled,
            self.overwrite.toggled,
            self.recursive.toggled,
            self.no_skip.toggled,
            self.use_calibration.toggled,
            self.min_confidence.valueChanged,
            self.extensions_input.textChanged,
            self.conflict_mode.currentTextChanged,
            self.policy.currentTextChanged,
            self.on_file_failure.currentTextChanged,
            self.duplicate_policy.currentTextChanged,
        ]:
            signal.connect(self._on_config_edited)

        self.organise_enabled.toggled.connect(self._organise_stage_toggled)
        self.source_input.textChanged.connect(self._source_path_changed)
        self.library_input.textChanged.connect(self._library_path_changed)
        self.compression_root_input.textEdited.connect(self._compression_root_manually_edited)
        self.link_compression_root.toggled.connect(self._compression_root_link_toggled)
        self.show_organise_advanced_button.toggled.connect(self.organise_advanced_panel.setVisible)
        self.show_compress_advanced_button.toggled.connect(self.compress_advanced_panel.setVisible)
        self.guided_button.clicked.connect(self._start_guided_pipeline)
        self.scan_button.clicked.connect(self._start_scan)
        self.prepare_compress_button.clicked.connect(self._prepare_compression_from_setup)
        self.reset_button.clicked.connect(lambda: self._reset_runtime_state("Cleared runtime state."))
        self.toggle_details_button.toggled.connect(self._toggle_details)
        self.review_filter_combo.currentTextChanged.connect(lambda *_: self._apply_review_filter())
        self.compression_filter_combo.currentTextChanged.connect(lambda *_: self._apply_compression_filter())

        self.review_table.itemSelectionChanged.connect(self._review_selection_changed)
        self.candidate_table.itemDoubleClicked.connect(lambda *_: self._accept_selected_candidate())
        self.prev_item_button.clicked.connect(lambda: self._move_review_selection(-1))
        self.next_item_button.clicked.connect(lambda: self._move_review_selection(1))
        self.next_blocked_button.clicked.connect(self._move_to_next_blocked_item)
        self.accept_button.clicked.connect(self._accept_selected_candidate)
        self.skip_button.clicked.connect(self._skip_selected_item)
        self.search_button.clicked.connect(self._search_current_item)
        self.manual_button.clicked.connect(self._manual_select_current_item)
        self.next_page_button.clicked.connect(self._load_next_candidate_page)
        self.auto_accept_button.clicked.connect(self._auto_accept_safe_matches)
        self.switch_button.clicked.connect(self._switch_current_item)
        self.folder_button.clicked.connect(self._apply_choice_to_folder)
        self.title_group_button.clicked.connect(self._apply_choice_to_title_group)
        self.preview_button.clicked.connect(self._preview_plan)
        self.apply_button.clicked.connect(self._apply_plan)
        self.start_compress_button.clicked.connect(self._start_compression)
        self.rebuild_safer_button.clicked.connect(self._prepare_safer_plan)
        self.prepare_followup_button.clicked.connect(self._prepare_followup_plan)
        self.retry_failed_button.clicked.connect(self._prepare_retry_plan)
        self.retry_summary_button.clicked.connect(self._prepare_retry_plan)
        self.include_risky_jobs.toggled.connect(lambda *_: self._refresh_plan_view())
        self.open_output_button.clicked.connect(self._open_output_folder)
        self.open_diagnostics_button.clicked.connect(self._open_diagnostics_folder)
        self.copy_diagnostics_button.clicked.connect(self._copy_diagnostics_path)
        self.active_open_diagnostics_button.clicked.connect(self._open_diagnostics_folder)
        self.active_copy_diagnostics_button.clicked.connect(self._copy_diagnostics_path)
        self.save_summary_button.clicked.connect(self._save_run_summary)
        self.toggle_encode_card_button.toggled.connect(self._on_toggle_encode_card)
        self.cancel_apply_button.clicked.connect(self._request_apply_cancel)
        self.summary_filter_combo.currentTextChanged.connect(lambda *_: self._apply_summary_filter())

        self._review_shortcuts = [
            QShortcut(QKeySequence("Alt+A"), self, activated=self._accept_selected_candidate),
            QShortcut(QKeySequence("Alt+S"), self, activated=self._skip_selected_item),
            QShortcut(QKeySequence("Alt+R"), self, activated=self._search_current_item),
            QShortcut(QKeySequence("Alt+M"), self, activated=self._manual_select_current_item),
            QShortcut(QKeySequence("Alt+F"), self, activated=self._apply_choice_to_folder),
            QShortcut(QKeySequence("Alt+T"), self, activated=self._apply_choice_to_title_group),
            QShortcut(QKeySequence("Alt+N"), self, activated=self._move_to_next_blocked_item),
            QShortcut(QKeySequence("Ctrl+P"), self, activated=self._preview_plan),
        ]

    def _restore_ui_state(
        self,
        *,
        default_source: Path | None,
        default_library: Path | None,
    ) -> None:
        saved = load_ui_state()
        if default_source is None and isinstance(saved.get("source"), str):
            self.source_input.setText(saved["source"])
        if default_library is None and isinstance(saved.get("library"), str):
            self.library_input.setText(saved["library"])
        if isinstance(saved.get("compression_root_linked"), bool):
            self._compression_root_linked = saved["compression_root_linked"]
            self.link_compression_root.setChecked(self._compression_root_linked)
        if isinstance(saved.get("compression_root"), str) and not self._compression_root_linked:
            self.compression_root_input.setText(saved["compression_root"])
        elif self._compression_root_linked:
            self.compression_root_input.setText(self.library_input.text())
        if isinstance(saved.get("organise_enabled"), bool):
            self.organise_enabled.setChecked(saved["organise_enabled"])
        if isinstance(saved.get("compress_enabled"), bool):
            self.compress_enabled.setChecked(saved["compress_enabled"])
        if isinstance(saved.get("apply_mode"), bool):
            self.apply_mode.setChecked(saved["apply_mode"])
        if isinstance(saved.get("copy_mode"), bool):
            self.copy_mode.setChecked(saved["copy_mode"])
        if isinstance(saved.get("use_cache"), bool):
            self.use_cache.setChecked(saved["use_cache"])
        if isinstance(saved.get("offline"), bool):
            self.offline.setChecked(saved["offline"])
        if isinstance(saved.get("min_confidence"), (float, int)):
            self.min_confidence.setValue(float(saved["min_confidence"]))
        if isinstance(saved.get("extensions"), str):
            self.extensions_input.setText(saved["extensions"])
        if isinstance(saved.get("conflict_mode"), str):
            self._set_combo_value(self.conflict_mode, saved["conflict_mode"])
        if isinstance(saved.get("overwrite"), bool):
            self.overwrite.setChecked(saved["overwrite"])
        if isinstance(saved.get("recursive"), bool):
            self.recursive.setChecked(saved["recursive"])
        if isinstance(saved.get("no_skip"), bool):
            self.no_skip.setChecked(saved["no_skip"])
        if isinstance(saved.get("policy"), str):
            self._set_combo_value(self.policy, saved["policy"])
        if isinstance(saved.get("on_file_failure"), str):
            self._set_combo_value(self.on_file_failure, saved["on_file_failure"])
        if isinstance(saved.get("use_calibration"), bool):
            self.use_calibration.setChecked(saved["use_calibration"])
        if isinstance(saved.get("duplicate_policy"), str):
            self._set_combo_value(self.duplicate_policy, saved["duplicate_policy"])
        if isinstance(saved.get("compression_filter"), str):
            self._set_combo_value(self.compression_filter_combo, saved["compression_filter"])
        if isinstance(saved.get("review_filter"), str):
            self._set_combo_value(self.review_filter_combo, saved["review_filter"])
        if isinstance(saved.get("summary_filter"), str):
            self._set_combo_value(self.summary_filter_combo, saved["summary_filter"])
        self._restored_state_warnings = self._restored_state_warning_messages()

    def _apply_initial_geometry(self) -> None:
        screen = QApplication.primaryScreen()
        if screen is None:
            self.resize(1280, 820)
            return
        available = screen.availableGeometry()
        max_width = max(960, int(available.width() * 0.96))
        max_height = max(700, int(available.height() * 0.94))
        saved = load_ui_state()
        width = int(saved.get("window_width", min(1320, max_width)) or min(1320, max_width))
        height = int(saved.get("window_height", min(860, max_height)) or min(860, max_height))
        self.resize(min(width, max_width), min(height, max_height))
        if all(key in saved for key in ("window_x", "window_y")):
            x = int(saved.get("window_x", available.x()) or available.x())
            y = int(saved.get("window_y", available.y()) or available.y())
            clamped_x = max(available.x(), min(x, available.right() - self.width()))
            clamped_y = max(available.y(), min(y, available.bottom() - self.height()))
            self.move(clamped_x, clamped_y)
        else:
            frame = self.frameGeometry()
            frame.moveCenter(available.center())
            self.move(frame.topLeft())
        if bool(saved.get("window_maximized", False)):
            self.showMaximized()

    def _set_combo_value(self, combo: QComboBox, value: str) -> None:
        index = combo.findText(value)
        if index >= 0:
            combo.setCurrentIndex(index)

    def _restored_state_warning_messages(self) -> list[str]:
        warnings: list[str] = []
        for label, widget in (
            ("Source", self.source_input),
            ("Library / Output Folder", self.library_input),
            ("Compression Root", self.compression_root_input),
        ):
            text = widget.text().strip()
            if not text:
                continue
            path = Path(text).expanduser()
            if not path.exists():
                warnings.append(f"{label} from the previous session is missing: {path}")
        return warnings

    @staticmethod
    def _is_default_review_filter(mode: str) -> bool:
        return mode == "All items"

    @staticmethod
    def _is_default_compression_filter(mode: str) -> bool:
        return mode == "All plan items"

    def _summarize_blocked_reasons(self) -> str:
        if self.controller is None:
            return ""
        counts = Counter(
            item.preview_block_reason
            for item in self.controller.items
            if item.preview_block_reason is not None
        )
        if not counts:
            return ""
        return ", ".join(f"{reason}: {count}" for reason, count in counts.most_common(3))

    def _summarize_deferred_plan_reasons(self) -> str:
        counts = Counter()
        for row in self._plan_classification.risky_follow_up:
            key = row.plain_reason or row.reason or row.issue or "Deferred by default"
            counts[key] += 1
        for row in self._plan_classification.missing_items:
            key = row.issue or "Missing from compression root"
            counts[key] += 1
        if not counts:
            return ""
        return ", ".join(f"{reason}: {count}" for reason, count in counts.most_common(3))

    def _reset_guided_filters(self) -> None:
        self._set_combo_value(self.review_filter_combo, "All items")
        self._set_combo_value(self.compression_filter_combo, "All plan items")
        self._apply_review_filter()
        self._apply_compression_filter()

    def _sync_summary_for_diagnostics(self) -> None:
        self._refresh_pipeline_summary()

    def _flush_runtime_diagnostics(self, *, failure_message: str | None = None, progress_only: bool = False) -> None:
        if self._syncing_diagnostics:
            return
        now = time.monotonic()
        if progress_only and now - self._last_diagnostics_flush_at < 5:
            return
        self._diagnostics.set_config(self._snapshot_config_for_diagnostics())
        self._syncing_diagnostics = True
        try:
            self._sync_summary_for_diagnostics()
            self._flush_diagnostics(failure_message=failure_message)
            self._last_diagnostics_flush_at = now
        finally:
            self._syncing_diagnostics = False

    def _ui_state_payload(self) -> dict[str, object]:
        geometry = self.normalGeometry() if self.isMaximized() else self.geometry()
        return {
            "source": self.source_input.text().strip(),
            "library": self.library_input.text().strip(),
            "compression_root": self.compression_root_input.text().strip(),
            "compression_root_linked": self.link_compression_root.isChecked(),
            "organise_enabled": self.organise_enabled.isChecked(),
            "compress_enabled": self.compress_enabled.isChecked(),
            "apply_mode": self.apply_mode.isChecked(),
            "copy_mode": self.copy_mode.isChecked(),
            "use_cache": self.use_cache.isChecked(),
            "offline": self.offline.isChecked(),
            "min_confidence": float(self.min_confidence.value()),
            "extensions": self.extensions_input.text().strip(),
            "conflict_mode": self.conflict_mode.currentText(),
            "overwrite": self.overwrite.isChecked(),
            "recursive": self.recursive.isChecked(),
            "no_skip": self.no_skip.isChecked(),
            "policy": self.policy.currentText(),
            "on_file_failure": self.on_file_failure.currentText(),
            "use_calibration": self.use_calibration.isChecked(),
            "duplicate_policy": self.duplicate_policy.currentText(),
            "window_x": geometry.x(),
            "window_y": geometry.y(),
            "window_width": geometry.width(),
            "window_height": geometry.height(),
            "window_maximized": self.isMaximized(),
            "compression_filter": self.compression_filter_combo.currentText(),
            "review_filter": self.review_filter_combo.currentText(),
            "summary_filter": self.summary_filter_combo.currentText(),
        }

    def _persist_ui_state(self) -> None:
        save_ui_state(self._ui_state_payload())

    def closeEvent(self, event: QCloseEvent) -> None:  # pragma: no cover - GUI runtime path
        self._shutting_down = True
        self._persist_ui_state()
        if self._active_worker_count > 0:
            reply = QMessageBox.question(
                self,
                "mediaflow",
                "Background work is still running. Close the window anyway?",
            )
            if reply != QMessageBox.Yes:
                event.ignore()
                self._shutting_down = False
                return
        super().closeEvent(event)

    def _browse_into(self, target: QLineEdit) -> None:
        selected = QFileDialog.getExistingDirectory(
            self, "Select directory", target.text() or str(Path.home())
        )
        if selected:
            target.setText(selected)

    def _library_path_changed(self, text: str) -> None:
        if self._compression_root_linked and self.organise_enabled.isChecked():
            self.compression_root_input.blockSignals(True)
            self.compression_root_input.setText(text)
            self.compression_root_input.blockSignals(False)
        self._on_config_edited()

    def _source_path_changed(self, text: str) -> None:
        if self._compression_root_linked and not self.organise_enabled.isChecked():
            self.compression_root_input.blockSignals(True)
            self.compression_root_input.setText(text)
            self.compression_root_input.blockSignals(False)
        self._on_config_edited()

    def _organise_stage_toggled(self, checked: bool) -> None:
        if not self._loading_state and self._compression_root_linked:
            target = self.library_input.text() if checked else self.source_input.text()
            self.compression_root_input.blockSignals(True)
            self.compression_root_input.setText(target)
            self.compression_root_input.blockSignals(False)
        self._refresh_compression_link_label()
        self._on_config_edited()

    @staticmethod
    def _format_bytes(n: int) -> str:
        for unit in ("B", "KB", "MB", "GB", "TB"):
            if abs(n) < 1024:
                return f"{n:.1f} {unit}"
            n /= 1024
        return f"{n:.1f} PB"

    @staticmethod
    def _strip_rich(text: str) -> str:
        """Strip Rich markup tags (e.g. [dim], [/white]) from mediashrink output."""
        return re.sub(r'\[/?[^\]]+\]', '', text)

    @staticmethod
    def _format_elapsed(seconds: float) -> str:
        s = int(seconds)
        if s < 60:
            return f"{s}s"
        return f"{s // 60}m {s % 60}s"

    @staticmethod
    def _summarize_path(path_text: str | None) -> str:
        if not path_text:
            return "(waiting for file details)"
        return Path(path_text).name or path_text

    @staticmethod
    def _phase_label(phase: str) -> str:
        normalized = phase.replace("_", "-").strip().lower()
        mapping = {
            "starting": "Starting",
            "report-opened": "Starting",
            "copying": "Copying",
            "moving": "Moving",
            "completed-item": "Completed",
            "skipped-item": "Skipped",
            "error-item": "Error",
            "cancelled": "Cancelled",
            "finalizing-report": "Finalizing report",
            "done": "Done",
        }
        return mapping.get(normalized, normalized.replace("-", " ").title() or "Working")

    def _tick_activity_indicator(self) -> None:
        active_states = {
            WorkflowState.SCANNING,
            WorkflowState.APPLYING,
            WorkflowState.PREPARING_COMPRESSION,
            WorkflowState.COMPRESSING,
        }
        if self.workflow_state not in active_states:
            self._activity_timer.stop()
            self.activity_indicator_label.setText("Idle")
            return
        self._activity_spinner_idx = (self._activity_spinner_idx + 1) % len(self._SPINNER_FRAMES)
        frame = self._SPINNER_FRAMES[self._activity_spinner_idx]
        label = {
            WorkflowState.SCANNING: "Scan",
            WorkflowState.APPLYING: "Apply",
            WorkflowState.PREPARING_COMPRESSION: "Prep",
            WorkflowState.COMPRESSING: "Encode",
        }.get(self.workflow_state, "Work")
        self.activity_indicator_label.setText(f"{frame} {label}")

    def _refresh_activity_indicator(self) -> None:
        if self.workflow_state in {
            WorkflowState.SCANNING,
            WorkflowState.APPLYING,
            WorkflowState.PREPARING_COMPRESSION,
            WorkflowState.COMPRESSING,
        }:
            if not self._activity_timer.isActive():
                self._activity_timer.start()
            self._tick_activity_indicator()
        else:
            self._activity_timer.stop()
            self.activity_indicator_label.setText("Idle")

    def _set_diagnostics_provenance(self) -> None:
        self._diagnostics.set_provenance(
            {
                "app_version": __version__,
                "python_executable": sys.executable,
                "python_version": sys.version.split()[0],
                "platform": sys.platform,
                "config_dir": str(get_config_dir()),
                "diagnostics_dir": str(diagnostics_dir()),
                "integrations": {
                    "plexify": self._module_origin_details("plexify"),
                    "mediashrink": self._module_origin_details("mediashrink"),
                },
            }
        )

    @staticmethod
    def _module_origin_details(module_name: str) -> dict[str, object]:
        try:
            module = importlib.import_module(module_name)
        except Exception as exc:  # noqa: BLE001
            return {"imported": False, "error": str(exc)}
        module_path = getattr(module, "__file__", None)
        resolved = Path(module_path).resolve(strict=False) if module_path else None
        return {
            "imported": True,
            "path": str(resolved) if resolved else None,
            "editable_local": bool(resolved and ("github" in str(resolved) or ".egg-link" in str(resolved))),
        }

    @staticmethod
    def _parse_title_with_year(text: str) -> tuple[str, int | None]:
        clean = text.strip()
        match = re.match(r"^(?P<title>.+?)\s*\((?P<year>\d{4})\)\s*$", clean)
        if not match:
            return clean, None
        return match.group("title").strip(), int(match.group("year"))

    @staticmethod
    def _optional_year_from_text(text: str) -> int | None:
        clean = text.strip()
        if not clean:
            return None
        if re.fullmatch(r"\d{4}", clean):
            return int(clean)
        return -1

    def _diagnostics_directory_path(self) -> Path:
        if self._last_diagnostics_path is not None:
            return self._last_diagnostics_path.parent
        return get_config_dir() / "runs"

    def _diagnostics_status_text(self) -> str:
        if self._last_diagnostics_path is not None:
            return f"Diagnostics file: {self._last_diagnostics_path}"
        if self._last_diagnostics_error:
            return f"Diagnostics warning: {self._last_diagnostics_error}"
        return f"Diagnostics folder: {self._diagnostics_directory_path()}"

    def _tick_scan(self) -> None:
        if self.workflow_state != WorkflowState.SCANNING or self._scan_started_at is None:
            self._scan_timer.stop()
            return
        elapsed = time.monotonic() - self._scan_started_at
        self.review_placeholder_label.setText(self._review_placeholder_text())
        discovered = (
            f"{self._scan_discovered_count} discovered"
            if self._scan_discovered_count
            else "waiting for first candidates"
        )
        current_file = f" • {Path(self._scan_last_path).name}" if self._scan_last_path else ""
        message = f"Scanning source with plexify ({self._format_elapsed(elapsed)} • {discovered}{current_file})"
        if elapsed >= 10 and self._scan_discovered_count == 0:
            message += ". Still working. Plexify has not returned first candidates yet."
        elif self._scan_last_update_at is not None and time.monotonic() - self._scan_last_update_at >= 10:
            message += f". No scan update for {self._format_elapsed(time.monotonic() - self._scan_last_update_at)}."
        self._set_current_action(message)

    def _tick_apply(self) -> None:
        if self.workflow_state != WorkflowState.APPLYING or self._apply_started_at is None:
            self._apply_timer.stop()
            return
        elapsed = time.monotonic() - self._apply_started_at
        progress = self._apply_progress
        if progress is None:
            message = f"Applying organisation ({self._format_elapsed(elapsed)} • waiting for first file operation)"
            if elapsed >= 10:
                message += ". Still working. Opening reports or starting the first copy can take time."
            self._set_current_action(message)
            self._apply_progress_model.update_stall(elapsed_seconds=elapsed, stalled_seconds=elapsed)
            self._refresh_apply_dashboard(stalled_seconds=elapsed)
            return
        stalled = bool(
            self._apply_last_update_at is not None
            and time.monotonic() - self._apply_last_update_at >= 10
        )
        stalled_seconds = (
            time.monotonic() - self._apply_last_update_at
            if self._apply_last_update_at is not None
            else 0.0
        )
        self._apply_progress_model.update_stall(elapsed_seconds=elapsed, stalled_seconds=stalled_seconds)
        self._set_current_action(
            self._format_apply_status_text(progress, elapsed=elapsed, stalled=stalled)
        )
        self._refresh_apply_dashboard(stalled_seconds=stalled_seconds)

    @staticmethod
    def _normalize_heartbeat_state(text: str) -> str:
        clean = MainWindow._strip_rich(text).strip().lower()
        mapping = {
            "active": "Encoding",
            "muxing": "Muxing",
            "finishing": "Finishing",
            "waiting": "Waiting",
            "queued": "Queued",
        }
        return mapping.get(clean, clean.replace("_", " ").title() or "Encoding")

    @staticmethod
    def _progress_bucket(progress: float) -> int:
        clipped = max(0.0, min(progress, 1.0))
        return int(clipped * 20)

    @staticmethod
    def _preparation_stage_title(stage: str) -> str:
        return preparation_stage_title(stage)

    @staticmethod
    def _preparation_stage_key_for(stage: str) -> str:
        lowered = stage.lower()
        if "discover" in lowered:
            return "discovering"
        if "analys" in lowered:
            return "analysing"
        if "benchmark" in lowered or "provisional profile" in lowered:
            return "benchmarking"
        if "smoke" in lowered:
            return "smoke-probing risky container/profile combinations"
        if "scoring" in lowered:
            return "scoring recommendations..."
        if "ready" in lowered:
            return "plan-ready"
        return lowered

    def _preparation_timeline_text(self, active_stage: str) -> str:
        return preparation_timeline_text(active_stage)

    def _refresh_compression_link_label(self) -> None:
        if self.organise_enabled.isChecked():
            self.link_compression_root.setText(
                "Use the library / output folder as the compression root"
            )
        else:
            self.link_compression_root.setText(
                "Use the source folder as the compression root"
            )

    def _compression_root_link_toggled(self, checked: bool) -> None:
        self._compression_root_linked = checked
        if checked:
            target = self.library_input.text() if self.organise_enabled.isChecked() else self.source_input.text()
            self.compression_root_input.blockSignals(True)
            self.compression_root_input.setText(target)
            self.compression_root_input.blockSignals(False)
        self._on_config_edited()

    def _compression_root_manually_edited(self, text: str) -> None:
        linked_path = self.library_input.text() if self.organise_enabled.isChecked() else self.source_input.text()
        if self._compression_root_linked and text.strip() != linked_path.strip():
            self.link_compression_root.setChecked(False)
            self._compression_root_linked = False
        self._on_config_edited()

    def _toggle_details(self, checked: bool) -> None:
        self.compress_status_log.setVisible(checked)
        self.toggle_details_button.setText("Hide Details" if checked else "Show Details")

    def _on_config_edited(self, *_args) -> None:
        if self._loading_state:
            return
        if (
            self.controller is not None
            or self.preview_state is not None
            or self.apply_result is not None
            or self.encode_preparation is not None
            or self.encode_results
        ):
            self._config_dirty = True
        self._set_state(self.workflow_state)

    def _current_config(self, *, allow_missing_compression_root: bool = False) -> PipelineConfig:
        return build_pipeline_config(
            source=self.source_input.text().strip(),
            library=self.library_input.text().strip(),
            compression_root=self.compression_root_input.text().strip(),
            plexify=PlexifySettings(
                enabled=self.organise_enabled.isChecked(),
                apply=self.apply_mode.isChecked(),
                copy_mode=self.copy_mode.isChecked(),
                use_cache=self.use_cache.isChecked(),
                offline=self.offline.isChecked(),
                min_confidence=float(self.min_confidence.value()),
                extensions=self.extensions_input.text().strip(),
                on_conflict=self.conflict_mode.currentText(),
            ),
            shrink=ShrinkSettings(
                enabled=self.compress_enabled.isChecked(),
                overwrite=self.overwrite.isChecked(),
                recursive=self.recursive.isChecked(),
                no_skip=self.no_skip.isChecked(),
                policy=self.policy.currentText(),
                on_file_failure=self.on_file_failure.currentText(),
                use_calibration=self.use_calibration.isChecked(),
                duplicate_policy=self.duplicate_policy.currentText(),
            ),
            allow_missing_compression_root=allow_missing_compression_root,
        )

    def _ensure_compatibility(self) -> bool:
        if self._compatibility_checked:
            return True
        issues = check_runtime_compatibility()
        if issues:
            self._show_error(compatibility_error_text(issues))
            return False
        self._compatibility_checked = True
        return True

    def _clear_warnings(self) -> None:
        self._custom_warnings.clear()

    def _record_warning(self, text: str) -> None:
        if text not in self._custom_warnings:
            self._custom_warnings.append(text)
        self._diagnostics.record_warning(text)

    def _set_current_action(self, text: str) -> None:
        self._current_action = text
        clean = self._strip_rich(text)
        self.current_action_label.setText(clean)
        self.activity_label.setText(f"Current activity: {clean}")
        self._refresh_activity_indicator()

    def _apply_progress_position(self, payload: ApplyProgress) -> tuple[int, int]:
        total = max(0, payload.total)
        completed = max(0, payload.completed)
        phase = payload.phase.replace("_", "-").strip().lower()
        if total <= 0:
            return completed, total
        if phase in {"copying", "moving", "starting", "report-opened"}:
            return min(total, completed + 1), total
        if phase == "completed-item":
            return min(total, completed), total
        if phase in {"finalizing-report", "done"}:
            return total, total
        return min(total, completed), total

    def _format_apply_status_text(
        self,
        payload: ApplyProgress,
        *,
        elapsed: float | None = None,
        stalled: bool = False,
    ) -> str:
        phase = self._phase_label(payload.phase)
        current_index, total = self._apply_progress_position(payload)
        if total:
            counts = f"current item {current_index} of {total} • completed {payload.completed} of {total}"
        else:
            counts = "starting"
        current_name = self._summarize_path(payload.current_source or payload.last_applied_source)
        first_line = f"{phase} organisation ({counts}"
        if elapsed is not None:
            first_line += f" • {self._format_elapsed(elapsed)}"
        first_line += f"): {current_name}"

        lines = [first_line]
        if payload.current_source:
            lines.append(f"Current file: {payload.current_source}")
        if payload.current_destination:
            lines.append(f"Destination: {payload.current_destination}")
        if payload.source_size_bytes:
            lines.append(f"Current file size: {self._format_bytes(payload.source_size_bytes)}")
        if payload.report_path:
            lines.append(f"Report: {payload.report_path}")
        if payload.conflict_action:
            lines.append(f"Conflict handling: {payload.conflict_action}")
        if payload.error:
            lines.append(f"Error: {payload.error}")
        if payload.message:
            lines.append(payload.message)
        if stalled:
            lines.append("No new apply update yet. Still working on the last reported file; large copies can spend a long time here.")
        if payload.cancel_requested or self._apply_cancel_requested:
            lines.append("Cancel requested. Mediaflow will stop before the next file operation.")
        return "\n".join(lines)

    def _complete_action(self, text: str) -> None:
        self._last_completed_action = text
        self.last_completed_label.setText(text)

    def _request_apply_cancel(self) -> None:
        if self.workflow_state != WorkflowState.APPLYING:
            return
        self._apply_cancel_requested = True
        self.cancel_apply_button.setEnabled(False)
        self._append_status("Cancel requested. Organisation will stop after the current file operation.")
        self._diagnostics.record_event("organisation_apply_cancel_requested")
        if self._apply_progress is not None:
            self._apply_progress_model.cancel_requested = True
            self._refresh_apply_dashboard(stalled_seconds=0.0)

    def _apply_cancel_requested_callback(self) -> bool:
        return self._apply_cancel_requested

    def _refresh_apply_dashboard(self, *, stalled_seconds: float = 0.0) -> None:
        model = self._apply_progress_model
        current_name = self._summarize_path(model.current_source)
        destination = model.current_destination or "(waiting for destination)"
        if model.total_items:
            pct = int(100 * min(model.completed_items, model.total_items) / model.total_items)
            count_text = (
                f"Current item: {model.current_item_index} of {model.total_items}  |  "
                f"Completed: {model.completed_items} of {model.total_items}"
            )
        else:
            pct = 0
            count_text = "Current item: starting  |  Completed: 0"
        size_text = (
            f"Current file size: {self._format_bytes(model.current_file_bytes)}"
            if model.current_file_bytes
            else "Current file size: unknown"
        )
        stalled_text = ""
        if stalled_seconds >= 10:
            stalled_text = (
                f"\nNo new apply update for {self._format_elapsed(stalled_seconds)}. "
                "Still working on the last reported file."
            )
        cancel_text = "\nCancel requested; mediaflow will stop before the next file." if model.cancel_requested else ""
        self.apply_dashboard_label.setText(
            f"{self._phase_label(model.phase)} organisation\n"
            f"{size_text}{stalled_text}{cancel_text}"
        )
        self.apply_counts_label.setText(count_text)
        self.apply_current_label.setText(f"Current file: {model.current_source or current_name}")
        self.apply_destination_label.setText(f"Destination: {destination}")
        report = f"  |  Report: {model.report_path}" if model.report_path else ""
        self.apply_elapsed_label.setText(
            f"Elapsed: {self._format_elapsed(model.elapsed_seconds)}{report}"
        )
        self.apply_progress_bar.setValue(pct)
        self.cancel_apply_button.setEnabled(
            self.workflow_state == WorkflowState.APPLYING and not self._apply_cancel_requested
        )
        if model.event_log:
            self.apply_log.setPlainText("\n".join(model.event_log))

    def _reset_runtime_state(self, status_message: str | None = None) -> None:
        self.controller = None
        self.preview_state = None
        self.apply_result = None
        self.encode_preparation = None
        self.encode_results = []
        self._compression_plan_rows = []
        self._summary_rows = []
        self._plan_classification = classify_compression_plan(())
        self._retry_sources = set()
        self._last_diagnostics_path = None
        self._last_diagnostics_error = None
        self._diagnostics = DiagnosticsRecorder()
        self._set_diagnostics_provenance()
        self._guided_mode = False
        self._continue_to_compress = False
        self._config_dirty = False
        self.review_table.setRowCount(0)
        self.candidate_table.setRowCount(0)
        self.compression_table.setRowCount(0)
        self.tabs.setTabText(1, "Review")
        self.details_log.clear()
        self.preview_log.clear()
        self.prepare_log.clear()
        self.apply_log.clear()
        self.apply_progress_bar.setValue(0)
        self.summary_log.clear()
        self.compress_status_log.clear()
        self.prepare_progress.setRange(0, 100)
        self.prepare_progress.setValue(0)
        self.file_progress.setValue(0)
        self.overall_progress.setValue(0)
        self._compression_timer.stop()
        self._preparation_timer.stop()
        self._apply_timer.stop()
        self._scan_timer.stop()
        self._scan_started_at = None
        self._scan_last_update_at = None
        self._scan_discovered_count = 0
        self._scan_last_path = ""
        self._preparation_last_update_at = None
        self._apply_started_at = None
        self._apply_last_update_at = None
        self._apply_progress = None
        self._apply_progress_model.reset()
        self._apply_cancel_requested = False
        self._last_apply_log_key = None
        self._encode_progress_model.reset()
        self._preparation_model = PreparationProgressModel()
        self._last_status_text = ""
        self.elapsed_label.setText("Elapsed: —")
        self.eta_label.setText("ETA: —")
        self.run_stats_label.setText("Files: —")
        self.savings_bar.setVisible(False)
        self.stat_files_label.setText("—")
        self.stat_saved_label.setText("—")
        self.stat_pct_label.setText("—")
        self.encode_card.setVisible(False)
        self.encode_visual_bar.setValue(0)
        self.encode_projection_bar.setValue(0)
        self.encode_projection_bar.setFormat("Projected retained size")
        self.encode_phase_label.setText("")
        self.encode_counts_label.setText("")
        self.encode_projection_label.setText("")
        self._last_encode_log_key = (-1, "")
        self._last_encode_bucket = -1
        self._last_encode_file = ""
        self.prepare_elapsed_label.setText("")
        self.prepare_stage_label.setText("Analysing files...")
        self.prepare_counts_label.setText("0 file(s) discovered • 0.0 B")
        self.prepare_timeline_label.setText(self._preparation_timeline_text("discovering"))
        self._clear_warnings()
        self._set_current_action("Not started")
        self._complete_action("Nothing completed yet")
        self.retry_failed_button.setVisible(False)
        self.retry_summary_button.setVisible(False)
        self.diagnostics_path_label.setText("")
        self.active_diagnostics_label.setText(self._diagnostics_status_text())
        self._refresh_pipeline_summary()
        if status_message:
            self._append_status(status_message)
            self._complete_action(status_message)
        self._set_state(WorkflowState.SETUP)

    def _summary_header_text(self) -> str:
        organise_on = self.organise_enabled.isChecked()
        compress_on = self.compress_enabled.isChecked()
        summary = build_pipeline_summary(self.apply_result, self.encode_results)
        if self.workflow_state == WorkflowState.FAILED:
            return "Pipeline failed"
        if self.workflow_state == WorkflowState.COMPLETED:
            if compress_on and self._is_degraded_completion(summary):
                if organise_on:
                    return "Pipeline completed with compression follow-up needed"
                return "Compression run completed with follow-up needed"
            if organise_on and compress_on:
                return "Full pipeline completed"
            if compress_on:
                return "Compression-only run completed"
            if organise_on:
                return "Organise-only run completed"
        if self.workflow_state == WorkflowState.READY_TO_COMPRESS:
            if self._compression_plan_is_blocked():
                return "Compression plan needs attention"
            return "Compression plan ready"
        if self.workflow_state == WorkflowState.PREPARING_COMPRESSION:
            return "Preparing compression plan"
        if self.workflow_state == WorkflowState.COMPRESSING:
            return "Compression in progress"
        if self.workflow_state == WorkflowState.READY_TO_APPLY:
            return "Organisation preview ready"
        if self.workflow_state == WorkflowState.APPLYING:
            return "Applying organisation"
        if self.workflow_state in {WorkflowState.REVIEW, WorkflowState.REVIEW_BLOCKED, WorkflowState.SCANNING}:
            return "Organise review in progress"
        return "Pipeline summary"

    def _set_state(self, state: WorkflowState) -> None:
        self.workflow_state = state
        presentation = describe_workflow_state(state, organise_enabled=self.organise_enabled.isChecked())
        if state == WorkflowState.READY_TO_COMPRESS and self._compression_plan_is_blocked():
            presentation = presentation.__class__(
                step_title=presentation.step_title.replace("Compression ready", "Compression review"),
                headline="Compression plan needs attention",
                guidance=(
                    "Review the blocked compression plan, rebuild with a safer profile, or prepare a follow-up plan."
                ),
            )
        self.step_label.setText(presentation.step_title)
        self.headline_label.setText(presentation.headline)
        guidance = presentation.guidance
        if self._config_dirty and state not in {
            WorkflowState.SCANNING,
            WorkflowState.APPLYING,
            WorkflowState.PREPARING_COMPRESSION,
            WorkflowState.COMPRESSING,
        }:
            guidance += "\nSettings have changed since the last scan or compression plan."
        self.guidance_label.setText(guidance)
        self.step_checklist_label.setText(self._workflow_checklist_text())
        self._update_ui()
        self._refresh_activity_indicator()

    def _compression_plan_is_blocked(self) -> bool:
        prep = self.encode_preparation
        if prep is None:
            return False
        if not prep.items:
            return False
        if prep.profile is None:
            return True
        if getattr(prep, "compatible_count", 0) <= 0:
            return True
        if self._compression_has_blocking_risk():
            return True
        return not bool(self._runnable_jobs(prep))

    def _compression_has_blocking_risk(self) -> bool:
        blocking_tokens = (
            "hardware encoder startup",
            "output/header",
            "output header",
            "container/header",
            "container compatibility",
        )
        runnable_sources = self._runnable_sources() if self.encode_preparation is not None else set()
        for row in self._compression_plan_rows:
            if row.source not in runnable_sources or not row.exists:
                continue
            haystack = " ".join([row.issue, row.reason, row.plain_reason, row.risk_reason]).lower()
            if any(token in haystack for token in blocking_tokens):
                return True
        prep = self.encode_preparation
        if prep is not None:
            grouped = getattr(prep, "grouped_incompatibilities", {}) or {}
            if any(any(token in str(key).lower() for token in blocking_tokens) for key in grouped):
                return True
        return False

    def _followup_sources(self) -> set[Path]:
        sources = set(self._retry_sources)
        for row in self._compression_plan_rows:
            if row.classification == "risky-follow-up":
                sources.add(row.source)
                continue
            if row.issue and (
                "compatibility" in row.issue.lower()
                or "container" in row.issue.lower()
                or "missing" in row.issue.lower()
            ):
                sources.add(row.source)
        return sources

    def _is_degraded_completion(self, summary) -> bool:
        if not self.compress_enabled.isChecked():
            return False
        if self._retry_sources:
            return True
        if summary.failed_files > 0:
            return True
        if summary.encoded_files == 0 and summary.skipped_files > 0:
            return True
        return False

    def _workflow_checklist_text(self) -> str:
        organise_on = self.organise_enabled.isChecked()
        compress_on = self.compress_enabled.isChecked()
        steps = [
            ("Setup", self.workflow_state == WorkflowState.SETUP, self.source_input.text().strip() and self.library_input.text().strip()),
        ]
        if organise_on:
            steps += [
                ("Review", self.workflow_state in {WorkflowState.SCANNING, WorkflowState.REVIEW, WorkflowState.REVIEW_BLOCKED}, self.preview_state is not None or self.apply_result is not None),
                ("Apply", self.workflow_state in {WorkflowState.READY_TO_APPLY, WorkflowState.APPLYING}, self.apply_result is not None),
            ]
        if compress_on:
            steps += [
                ("Compress", self.workflow_state in {WorkflowState.PREPARING_COMPRESSION, WorkflowState.READY_TO_COMPRESS, WorkflowState.COMPRESSING}, self.encode_preparation is not None or bool(self.encode_results)),
            ]
        steps += [
            ("Summary", self.workflow_state in {WorkflowState.COMPLETED, WorkflowState.FAILED}, self.workflow_state in {WorkflowState.COMPLETED, WorkflowState.FAILED}),
        ]
        lines = []
        for name, current, done in steps:
            prefix = "Current" if current else "Done" if done else "Pending"
            lines.append(f"{prefix}: {name}")
        return " | ".join(lines)

    def _switch_tab(self, name: str) -> None:
        mapping = {"setup": 0, "review": 1, "compress": 2, "summary": 3}
        self.tabs.setCurrentIndex(mapping[name])

    def _update_ui(self) -> None:
        busy = self._active_worker_count > 0
        has_controller = self.controller is not None and bool(self.controller.items)
        review_index = self._current_review_index()
        has_review_selection = has_controller and review_index is not None
        has_compression_plan = self.encode_preparation is not None
        can_preview = has_controller and not busy and not self._config_dirty
        can_apply = bool(self.preview_state and self.preview_state.can_apply and not busy and not self._config_dirty)
        can_start_compression = bool(
            has_compression_plan
            and self._runnable_jobs(self.encode_preparation)
            and not self._compression_plan_is_blocked()
            and self.workflow_state == WorkflowState.READY_TO_COMPRESS
            and not busy
            and not self._config_dirty
        )
        current_has_more = False
        if has_review_selection and self.controller is not None:
            current_has_more = self.controller.items[review_index or 0].has_more

        organise_on = self.organise_enabled.isChecked()
        self.library_label.setVisible(organise_on)
        self.library_input.setVisible(organise_on)
        self.library_browse.setVisible(organise_on)
        self.library_help.setVisible(organise_on)
        self.organise_options_group.setVisible(organise_on)
        self.source_help_label.setText(
            "Incoming folder scanned by plexify for new media."
            if organise_on
            else "Folder containing video files to compress."
        )
        self.tabs.tabBar().setTabVisible(1, organise_on)

        self.tabs.setTabEnabled(0, True)
        self.tabs.setTabEnabled(1, has_controller or self.workflow_state in {WorkflowState.SCANNING, WorkflowState.REVIEW, WorkflowState.REVIEW_BLOCKED, WorkflowState.READY_TO_APPLY, WorkflowState.APPLYING})
        self.tabs.setTabEnabled(2, has_compression_plan or self.workflow_state in {WorkflowState.PREPARING_COMPRESSION, WorkflowState.READY_TO_COMPRESS, WorkflowState.COMPRESSING})
        self.tabs.setTabEnabled(3, True)

        self.guided_button.setEnabled(not busy)
        self.scan_button.setEnabled(self.organise_enabled.isChecked() and not busy)
        self.prepare_compress_button.setEnabled(self.compress_enabled.isChecked() and not busy)
        self.reset_button.setEnabled(not busy)

        review_actions_enabled = has_review_selection and not busy and not self._config_dirty
        self.prev_item_button.setEnabled(review_actions_enabled and review_index not in {None, 0})
        self.next_item_button.setEnabled(review_actions_enabled and review_index is not None and review_index < self.review_table.rowCount() - 1)
        self.next_blocked_button.setEnabled(
            has_controller
            and not busy
            and any(item.preview_block_reason is not None for item in self.controller.items)
        )
        self.accept_button.setEnabled(review_actions_enabled)
        self.skip_button.setEnabled(review_actions_enabled)
        self.search_button.setEnabled(review_actions_enabled)
        self.manual_button.setEnabled(review_actions_enabled)
        self.switch_button.setEnabled(review_actions_enabled)
        self.folder_button.setEnabled(review_actions_enabled)
        self.title_group_button.setEnabled(review_actions_enabled)
        self.next_page_button.setEnabled(review_actions_enabled and current_has_more)
        self.auto_accept_button.setEnabled(has_controller and not busy and not self._config_dirty)
        self.preview_button.setEnabled(can_preview)
        self.apply_button.setEnabled(can_apply)

        self.start_compress_button.setEnabled(can_start_compression)
        self.start_compress_button.setToolTip(self._compression_start_tooltip())
        self.include_risky_jobs.setVisible(bool(self._plan_classification.risky_follow_up))
        self.include_risky_jobs.setEnabled(has_compression_plan and not busy and not self._config_dirty)
        can_rebuild_safer = bool(
            has_compression_plan
            and not busy
            and not self._config_dirty
            and (
                self._compression_plan_is_blocked()
                or getattr(self.encode_preparation, "incompatible_count", 0) > 0
            )
        )
        self.rebuild_safer_button.setVisible(bool(has_compression_plan))
        self.rebuild_safer_button.setEnabled(can_rebuild_safer)
        followup_sources = self._followup_sources()
        can_followup = bool(has_compression_plan and followup_sources and not busy and not self._config_dirty)
        self.prepare_followup_button.setVisible(bool(has_compression_plan and followup_sources))
        self.prepare_followup_button.setEnabled(can_followup)
        can_retry = bool(self._retry_sources) and not busy and not self._config_dirty
        self.retry_failed_button.setVisible(bool(self._retry_sources))
        self.retry_failed_button.setEnabled(can_retry)
        self.retry_summary_button.setVisible(self.workflow_state == WorkflowState.COMPLETED and bool(self._retry_sources))
        self.retry_summary_button.setEnabled(can_retry)
        self.open_output_button.setVisible(self.workflow_state == WorkflowState.COMPLETED)
        diagnostics_visible = self._last_diagnostics_path is not None or self._last_diagnostics_error is not None
        self.open_diagnostics_button.setVisible(diagnostics_visible)
        self.copy_diagnostics_button.setVisible(diagnostics_visible)
        self.active_open_diagnostics_button.setVisible(True)
        self.active_copy_diagnostics_button.setVisible(True)
        self.save_summary_button.setVisible(self.workflow_state == WorkflowState.COMPLETED)

        show_encode_dashboard = has_compression_plan and self.compress_stack.currentIndex() == 2
        self.toggle_encode_card_button.setVisible(show_encode_dashboard)
        if show_encode_dashboard:
            self.encode_card.setVisible(not self.toggle_encode_card_button.isChecked())
        else:
            self.encode_card.setVisible(False)

        if self.workflow_state == WorkflowState.APPLYING:
            self.review_stack.setCurrentIndex(2)
        else:
            self.review_stack.setCurrentIndex(1 if has_controller else 0)
        if self.workflow_state == WorkflowState.PREPARING_COMPRESSION:
            self.compress_stack.setCurrentIndex(1)
        elif self.encode_preparation is None:
            self.compress_stack.setCurrentIndex(0)
        else:
            self.compress_stack.setCurrentIndex(2)

        warnings = list(self._custom_warnings)
        if self.overwrite.isChecked() and self.compress_enabled.isChecked():
            warnings.append("Overwrite is enabled. Successful compression will replace originals in-place.")
        if busy:
            warnings.append(self._active_worker_warning_text())
        warning_text = "\n".join(warnings)
        self.warning_label.setText(warning_text)
        self.runtime_warnings_label.setText(warning_text or "No active warnings.")

        self.setup_hint_label.setText(self._setup_hint_text())
        self.review_hint_label.setText(self._review_hint_text())
        self.compress_hint_label.setText(self._compress_hint_text())
        self.review_placeholder_label.setText(self._review_placeholder_text())
        self.compress_empty_label.setText(self._compress_empty_text())
        self.next_action_label.setText(f"Recommended next action: {self._recommended_next_action()}")
        self.overwrite_warning_label.setText(
            "Compression replaces originals after successful encodes. Review the plan carefully before starting."
            if self.overwrite.isChecked() and self.compress_enabled.isChecked()
            else ""
        )
        self.current_action_label.setText(self._current_action)
        self.last_completed_label.setText(self._last_completed_action)
        self.activity_label.setText(f"Current activity: {self._strip_rich(self._current_action)}")
        self.active_diagnostics_label.setText(self._diagnostics_status_text())
        self._update_setup_summary()
        self._update_review_summary()
        self._update_compress_summary()

    def _setup_hint_text(self) -> str:
        if self._config_dirty and (self.controller is not None or self.encode_preparation is not None):
            return "Settings changed after runtime data was created. Re-run the affected stage before continuing."
        if self._restored_state_warnings:
            return "\n".join(self._restored_state_warnings)
        if self.workflow_state == WorkflowState.SETUP:
            if not self.organise_enabled.isChecked() and self.compress_enabled.isChecked():
                return (
                    "Compression-only mode: mediashrink will scan the Source folder "
                    "(or your chosen Compression Root) directly. Library / Output Folder is not used in this run."
                )
            return "Start with the guided pipeline unless you only want a manual organise review or a compression-only run."
        return "Setup controls stay available, but later stages will ask you to rebuild stale data after changes."

    def _active_worker_warning_text(self) -> str:
        if self.workflow_state == WorkflowState.SCANNING:
            return "Organise scan is running. Mediaflow will move to review automatically when plexify finishes."
        if self.workflow_state == WorkflowState.APPLYING:
            return "Organisation is still being applied. Compression will not start until every planned copy or move finishes."
        if self.workflow_state == WorkflowState.PREPARING_COMPRESSION:
            return "Compression planning is running. Mediaflow will switch to the compression plan as soon as mediashrink finishes analysing files."
        if self.workflow_state == WorkflowState.COMPRESSING:
            return "Compression is running. Originals are only replaced after each successful encode."
        return "A background task is currently running."

    def _review_hint_text(self) -> str:
        if self.controller is None:
            return "This step loads suggested plexify matches for each discovered item."
        if self._config_dirty:
            return "Review data is stale because setup changed. Start a new organise review."
        blocked_count = sum(1 for item in self.controller.items if item.preview_block_reason is not None)
        if self.controller.items and blocked_count == len(self.controller.items):
            return "Scan finished, but every item still needs attention before organisation can continue."
        if self.preview_state is None:
            return "Accept, skip, or refine each item. Then build a preview."
        if self.preview_state.can_apply:
            return "Organisation preview is ready to apply."
        reason_summary = self._summarize_blocked_reasons()
        if reason_summary:
            return (
                "Some items are still blocked. Fix the listed reasons or skip them before applying organisation.\n"
                f"Top blocker reasons: {reason_summary}"
            )
        return "Some items are still blocked. Fix the listed reasons or skip them before applying organisation."

    def _review_placeholder_text(self) -> str:
        if self.workflow_state == WorkflowState.SCANNING:
            elapsed = (
                f" ({self._format_elapsed(time.monotonic() - self._scan_started_at)})"
                if self._scan_started_at is not None
                else ""
            )
            detail = (
                f"\nDiscovered so far: {self._scan_discovered_count}"
                if self._scan_discovered_count
                else "\nDiscovered so far: 0"
            )
            current_file = (
                f"\nCurrent file: {Path(self._scan_last_path).name}"
                if self._scan_last_path
                else "\nCurrent file: waiting for plexify results"
            )
            return (
                f"Scanning source with plexify{elapsed}...\n\n"
                "Discovered items and candidate matches will appear here when the scan finishes."
                f"{detail}{current_file}"
            )
        if self.controller is not None and not self.controller.items:
            return (
                "No organise candidates were discovered in the source folder.\n\n"
                "Check the Source path and organise filters, then start a new organise review."
            )
        return "No organise review is loaded yet.\n\nStart the guided pipeline or load organise matches from Setup."

    def _compress_hint_text(self) -> str:
        if self.workflow_state == WorkflowState.PREPARING_COMPRESSION:
            return "Scanning the compression root and assembling a compression plan."
        if self.encode_preparation is None:
            return "Compression planning only starts after you prepare a plan from Setup or after organisation finishes."
        if self._config_dirty:
            return "Compression plan is stale because setup changed. Prepare the plan again."
        if not self.encode_preparation.jobs:
            return "No compressible files are currently selected in the compression plan."
        if self._compression_plan_is_blocked():
            return "Compression needs attention before it can start. Rebuild with safer compatibility-first settings or prepare follow-up."
        if self.workflow_state == WorkflowState.COMPRESSING:
            return "Compression is in progress. Avoid moving files in the compression root until the run finishes."
        if self.overwrite.isChecked():
            return "Review the plan and start compression. Originals will be replaced in-place after a successful encode."
        return "Review the compression plan and start encoding when you are ready."

    def _compress_empty_text(self) -> str:
        root = self.compression_root_input.text().strip() or "(not set)"
        linked = self.link_compression_root.isChecked()
        linked_to = "library / output folder" if self.organise_enabled.isChecked() else "source folder"
        reason = f" (linked to {linked_to})" if linked else ""
        return (
            "No compression plan is ready yet.\n\n"
            f"Compression Root: {root}{reason}\n"
            "Prepare a compression plan from Setup to continue."
        )

    def _refresh_plan_view(self) -> None:
        if self.encode_preparation is None:
            return
        self._populate_compression_table(self.encode_preparation)
        self._update_compress_summary()

    def _recommended_next_action(self) -> str:
        if self.workflow_state == WorkflowState.SETUP:
            if not self.organise_enabled.isChecked() and self.compress_enabled.isChecked():
                return "Prepare Compression Plan"
            return "Start Guided Pipeline"
        if self.workflow_state in {WorkflowState.REVIEW, WorkflowState.REVIEW_BLOCKED}:
            return "Resolve review items and build an organisation preview"
        if self.workflow_state == WorkflowState.READY_TO_APPLY:
            return "Apply Organisation"
        if self.workflow_state == WorkflowState.READY_TO_COMPRESS:
            return "Start Compression"
        if self.workflow_state == WorkflowState.FAILED:
            return "Read the error summary, adjust settings, and rerun the affected stage"
        return "Wait for the current stage to finish"

    def _update_setup_summary(self) -> None:
        lines = [f"Source: {self.source_input.text().strip() or '(not set)'}"]
        if self.organise_enabled.isChecked():
            lines.append(f"Library / Output Folder: {self.library_input.text().strip() or '(not set)'}")
        lines += [
            f"Compression Root: {self.compression_root_input.text().strip() or '(not set)'}",
            f"Organise enabled: {'yes' if self.organise_enabled.isChecked() else 'no'}",
            f"Compress enabled: {'yes' if self.compress_enabled.isChecked() else 'no'}",
        ]
        if self.compress_enabled.isChecked() and self.overwrite.isChecked() and not self.organise_enabled.isChecked():
            lines.append("Output mode: files will be replaced in-place in the compression root.")
        self.setup_summary_label.setText("\n".join(lines))

    def _update_review_summary(self) -> None:
        if self.controller is None:
            self.review_summary_label.setText("No organise review loaded.")
            self.review_blocked_label.setText("")
            return
        if not self.controller.items:
            self.review_summary_label.setText("No organise candidates found in the last scan.")
            self.review_blocked_label.setText("")
            return
        total = len(self.controller.items)
        blocked = [item for item in self.controller.items if item.preview_block_reason is not None]
        accepted = sum(
            1 for item in self.controller.items
            if item.decision_status == "accepted" and item.preview_block_reason is None
        )
        manual = sum(
            1 for item in self.controller.items
            if item.decision_status == "manual" and item.preview_block_reason is None
        )
        skipped = sum(1 for item in self.controller.items if item.decision_status == "skipped")
        unresolved = len(blocked)
        self.review_summary_label.setText(
            f"Items: {total} | Accepted: {accepted} | Manual: {manual} | Skipped: {skipped} | Unresolved: {unresolved}"
        )
        if blocked:
            sample = blocked[0]
            grouped = self._summarize_blocked_reasons()
            self.review_blocked_label.setText(
                f"Why apply is blocked: {len(blocked)} item(s) still need attention. "
                f"Next blocker: {sample.item.path.name} — {sample.preview_block_reason}"
                + (f" | Top reasons: {grouped}" if grouped else "")
            )
        else:
            self.review_blocked_label.setText("All reviewed items are preview-valid.")

    def _update_compress_summary(self) -> None:
        if self.encode_preparation is None:
            self.compress_summary_label.setText("No compression plan prepared.")
            return
        prep = self.encode_preparation
        runnable_sources = self._runnable_sources()
        risky_sources = {row.source for row in self._plan_classification.risky_follow_up}
        deferred_sources = risky_sources - runnable_sources
        selected_input_bytes = sum(
            row.estimated_output_bytes + row.estimated_savings_bytes
            for row in self._compression_plan_rows
            if row.source in runnable_sources
        )
        selected_estimated_output_bytes = sum(
            row.estimated_output_bytes
            for row in self._compression_plan_rows
            if row.source in runnable_sources
        )
        savings = max(selected_input_bytes - selected_estimated_output_bytes, 0)
        lines = [
            f"Compression Root: {prep.directory}",
            f"Input:    {self._format_bytes(selected_input_bytes)} across {len(runnable_sources)} file(s)",
            f"Output:   {self._format_bytes(selected_estimated_output_bytes)} estimated",
            f"Savings:  {self._format_bytes(savings)} expected",
            f"Plan:     {prep.recommended_count} recommended  |  {prep.maybe_count} maybe  |  {prep.skip_count} skipped",
            f"Selected: {len(prep.jobs)} planned job(s)  |  {len(runnable_sources)} runnable now",
        ]
        if not prep.jobs:
            lines.append(
                "No encode jobs were auto-selected from this analysis. Recommended rows are analysis results only until "
                "a runnable profile and job set are chosen."
            )
        elif getattr(prep, "compatible_count", 0) <= 0:
            lines.append(
                "The current profile is predicted to work for 0 file(s). Rebuild with a safer compatibility-first profile before starting."
            )
        elif self._compression_has_blocking_risk():
            lines.append(
                "Start blocked: hardware or container compatibility risk was detected. Rebuild with a safer compatibility-first profile before encoding."
            )
        elif not runnable_sources:
            lines.append(
                "The current plan has no runnable jobs in this view. Include risky follow-up jobs or rebuild the plan."
            )
        lines.append(
            f"Run split: {len(self._plan_classification.safe_selected)} safe now  |  "
            f"{len(self._plan_classification.risky_follow_up)} risky follow-up  |  "
            f"{len(self._plan_classification.informational_skips)} informational skips"
        )
        deferred_summary = self._summarize_deferred_plan_reasons()
        if deferred_sources and not self.include_risky_jobs.isChecked():
            lines.append(
                f"First run default: {len(runnable_sources)} safe file(s) selected now. "
                f"{len(deferred_sources)} risky file(s) are deferred unless you explicitly include them."
            )
            if deferred_summary:
                lines.append(f"Deferred details: {deferred_summary}")
        elif self.include_risky_jobs.isChecked() and risky_sources:
            lines.append(
                f"Risky follow-up items are included in the next run ({len(risky_sources)} file(s))."
            )
        elif deferred_summary:
            lines.append(f"Follow-up details: {deferred_summary}")
        if prep.compatible_count or prep.incompatible_count:
            lines.append(f"Compat:   {prep.compatible_count} compatible  |  {prep.incompatible_count} incompatible")
        if prep.size_confidence or prep.time_confidence:
            conf_parts = []
            if prep.size_confidence:
                conf_parts.append(f"size: {prep.size_confidence}")
            if prep.time_confidence:
                conf_parts.append(f"time: {prep.time_confidence}")
            lines.append(f"Confidence: {', '.join(conf_parts)}")
        if prep.grouped_incompatibilities:
            top = sorted(prep.grouped_incompatibilities.items(), key=lambda x: -x[1])[:3]
            lines.append("Incompatible codecs: " + ", ".join(f"{k}: {v}" for k, v in top))
        if prep.profile is not None:
            lines.append(
                f"Profile: {prep.profile.name} ({prep.profile.encoder_key}, CRF {prep.profile.crf})"
            )
        if prep.recommendation_reason:
            lines.append(f"Reason: {prep.recommendation_reason}")
        if prep.followup_manifest_path:
            lines.append(f"Follow-up manifest: {prep.followup_manifest_path}")
        if self._retry_sources:
            lines.append(
                "Retry mode: compatibility-first review plan for failed or risky files."
            )
        if self._compression_has_blocking_risk():
            lines.append("Suggested follow-up for output/header failures: review an MKV sidecar retry plan where available.")
        self.compress_summary_label.setText("\n".join(lines))

    def _append_status(self, text: str) -> None:
        clean = self._strip_rich(text)
        if not clean or clean == self._last_status_text:
            return
        self._last_status_text = clean
        self.compress_status_log.appendPlainText(clean)
        self._diagnostics.record_event("status", text=clean)

    def _set_summary_text(self, text: str) -> None:
        self.summary_log.setPlainText(text)

    def _summarise_error(self, message: str) -> tuple[str, str | None]:
        text = message.strip()
        if "Traceback (most recent call last):" in text:
            lines = [line.strip() for line in text.splitlines() if line.strip()]
            final_line = next((line for line in reversed(lines) if not line.startswith("^")), text)
            translated = self._translate_common_error(final_line)
            return translated, text
        return self._translate_common_error(text), None

    def _translate_common_error(self, text: str) -> str:
        lowered = text.lower()
        if "cannot find the file specified" in lowered:
            match = re.search(r"'([^']+)'", text)
            path_text = match.group(1) if match else None
            if path_text:
                return (
                    "A planned compression file is missing from the compression root. "
                    f"Expected file: {path_text}. Avoid moving files after planning starts."
                )
            return "A planned compression file is missing from the compression root."
        if "ffmpeg" in lowered or "ffprobe" in lowered:
            return "FFmpeg tools are unavailable. Run `mediaflow doctor` to confirm the compression toolchain."
        if "output header failure" in lowered:
            return (
                "The planned output format was not compatible with one or more streams. "
                "Prepare a retry plan for the failed items to use safer compatibility-first defaults."
            )
        if "container" in lowered and "incompat" in lowered:
            return (
                "A container compatibility issue blocked encoding. "
                "A retry plan can review safer output assumptions for the affected files."
            )
        if "compatibility check failed" in lowered:
            return "Installed plexify or mediashrink components are incompatible with this mediaflow build."
        return text

    def _on_toggle_encode_card(self, checked: bool) -> None:
        self.encode_card.setVisible(not checked)
        self.toggle_encode_card_button.setText("Show live view" if checked else "Hide live view")

    def _open_output_folder(self) -> None:
        path = self.compression_root_input.text().strip() or self.library_input.text().strip()
        if not path:
            return
        self._open_path(path)

    def _open_diagnostics_folder(self) -> None:
        target = self._diagnostics_directory_path()
        try:
            target.mkdir(parents=True, exist_ok=True)
        except OSError:
            fallback = get_config_dir()
            try:
                fallback.mkdir(parents=True, exist_ok=True)
            except OSError as exc:
                message = f"Unable to open diagnostics folder: {exc}"
                self._append_status(message)
                QMessageBox.warning(self, "mediaflow", message)
                return
            target = fallback
        self._open_path(str(target))

    def _copy_diagnostics_path(self) -> None:
        target = str(self._last_diagnostics_path or self._diagnostics_directory_path())
        QApplication.clipboard().setText(target)
        self._append_status(f"Copied diagnostics path: {target}")

    @staticmethod
    def _open_path(path: str) -> None:
        if sys.platform == "win32":
            import os
            os.startfile(path)  # noqa: S606
        else:
            import subprocess
            subprocess.run(["xdg-open", path], check=False)  # noqa: S603, S607

    def _preflight_check(self, preparation: EncodePreparation) -> str | None:
        """Return an error string if the output directory fails space or writability checks."""
        import shutil
        try:
            root = Path(str(preparation.directory))
            if not root.exists():
                return f"Compression root does not exist: {root}"
            usage = shutil.disk_usage(root)
            free_gb = usage.free / (1024 ** 3)
            required_bytes = recommended_headroom_bytes(preparation)
            if usage.free < required_bytes:
                required_gb = required_bytes / (1024 ** 3)
                return (
                    "The compression root may not have enough temporary working space for a safe in-place run. "
                    f"Available: {free_gb:.1f} GB. Recommended headroom: {required_gb:.1f} GB."
                )
            probe = root / ".mediaflow_write_probe"
            try:
                probe.write_bytes(b"")
                probe.unlink()
            except OSError:
                return f"Compression root is not writable: {root}"
        except Exception as exc:  # noqa: BLE001
            return f"Preflight check failed: {exc}"
        return None

    def _save_run_summary(self) -> None:
        path, _ = QFileDialog.getSaveFileName(
            self, "Save run summary", "mediaflow-summary.txt", "Text files (*.txt);;All files (*)"
        )
        if not path:
            return
        Path(path).write_text(self._build_summary_export_text(), encoding="utf-8")

    def _build_summary_export_text(self) -> str:
        sections = [
            self.summary_headline_label.text().strip(),
            self.summary_mode_label.text().strip(),
            self.diagnostics_path_label.text().strip(),
            self.summary_overview_label.text().strip(),
            self.summary_log.toPlainText().strip(),
        ]
        return "\n\n".join(section for section in sections if section)

    def _snapshot_config_for_diagnostics(self) -> dict[str, object]:
        payload = self._ui_state_payload()
        payload["workflow_state"] = self.workflow_state.value
        return payload

    def _summary_timeline_text(self) -> str:
        labels: list[str] = []
        started_at = self._diagnostics.started_at
        seen_kinds: set[str] = set()
        for event in self._diagnostics.events:
            kind = str(event.get("kind", ""))
            label = ""
            if kind == "guided_pipeline_started":
                label = "Guided start"
            elif kind == "scan_started":
                label = "Scan"
            elif kind == "scan_finished":
                label = "Scan finished"
            elif kind == "organisation_apply_started":
                label = "Apply start"
            elif kind == "manual_match":
                label = "Manual match"
            elif kind == "bulk_apply":
                label = "Bulk apply"
            elif kind == "organisation_preview_ready":
                label = "Preview"
            elif kind == "organisation_applied":
                label = "Apply"
            elif kind == "compression_preparation_started":
                label = "Prepare compression start"
            elif kind == "compression_prepared":
                label = "Prepare compression"
            elif kind == "compression_started":
                label = "Compression start"
            elif kind == "compression_complete":
                label = "Completion"
            if not label or kind in seen_kinds:
                continue
            timestamp = str(event.get("timestamp", ""))
            if timestamp:
                try:
                    seconds = max(0.0, (datetime.fromisoformat(timestamp) - started_at).total_seconds())
                    label = f"{label} ({self._format_elapsed(seconds)})"
                except ValueError:
                    pass
            seen_kinds.add(kind)
            labels.append(label)
        return f"Run timeline: {'  •  '.join(labels)}" if labels else ""

    def _event_time(self, kind: str) -> datetime | None:
        for event in self._diagnostics.events:
            if event.get("kind") != kind:
                continue
            timestamp = str(event.get("timestamp", ""))
            if not timestamp:
                return None
            try:
                return datetime.fromisoformat(timestamp)
            except ValueError:
                return None
        return None

    def _duration_between(self, start_kind: str, end_kind: str) -> str | None:
        start = self._event_time(start_kind)
        end = self._event_time(end_kind)
        if start is None or end is None:
            return None
        seconds = max(0.0, (end - start).total_seconds())
        return self._format_elapsed(seconds)

    def _timing_breakdown_lines(self) -> list[str]:
        lines: list[str] = []
        if self._startup_duration is not None:
            lines.append(f"Startup time: {self._format_elapsed(self._startup_duration)}")
        scan_duration = self._duration_between("scan_started", "scan_finished")
        if scan_duration is not None:
            lines.append(f"Scan duration: {scan_duration}")
        review_duration = self._duration_between("scan_finished", "organisation_apply_started")
        if review_duration is not None:
            lines.append(f"Review and manual match time: {review_duration}")
        apply_duration = self._duration_between("organisation_apply_started", "organisation_applied")
        if apply_duration is not None:
            lines.append(f"Apply time: {apply_duration}")
        prep_duration = self._duration_between("compression_preparation_started", "compression_prepared")
        if prep_duration is not None:
            lines.append(f"Plan preparation time: {prep_duration}")
        elif self._preparation_duration is not None:
            lines.append(f"Plan preparation time: {self._format_elapsed(self._preparation_duration)}")
        encode_duration = self._duration_between("compression_started", "compression_complete")
        if encode_duration is not None:
            lines.append(f"Compression time: {encode_duration}")
        if self._first_progress_delay is not None:
            lines.append(f"First encode progress update: {self._format_elapsed(self._first_progress_delay)}")
        return lines

    def _checkpoint_review_diagnostics(self) -> None:
        if self.controller is None:
            return
        self._flush_runtime_diagnostics()

    def _guided_preflight_text(self, config: PipelineConfig) -> str:
        lines = [
            f"Source: {config.source}",
            f"Library / Output Folder: {config.library}",
            f"Compression Root: {config.compression_root}",
            f"Organise enabled: {'yes' if config.plexify.enabled else 'no'}",
            f"Compress enabled: {'yes' if config.shrink.enabled else 'no'}",
            f"Compression root linked to output: {'yes' if self.link_compression_root.isChecked() else 'no'}",
            f"Overwrite originals after encode: {'yes' if config.shrink.overwrite else 'no'}",
        ]
        if config.plexify.enabled and not config.library.exists():
            lines.append("Output folder does not exist yet and will be created during organise.")
        if config.plexify.enabled and config.plexify.copy_mode:
            lines.append(
                "Copy mode is enabled. Organisation may take time on large files, and compression will only begin after every copy finishes."
            )
            try:
                exts = {ext.strip().lower() for ext in config.plexify.extensions.split(",") if ext.strip()}
                source_sizes = [
                    path.stat().st_size
                    for path in config.source.rglob("*")
                    if path.is_file() and path.suffix.lower() in exts
                ]
            except OSError:
                source_sizes = []
            if source_sizes:
                lines.append(
                    f"Visible source media: {len(source_sizes)} file(s), "
                    f"{self._format_bytes(sum(source_sizes))} total, largest {self._format_bytes(max(source_sizes))}."
                )
        return "\n".join(lines)

    def _flush_diagnostics(self, *, failure_message: str | None = None) -> None:
        summary = {
            "workflow_state": self.workflow_state.value,
            "organise_enabled": self.organise_enabled.isChecked(),
            "compress_enabled": self.compress_enabled.isChecked(),
            "warnings": list(self._custom_warnings),
            "summary_headline": self.summary_headline_label.text().strip(),
            "summary_overview": self.summary_overview_label.text().strip(),
            "last_known_activity": self._strip_rich(self._current_action),
            "active_diagnostics": self._diagnostics_status_text(),
        }
        failure = {"message": failure_message} if failure_message else None
        try:
            self._last_diagnostics_path = self._diagnostics.write(summary=summary, failure=failure)
            self._last_diagnostics_error = None
            self.diagnostics_path_label.setText(f"Diagnostics: {self._last_diagnostics_path}")
        except OSError as exc:
            self._last_diagnostics_path = None
            self._last_diagnostics_error = f"Unable to write diagnostics: {exc}"
            self.diagnostics_path_label.setText(self._last_diagnostics_error)
            if self._last_diagnostics_error not in self._custom_warnings:
                self._custom_warnings.append(self._last_diagnostics_error)
        self.active_diagnostics_label.setText(self._diagnostics_status_text())

    def _show_error(self, message: str) -> None:
        if self._shutting_down:
            return
        self._scan_timer.stop()
        self._apply_timer.stop()
        self._preparation_timer.stop()
        self._scan_started_at = None
        self._scan_last_update_at = None
        self._apply_started_at = None
        self._apply_last_update_at = None
        self._apply_progress = None
        self._preparation_last_update_at = None
        self._scan_discovered_count = 0
        self._scan_last_path = ""
        self._complete_action("Last operation failed")
        summary, technical_detail = self._summarise_error(message)
        self._record_warning(summary)
        self._diagnostics.record_event(
            "error",
            summary=summary,
            technical_detail=technical_detail or message,
            workflow_state=self.workflow_state.value,
        )
        self._set_state(WorkflowState.FAILED)
        self._set_summary_text(
            "\n".join(
                [
                    "Last operation failed.",
                    "",
                    summary,
                    "",
                    "Technical details are available in the error dialog." if technical_detail else "",
                ]
            ).strip()
        )
        self._switch_tab("summary")
        self._flush_runtime_diagnostics(failure_message=summary)
        dialog = QMessageBox(self)
        dialog.setIcon(QMessageBox.Critical)
        dialog.setWindowTitle("mediaflow")
        dialog.setText(summary)
        if technical_detail:
            dialog.setInformativeText("Technical details are available below.")
            dialog.setDetailedText(technical_detail)
        dialog.exec()

    def _start_worker(self, worker: FunctionWorker, on_result, on_progress=None) -> None:
        self._active_worker_count += 1
        self._worker_refs.add(worker)
        worker.signals.result.connect(on_result)
        worker.signals.error.connect(self._show_error)
        if on_progress is not None:
            worker.signals.progress.connect(on_progress)
        worker.signals.finished.connect(self._worker_finished)
        worker.signals.finished.connect(lambda w=worker: self._release_worker_ref(w))
        self.thread_pool.start(worker)
        self._update_ui()

    def _release_worker_ref(self, worker: FunctionWorker) -> None:
        self._worker_refs.discard(worker)

    def _worker_finished(self) -> None:
        self._active_worker_count = max(0, self._active_worker_count - 1)
        self._update_ui()

    def _start_scan(self) -> None:
        self._diagnostics.set_config(self._snapshot_config_for_diagnostics())
        try:
            linked_output_root = self.organise_enabled.isChecked() and self.compress_enabled.isChecked() and (
                self.compression_root_input.text().strip() == self.library_input.text().strip()
            )
            config = self._current_config(allow_missing_compression_root=linked_output_root)
        except ValueError as exc:
            self._show_error(str(exc))
            return
        if not self._ensure_compatibility():
            return
        if not config.plexify.enabled:
            self._show_error("Organise stage is disabled.")
            return
        self._persist_ui_state()
        self._reset_runtime_state()
        self._diagnostics.set_config(self._snapshot_config_for_diagnostics())
        self._diagnostics.record_event("scan_started", source=str(config.source), library=str(config.library))
        self._guided_mode = False
        self._continue_to_compress = False
        self._scan_discovered_count = 0
        self._scan_last_path = ""
        self._scan_started_at = time.monotonic()
        self._scan_last_update_at = self._scan_started_at
        self._scan_timer.start()
        self._set_current_action("Scanning source with plexify")
        self._set_state(WorkflowState.SCANNING)
        self._switch_tab("review")
        self._append_status("Scanning source with plexify...")
        self._flush_runtime_diagnostics()
        worker = FunctionWorker(scan_controller, build_video_controller(config))
        self._start_worker(worker, self._scan_complete, self._scan_progress)

    def _start_guided_pipeline(self) -> None:
        self._diagnostics.set_config(self._snapshot_config_for_diagnostics())
        try:
            linked_output_root = self.organise_enabled.isChecked() and self.compress_enabled.isChecked() and (
                self.compression_root_input.text().strip() == self.library_input.text().strip()
            )
            config = self._current_config(allow_missing_compression_root=linked_output_root)
        except ValueError as exc:
            self._show_error(str(exc))
            return
        if not self._ensure_compatibility():
            return
        if QMessageBox.question(
            self,
            "mediaflow",
            "Start guided pipeline with these settings?\n\n" + self._guided_preflight_text(config),
        ) != QMessageBox.Yes:
            return
        filters_reset = (
            not self._is_default_review_filter(self.review_filter_combo.currentText())
            or not self._is_default_compression_filter(self.compression_filter_combo.currentText())
        )
        self._reset_guided_filters()
        self._persist_ui_state()
        self._reset_runtime_state()
        self._diagnostics.set_config(self._snapshot_config_for_diagnostics())
        self._diagnostics.record_event(
            "guided_pipeline_started",
            source=str(config.source),
            library=str(config.library),
            compression_root=str(config.compression_root),
        )
        if filters_reset:
            self._diagnostics.record_event("guided_filters_reset", review_filter="All items", compression_filter="All plan items")
        self._guided_mode = True
        self._continue_to_compress = config.shrink.enabled
        if config.plexify.enabled:
            self._diagnostics.record_event("scan_started", source=str(config.source), library=str(config.library))
            self._scan_discovered_count = 0
            self._scan_last_path = ""
            self._scan_started_at = time.monotonic()
            self._scan_last_update_at = self._scan_started_at
            self._scan_timer.start()
            self._set_current_action("Starting guided organise review")
            self._set_state(WorkflowState.SCANNING)
            self._switch_tab("review")
            self._append_status("Starting guided pipeline with organise scan.")
            self._flush_runtime_diagnostics()
            worker = FunctionWorker(scan_controller, build_video_controller(config))
            self._start_worker(worker, self._scan_complete, self._scan_progress)
        else:
            self._append_status("Guided pipeline is skipping organisation and preparing compression.")
            self._prepare_compression_from_setup()

    def _scan_complete(self, controller: VideoUIController) -> None:
        self._scan_timer.stop()
        self._scan_started_at = None
        self._scan_last_update_at = None
        self.controller = controller
        self.preview_state = None
        self.apply_result = None
        self._config_dirty = False
        blocked_count = sum(1 for item in controller.items if item.preview_block_reason is not None)
        self._diagnostics.record_event(
            "scan_finished",
            discovered=len(controller.items),
            blocked=blocked_count,
        )
        self._populate_review_table()
        self._switch_tab("review")
        self._complete_action("Finished organise scan")
        if not controller.items:
            self._set_state(WorkflowState.REVIEW)
            self._set_current_action("Organise scan finished with no review items")
            self._append_status("No organise candidates were discovered in the source folder.")
            self._checkpoint_review_diagnostics()
            return
        if self._guided_mode:
            accepted = self._auto_accept_safe_matches()
            self._append_status(f"Auto-accepted {accepted} safe match(es).")
            self._preview_plan()
            if self.preview_state is not None and self.preview_state.can_apply:
                self._set_state(WorkflowState.READY_TO_APPLY)
                self._set_current_action("Organisation preview is ready")
            else:
                self._set_state(WorkflowState.REVIEW_BLOCKED)
                self._set_current_action("Manual review is required before organisation can continue")
        else:
            self._set_state(WorkflowState.REVIEW)
            self._set_current_action("Review organise matches")
            self._append_status(f"Loaded {len(controller.items)} item(s) for manual review.")
        self._checkpoint_review_diagnostics()

    def _scan_progress(self, payload: object) -> None:
        if not isinstance(payload, dict):
            return
        if payload.get("kind") != "scan_progress":
            return
        self._scan_discovered_count = int(payload.get("discovered", 0) or 0)
        self._scan_last_path = str(payload.get("path", "") or "")
        self._scan_last_update_at = time.monotonic()
        self.review_placeholder_label.setText(self._review_placeholder_text())
        current_file = Path(self._scan_last_path).name if self._scan_last_path else "waiting for plexify results"
        self._set_current_action(
            f"Scanning source with plexify: {self._scan_discovered_count} discovered • {current_file}"
        )
        self._diagnostics.record_event(
            "scan_progress",
            discovered=self._scan_discovered_count,
            path=self._scan_last_path,
        )
        self._flush_runtime_diagnostics(progress_only=True)

    def _populate_review_table(self) -> None:
        self.review_table.setRowCount(0)
        if self.controller is None:
            self._update_review_summary()
            return
        for row, item in enumerate(self.controller.items):
            self.review_table.insertRow(row)
            selected = ""
            if item.manual_candidate is not None:
                selected = f"Manual: {item.manual_candidate.title}"
            elif item.selected_candidate_index is not None and item.candidates:
                selected = item.candidates[item.selected_candidate_index].title
            season_episode = ""
            if item.item.season is not None and item.item.episode is not None:
                season_episode = f"S{item.item.season:02d}E{item.item.episode:02d}"
            values = [
                item.item.path.name,
                item.item.media_type,
                item.item.title,
                season_episode,
                selected,
                item.status_label,
                item.warning or item.preview_block_reason or item.unresolved_reason or "",
            ]
            for column, value in enumerate(values):
                self.review_table.setItem(row, column, QTableWidgetItem(str(value)))
        if self.controller.items:
            self.review_table.selectRow(0)
            self._populate_candidate_table(0)
            self._populate_detail_view(0)
        self._update_review_summary()
        count = len(self.controller.items) if self.controller else 0
        label = f"Review ({count})" if count else "Review"
        self.tabs.setTabText(1, label)
        self._apply_review_filter()
        self._update_ui()

    def _review_selection_changed(self) -> None:
        index = self._current_review_index()
        if index is None:
            self.details_log.clear()
            self.candidate_table.setRowCount(0)
            self.search_input.setPlaceholderText("Search query or manual title")
            self._update_ui()
            return
        self.search_input.clear()
        self._populate_candidate_table(index)
        self._populate_detail_view(index)
        self._update_ui()

    def _current_review_index(self) -> int | None:
        indexes = self.review_table.selectionModel().selectedRows() if self.review_table.selectionModel() else []
        if not indexes:
            return None
        return indexes[0].row()

    def _review_items_matching_filter(self) -> list[int]:
        if self.controller is None:
            return []
        mode = self.review_filter_combo.currentText()
        matches: list[int] = []
        for idx, item in enumerate(self.controller.items):
            show = True
            if mode == "Blocked only":
                show = item.preview_block_reason is not None
            elif mode == "Unresolved only":
                show = item.decision_status == "unresolved"
            elif mode == "Accepted/manual only":
                show = item.decision_status in {"accepted", "manual"} and item.preview_block_reason is None
            elif mode == "TV only":
                show = item.item.media_type == "tv"
            if show:
                matches.append(idx)
        return matches

    def _apply_review_filter(self) -> None:
        mode = self.review_filter_combo.currentText()
        matches = set(self._review_items_matching_filter())
        visible = 0
        for row in range(self.review_table.rowCount()):
            hide = row not in matches
            self.review_table.setRowHidden(row, hide)
            if not hide:
                visible += 1
        if self.review_table.rowCount() == 0:
            self.review_filter_status_label.setText("")
        elif visible == 0:
            self.review_filter_status_label.setText(
                f"Filtered view: '{mode}' is hiding every review row. Switch back to 'All items' to see the full review."
            )
        elif not self._is_default_review_filter(mode):
            self.review_filter_status_label.setText(
                f"Filtered view: showing {visible} of {self.review_table.rowCount()} review row(s) with '{mode}'."
            )
        else:
            self.review_filter_status_label.setText("")
        current = self._current_review_index()
        if current is None or self.review_table.isRowHidden(current):
            if matches:
                self.review_table.selectRow(next(iter(sorted(matches))))
            else:
                self.details_log.clear()
                self.candidate_table.setRowCount(0)
        if self.review_table.rowCount():
            self._diagnostics.record_event(
                "review_filter_applied",
                filter=mode,
                visible_rows=visible,
                total_rows=self.review_table.rowCount(),
            )
            if (not self._is_default_review_filter(mode) or visible == 0) and not self._syncing_diagnostics:
                self._flush_runtime_diagnostics()
        self._update_review_summary()

    def _visible_review_rows(self) -> list[int]:
        return [row for row in range(self.review_table.rowCount()) if not self.review_table.isRowHidden(row)]

    def _populate_candidate_table(self, review_index: int) -> None:
        self.candidate_table.setRowCount(0)
        if self.controller is None:
            return
        item = self.controller.items[review_index]
        for row, candidate in enumerate(item.candidate_states):
            self.candidate_table.insertRow(row)
            values = [candidate.title, candidate.year or "", candidate.source, f"{candidate.confidence:.2f}"]
            for column, value in enumerate(values):
                self.candidate_table.setItem(row, column, QTableWidgetItem(str(value)))
        if item.selected_candidate_index is not None and item.candidate_states:
            self.candidate_table.selectRow(item.selected_candidate_index)
        elif item.candidate_states:
            self.candidate_table.selectRow(0)

    def _populate_detail_view(self, review_index: int) -> None:
        if self.controller is None:
            self.details_log.clear()
            return
        item = self.controller.items[review_index]
        suggested_search = getattr(self.controller, "suggested_search_query", None)
        suggested_query = suggested_search(review_index) if callable(suggested_search) else None
        lines = [
            f"Path: {item.item.path}",
            f"Media type: {item.item.media_type}",
            f"Title: {item.item.title}",
            f"Search query: {getattr(item, 'search_query', '')}",
            f"Status: {item.status_label}",
            f"Cache context: {getattr(item, 'cache_context', '')}",
            f"Auto-selectable: {getattr(item, 'auto_selectable', False)}",
            f"Preview-valid: {'yes' if getattr(item, 'preview_valid', False) else 'no'}",
        ]
        if item.warning:
            lines.append(f"Warning: {item.warning}")
        if item.preview_block_reason:
            lines.append(f"Blocked: {item.preview_block_reason}")
        elif item.unresolved_reason:
            lines.append(f"Unresolved: {item.unresolved_reason}")
        if suggested_query and suggested_query.casefold() != item.item.title.casefold():
            lines.append(f"Suggested search: {suggested_query}")
        self.details_log.setPlainText("\n".join(lines))
        placeholder = "Search query or manual title"
        if suggested_query and suggested_query.casefold() != item.item.title.casefold():
            placeholder = f"Suggested search: {suggested_query}"
        self.search_input.setPlaceholderText(placeholder)

    def _selected_candidate_index(self) -> int:
        indexes = self.candidate_table.selectionModel().selectedRows() if self.candidate_table.selectionModel() else []
        if not indexes:
            return 0
        return indexes[0].row()

    def _prompt_manual_movie_selection(self, item) -> dict[str, object] | None:
        dialog = QDialog(self)
        dialog.setWindowTitle("Manual Movie Match")
        layout = QVBoxLayout(dialog)
        form = QFormLayout()

        typed_title, typed_year = self._parse_title_with_year(self.search_input.text())
        title_input = QLineEdit(typed_title or item.item.title)
        year_input = QLineEdit(str(item.item.year or typed_year or ""))
        year_input.setPlaceholderText("optional")

        form.addRow("Movie title", title_input)
        form.addRow("Year", year_input)
        layout.addLayout(form)

        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(dialog.accept)
        buttons.rejected.connect(dialog.reject)
        layout.addWidget(buttons)

        if dialog.exec() != QDialog.Accepted:
            return None
        title = title_input.text().strip()
        if not title:
            self._show_error("Enter a movie title first.")
            return None
        year = self._optional_year_from_text(year_input.text())
        if year == -1:
            self._show_error("Enter a four-digit year or leave it blank.")
            return None
        return {"title": title, "year": year}

    def _prompt_manual_tv_selection(self, item) -> dict[str, object] | None:
        dialog = QDialog(self)
        dialog.setWindowTitle("Manual TV Match")
        layout = QVBoxLayout(dialog)
        form = QFormLayout()

        suggested_query = None
        if self.controller is not None:
            try:
                suggested_search = getattr(self.controller, "suggested_search_query", None)
                if callable(suggested_search):
                    suggested_query = suggested_search(self.controller.items.index(item))
            except ValueError:
                suggested_query = None
        typed_title, typed_year = self._parse_title_with_year(self.search_input.text())
        title_input = QLineEdit(typed_title or suggested_query or item.item.title)
        year_input = QLineEdit(str(typed_year or item.item.year or ""))
        year_input.setPlaceholderText("optional")
        season_input = QSpinBox()
        season_input.setRange(0, 999)
        season_input.setSpecialValueText("(optional)")
        season_input.setValue(int(item.item.season or 0))
        episode_input = QSpinBox()
        episode_input.setRange(0, 9999)
        episode_input.setSpecialValueText("(optional)")
        episode_input.setValue(int(item.item.episode or 0))
        episode_title_input = QLineEdit(item.item.episode_title or "")

        form.addRow("Show title", title_input)
        form.addRow("Year", year_input)
        form.addRow("Season", season_input)
        form.addRow("Episode", episode_input)
        form.addRow("Episode title", episode_title_input)
        layout.addLayout(form)

        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        buttons.accepted.connect(dialog.accept)
        buttons.rejected.connect(dialog.reject)
        layout.addWidget(buttons)

        if dialog.exec() != QDialog.Accepted:
            return None
        title = title_input.text().strip()
        if not title:
            self._show_error("Enter a show title first.")
            return None
        year = self._optional_year_from_text(year_input.text())
        if year == -1:
            self._show_error("Enter a four-digit year or leave it blank.")
            return None
        return {
            "title": title,
            "year": year,
            "season": season_input.value() or None,
            "episode": episode_input.value() or None,
            "episode_title": episode_title_input.text().strip() or None,
        }

    def _move_review_selection(self, delta: int) -> None:
        rows = self._visible_review_rows()
        if not rows:
            return
        current = self._current_review_index()
        if current not in rows:
            self.review_table.selectRow(rows[0])
            return
        pos = rows.index(current)
        target = rows[max(0, min(len(rows) - 1, pos + delta))]
        self.review_table.selectRow(target)

    def _move_to_next_blocked_item(self) -> None:
        if self.controller is None:
            return
        blocked_rows = [
            idx for idx, item in enumerate(self.controller.items)
            if item.preview_block_reason is not None and not self.review_table.isRowHidden(idx)
        ]
        if not blocked_rows:
            return
        current = self._current_review_index()
        if current is None:
            self.review_table.selectRow(blocked_rows[0])
            return
        for idx in blocked_rows:
            if idx > current:
                self.review_table.selectRow(idx)
                return
        self.review_table.selectRow(blocked_rows[0])

    def _refresh_review(self) -> None:
        current = self._current_review_index()
        self._populate_review_table()
        if current is not None and self.review_table.rowCount() > 0:
            self.review_table.selectRow(min(current, self.review_table.rowCount() - 1))
        self._preview_plan(rebuild_only=True)

    def _accept_selected_candidate(self) -> None:
        if self.controller is None:
            return
        index = self._current_review_index()
        if index is None:
            return
        self.controller.accept_candidate(index, self._selected_candidate_index())
        self._refresh_review()

    def _skip_selected_item(self) -> None:
        if self.controller is None:
            return
        index = self._current_review_index()
        if index is None:
            return
        self.controller.skip_item(index)
        self._refresh_review()

    def _load_next_candidate_page(self) -> None:
        if self.controller is None:
            return
        index = self._current_review_index()
        if index is None:
            return
        self.controller.next_page(index)
        self._refresh_review()

    def _auto_accept_safe_matches(self) -> int:
        if self.controller is None:
            return 0
        accepted = 0
        for idx, item in enumerate(self.controller.items):
            if item.resolved or not item.auto_selectable or not item.candidates:
                continue
            self.controller.accept_candidate(idx, 0)
            accepted += 1
        self._refresh_review()
        return accepted

    def _search_current_item(self) -> None:
        if self.controller is None:
            return
        index = self._current_review_index()
        if index is None:
            return
        query = self.search_input.text().strip()
        if not query:
            suggested_search = getattr(self.controller, "suggested_search_query", None)
            query = suggested_search(index) if callable(suggested_search) else ""
        if not query:
            self._show_error("Enter a search query first.")
            return
        self.controller.refine_search(index, query)
        self._append_status(f"Ran a fresh search for item {index + 1}.")
        self._refresh_review()

    def _switch_current_item(self) -> None:
        if self.controller is None:
            return
        index = self._current_review_index()
        if index is None:
            return
        current = self.controller.items[index]
        target = "movie" if current.item.media_type == "tv" else "tv"
        self.controller.switch_media_type(index, target)
        self._append_status(f"Switched item {index + 1} to {target} lookup.")
        self._refresh_review()

    def _manual_select_current_item(self) -> None:
        if self.controller is None:
            return
        index = self._current_review_index()
        if index is None:
            return
        item = self.controller.items[index]
        if item.item.media_type == "tv":
            payload = self._prompt_manual_tv_selection(item)
            if payload is None:
                return
            self.controller.manual_select(index, **payload)
            self._diagnostics.record_event("manual_match", item=index + 1, **payload)
            season_text = payload["season"] if payload["season"] is not None else "-"
            episode_text = payload["episode"] if payload["episode"] is not None else "-"
            self._append_status(
                f"Manually selected '{payload['title']}' for item {index + 1} "
                f"(S{season_text} E{episode_text})."
            )
        else:
            payload = self._prompt_manual_movie_selection(item)
            if payload is None:
                return
            self.controller.manual_select(index, **payload)
            self._diagnostics.record_event("manual_match", item=index + 1, **payload)
            year_text = f" ({payload['year']})" if payload.get("year") else ""
            self._append_status(f"Manually selected '{payload['title']}{year_text}' for item {index + 1}.")
        self.search_input.clear()
        self._refresh_review()

    def _apply_choice_to_folder(self) -> None:
        if self.controller is None:
            return
        index = self._current_review_index()
        if index is None:
            return
        result = self.controller.apply_choice_to_folder(index)
        self._diagnostics.record_event(
            "bulk_apply",
            mode="folder",
            item=index + 1,
            affected=result.affected_count,
            preview_valid=result.preview_valid_count,
            blocked=result.blocked_count,
        )
        self._append_status(
            f"Applied the current decision to {result.affected_count} folder item(s): "
            f"{result.preview_valid_count} preview-valid, {result.blocked_count} blocked."
        )
        self._refresh_review()

    def _apply_choice_to_title_group(self) -> None:
        if self.controller is None:
            return
        index = self._current_review_index()
        if index is None:
            return
        result = self.controller.apply_choice_to_title_group(index)
        self._diagnostics.record_event(
            "bulk_apply",
            mode="title-group",
            item=index + 1,
            affected=result.affected_count,
            preview_valid=result.preview_valid_count,
            blocked=result.blocked_count,
        )
        self._append_status(
            f"Applied the current decision to {result.affected_count} title-group item(s): "
            f"{result.preview_valid_count} preview-valid, {result.blocked_count} blocked."
        )
        self._refresh_review()

    def _render_preview_summary(self) -> None:
        if self.preview_state is None:
            self.preview_log.clear()
            return
        lines = list(self.preview_state.summary_lines)
        if self.preview_state.unresolved_items:
            lines.extend(["", "Unresolved:"])
            lines.extend(self.preview_state.unresolved_items[:10])
        if self.preview_state.warnings:
            lines.extend([""])
            lines.extend(f"Warning: {warning}" for warning in self.preview_state.warnings[:10])
        self.preview_log.setPlainText("\n".join(lines))

    def _preview_plan(self, rebuild_only: bool = False) -> None:
        if self.controller is None:
            return
        self.preview_state = build_preview(self.controller)
        self._render_preview_summary()
        if self.preview_state.can_apply:
            self._set_state(WorkflowState.READY_TO_APPLY)
            self._set_current_action("Organisation preview is ready")
        else:
            self._set_state(WorkflowState.REVIEW_BLOCKED)
            self._set_current_action("Manual review is still required")
        if not rebuild_only:
            self._diagnostics.record_event(
                "organisation_preview_ready",
                can_apply=self.preview_state.can_apply,
                blocked=self.preview_state.unresolved_count,
            )
            self._append_status("Built organisation preview.")
            self._complete_action("Built organisation preview")
        self._checkpoint_review_diagnostics()

    def _organisation_preflight_lines(self) -> list[str]:
        if self.preview_state is None:
            return ["No organisation preview is ready."]
        plans = list(self.preview_state.plans)
        operation = "copy" if self.copy_mode.isChecked() else "move"
        sizes: list[tuple[Path, int]] = []
        for plan in plans:
            try:
                sizes.append((plan.source, plan.source.stat().st_size))
            except OSError:
                sizes.append((plan.source, 0))
        total_bytes = sum(size for _path, size in sizes)
        largest_path, largest_size = max(sizes, key=lambda item: item[1], default=(Path(), 0))
        lines = [
            f"Planned operations: {len(plans)} {operation}(s)",
            f"Total source size: {self._format_bytes(total_bytes)}",
            f"Largest file: {largest_path.name if largest_size else '(unknown)'}"
            + (f" ({self._format_bytes(largest_size)})" if largest_size else ""),
            f"Conflict policy: {self.conflict_mode.currentText()}",
        ]
        if self.copy_mode.isChecked():
            lines.append(f"Estimated destination space required: {self._format_bytes(total_bytes)}")
        lines.append("")
        lines.append("Plan:")
        for index, plan in enumerate(plans[:12], start=1):
            try:
                size = plan.source.stat().st_size
            except OSError:
                size = 0
            destination_state = "exists" if plan.destination.exists() else "new"
            lines.append(
                f"{index}. {plan.source.name} -> {plan.destination.name} "
                f"({self._format_bytes(size)}, {destination_state})"
            )
        return lines

    def _organisation_preflight_error(self) -> str | None:
        if self.preview_state is None or not self.copy_mode.isChecked():
            return None
        import shutil
        plans = list(self.preview_state.plans)
        if not plans:
            return None
        required = 0
        for plan in plans:
            try:
                required += plan.source.stat().st_size
            except OSError:
                continue
        try:
            root = Path(self.library_input.text().strip())
            usage_root = root if root.exists() else next((parent for parent in root.parents if parent.exists()), root)
            usage = shutil.disk_usage(usage_root)
        except OSError as exc:
            return f"Organisation output folder is not writable: {exc}"
        headroom = 512 * 1024 * 1024
        if usage.free < required + headroom:
            return (
                "The organisation output folder may not have enough free space for copy mode. "
                f"Available: {self._format_bytes(usage.free)}. "
                f"Required plus headroom: {self._format_bytes(required + headroom)}."
            )
        return None

    def _apply_plan(self) -> None:
        if self.controller is None:
            self._show_error("No organise review is loaded.")
            return
        if self._config_dirty:
            self._show_error("Settings changed after the organise scan. Start a new organise review before applying.")
            return
        if self.preview_state is None:
            self._preview_plan(rebuild_only=True)
        if self.preview_state is None or not self.preview_state.can_apply:
            self._show_error("Resolve or skip all unresolved items before applying organisation.")
            return
        preflight_lines = self._organisation_preflight_lines()
        preflight_error = self._organisation_preflight_error()
        if preflight_error:
            self._show_error(preflight_error)
            return
        confirm_text = "Apply the current organisation plan to disk?\n\n" + "\n".join(preflight_lines[:18])
        if len(preflight_lines) > 18:
            confirm_text += f"\n... {len(preflight_lines) - 18} more planned operation(s)"
        if QMessageBox.question(self, "mediaflow", confirm_text) != QMessageBox.Yes:
            return
        self._set_current_action("Applying organisation to disk")
        self._set_state(WorkflowState.APPLYING)
        self._diagnostics.record_event(
            "organisation_apply_started",
            planned=self.preview_state.planned_count if self.preview_state is not None else 0,
        )
        self._append_status("Applying organisation plan...")
        self._apply_started_at = time.monotonic()
        self._apply_last_update_at = self._apply_started_at
        self._apply_progress = ApplyProgress(
            phase="starting",
            completed=0,
            total=self.preview_state.planned_count if self.preview_state is not None else 0,
            message="Opening organise report and starting file operations.",
        )
        self._apply_progress_model.reset()
        self._apply_cancel_requested = False
        self.apply_log.clear()
        for line in preflight_lines:
            self.apply_log.appendPlainText(line)
            self._apply_progress_model.event_log.append(line)
        self._apply_progress_model.update_from_progress(self._apply_progress, now=0.0)
        self._last_apply_log_key = None
        self._refresh_apply_dashboard()
        self._apply_timer.start()
        self._flush_runtime_diagnostics()
        worker = FunctionWorker(
            apply_preview_controller,
            self.controller,
            self.preview_state,
            cancel_callback=self._apply_cancel_requested_callback,
        )
        self._start_worker(worker, self._apply_complete, self._apply_progress_update)

    def _apply_complete(self, result: ApplyResultState) -> None:
        self._apply_timer.stop()
        self._apply_started_at = None
        self._apply_last_update_at = None
        self.apply_result = result
        self._diagnostics.record_event(
            "organisation_applied",
            moved_count=len(tuple(getattr(getattr(result, "result", None), "moved", []) or [])),
            skipped_count=len(tuple(getattr(getattr(result, "result", None), "skipped", []) or [])),
            error_count=len(tuple(getattr(getattr(result, "result", None), "errors", []) or [])),
        )
        for warning in getattr(result, "warnings", []) or []:
            self._record_warning(str(warning))
        self._complete_action("Organisation stage complete")
        self._append_status("Organisation stage complete.")
        self._refresh_pipeline_summary()
        self._flush_runtime_diagnostics()
        if self._guided_mode and self._continue_to_compress:
            if not self._guided_compression_can_continue():
                self._set_current_action("Organisation completed, but guided compression cannot continue automatically")
                self._append_status("Organisation completed, but compression cannot continue automatically.")
                self._set_state(WorkflowState.COMPLETED)
                self._switch_tab("summary")
                self._flush_runtime_diagnostics()
                return
            self._set_current_action("Organisation complete. Preparing compression plan for organised output")
            self._append_status("Organisation complete. Starting compression preparation.")
            self._append_status("Preparing compression plan after organisation.")
            self._prepare_compression_after_apply()
            return
        self._set_state(WorkflowState.COMPLETED)
        self._set_current_action("Pipeline finished")
        self._switch_tab("summary")
        self._flush_runtime_diagnostics()
        self._notify_completion("Organisation complete", "Organisation stage finished.")

    def _apply_progress_update(self, payload: object) -> None:
        if not isinstance(payload, ApplyProgress):
            return
        if self._apply_cancel_requested and not payload.cancel_requested:
            payload = replace(payload, cancel_requested=True)
        self._apply_progress = payload
        self._apply_last_update_at = time.monotonic()
        elapsed = (
            self._apply_last_update_at - self._apply_started_at
            if self._apply_started_at is not None
            else 0.0
        )
        self._apply_progress_model.update_from_progress(payload, now=elapsed)
        status_line = self._format_apply_status_text(payload)
        self._set_current_action(status_line)
        self._refresh_apply_dashboard()
        log_key = (
            payload.phase,
            payload.completed,
            payload.total,
            payload.current_source or payload.last_applied_source or "",
        )
        if log_key == self._last_apply_log_key:
            return
        self._last_apply_log_key = log_key
        self._diagnostics.record_event(
            "organisation_apply_progress",
            phase=payload.phase,
            completed=payload.completed,
            total=payload.total,
            current_source=payload.current_source,
            current_destination=payload.current_destination,
            last_applied_source=payload.last_applied_source,
            message=payload.message,
            operation=payload.operation,
            source_size_bytes=payload.source_size_bytes,
            report_path=payload.report_path,
            conflict_action=payload.conflict_action,
            error=payload.error,
            cancel_requested=payload.cancel_requested,
        )
        self._append_status(status_line.replace("\n", " • "))
        self._flush_runtime_diagnostics(progress_only=True)

    def _guided_compression_can_continue(self) -> bool:
        compression_root = Path(self.compression_root_input.text().strip())
        if not compression_root.exists():
            self._show_error("Compression root does not exist after the organise stage completed.")
            return False
        if self.link_compression_root.isChecked() and self.preview_state is not None and self.preview_state.plans:
            existing_outputs = [plan.destination for plan in self.preview_state.plans if plan.destination.exists()]
            if not existing_outputs:
                self._record_warning(
                    "Organisation finished, but no planned outputs were found in the library / output folder. Compression will not start automatically."
                )
                self._refresh_pipeline_summary()
                return False
        return True

    def _prepare_compression_from_setup(self) -> None:
        self._guided_mode = False
        self._continue_to_compress = False
        self._start_compression_preparation("Preparing compression plan from Setup.")

    def _prepare_compression_after_apply(self) -> None:
        self._start_compression_preparation("Preparing compression plan for the organised output.")

    def _start_compression_preparation(self, status_message: str) -> None:
        self._diagnostics.set_config(self._snapshot_config_for_diagnostics())
        try:
            config = self._current_config()
        except ValueError as exc:
            self._show_error(str(exc))
            return
        if not self._ensure_compatibility():
            return
        if not config.shrink.enabled:
            self._show_error("Compress stage is disabled.")
            return
        self._persist_ui_state()
        self._diagnostics.set_config(self._snapshot_config_for_diagnostics())
        self._diagnostics.record_event(
            "compression_preparation_started",
            compression_root=str(config.compression_root),
            overwrite=config.shrink.overwrite,
            policy=config.shrink.policy,
            on_file_failure=config.shrink.on_file_failure,
        )
        self._apply_progress = None
        self._retry_sources = set()
        if self._config_dirty and self.encode_preparation is not None:
            self.encode_preparation = None
            self.compression_table.setRowCount(0)
        self.prepare_progress.setRange(0, 0)
        self.file_progress.setValue(0)
        self.overall_progress.setValue(0)
        self.compress_status_log.clear()
        self._preparation_model = PreparationProgressModel()
        self.compress_preparing_label.setText("Preparing a compression plan...")
        self.prepare_elapsed_label.setText("")
        self.prepare_stage_label.setText("Discovering files...")
        self.prepare_counts_label.setText("0 file(s) discovered • 0.0 B")
        self.prepare_timeline_label.setText(self._preparation_timeline_text("discovering"))
        self._preparation_duration = None
        self.prepare_log.clear()
        self.prepare_log.appendPlainText("Preparing compression plan...")
        self._set_current_action(f"Scanning compression root {config.compression_root}")
        self._set_state(WorkflowState.PREPARING_COMPRESSION)
        self._switch_tab("compress")
        self._append_status(status_message)
        self._preparation_start = time.monotonic()
        self._preparation_last_update_at = self._preparation_start
        self._preparation_timer.start()
        self._flush_runtime_diagnostics()
        worker = FunctionWorker(prepare_compression, config)
        self._start_worker(worker, self._compression_prepared, self._preparation_progress)

    def _preparation_progress(self, payload: object) -> None:
        if isinstance(payload, PreparationStageUpdate):
            clean = self._strip_rich(payload.message)
            self._preparation_last_update_at = time.monotonic()
            self._preparation_model.update_stage(
                payload.stage,
                clean,
                completed=payload.completed,
                total=payload.total,
            )
            self.prepare_stage_label.setText(self._preparation_stage_title(payload.stage))
            self.prepare_timeline_label.setText(self._preparation_timeline_text(self._preparation_model.stage_key))
            self.compress_preparing_label.setText(clean)
            self.prepare_log.appendPlainText(clean)
            self._set_current_action(clean)
            self.prepare_progress.setRange(0, 100)
            self.prepare_progress.setValue(int(self._preparation_model.progress_ratio * 100))
            self._diagnostics.record_event(
                "preparation_stage",
                stage=self._preparation_model.stage_key,
                message=clean,
                completed=payload.completed,
                total=payload.total,
            )
            self._flush_runtime_diagnostics()
            return
        if not isinstance(payload, PreparationProgress):
            return
        self._preparation_last_update_at = time.monotonic()
        completed, total, path = payload.completed, payload.total, payload.path
        file_size = 0
        if path:
            try:
                file_size = Path(path).stat().st_size
            except OSError:
                file_size = 0
        file_name = Path(path).name
        self._preparation_model.update_analysis(completed, total, file_name, file_size)
        self.prepare_stage_label.setText("Analysing files")
        self.prepare_timeline_label.setText(self._preparation_timeline_text("analysing"))
        self.prepare_progress.setRange(0, 100)
        self.prepare_progress.setValue(int(self._preparation_model.progress_ratio * 100))
        self.prepare_counts_label.setText(
            f"{self._preparation_model.discovered_files} file(s) discovered • "
            f"{self._format_bytes(self._preparation_model.discovered_bytes)}"
        )
        self.compress_preparing_label.setText(f"Analysing {completed} of {total} file(s)")
        self._set_current_action(f"Analysing {completed}/{total}: {file_name}")
        self.prepare_log.appendPlainText(f"[{completed}/{total}] {file_name}")
        self._diagnostics.record_event(
            "preparation_file",
            completed=completed,
            total=total,
            path=path,
            size_bytes=file_size,
        )

    def _compression_prepared(self, preparation: EncodePreparation) -> None:
        self._preparation_timer.stop()
        self._preparation_last_update_at = None
        self._preparation_duration = time.monotonic() - self._preparation_start
        self.prepare_elapsed_label.setText("")
        self._preparation_model.mark_ready()
        self.prepare_stage_label.setText("Plan ready.")
        self.prepare_timeline_label.setText(self._preparation_timeline_text("plan-ready"))
        self.encode_preparation = preparation
        self.prepare_progress.setRange(0, 100)
        self.prepare_progress.setValue(100)
        self.include_risky_jobs.setChecked(False)
        self._populate_compression_table(preparation)
        self._config_dirty = False
        self._refresh_pipeline_summary()
        self._switch_tab("compress")
        self._complete_action("Compression plan prepared")
        if not preparation.items:
            self._set_current_action("Compression root scan finished with no supported video files")
            self._append_status("No supported video files found in the compression root.")
            self._set_state(WorkflowState.READY_TO_COMPRESS)
            self._diagnostics.record_event("compression_prepared", selected_count=0, recommended_count=0, maybe_count=0, skip_count=0)
            self._flush_runtime_diagnostics()
            return
        if not preparation.jobs:
            detail = self._compression_zero_jobs_message(preparation)
            self._set_current_action(detail)
            self._append_status(detail)
            self._set_state(WorkflowState.READY_TO_COMPRESS)
            self._diagnostics.record_event(
                "compression_prepared",
                selected_count=0,
                recommended_count=preparation.recommended_count,
                maybe_count=preparation.maybe_count,
                skip_count=preparation.skip_count,
            )
            self._flush_runtime_diagnostics()
            return
        if preparation.stage_messages:
            for line in preparation.stage_messages:
                self._append_status(line)
                clean = self._strip_rich(line)
                if clean not in self.prepare_log.toPlainText():
                    self.prepare_log.appendPlainText(clean)
        if preparation.duplicate_warnings:
            for warning in preparation.duplicate_warnings[:10]:
                self._record_warning(warning)
                self._append_status(f"Duplicate warning: {warning}")
        blocked_detail = None
        if getattr(preparation, "compatible_count", 0) <= 0:
            blocked_detail = (
                "Compression plan needs attention. The selected profile is not safe for this batch."
            )
        elif self._compression_has_blocking_risk():
            blocked_detail = (
                "Compression plan needs attention. Hardware or container compatibility risk blocks the default run."
            )
        elif not self._runnable_jobs(preparation):
            blocked_detail = self._compression_zero_jobs_message(preparation)
        self._set_current_action(blocked_detail or "Compression plan is ready to review")
        self._append_status(
            f"Prepared compression plan for {preparation.selected_count} file(s) from {preparation.directory}."
        )
        self._diagnostics.record_event(
            "compression_prepared",
            selected_count=preparation.selected_count,
            recommended_count=preparation.recommended_count,
            maybe_count=preparation.maybe_count,
            skip_count=preparation.skip_count,
            selected_input_bytes=preparation.selected_input_bytes,
            selected_estimated_output_bytes=preparation.selected_estimated_output_bytes,
            runnable_now=len(self._runnable_jobs(preparation)),
            deferred_risky=len({row.source for row in self._plan_classification.risky_follow_up} - self._runnable_sources()),
            missing_items=len(self._plan_classification.missing_items),
        )
        self._update_encode_dashboard(None)
        self._set_state(WorkflowState.READY_TO_COMPRESS)
        self._flush_runtime_diagnostics()

    def _compression_zero_jobs_message(self, preparation: EncodePreparation) -> str:
        if preparation.profile is None:
            return (
                "Compression analysis finished, but no encoder profile could be auto-selected. "
                "Review the plan details or rebuild the plan with different settings."
            )
        if getattr(preparation, "compatible_count", 0) <= 0:
            return (
                "Compression analysis finished, but the selected profile is not safe for this batch. "
                "Rebuild the plan with safer compatibility-first settings before starting."
            )
        if preparation.recommended_count or preparation.maybe_count:
            return (
                "Compression analysis finished, but no runnable jobs were selected. "
                "Recommended rows are available for review, but the current plan cannot start yet."
            )
        return "Compression plan contains no selected jobs."

    def _populate_compression_table(self, preparation: EncodePreparation | None) -> None:
        self.compression_table.setRowCount(0)
        self._compression_plan_rows = build_compression_plan_rows(preparation)
        self._plan_classification = classify_compression_plan(self._compression_plan_rows)
        if preparation is None:
            return
        self.compression_table.setSortingEnabled(False)
        for row, item in enumerate(self._compression_plan_rows):
            self.compression_table.insertRow(row)
            selected_text = "yes" if item.selected else "no"
            if item.selected and not item.exists:
                selected_text = "missing"
            elif item.classification == "risky-follow-up" and not self.include_risky_jobs.isChecked():
                selected_text = "deferred"
            est_output = self._format_bytes(item.estimated_output_bytes) if item.estimated_output_bytes else ""
            if item.estimated_savings_bytes and item.estimated_output_bytes:
                total_size = item.estimated_output_bytes + item.estimated_savings_bytes
                pct = int(100 * item.estimated_savings_bytes / total_size) if total_size else 0
                est_saving = f"{self._format_bytes(item.estimated_savings_bytes)} ({pct}%)"
            elif item.estimated_savings_bytes:
                est_saving = self._format_bytes(item.estimated_savings_bytes)
            else:
                est_saving = ""
            values = [
                item.display_name,
                item.codec or "",
                item.recommendation,
                item.plain_reason or item.reason,
                item.risk_reason or item.issue,
                est_output,
                est_saving,
                selected_text,
            ]
            for column, value in enumerate(values):
                cell = QTableWidgetItem(str(value))
                cell.setToolTip(str(value))
                self.compression_table.setItem(row, column, cell)
            self.compression_table.item(row, 0).setToolTip(str(item.source))
        self.compression_table.setSortingEnabled(True)
        self._apply_compression_filter()

    def _apply_compression_filter(self) -> None:
        mode = self.compression_filter_combo.currentText()
        visible_rows = 0
        for row_index in range(self.compression_table.rowCount()):
            selected_text = (self.compression_table.item(row_index, 7).text() if self.compression_table.item(row_index, 7) else "")
            recommendation = (self.compression_table.item(row_index, 2).text() if self.compression_table.item(row_index, 2) else "")
            issue = (self.compression_table.item(row_index, 4).text() if self.compression_table.item(row_index, 4) else "")
            reason = (self.compression_table.item(row_index, 3).text() if self.compression_table.item(row_index, 3) else "")
            hide = False
            if mode == "Runnable now":
                hide = selected_text != "yes" or "Deferred by default" in issue
            elif mode == "Follow-up / incompatible":
                hide = "Deferred by default" not in issue and not issue
            elif mode == "Informational skips":
                hide = "Already efficient codec" not in reason
            elif mode == "Selected only":
                hide = selected_text not in {"yes", "missing"}
            elif mode == "Recommended only":
                hide = recommendation != "recommended"
            elif mode == "Problem items":
                hide = not bool(issue)
            elif mode == "Missing items":
                hide = selected_text != "missing"
            self.compression_table.setRowHidden(row_index, hide)
            if not hide:
                visible_rows += 1
        if self.compression_table.rowCount() == 0:
            self.compression_filter_status_label.setText("")
        elif visible_rows == 0:
            self.compression_filter_status_label.setText(
                f"Filtered view: '{mode}' is hiding every compression row. Switch back to 'All plan items' to inspect the full plan."
            )
        elif not self._is_default_compression_filter(mode):
            self.compression_filter_status_label.setText(
                f"Filtered view: showing {visible_rows} of {self.compression_table.rowCount()} compression row(s) with '{mode}'."
            )
        else:
            self.compression_filter_status_label.setText("")
        if self.compression_table.rowCount():
            self._diagnostics.record_event(
                "compression_filter_applied",
                filter=mode,
                visible_rows=visible_rows,
                total_rows=self.compression_table.rowCount(),
            )
            if (not self._is_default_compression_filter(mode) or visible_rows == 0) and not self._syncing_diagnostics:
                self._flush_runtime_diagnostics()

    def _apply_summary_filter(self) -> None:
        mode = self.summary_filter_combo.currentText()
        visible_rows = 0
        for row_index in range(self.summary_table.rowCount()):
            status = (self.summary_table.item(row_index, 1).text() if self.summary_table.item(row_index, 1) else "")
            retry_ready = (
                self.summary_table.item(row_index, 5).data(Qt.UserRole)
                if self.summary_table.item(row_index, 5)
                else False
            )
            hide = False
            if mode == "Encoded only":
                hide = status != "Encoded"
            elif mode == "Failed only":
                hide = status != "Failed"
            elif mode == "Skipped only":
                hide = status != "Skipped"
            elif mode == "Retry-ready":
                hide = not bool(retry_ready)
            self.summary_table.setRowHidden(row_index, hide)
            if not hide:
                visible_rows += 1
        self.summary_filter_status_label.setText(
            f"No results match the '{mode}' filter." if visible_rows == 0 and self.summary_table.rowCount() else ""
        )

    def _prepare_retry_plan(self) -> None:
        if not self._retry_sources:
            self._show_error("There are no failed or compatibility-risk items to retry.")
            return
        self._diagnostics.set_config(self._snapshot_config_for_diagnostics())
        try:
            config = self._current_config()
        except ValueError as exc:
            self._show_error(str(exc))
            return
        self._persist_ui_state()
        self.encode_preparation = None
        self.encode_results = []
        self._config_dirty = False
        self._diagnostics.record_event(
            "retry_preparation_started",
            retry_sources=[str(path) for path in sorted(self._retry_sources)],
        )
        self.prepare_progress.setRange(0, 0)
        self.prepare_log.clear()
        self.prepare_log.appendPlainText("Preparing compatibility-first retry plan...")
        self.compress_preparing_label.setText("Preparing compatibility-first retry plan...")
        self.prepare_stage_label.setText("Discovering files...")
        self.prepare_counts_label.setText("0 file(s) discovered • 0.0 B")
        self.prepare_timeline_label.setText(self._preparation_timeline_text("discovering"))
        self._preparation_model = PreparationProgressModel()
        self._set_current_action("Preparing retry plan for failed or risky files")
        self._set_state(WorkflowState.PREPARING_COMPRESSION)
        self._switch_tab("compress")
        self._preparation_start = time.monotonic()
        self._preparation_timer.start()
        self._flush_runtime_diagnostics()
        worker = FunctionWorker(prepare_retry_compression, config, set(self._retry_sources))
        self._start_worker(worker, self._compression_prepared, self._preparation_progress)

    def _prepare_safer_plan(self) -> None:
        if self.encode_preparation is None:
            self._show_error("Prepare a compression plan before rebuilding it.")
            return
        self._diagnostics.set_config(self._snapshot_config_for_diagnostics())
        try:
            config = self._current_config()
        except ValueError as exc:
            self._show_error(str(exc))
            return
        self._persist_ui_state()
        self._diagnostics.record_event(
            "safer_preparation_started",
            compression_root=str(config.compression_root),
            previous_profile=(
                self.encode_preparation.profile.name if self.encode_preparation.profile is not None else None
            ),
        )
        self.prepare_progress.setRange(0, 0)
        self.prepare_log.clear()
        self.prepare_log.appendPlainText("Rebuilding compression plan with safer compatibility-first defaults...")
        self.compress_preparing_label.setText("Rebuilding compression plan with safer defaults...")
        self.prepare_stage_label.setText("Discovering files...")
        self.prepare_counts_label.setText("0 file(s) discovered • 0.0 B")
        self.prepare_timeline_label.setText(self._preparation_timeline_text("discovering"))
        self._preparation_model = PreparationProgressModel()
        self._set_current_action("Rebuilding compression plan with a safer profile")
        self._set_state(WorkflowState.PREPARING_COMPRESSION)
        self._switch_tab("compress")
        self._preparation_start = time.monotonic()
        self._preparation_last_update_at = self._preparation_start
        self._preparation_timer.start()
        self._flush_runtime_diagnostics()
        worker = FunctionWorker(prepare_safer_compression, config)
        self._start_worker(worker, self._compression_prepared, self._preparation_progress)

    def _prepare_followup_plan(self) -> None:
        sources = self._followup_sources()
        if not sources:
            self._show_error("There are no deferred or compatibility-risk items to prepare for follow-up.")
            return
        self._diagnostics.set_config(self._snapshot_config_for_diagnostics())
        try:
            config = self._current_config()
        except ValueError as exc:
            self._show_error(str(exc))
            return
        self._persist_ui_state()
        self.encode_results = []
        self._retry_sources = set(sources)
        self._diagnostics.record_event(
            "followup_preparation_started",
            retry_sources=[str(path) for path in sorted(sources)],
        )
        self.prepare_progress.setRange(0, 0)
        self.prepare_log.clear()
        self.prepare_log.appendPlainText("Preparing follow-up plan for deferred or incompatible items...")
        self.compress_preparing_label.setText("Preparing follow-up plan...")
        self.prepare_stage_label.setText("Discovering files...")
        self.prepare_counts_label.setText("0 file(s) discovered • 0.0 B")
        self.prepare_timeline_label.setText(self._preparation_timeline_text("discovering"))
        self._preparation_model = PreparationProgressModel()
        self._set_current_action("Preparing follow-up plan for deferred or incompatible items")
        self._set_state(WorkflowState.PREPARING_COMPRESSION)
        self._switch_tab("compress")
        self._preparation_start = time.monotonic()
        self._preparation_last_update_at = self._preparation_start
        self._preparation_timer.start()
        self._flush_runtime_diagnostics()
        worker = FunctionWorker(prepare_retry_compression, config, sources)
        self._start_worker(worker, self._compression_prepared, self._preparation_progress)

    def _runnable_sources(self) -> set[Path]:
        if self.include_risky_jobs.isChecked():
            return {row.source for row in self._compression_plan_rows if row.selected and row.exists}
        return {
            row.source
            for row in self._compression_plan_rows
            if row.selected and row.exists and row.classification != "risky-follow-up"
        }

    def _runnable_jobs(self, preparation: EncodePreparation) -> list:
        runnable_sources = self._runnable_sources()
        return [job for job in preparation.jobs if getattr(job, "source", None) in runnable_sources]

    def _compression_start_tooltip(self) -> str:
        prep = self.encode_preparation
        if prep is None:
            return "Prepare a compression plan first."
        if self._active_worker_count > 0:
            return "Wait for the current background task to finish."
        if self._config_dirty:
            return "Settings changed after planning. Rebuild the compression plan first."
        if self.workflow_state != WorkflowState.READY_TO_COMPRESS:
            return "Finish preparing the compression plan before starting encoding."
        if prep.profile is None:
            return "No encoder profile is selected for this plan. Rebuild with safer settings first."
        if getattr(prep, "compatible_count", 0) <= 0:
            return "The selected profile is predicted to work for 0 files. Rebuild with a safer compatibility-first profile."
        if self._compression_has_blocking_risk():
            return "This plan has hardware or container compatibility risk. Rebuild with a safer compatibility-first profile before starting."
        if not prep.jobs:
            return self._compression_zero_jobs_message(prep)
        if not self._runnable_jobs(prep):
            return "No runnable jobs are selected in the current view. Include risky follow-up jobs or rebuild the plan."
        return "Start the current compression plan."

    def _build_runnable_preparation(self, preparation: EncodePreparation) -> EncodePreparation:
        jobs = self._runnable_jobs(preparation)
        job_sources = {getattr(job, "source", None) for job in jobs}
        selected_input_bytes = sum(
            row.estimated_output_bytes + row.estimated_savings_bytes
            for row in self._compression_plan_rows
            if row.source in job_sources
        )
        selected_estimated_output_bytes = sum(
            row.estimated_output_bytes
            for row in self._compression_plan_rows
            if row.source in job_sources
        )
        if is_dataclass(preparation):
            return replace(
                preparation,
                jobs=jobs,
                selected_count=len(jobs),
                selected_input_bytes=selected_input_bytes,
                selected_estimated_output_bytes=selected_estimated_output_bytes,
            )
        payload = dict(vars(preparation))
        payload.update(
            jobs=jobs,
            selected_count=len(jobs),
            selected_input_bytes=selected_input_bytes,
            selected_estimated_output_bytes=selected_estimated_output_bytes,
        )
        return preparation.__class__(**payload)

    def _update_encode_dashboard(self, progress: EncodeProgress | None) -> None:
        prep = self.encode_preparation
        if prep is None:
            self.encode_filename_label.setText("")
            self.encode_phase_label.setText("")
            self.encode_counts_label.setText("")
            self.encode_projection_label.setText("")
            self.encode_projection_bar.setValue(0)
            self.encode_projection_bar.setFormat("Projected retained size")
            return

        profile_text = (
            f"{prep.profile.name} ({prep.profile.encoder_key}, CRF {prep.profile.crf})"
            if prep.profile is not None
            else "No profile selected"
        )
        runnable_jobs = self._runnable_jobs(prep)
        if progress is None:
            filename = display_name_for_ui(runnable_jobs[0].source.name) if runnable_jobs else "Compression plan ready"
            phase = "Ready to encode"
            file_progress = self._encode_progress_model.displayed_file_progress
            overall_progress = self._encode_progress_model.overall_progress
            completed = self._encode_progress_model.completed_files
            remaining = len(runnable_jobs) if prep.jobs else self._encode_progress_model.remaining_files
        else:
            filename = self._encode_progress_model.current_file_name
            phase = self._encode_progress_model.phase
            file_progress = self._encode_progress_model.displayed_file_progress
            overall_progress = self._encode_progress_model.overall_progress
            completed = self._encode_progress_model.completed_files
            remaining = self._encode_progress_model.remaining_files

        self.encode_filename_label.setText(filename)
        self.encode_phase_label.setText(f"Phase: {phase} • Profile: {profile_text}")
        self.encode_counts_label.setText(
            f"Files done: {completed} • Remaining: {remaining} • Overall: {int(overall_progress * 100)}%"
        )
        self.encode_visual_bar.setValue(int(file_progress * 100))
        self.encode_visual_bar.setFormat(f"Current file {int(file_progress * 100)}%")

        if prep.selected_input_bytes > 0 and prep.selected_estimated_output_bytes > 0:
            saved = prep.selected_input_bytes - prep.selected_estimated_output_bytes
            retained_pct = int(100 * prep.selected_estimated_output_bytes / prep.selected_input_bytes)
            self.encode_projection_label.setText(
                f"Projected run size: {self._format_bytes(prep.selected_input_bytes)} → "
                f"{self._format_bytes(prep.selected_estimated_output_bytes)} • "
                f"save {self._format_bytes(saved)}"
            )
            self.encode_projection_bar.setValue(retained_pct)
            self.encode_projection_bar.setFormat(
                f"Projected retained size: {retained_pct}%"
            )
        else:
            self.encode_projection_label.setText("Projected savings are unavailable for this plan.")
            self.encode_projection_bar.setValue(0)
            self.encode_projection_bar.setFormat("Projected retained size")

    def _log_encode_milestones(self, progress: EncodeProgress) -> None:
        total = progress.completed_files + progress.remaining_files
        file_name = display_name_for_ui(self._strip_rich(progress.current_file))
        phase = self._normalize_heartbeat_state(progress.heartbeat_state)
        bucket = self._progress_bucket(progress.current_file_progress)

        if file_name != self._last_encode_file:
            self._last_encode_file = file_name
            self._last_encode_bucket = -1
            self._append_status(f"Started encoding {file_name}.")

        if phase != self._last_encode_log_key[1]:
            self._append_status(f"{file_name}: {phase}.")

        if bucket >= 0 and bucket != self._last_encode_bucket and bucket % 5 == 0:
            self._last_encode_bucket = bucket
            pct = min(100, bucket * 5)
            if 0 < pct < 100:
                self._append_status(f"{file_name} reached {pct}%.")

        log_key = (progress.completed_files, phase)
        if log_key != self._last_encode_log_key:
            self._last_encode_log_key = log_key
            self._append_status(
                f"Progress update: {progress.completed_files} complete • {progress.remaining_files} remaining • phase {phase.lower()}."
            )

        if total and progress.completed_files >= total:
            self._append_status("Compression complete.")

    def _start_compression(self) -> None:
        if self.encode_preparation is None:
            self._show_error("Prepare a compression plan before starting compression.")
            return
        if self._config_dirty:
            self._show_error("Settings changed after the compression plan was prepared. Prepare the plan again.")
            return
        if self._compression_plan_is_blocked():
            self._show_error(self._compression_start_tooltip())
            return
        runnable_preparation = self._build_runnable_preparation(self.encode_preparation)
        if not runnable_preparation.jobs:
            self._show_error("There are no jobs selected in the current compression plan.")
            return
        preflight_error = self._preflight_check(runnable_preparation)
        if preflight_error:
            self._show_error(preflight_error)
            return
        missing_sources = missing_job_sources(runnable_preparation)
        if missing_sources and len(missing_sources) == len(runnable_preparation.jobs):
            self._show_error(
                f"All planned compression files are missing from the compression root. First missing file: {missing_sources[0]}"
            )
            return
        if missing_sources:
            self._record_warning(
                "Some planned files disappeared after planning. Mediaflow will skip those files and continue with the remaining jobs."
            )
            self._append_status(
                f"Skipping {len(missing_sources)} missing file(s) before compression starts."
            )
        if self.overwrite.isChecked():
            job_count = len(runnable_preparation.jobs)
            input_size = self._format_bytes(runnable_preparation.selected_input_bytes)
            projected = self._format_bytes(runnable_preparation.selected_estimated_output_bytes)
            confirm_msg = (
                "Start compression?\n\n"
                f"About to process {job_count} file(s).\n"
                f"Input size: {input_size}\n"
                f"Estimated output: {projected}\n\n"
                "WARNING: Originals will be replaced in-place after each successful encode. "
                "Keep the compression root unchanged during the run."
            )
        else:
            confirm_msg = "Start compression with the current plan?"
        if QMessageBox.question(self, "mediaflow", confirm_msg) != QMessageBox.Yes:
            return
        self.file_progress.setValue(0)
        self.overall_progress.setValue(0)
        self._set_current_action("Starting compression run")
        self._set_state(WorkflowState.COMPRESSING)
        self._append_status("Starting compression.")
        self._compression_start = time.monotonic()
        self._encode_progress_model.reset()
        self._last_encode_log_key = (-1, "")
        self._last_encode_bucket = -1
        self._last_encode_file = ""
        self._last_status_text = ""
        self._first_progress_delay = None
        self.elapsed_label.setText("Elapsed: 0s")
        self.eta_label.setText("ETA: settling...")
        self.encode_speed_label.setText("")
        self.encode_visual_bar.setValue(0)
        self.encode_projection_bar.setValue(0)
        self.encode_filename_label.setText("Starting...")
        self.toggle_encode_card_button.setChecked(False)  # always show card on new run
        self._update_encode_dashboard(None)
        self._diagnostics.set_config(self._snapshot_config_for_diagnostics())
        self._diagnostics.record_event(
            "compression_started",
            jobs=len(runnable_preparation.jobs),
            selected_input_bytes=runnable_preparation.selected_input_bytes,
            selected_estimated_output_bytes=runnable_preparation.selected_estimated_output_bytes,
            deferred_risky=[str(path) for path in sorted({row.source for row in self._plan_classification.risky_follow_up} - self._runnable_sources())],
        )
        self._flush_runtime_diagnostics()
        self._compression_timer.start()
        worker = FunctionWorker(run_compression, runnable_preparation)
        self._start_worker(worker, self._compression_complete, self._encode_progress)

    def _encode_progress(self, progress: object) -> None:
        if not isinstance(progress, EncodeProgress):
            return
        if self._first_progress_delay is None and self._compression_start > 0:
            self._first_progress_delay = time.monotonic() - self._compression_start
        now = time.monotonic()
        file_name = display_name_for_ui(self._strip_rich(progress.current_file))
        phase = self._normalize_heartbeat_state(progress.heartbeat_state)
        self._encode_progress_model.update_from_progress(
            current_file_name=file_name,
            phase=phase,
            current_file_progress=progress.current_file_progress,
            overall_progress=progress.overall_progress,
            completed_files=progress.completed_files,
            remaining_files=progress.remaining_files,
            bytes_processed=int(getattr(progress, "bytes_processed", 0) or 0),
            total_bytes=int(getattr(progress, "total_bytes", 0) or 0),
            now=now,
        )
        self._encode_progress_model.tick(now, now - self._compression_start)
        self.file_progress.setValue(int(self._encode_progress_model.displayed_file_progress * 100))
        self.overall_progress.setValue(int(self._encode_progress_model.overall_progress * 100))
        total = progress.completed_files + progress.remaining_files
        self.run_stats_label.setText(f"Files: {progress.completed_files} done / {total} total")
        self._set_current_action(
            f"{file_name}\nCompleted: {progress.completed_files}  |  Remaining: {progress.remaining_files}  |  Phase: {phase}"
        )
        self._diagnostics.record_event(
            "encode_progress",
            current_file=file_name,
            phase=phase,
            current_file_progress=round(progress.current_file_progress, 2),
            overall_progress=round(progress.overall_progress, 2),
            completed_files=progress.completed_files,
            remaining_files=progress.remaining_files,
            bytes_processed=int(getattr(progress, "bytes_processed", 0) or 0),
            total_bytes=getattr(progress, "total_bytes", 0),
        )
        self._update_encode_dashboard(progress)
        self._log_encode_milestones(progress)

    def _compression_complete(self, results: list) -> None:
        self._compression_timer.stop()
        self.encode_results = list(results)
        self._complete_action("Compression stage complete")
        self._set_current_action("Compression finished")
        missing_count = 0
        for result in self.encode_results:
            if getattr(result, "error_message", "") and "missing" in getattr(result, "error_message", "").lower():
                missing_count += 1
        if missing_count:
            self._record_warning(
                f"{missing_count} planned file(s) were missing when compression started. The compression root changed after planning."
            )
        self._retry_sources = collect_retry_sources(self.encode_preparation, self.encode_results)
        self._diagnostics.record_event(
            "compression_complete",
            encoded=sum(1 for row in build_encode_result_rows(self.encode_results) if row.is_encoded),
            failed=sum(1 for row in build_encode_result_rows(self.encode_results) if row.is_failed),
            skipped=sum(1 for row in build_encode_result_rows(self.encode_results) if row.is_skipped),
            retry_sources=[str(path) for path in sorted(self._retry_sources)],
        )
        self._append_status("Compression stage complete.")
        self._update_encode_dashboard(None)
        self._refresh_pipeline_summary()
        self._switch_tab("summary")
        self._set_state(WorkflowState.COMPLETED)
        self._flush_runtime_diagnostics()
        rows = build_encode_result_rows(self.encode_results)
        encoded = sum(1 for row in rows if row.is_encoded)
        skipped = sum(1 for row in rows if row.is_skipped)
        failed = sum(1 for row in rows if row.is_failed)
        self._notify_completion(
            "Compression complete",
            f"Encoded {encoded} file(s), skipped {skipped}, failed {failed}.",
        )

    def _notify_completion(self, title: str, message: str) -> None:
        if self.isActiveWindow() or not QSystemTrayIcon.isSystemTrayAvailable():
            return
        icon = self.windowIcon()
        if icon.isNull():
            return
        if self._tray is None:
            self._tray = QSystemTrayIcon(icon, self)
        if not self._tray.isVisible():
            self._tray.show()
        self._tray.showMessage(title, message, QSystemTrayIcon.Information, 5000)

    def note_startup_complete(self, started_at: float) -> None:
        self._startup_duration = max(0.0, time.monotonic() - started_at)
        self._set_diagnostics_provenance()
        self._diagnostics.record_event("startup_complete", seconds=round(self._startup_duration, 2))
        self._flush_runtime_diagnostics()

    _SPINNER_FRAMES = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"

    def _tick_compression(self) -> None:
        elapsed = time.monotonic() - self._compression_start
        self.elapsed_label.setText(f"Elapsed: {self._format_elapsed(elapsed)}")
        self._encode_progress_model.tick(time.monotonic(), elapsed)
        p = self._encode_progress_model.overall_progress

        # Animate spinner and update encode card
        self._spinner_idx = (self._spinner_idx + 1) % len(self._SPINNER_FRAMES)
        self.spinner_label.setText(self._SPINNER_FRAMES[self._spinner_idx])
        self.file_progress.setValue(int(self._encode_progress_model.displayed_file_progress * 100))
        self.overall_progress.setValue(int(p * 100))
        self.encode_visual_bar.setValue(int(self._encode_progress_model.displayed_file_progress * 100))

        if self._encode_progress_model.eta_seconds is not None:
            eta = self._encode_progress_model.eta_seconds
            self.eta_label.setText(f"ETA: {self._format_elapsed(eta)}")
            if self._encode_progress_model.speed_mbps is not None:
                self.encode_speed_label.setText(f"~{self._encode_progress_model.speed_mbps:.1f} MB/s")
            self.encode_counts_label.setText(
                f"{self.run_stats_label.text()} • Elapsed {self._format_elapsed(elapsed)} • ETA {self._format_elapsed(eta)}"
            )
        else:
            self.eta_label.setText("ETA: settling...")
            self.encode_counts_label.setText(
                f"{self.run_stats_label.text()} • Elapsed {self._format_elapsed(elapsed)} • ETA settling"
            )

    def _tick_preparation(self) -> None:
        elapsed = time.monotonic() - self._preparation_start
        self.prepare_elapsed_label.setText(f"Elapsed: {self._format_elapsed(elapsed)}")
        if self.workflow_state == WorkflowState.PREPARING_COMPRESSION:
            stage = self._preparation_stage_title(self._preparation_model.stage_key)
            message = f"{stage} ({self._format_elapsed(elapsed)})"
            if self._preparation_last_update_at is not None and time.monotonic() - self._preparation_last_update_at >= 10:
                message += ". Still working on the current preparation step."
            self._set_current_action(message)

    def _refresh_pipeline_summary(self) -> None:
        summary = build_pipeline_summary(self.apply_result, self.encode_results)
        apply_stats = summarise_apply_result(self.apply_result)
        organise_on = self.organise_enabled.isChecked()
        compress_on = self.compress_enabled.isChecked()

        header = self._summary_header_text()

        self.summary_headline_label.setText(header)
        lines = [header, ""]
        if self.workflow_state not in {WorkflowState.COMPLETED, WorkflowState.FAILED}:
            lines.append(f"Current workflow state: {self.workflow_state.value}")
            lines.append("")

        if organise_on or self.apply_result is not None:
            lines += [
                f"Organised:        {summary.organised_files} file(s)",
                f"Organise skipped: {summary.organise_skipped} file(s)",
                f"Organise errors:  {summary.organised_errors}",
            ]

        total_input = sum(
            int(getattr(r, "input_size_bytes", 0) or 0)
            for r in self.encode_results
            if getattr(r, "success", False)
        )
        ratio = f" ({100 * summary.bytes_saved / total_input:.1f}%)" if total_input > 0 else ""

        lines += [
            f"Encoded:          {summary.encoded_files} file(s)",
            f"Skipped:          {summary.skipped_files} file(s)",
            f"Failed:           {summary.failed_files} file(s)",
            f"Saved:            {self._format_bytes(summary.bytes_saved)}{ratio}" if summary.bytes_saved > 0 else "Saved:            0",
        ]

        if compress_on:
            if self.overwrite.isChecked():
                output_mode = "in-place (originals replaced)"
            else:
                output_mode = "in-place (originals preserved)"
            lines.append(f"Output mode:      {output_mode}")
            lines.append(f"Compression root: {self.compression_root_input.text().strip() or '(not set)'}")
            self.summary_mode_label.setText(
                f"Compression output mode: {output_mode} • Root: {self.compression_root_input.text().strip() or '(not set)'}"
            )
        elif organise_on:
            self.summary_mode_label.setText(
                f"Organised output is written to: {self.library_input.text().strip() or '(not set)'}"
            )
        else:
            self.summary_mode_label.setText("")

        if organise_on:
            lines.append(f"Library:          {self.library_input.text().strip() or '(not set)'}")

        if summary.organise_report_path:
            lines.append(f"Organise report: {summary.organise_report_path}")
        if summary.organise_apply_report_path:
            lines.append(f"Organise apply report: {summary.organise_apply_report_path}")
        if self._last_diagnostics_path is not None:
            lines.append(f"Diagnostics:     {self._last_diagnostics_path}")
        elif self._last_diagnostics_error:
            lines.append(f"Diagnostics:     {self._last_diagnostics_error}")

        self.summary_overview_label.setText("\n".join(lines))
        if self._last_diagnostics_path is not None:
            self.diagnostics_path_label.setText(f"Diagnostics: {self._last_diagnostics_path}")
        elif self._last_diagnostics_error:
            self.diagnostics_path_label.setText(self._last_diagnostics_error)
        else:
            self.diagnostics_path_label.setText(f"Diagnostics folder: {self._diagnostics_directory_path()}")
        # Stat tiles
        encoded = summary.encoded_files
        self.stat_files_label.setText(f"{encoded}\nfile{'s' if encoded != 1 else ''} encoded")
        if summary.bytes_saved > 0 and total_input > 0:
            pct_val = int(100 * summary.bytes_saved / total_input)
            self.stat_saved_label.setText(f"{self._format_bytes(summary.bytes_saved)}\nsaved")
            self.stat_pct_label.setText(f"{pct_val}%\nreduction")
        else:
            self.stat_saved_label.setText("—\nsaved")
            self.stat_pct_label.setText("—\nreduction")

        # Savings bar
        if total_input > 0 and summary.bytes_saved > 0:
            pct = min(100, int(100 * summary.bytes_saved / total_input))
            self.savings_bar.setValue(pct)
            self.savings_bar.setFormat(
                f"Space saved: {pct}%  ({self._format_bytes(summary.bytes_saved)} of {self._format_bytes(total_input)})"
            )
            self.savings_bar.setVisible(True)
        else:
            self.savings_bar.setVisible(False)

        # Per-file results table
        self.summary_table.setRowCount(0)
        self._summary_rows = build_encode_result_rows(self.encode_results)
        self.summary_table.setSortingEnabled(False)
        for result in self._summary_rows:
            row = self.summary_table.rowCount()
            self.summary_table.insertRow(row)
            orig = self._format_bytes(result.original_bytes) if result.original_bytes else ""
            final = self._format_bytes(result.final_bytes) if result.final_bytes else ""
            if result.saved_bytes and result.original_bytes:
                pct = 100 * result.saved_bytes / result.original_bytes
                saved_str = f"{self._format_bytes(result.saved_bytes)} ({pct:.1f}%)"
            else:
                saved_str = ""
            values = [
                result.display_name,
                result.status,
                orig,
                final,
                saved_str,
                result.reason,
                str(result.source),
            ]
            for col, val in enumerate(values):
                cell = QTableWidgetItem(val)
                if col == 5:
                    cell.setData(Qt.UserRole, result.retry_ready)
                    if result.raw_reason and result.raw_reason != result.reason:
                        cell.setToolTip(f"{result.reason}\n\nRaw detail:\n{result.raw_reason}")
                    else:
                        cell.setToolTip(val)
                else:
                    cell.setToolTip(val)
                self.summary_table.setItem(row, col, cell)
        self.summary_table.setSortingEnabled(True)
        self.summary_table.resizeColumnsToContents()
        self.summary_table.setVisible(bool(self.encode_results))
        self._apply_summary_filter()
        failure_groups = group_failure_rows(self._summary_rows) if self._summary_rows else []
        if failure_groups:
            self.summary_failure_label.setText(
                "\n".join(
                    f"Failure summary: {group.summary} ({group.count})\nNext step: {group.guidance}"
                    for group in failure_groups
                )
            )
        else:
            self.summary_failure_label.setText("")
        self.summary_timeline_label.setText(self._summary_timeline_text())

        details: list[str] = []
        if self.preview_state is not None:
            details.extend(["Organisation preview", *self.preview_state.summary_lines, ""])
        if self.apply_result is not None:
            details.extend(["Organisation apply", *self.apply_result.summary_lines, ""])
            if apply_stats.warnings:
                details.extend(["Apply warnings", *(f"- {warning}" for warning in apply_stats.warnings), ""])
        if self.encode_preparation is not None:
            details.extend(["Compression plan", self.compress_summary_label.text(), ""])
        if self._custom_warnings:
            details.append("Warnings")
            details.extend(f"- {w}" for w in self._custom_warnings)
            details.append("")
        if self.workflow_state == WorkflowState.COMPLETED and self._is_degraded_completion(summary):
            details.append("Completion outcome")
            if summary.encoded_files == 0 and summary.skipped_files > 0:
                details.append(
                    "Organisation completed, but compression produced no successful encodes. "
                    "Review the skipped reasons below and prepare a safer follow-up plan."
                )
            elif summary.failed_files > 0:
                details.append(
                    "Organisation completed, but compression still needs follow-up because some files failed."
                )
            elif self._retry_sources:
                details.append(
                    "Organisation completed, but some compression items were deferred or need a compatibility-first retry."
                )
            details.append("")
        if self.encode_results:
            details.append("Compression results")
            profile_name = ""
            if self.encode_preparation and self.encode_preparation.profile:
                p = self.encode_preparation.profile
                profile_name = f"{p.name} ({p.encoder_key}, CRF {p.crf})"
            any_success = False
            for result in self.encode_results:
                if result.skipped:
                    translated = next(
                        (row.reason for row in self._summary_rows if row.source == result.job.source),
                        translate_result_reason(getattr(result, "skip_reason", "") or "Skipped by plan"),
                    )
                    details.append(f"- {display_name_for_ui(result.job.source.name)}: skipped")
                    if translated:
                        details.append(f"  {translated}")
                elif result.success:
                    any_success = True
                    input_b = int(getattr(result, "input_size_bytes", 0) or 0)
                    output_b = int(getattr(result, "output_size_bytes", 0) or 0)
                    if input_b > 0 and output_b > 0:
                        saved = input_b - output_b
                        pct = 100 * saved / input_b
                        size_line = f"  encoded:  {self._format_bytes(input_b)} → {self._format_bytes(output_b)}  (saved {self._format_bytes(saved)}, {pct:.1f}%)"
                    else:
                        size_line = "  encoded"
                    details.append(f"- {display_name_for_ui(result.job.source.name)}")
                    details.append(size_line)
                    if profile_name:
                        details.append(f"  profile:  {profile_name}")
                    details.append(f"  location: {result.job.source}")
                else:
                    label = "missing" if "missing" in (result.error_message or "").lower() else "failed"
                    details.append(f"- {display_name_for_ui(result.job.source.name)}: {label}")
                    if result.error_message:
                        translated = next(
                            (row.reason for row in self._summary_rows if row.source == result.job.source),
                            self._translate_common_error(result.error_message),
                        )
                        details.append(f"  {translated}")
                    if translated != result.error_message:
                        details.append(f"  raw: {result.error_message}")
            if any_success and self.overwrite.isChecked():
                details.append("")
                details.append("All encoded files replaced in-place. Originals no longer exist.")
        timing_lines = self._timing_breakdown_lines()
        if timing_lines:
            details.extend(["", "Timing", *timing_lines])
        self._set_summary_text("\n".join(details).strip())
