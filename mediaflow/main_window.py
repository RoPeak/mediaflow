from __future__ import annotations

import re
import time
from pathlib import Path

from PySide6.QtCore import Qt, QThreadPool, QTimer
from PySide6.QtGui import QCloseEvent
from PySide6.QtWidgets import QSystemTrayIcon
from PySide6.QtWidgets import (
    QCheckBox,
    QComboBox,
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
    QStackedWidget,
    QTableWidget,
    QTableWidgetItem,
    QTabWidget,
    QVBoxLayout,
    QWidget,
)

from mediashrink.gui_api import EncodePreparation, EncodeProgress
from plexify.ui_controller import ApplyResultState, PreviewState, VideoUIController

from .compat import check_runtime_compatibility, compatibility_error_text
from .config import PipelineConfig, PlexifySettings, ShrinkSettings, build_pipeline_config
from .mediashrink_adapter import missing_job_sources, prepare_compression, run_compression
from .pipeline import build_pipeline_summary
from .plexify_adapter import build_preview, build_video_controller, scan_controller
from .settings import load_ui_state, save_ui_state
from .callback_types import PreparationProgress, PreparationStageUpdate
from .workers import FunctionWorker
from .workflow import WorkflowState, describe_workflow_state


class MainWindow(QMainWindow):
    def __init__(self, *, default_source: Path | None = None, default_library: Path | None = None) -> None:
        super().__init__()
        self.setWindowTitle("mediaflow")
        self.resize(1420, 920)

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

        self._compression_start: float = 0.0
        self._current_overall_progress: float = 0.0
        self._preparation_start: float = 0.0
        self._spinner_idx: int = 0
        self._last_encode_log_key: tuple[int, str] = (-1, "")
        self._last_encode_bucket: int = -1
        self._last_encode_file: str = ""
        self._prepare_seen_files: int = 0
        self._prepare_seen_bytes: int = 0
        self._prepare_stage_key: str = "discovering"
        self._startup_duration: float | None = None
        self._preparation_duration: float | None = None
        self._first_progress_delay: float | None = None

        self._build_widgets(default_source=default_source, default_library=default_library)
        self._build_ui()
        self._apply_styles()
        self._connect_signals()
        self._restore_ui_state(default_source=default_source, default_library=default_library)
        self._loading_state = False
        self._refresh_compression_link_label()
        if self._compression_root_linked:
            target = self.library_input.text() if self.organise_enabled.isChecked() else self.source_input.text()
            self.compression_root_input.setText(target)
        self._set_state(WorkflowState.SETUP)

        self._tray: QSystemTrayIcon | None = None

    def _build_widgets(self, *, default_source: Path | None, default_library: Path | None) -> None:
        self.step_label = QLabel()
        self.headline_label = QLabel()
        self.headline_label.setWordWrap(True)
        self.guidance_label = QLabel()
        self.guidance_label.setWordWrap(True)
        self.warning_label = QLabel()
        self.warning_label.setWordWrap(True)
        self.step_checklist_label = QLabel()
        self.step_checklist_label.setWordWrap(True)

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
        self.review_stack = QStackedWidget()
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
        self.current_action_label = QLabel()
        self.current_action_label.setWordWrap(True)
        self.last_completed_label = QLabel()
        self.last_completed_label.setWordWrap(True)
        self.runtime_warnings_label = QLabel()
        self.runtime_warnings_label.setWordWrap(True)
        self.toggle_details_button = QPushButton("Show Details")
        self.toggle_details_button.setCheckable(True)
        self.compression_table = QTableWidget(0, 7)
        self.compression_table.setHorizontalHeaderLabels(
            ["File", "Codec", "Recommendation", "Reason", "Est. Output", "Est. Saving", "Selected"]
        )
        self.compression_table.setSelectionBehavior(QTableWidget.SelectRows)
        self.compression_table.setSelectionMode(QTableWidget.NoSelection)
        self.compression_table.setEditTriggers(QTableWidget.NoEditTriggers)
        self.compression_table.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive)
        self.compression_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.Stretch)   # File
        self.compression_table.horizontalHeader().setSectionResizeMode(3, QHeaderView.Stretch)   # Reason
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
        self._compression_timer.setInterval(1000)
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
        self.summary_table = QTableWidget(0, 6)
        self.summary_table.setHorizontalHeaderLabels(["File", "Status", "Original", "Final", "Saved", "Location"])
        self.summary_table.horizontalHeader().setStretchLastSection(True)
        self.summary_table.setEditTriggers(QTableWidget.NoEditTriggers)
        self.summary_table.setSelectionBehavior(QTableWidget.SelectRows)
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
        self.save_summary_button = QPushButton("Save run summary...")
        self.save_summary_button.setVisible(False)

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
        layout.addWidget(banner)

        self.tabs.addTab(self._build_setup_tab(), "Setup")
        self.tabs.addTab(self._build_review_tab(), "Review")
        self.tabs.addTab(self._build_compress_tab(), "Compress")
        self.tabs.addTab(self._build_summary_tab(), "Summary")
        layout.addWidget(self.tabs, stretch=1)

    def _apply_styles(self) -> None:
        self.setStyleSheet(
            """
            QWidget { font-size: 13px; }
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
        left_layout.addWidget(self.review_table, stretch=1)

        nav_row = QHBoxLayout()
        nav_row.addWidget(self.prev_item_button)
        nav_row.addWidget(self.next_item_button)
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
        layout.addWidget(self.review_stack, stretch=1)
        return panel

    def _build_compress_tab(self) -> QWidget:
        panel = QWidget()
        layout = QVBoxLayout(panel)
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
        left_layout.addWidget(self.start_compress_button)
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
        return panel

    def _build_summary_tab(self) -> QWidget:
        panel = QWidget()
        layout = QVBoxLayout(panel)
        layout.addWidget(self.summary_headline_label)
        layout.addWidget(self.summary_mode_label)
        tile_row = QHBoxLayout()
        tile_row.addWidget(self.stat_files_label)
        tile_row.addWidget(self.stat_saved_label)
        tile_row.addWidget(self.stat_pct_label)
        tile_row.addStretch(1)
        layout.addLayout(tile_row)
        layout.addWidget(self.summary_overview_label)
        layout.addWidget(self.savings_bar)
        layout.addWidget(self.summary_table, stretch=1)
        layout.addWidget(self.summary_log, stretch=1)
        btn_row = QHBoxLayout()
        btn_row.addWidget(self.open_output_button)
        btn_row.addWidget(self.save_summary_button)
        btn_row.addStretch(1)
        layout.addLayout(btn_row)
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

        self.review_table.itemSelectionChanged.connect(self._review_selection_changed)
        self.candidate_table.itemDoubleClicked.connect(lambda *_: self._accept_selected_candidate())
        self.prev_item_button.clicked.connect(lambda: self._move_review_selection(-1))
        self.next_item_button.clicked.connect(lambda: self._move_review_selection(1))
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
        self.open_output_button.clicked.connect(self._open_output_folder)
        self.save_summary_button.clicked.connect(self._save_run_summary)
        self.toggle_encode_card_button.toggled.connect(self._on_toggle_encode_card)

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

    def _set_combo_value(self, combo: QComboBox, value: str) -> None:
        index = combo.findText(value)
        if index >= 0:
            combo.setCurrentIndex(index)

    def _ui_state_payload(self) -> dict[str, object]:
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
        lowered = stage.lower()
        mapping = {
            "discovering": "Discovering files",
            "analysing": "Analysing files",
            "benchmarking": "Benchmarking profiles",
            "building provisional profiles...": "Building profile candidates",
            "preparing smoke probes...": "Preparing smoke probes",
            "smoke-probing risky container/profile combinations": "Smoke probing",
            "scoring recommendations...": "Scoring recommendations",
            "plan-ready": "Plan ready",
        }
        return mapping.get(lowered, stage.replace("_", " ").replace("-", " ").title())

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
        stages = [
            ("discovering", "Discovering"),
            ("analysing", "Analysing"),
            ("benchmarking", "Benchmarking"),
            ("smoke-probing risky container/profile combinations", "Smoke probing"),
            ("scoring recommendations...", "Scoring"),
            ("plan-ready", "Plan ready"),
        ]
        current_index = next(
            (idx for idx, (key, _label) in enumerate(stages) if key == active_stage),
            0,
        )
        parts: list[str] = []
        for idx, (_key, label) in enumerate(stages):
            if idx < current_index:
                prefix = "✓"
            elif idx == current_index:
                prefix = "▶"
            else:
                prefix = "○"
            parts.append(f"{prefix} {label}")
        return "  •  ".join(parts)

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

    def _current_config(self) -> PipelineConfig:
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

    def _set_current_action(self, text: str) -> None:
        self._current_action = text
        self.current_action_label.setText(self._strip_rich(text))

    def _complete_action(self, text: str) -> None:
        self._last_completed_action = text
        self.last_completed_label.setText(text)

    def _reset_runtime_state(self, status_message: str | None = None) -> None:
        self.controller = None
        self.preview_state = None
        self.apply_result = None
        self.encode_preparation = None
        self.encode_results = []
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
        self.summary_log.clear()
        self.compress_status_log.clear()
        self.prepare_progress.setRange(0, 100)
        self.prepare_progress.setValue(0)
        self.file_progress.setValue(0)
        self.overall_progress.setValue(0)
        self._compression_timer.stop()
        self._preparation_timer.stop()
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
        self._prepare_seen_files = 0
        self._prepare_seen_bytes = 0
        self._prepare_stage_key = "discovering"
        self._clear_warnings()
        self._set_current_action("Not started")
        self._complete_action("Nothing completed yet")
        self._refresh_pipeline_summary()
        if status_message:
            self._append_status(status_message)
            self._complete_action(status_message)
        self._set_state(WorkflowState.SETUP)

    def _set_state(self, state: WorkflowState) -> None:
        self.workflow_state = state
        presentation = describe_workflow_state(state, organise_enabled=self.organise_enabled.isChecked())
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
            and self.encode_preparation.jobs
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
        self.open_output_button.setVisible(self.workflow_state == WorkflowState.COMPLETED)
        self.save_summary_button.setVisible(self.workflow_state == WorkflowState.COMPLETED)

        show_encode_dashboard = has_compression_plan and self.compress_stack.currentIndex() == 2
        self.toggle_encode_card_button.setVisible(show_encode_dashboard)
        if show_encode_dashboard:
            self.encode_card.setVisible(not self.toggle_encode_card_button.isChecked())
        else:
            self.encode_card.setVisible(False)

        self.review_stack.setCurrentIndex(1 if has_controller else 0)
        if self.workflow_state == WorkflowState.PREPARING_COMPRESSION:
            self.compress_stack.setCurrentIndex(1)
        elif self.encode_preparation is None:
            self.compress_stack.setCurrentIndex(0)
        else:
            self.compress_stack.setCurrentIndex(2)

        warnings = list(self._custom_warnings)
        if self.overwrite.isChecked() and self.compress_enabled.isChecked():
            warnings.append("Overwrite is enabled. Successful compression will replace originals.")
        if busy:
            warnings.append("A background task is currently running.")
        warning_text = "\n".join(warnings)
        self.warning_label.setText(warning_text)
        self.runtime_warnings_label.setText(warning_text or "No active warnings.")

        self.setup_hint_label.setText(self._setup_hint_text())
        self.review_hint_label.setText(self._review_hint_text())
        self.compress_hint_label.setText(self._compress_hint_text())
        self.review_placeholder_label.setText(
            "No organise review is loaded yet.\n\nStart the guided pipeline or load organise matches from Setup."
        )
        self.compress_empty_label.setText(self._compress_empty_text())
        self.next_action_label.setText(f"Recommended next action: {self._recommended_next_action()}")
        self.overwrite_warning_label.setText(
            "Compression will replace originals after successful encodes."
            if self.overwrite.isChecked() and self.compress_enabled.isChecked()
            else ""
        )
        self.current_action_label.setText(self._current_action)
        self.last_completed_label.setText(self._last_completed_action)
        self._update_setup_summary()
        self._update_review_summary()
        self._update_compress_summary()

    def _setup_hint_text(self) -> str:
        if self._config_dirty and (self.controller is not None or self.encode_preparation is not None):
            return "Settings changed after runtime data was created. Re-run the affected stage before continuing."
        if self.workflow_state == WorkflowState.SETUP:
            if not self.organise_enabled.isChecked() and self.compress_enabled.isChecked():
                return (
                    "Compression-only mode: mediashrink will scan the Source folder "
                    "(or your chosen Compression Root) directly. Library / Output Folder is not used in this run."
                )
            return "Start with the guided pipeline unless you only want a manual organise review or a compression-only run."
        return "Setup controls stay available, but later stages will ask you to rebuild stale data after changes."

    def _review_hint_text(self) -> str:
        if self.controller is None:
            return "This step loads suggested plexify matches for each discovered item."
        if self._config_dirty:
            return "Review data is stale because setup changed. Start a new organise review."
        if self.preview_state is None:
            return "Accept, skip, or refine each item. Then build a preview."
        if self.preview_state.can_apply:
            return "Organisation preview is ready to apply."
        return "Some items still need a decision before organisation can continue."

    def _compress_hint_text(self) -> str:
        if self.workflow_state == WorkflowState.PREPARING_COMPRESSION:
            return "Scanning the compression root and assembling a compression plan."
        if self.encode_preparation is None:
            return "Compression planning only starts after you prepare a plan from Setup or after organisation finishes."
        if self._config_dirty:
            return "Compression plan is stale because setup changed. Prepare the plan again."
        if not self.encode_preparation.jobs:
            return "No compressible files are currently selected in the compression plan."
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
            return
        total = len(self.controller.items)
        accepted = sum(1 for item in self.controller.items if item.decision_status == "accepted")
        manual = sum(1 for item in self.controller.items if item.decision_status == "manual")
        skipped = sum(1 for item in self.controller.items if item.decision_status == "skipped")
        unresolved = sum(1 for item in self.controller.items if not item.resolved and not item.skipped)
        self.review_summary_label.setText(
            f"Items: {total} | Accepted: {accepted} | Manual: {manual} | Skipped: {skipped} | Unresolved: {unresolved}"
        )

    def _update_compress_summary(self) -> None:
        if self.encode_preparation is None:
            self.compress_summary_label.setText("No compression plan prepared.")
            return
        prep = self.encode_preparation
        savings = prep.selected_input_bytes - prep.selected_estimated_output_bytes
        lines = [
            f"Compression Root: {prep.directory}",
            f"Input:    {self._format_bytes(prep.selected_input_bytes)} across {prep.selected_count} file(s)",
            f"Output:   {self._format_bytes(prep.selected_estimated_output_bytes)} estimated",
            f"Savings:  {self._format_bytes(savings)} expected",
            f"Plan:     {prep.recommended_count} recommended  |  {prep.maybe_count} maybe  |  {prep.skip_count} skipped",
        ]
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
        self.compress_summary_label.setText("\n".join(lines))

    def _append_status(self, text: str) -> None:
        self.compress_status_log.appendPlainText(self._strip_rich(text))

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
        if "compatibility check failed" in lowered:
            return "Installed plexify or mediashrink components are incompatible with this mediaflow build."
        return text

    def _on_toggle_encode_card(self, checked: bool) -> None:
        self.encode_card.setVisible(not checked)
        self.toggle_encode_card_button.setText("Show live view" if checked else "Hide live view")

    def _open_output_folder(self) -> None:
        import sys
        path = self.compression_root_input.text().strip() or self.library_input.text().strip()
        if not path:
            return
        if sys.platform == "win32":
            import os
            os.startfile(path)  # noqa: S606
        else:
            import subprocess
            subprocess.run(["xdg-open", path], check=False)  # noqa: S603, S607

    def _preflight_check(self, directory: object) -> str | None:
        """Return an error string if the output directory fails space or writability checks."""
        import shutil
        try:
            root = Path(str(directory))
            if not root.exists():
                return f"Compression root does not exist: {root}"
            usage = shutil.disk_usage(root)
            free_gb = usage.free / (1024 ** 3)
            if free_gb < 1.0:
                return f"Less than 1 GB free on the compression root drive ({free_gb:.1f} GB available). Free up space before starting."
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
            self.summary_overview_label.text().strip(),
            self.summary_log.toPlainText().strip(),
        ]
        return "\n\n".join(section for section in sections if section)

    def _show_error(self, message: str) -> None:
        if self._shutting_down:
            return
        self._complete_action("Last operation failed")
        summary, technical_detail = self._summarise_error(message)
        self._record_warning(summary)
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
        try:
            config = self._current_config()
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
        self._guided_mode = False
        self._continue_to_compress = False
        self._set_current_action("Scanning source with plexify")
        self._set_state(WorkflowState.SCANNING)
        self._switch_tab("review")
        self._append_status("Scanning source with plexify...")
        worker = FunctionWorker(scan_controller, build_video_controller(config))
        self._start_worker(worker, self._scan_complete)

    def _start_guided_pipeline(self) -> None:
        try:
            config = self._current_config()
        except ValueError as exc:
            self._show_error(str(exc))
            return
        if not self._ensure_compatibility():
            return
        self._persist_ui_state()
        self._reset_runtime_state()
        self._guided_mode = True
        self._continue_to_compress = config.shrink.enabled
        if config.plexify.enabled:
            self._set_current_action("Starting guided organise review")
            self._set_state(WorkflowState.SCANNING)
            self._switch_tab("review")
            self._append_status("Starting guided pipeline with organise scan.")
            worker = FunctionWorker(scan_controller, build_video_controller(config))
            self._start_worker(worker, self._scan_complete)
        else:
            self._append_status("Guided pipeline is skipping organisation and preparing compression.")
            self._prepare_compression_from_setup()

    def _scan_complete(self, controller: VideoUIController) -> None:
        self.controller = controller
        self.preview_state = None
        self.apply_result = None
        self._config_dirty = False
        self._populate_review_table()
        self._switch_tab("review")
        self._complete_action("Finished organise scan")
        if not controller.items:
            self._set_state(WorkflowState.REVIEW)
            self._set_current_action("Organise scan finished with no review items")
            self._append_status("No organise candidates were discovered in the source folder.")
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
                item.warning or item.unresolved_reason or "",
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
        self._update_ui()

    def _review_selection_changed(self) -> None:
        index = self._current_review_index()
        if index is None:
            self.details_log.clear()
            self.candidate_table.setRowCount(0)
            self._update_ui()
            return
        self._populate_candidate_table(index)
        self._populate_detail_view(index)
        self._update_ui()

    def _current_review_index(self) -> int | None:
        indexes = self.review_table.selectionModel().selectedRows() if self.review_table.selectionModel() else []
        if not indexes:
            return None
        return indexes[0].row()

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
        lines = [
            f"Path: {item.item.path}",
            f"Media type: {item.item.media_type}",
            f"Title: {item.item.title}",
            f"Search query: {item.search_query}",
            f"Status: {item.status_label}",
            f"Cache context: {item.cache_context}",
            f"Auto-selectable: {item.auto_selectable}",
        ]
        if item.warning:
            lines.append(f"Warning: {item.warning}")
        if item.unresolved_reason:
            lines.append(f"Unresolved: {item.unresolved_reason}")
        self.details_log.setPlainText("\n".join(lines))

    def _selected_candidate_index(self) -> int:
        indexes = self.candidate_table.selectionModel().selectedRows() if self.candidate_table.selectionModel() else []
        if not indexes:
            return 0
        return indexes[0].row()

    def _move_review_selection(self, delta: int) -> None:
        if self.review_table.rowCount() == 0:
            return
        current = self._current_review_index() or 0
        target = max(0, min(self.review_table.rowCount() - 1, current + delta))
        self.review_table.selectRow(target)

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
        title = self.search_input.text().strip()
        if not title:
            self._show_error("Enter a manual title first.")
            return
        self.controller.manual_select(index, title=title)
        self._append_status(f"Manually selected a title for item {index + 1}.")
        self._refresh_review()

    def _apply_choice_to_folder(self) -> None:
        if self.controller is None:
            return
        index = self._current_review_index()
        if index is None:
            return
        self.controller.apply_choice_to_folder(index)
        self._append_status(f"Applied the current decision to the folder for item {index + 1}.")
        self._refresh_review()

    def _apply_choice_to_title_group(self) -> None:
        if self.controller is None:
            return
        index = self._current_review_index()
        if index is None:
            return
        self.controller.apply_choice_to_title_group(index)
        self._append_status(f"Applied the current decision to the title group for item {index + 1}.")
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
            self._append_status("Built organisation preview.")
            self._complete_action("Built organisation preview")

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
        if QMessageBox.question(self, "mediaflow", "Apply the current organisation plan to disk?") != QMessageBox.Yes:
            return
        self._set_current_action("Applying organisation to disk")
        self._set_state(WorkflowState.APPLYING)
        self._append_status("Applying organisation plan...")
        worker = FunctionWorker(self.controller.apply_preview, self.preview_state)
        self._start_worker(worker, self._apply_complete)

    def _apply_complete(self, result: ApplyResultState) -> None:
        self.apply_result = result
        self._complete_action("Organisation stage complete")
        self._append_status("Organisation stage complete.")
        self._refresh_pipeline_summary()
        if self._guided_mode and self._continue_to_compress:
            if not self._guided_compression_can_continue():
                self._set_state(WorkflowState.COMPLETED)
                self._switch_tab("summary")
                return
            self._append_status("Preparing compression plan after organisation.")
            self._prepare_compression_after_apply()
            return
        self._set_state(WorkflowState.COMPLETED)
        self._set_current_action("Pipeline finished")
        self._switch_tab("summary")
        self._notify_completion("Organisation complete", "Organisation stage finished.")

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
        if self._config_dirty and self.encode_preparation is not None:
            self.encode_preparation = None
            self.compression_table.setRowCount(0)
        self.prepare_progress.setRange(0, 0)
        self.file_progress.setValue(0)
        self.overall_progress.setValue(0)
        self.compress_status_log.clear()
        self.compress_preparing_label.setText("Preparing a compression plan...")
        self.prepare_elapsed_label.setText("")
        self.prepare_stage_label.setText("Discovering files...")
        self.prepare_counts_label.setText("0 file(s) discovered • 0.0 B")
        self.prepare_timeline_label.setText(self._preparation_timeline_text("discovering"))
        self._prepare_seen_files = 0
        self._prepare_seen_bytes = 0
        self._prepare_stage_key = "discovering"
        self._preparation_duration = None
        self.prepare_log.clear()
        self.prepare_log.appendPlainText("Preparing compression plan...")
        self._set_current_action(f"Scanning compression root {config.compression_root}")
        self._set_state(WorkflowState.PREPARING_COMPRESSION)
        self._switch_tab("compress")
        self._append_status(status_message)
        self._preparation_start = time.monotonic()
        self._preparation_timer.start()
        worker = FunctionWorker(prepare_compression, config)
        self._start_worker(worker, self._compression_prepared, self._preparation_progress)

    def _preparation_progress(self, payload: object) -> None:
        if isinstance(payload, PreparationStageUpdate):
            self._prepare_stage_key = self._preparation_stage_key_for(payload.stage)
            self.prepare_stage_label.setText(self._preparation_stage_title(payload.stage))
            self.prepare_timeline_label.setText(self._preparation_timeline_text(self._prepare_stage_key))
            self.compress_preparing_label.setText(payload.message)
            self.prepare_log.appendPlainText(self._strip_rich(payload.message))
            self._set_current_action(self._strip_rich(payload.message))
            if payload.completed is not None and payload.total:
                self.prepare_progress.setRange(0, 100)
                self.prepare_progress.setValue(int((payload.completed / payload.total) * 100))
            return
        if not isinstance(payload, PreparationProgress):
            return
        completed, total, path = payload.completed, payload.total, payload.path
        self._prepare_stage_key = "analysing"
        self.prepare_stage_label.setText("Analysing files")
        self.prepare_timeline_label.setText(self._preparation_timeline_text("analysing"))
        self.prepare_progress.setRange(0, 100)
        if total:
            self.prepare_progress.setValue(int((completed / total) * 100))
        file_name = Path(path).name
        if path:
            try:
                self._prepare_seen_bytes += Path(path).stat().st_size
            except OSError:
                pass
        self._prepare_seen_files = max(self._prepare_seen_files, completed)
        self.prepare_counts_label.setText(
            f"{self._prepare_seen_files} file(s) discovered • {self._format_bytes(self._prepare_seen_bytes)}"
        )
        self.compress_preparing_label.setText(f"Analysing {completed} of {total} file(s)")
        self._set_current_action(f"Analysing {completed}/{total}: {file_name}")
        self.prepare_log.appendPlainText(f"[{completed}/{total}] {file_name}")
        if total and completed >= total:
            self._prepare_stage_key = "benchmarking"
            self.prepare_stage_label.setText("Benchmarking profiles")
            self.prepare_timeline_label.setText(self._preparation_timeline_text("benchmarking"))

    def _compression_prepared(self, preparation: EncodePreparation) -> None:
        self._preparation_timer.stop()
        self._preparation_duration = time.monotonic() - self._preparation_start
        self.prepare_elapsed_label.setText("")
        self.prepare_stage_label.setText("Plan ready.")
        self.prepare_timeline_label.setText(self._preparation_timeline_text("plan-ready"))
        self.encode_preparation = preparation
        self.prepare_progress.setRange(0, 100)
        self.prepare_progress.setValue(100)
        self._populate_compression_table(preparation)
        self._config_dirty = False
        self._refresh_pipeline_summary()
        self._switch_tab("compress")
        self._complete_action("Compression plan prepared")
        if not preparation.items:
            self._set_current_action("Compression root scan finished with no supported video files")
            self._append_status("No supported video files found in the compression root.")
            self._set_state(WorkflowState.READY_TO_COMPRESS)
            return
        if not preparation.jobs:
            self._set_current_action("Compression plan contains no selected jobs")
            self._append_status("Compression plan contains no selected jobs.")
            self._set_state(WorkflowState.READY_TO_COMPRESS)
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
        self._set_current_action("Compression plan is ready to review")
        self._append_status(
            f"Prepared compression plan for {preparation.selected_count} file(s) from {preparation.directory}."
        )
        self._update_encode_dashboard(None)
        self._set_state(WorkflowState.READY_TO_COMPRESS)

    def _populate_compression_table(self, preparation: EncodePreparation | None) -> None:
        self.compression_table.setRowCount(0)
        if preparation is None:
            return
        selected_sources = {job.source for job in preparation.jobs}
        for row, item in enumerate(preparation.items):
            self.compression_table.insertRow(row)
            selected_text = "yes" if item.source in selected_sources else "no"
            if item.source in selected_sources and not item.source.exists():
                selected_text = "missing"
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
                item.source.name,
                item.codec or "",
                item.recommendation,
                item.reason_text,
                est_output,
                est_saving,
                selected_text,
            ]
            for column, value in enumerate(values):
                cell = QTableWidgetItem(str(value))
                cell.setToolTip(str(value))
                self.compression_table.setItem(row, column, cell)
            self.compression_table.item(row, 0).setToolTip(str(item.source))

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
        if progress is None:
            filename = prep.jobs[0].source.name if prep.jobs else "Compression plan ready"
            phase = "Ready to encode"
            file_progress = 0.0
            overall_progress = 0.0
            completed = 0
            remaining = len(prep.jobs)
        else:
            filename = self._strip_rich(progress.current_file)
            phase = self._normalize_heartbeat_state(progress.heartbeat_state)
            file_progress = progress.current_file_progress
            overall_progress = progress.overall_progress
            completed = progress.completed_files
            remaining = progress.remaining_files

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
        file_name = self._strip_rich(progress.current_file)
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
        if not self.encode_preparation.jobs:
            self._show_error("There are no jobs selected in the current compression plan.")
            return
        preflight_error = self._preflight_check(self.encode_preparation.directory)
        if preflight_error:
            self._show_error(preflight_error)
            return
        missing_sources = missing_job_sources(self.encode_preparation)
        if missing_sources and len(missing_sources) == len(self.encode_preparation.jobs):
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
            confirm_msg = (
                "Start compression?\n\n"
                "WARNING: Originals will be permanently replaced after a successful encode. "
                "This cannot be undone."
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
        self._current_overall_progress = 0.0
        self._last_encode_log_key = (-1, "")
        self._last_encode_bucket = -1
        self._last_encode_file = ""
        self._first_progress_delay = None
        self.elapsed_label.setText("Elapsed: 0s")
        self.eta_label.setText("ETA: calculating...")
        self.encode_speed_label.setText("")
        self.encode_visual_bar.setValue(0)
        self.encode_projection_bar.setValue(0)
        self.encode_filename_label.setText("Starting...")
        self.toggle_encode_card_button.setChecked(False)  # always show card on new run
        self._update_encode_dashboard(None)
        self._compression_timer.start()
        worker = FunctionWorker(run_compression, self.encode_preparation)
        self._start_worker(worker, self._compression_complete, self._encode_progress)

    def _encode_progress(self, progress: object) -> None:
        if not isinstance(progress, EncodeProgress):
            return
        if self._first_progress_delay is None and self._compression_start > 0:
            self._first_progress_delay = time.monotonic() - self._compression_start
        self.file_progress.setValue(int(progress.current_file_progress * 100))
        self.overall_progress.setValue(int(progress.overall_progress * 100))
        self._current_overall_progress = progress.overall_progress
        total = progress.completed_files + progress.remaining_files
        self.run_stats_label.setText(f"Files: {progress.completed_files} done / {total} total")
        file_name = self._strip_rich(progress.current_file)
        phase = self._normalize_heartbeat_state(progress.heartbeat_state)
        self._set_current_action(
            f"{file_name}\nCompleted: {progress.completed_files}  |  Remaining: {progress.remaining_files}  |  Phase: {phase}"
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
        self._append_status("Compression stage complete.")
        self._update_encode_dashboard(None)
        self._refresh_pipeline_summary()
        self._switch_tab("summary")
        self._set_state(WorkflowState.COMPLETED)
        self._notify_completion("Compression complete", f"Encoded {len(self.encode_results)} file(s).")

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

    _SPINNER_FRAMES = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"

    def _tick_compression(self) -> None:
        elapsed = time.monotonic() - self._compression_start
        self.elapsed_label.setText(f"Elapsed: {self._format_elapsed(elapsed)}")
        p = self._current_overall_progress

        # Animate spinner and update encode card
        self._spinner_idx = (self._spinner_idx + 1) % len(self._SPINNER_FRAMES)
        self.spinner_label.setText(self._SPINNER_FRAMES[self._spinner_idx])
        self.encode_visual_bar.setValue(int(p * 100))

        if p > 0.02:
            eta = elapsed / p * (1 - p)
            self.eta_label.setText(f"ETA: {self._format_elapsed(eta)}")
            # Estimate encode speed from known input size and progress
            if self.encode_preparation and self.encode_preparation.selected_input_bytes > 0:
                bytes_done = p * self.encode_preparation.selected_input_bytes
                speed_mbs = bytes_done / max(elapsed, 0.1) / 1_048_576
                self.encode_speed_label.setText(f"~{speed_mbs:.1f} MB/s")
                self.encode_counts_label.setText(
                    f"{self.run_stats_label.text()} • Elapsed {self._format_elapsed(elapsed)} • ETA {self._format_elapsed(eta)}"
                )
        else:
            self.encode_counts_label.setText(
                f"{self.run_stats_label.text()} • Elapsed {self._format_elapsed(elapsed)}"
            )

    def _tick_preparation(self) -> None:
        elapsed = time.monotonic() - self._preparation_start
        self.prepare_elapsed_label.setText(f"Elapsed: {self._format_elapsed(elapsed)}")

    def _refresh_pipeline_summary(self) -> None:
        summary = build_pipeline_summary(self.apply_result, self.encode_results)
        organise_on = self.organise_enabled.isChecked()
        compress_on = self.compress_enabled.isChecked()

        if organise_on and compress_on:
            header = "Full pipeline completed"
        elif compress_on:
            header = "Compression-only run completed"
        elif organise_on:
            header = "Organise-only run completed"
        else:
            header = "Pipeline Summary"

        self.summary_headline_label.setText(header)
        lines = [header, ""]

        if organise_on or self.apply_result is not None:
            lines += [
                f"Organised:        {summary.organised_plans} file(s)",
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

        self.summary_overview_label.setText("\n".join(lines))

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
        for result in self.encode_results:
            row = self.summary_table.rowCount()
            self.summary_table.insertRow(row)
            if result.skipped:
                status, orig, final, saved_str = "Skipped", "", "", ""
            elif result.success:
                input_b = int(getattr(result, "input_size_bytes", 0) or 0)
                output_b = int(getattr(result, "output_size_bytes", 0) or 0)
                status = "Encoded"
                orig = self._format_bytes(input_b) if input_b else ""
                final = self._format_bytes(output_b) if output_b else ""
                if input_b > 0 and output_b > 0:
                    saved_b = input_b - output_b
                    pct = 100 * saved_b / input_b
                    saved_str = f"{self._format_bytes(saved_b)} ({pct:.1f}%)"
                else:
                    saved_str = ""
            else:
                status, orig, final, saved_str = "Failed", "", "", ""
            values = [result.job.source.name, status, orig, final, saved_str, str(result.job.source)]
            for col, val in enumerate(values):
                cell = QTableWidgetItem(val)
                cell.setToolTip(val)
                self.summary_table.setItem(row, col, cell)
        self.summary_table.resizeColumnsToContents()
        self.summary_table.setVisible(bool(self.encode_results))

        details: list[str] = []
        if self.preview_state is not None:
            details.extend(["Organisation preview", *self.preview_state.summary_lines, ""])
        if self.apply_result is not None:
            details.extend(["Organisation apply", *self.apply_result.summary_lines, ""])
        if self.encode_preparation is not None:
            details.extend(["Compression plan", self.compress_summary_label.text(), ""])
        if self._custom_warnings:
            details.append("Warnings")
            details.extend(f"- {w}" for w in self._custom_warnings)
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
                    details.append(f"- {result.job.source.name}: skipped")
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
                    details.append(f"- {result.job.source.name}")
                    details.append(size_line)
                    if profile_name:
                        details.append(f"  profile:  {profile_name}")
                    details.append(f"  location: {result.job.source}")
                else:
                    label = "missing" if "missing" in (result.error_message or "").lower() else "failed"
                    details.append(f"- {result.job.source.name}: {label}")
                    if result.error_message:
                        details.append(f"  {result.error_message}")
            if any_success and self.overwrite.isChecked():
                details.append("")
                details.append("All encoded files replaced in-place. Originals no longer exist.")
        timing_lines: list[str] = []
        if self._startup_duration is not None:
            timing_lines.append(f"Startup time: {self._format_elapsed(self._startup_duration)}")
        if self._preparation_duration is not None:
            timing_lines.append(f"Plan preparation time: {self._format_elapsed(self._preparation_duration)}")
        if self._first_progress_delay is not None:
            timing_lines.append(f"First encode progress update: {self._format_elapsed(self._first_progress_delay)}")
        if timing_lines:
            details.extend(["", "Timing", *timing_lines])
        self._set_summary_text("\n".join(details).strip())
