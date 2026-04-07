from __future__ import annotations

from pathlib import Path

from PySide6.QtCore import QThreadPool, Qt
from PySide6.QtGui import QCloseEvent
from PySide6.QtWidgets import (
    QCheckBox,
    QComboBox,
    QDoubleSpinBox,
    QFileDialog,
    QFormLayout,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QPlainTextEdit,
    QProgressBar,
    QHeaderView,
    QScrollArea,
    QTableWidget,
    QTableWidgetItem,
    QTabWidget,
    QToolBox,
    QVBoxLayout,
    QWidget,
)

from mediashrink.gui_api import EncodePreparation, EncodeProgress
from plexify.ui_controller import ApplyResultState, PreviewState, VideoUIController

from .compat import check_runtime_compatibility, compatibility_error_text
from .config import PipelineConfig, PlexifySettings, ShrinkSettings, build_pipeline_config
from .mediashrink_adapter import prepare_compression, run_compression
from .pipeline import build_pipeline_summary
from .plexify_adapter import build_preview, build_video_controller, scan_controller
from .settings import load_ui_state, save_ui_state
from .workers import FunctionWorker
from .workflow import WorkflowState, describe_workflow_state


class MainWindow(QMainWindow):
    def __init__(self, *, default_source: Path | None = None, default_library: Path | None = None) -> None:
        super().__init__()
        self.setWindowTitle("mediaflow")
        self.resize(1480, 940)

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

        self._build_widgets(default_source=default_source, default_library=default_library)
        self._build_ui()
        self._connect_signals()
        self._restore_ui_state(default_source=default_source, default_library=default_library)
        self._loading_state = False
        self._set_state(WorkflowState.SETUP)

    def _build_widgets(self, *, default_source: Path | None, default_library: Path | None) -> None:
        self.step_label = QLabel()
        self.step_label.setObjectName("step-label")
        self.headline_label = QLabel()
        self.headline_label.setWordWrap(True)
        self.guidance_label = QLabel()
        self.guidance_label.setWordWrap(True)
        self.guidance_label.setObjectName("guidance-label")
        self.warning_label = QLabel()
        self.warning_label.setWordWrap(True)
        self.warning_label.setObjectName("warning-label")

        self.tabs = QTabWidget()

        self.source_input = QLineEdit(str(default_source) if default_source else "")
        self.library_input = QLineEdit(str(default_library) if default_library else "")
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
        self.recursive = QCheckBox("Scan library recursively")
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

        self.setup_summary_label = QLabel()
        self.setup_summary_label.setWordWrap(True)
        self.setup_hint_label = QLabel()
        self.setup_hint_label.setWordWrap(True)
        self.overwrite_warning_label = QLabel()
        self.overwrite_warning_label.setWordWrap(True)

        self.scan_button = QPushButton("Start Organise Review")
        self.guided_button = QPushButton("Start Guided Pipeline")
        self.prepare_compress_button = QPushButton("Prepare Compression Plan")
        self.reset_button = QPushButton("Reset Runtime State")

        self.review_summary_label = QLabel()
        self.review_summary_label.setWordWrap(True)
        self.review_hint_label = QLabel()
        self.review_hint_label.setWordWrap(True)
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

        self.prev_item_button = QPushButton("Prev")
        self.next_item_button = QPushButton("Next")
        self.accept_button = QPushButton("Accept")
        self.skip_button = QPushButton("Skip")
        self.search_button = QPushButton("Search Again")
        self.manual_button = QPushButton("Manual Match")
        self.next_page_button = QPushButton("More Candidates")
        self.auto_accept_button = QPushButton("Auto-Accept Safe")
        self.switch_button = QPushButton("Switch TV/Movie")
        self.folder_button = QPushButton("Apply To Folder")
        self.title_group_button = QPushButton("Apply To Title Group")
        self.preview_button = QPushButton("Build Preview")
        self.apply_button = QPushButton("Apply Organisation")

        self.compress_summary_label = QLabel()
        self.compress_summary_label.setWordWrap(True)
        self.compress_hint_label = QLabel()
        self.compress_hint_label.setWordWrap(True)
        self.prepare_progress = QProgressBar()
        self.file_progress = QProgressBar()
        self.overall_progress = QProgressBar()
        self.compression_table = QTableWidget(0, 7)
        self.compression_table.setHorizontalHeaderLabels(
            ["File", "Codec", "Recommendation", "Reason", "Est. Output", "Est. Saving", "Selected"]
        )
        self.compression_table.setSelectionBehavior(QTableWidget.SelectRows)
        self.compression_table.setSelectionMode(QTableWidget.NoSelection)
        self.compression_table.setEditTriggers(QTableWidget.NoEditTriggers)
        self.compression_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.start_compress_button = QPushButton("Start Compression")
        self.compress_status_log = QPlainTextEdit()
        self.compress_status_log.setReadOnly(True)
        self.compress_status_log.document().setMaximumBlockCount(200)

        self.summary_overview_label = QLabel()
        self.summary_overview_label.setWordWrap(True)
        self.summary_log = QPlainTextEdit()
        self.summary_log.setReadOnly(True)

    def _build_ui(self) -> None:
        root = QWidget()
        self.setCentralWidget(root)
        layout = QVBoxLayout(root)
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(10)

        banner = QGroupBox("Current Step")
        banner_layout = QVBoxLayout(banner)
        banner_layout.addWidget(self.step_label)
        banner_layout.addWidget(self.headline_label)
        banner_layout.addWidget(self.guidance_label)
        banner_layout.addWidget(self.warning_label)
        layout.addWidget(banner)

        self.tabs.addTab(self._build_setup_tab(), "Setup")
        self.tabs.addTab(self._build_review_tab(), "Review")
        self.tabs.addTab(self._build_compress_tab(), "Compress")
        self.tabs.addTab(self._build_summary_tab(), "Summary")
        layout.addWidget(self.tabs, stretch=1)

    def _build_setup_tab(self) -> QWidget:
        panel = QWidget()
        outer = QVBoxLayout(panel)
        outer.setSpacing(12)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        outer.addWidget(scroll)

        content = QWidget()
        scroll.setWidget(content)
        layout = QVBoxLayout(content)
        layout.setSpacing(12)

        path_group = QGroupBox("Paths")
        path_form = QGridLayout(path_group)
        path_form.setHorizontalSpacing(10)
        source_browse = QPushButton("Browse")
        library_browse = QPushButton("Browse")
        source_browse.clicked.connect(lambda: self._browse_into(self.source_input))
        library_browse.clicked.connect(lambda: self._browse_into(self.library_input))
        path_form.addWidget(QLabel("Source"), 0, 0)
        path_form.addWidget(self.source_input, 0, 1)
        path_form.addWidget(source_browse, 0, 2)
        path_form.addWidget(QLabel("Library"), 1, 0)
        path_form.addWidget(self.library_input, 1, 1)
        path_form.addWidget(library_browse, 1, 2)
        layout.addWidget(path_group)

        stage_group = QGroupBox("Stages")
        stage_layout = QVBoxLayout(stage_group)
        stage_layout.addWidget(self.organise_enabled)
        stage_layout.addWidget(self.compress_enabled)
        layout.addWidget(stage_group)

        advanced = QToolBox()
        advanced.addItem(self._build_organise_settings_page(), "Advanced Organise Settings")
        advanced.addItem(self._build_compress_settings_page(), "Advanced Compression Settings")
        layout.addWidget(advanced)

        action_group = QGroupBox("Actions")
        action_layout = QVBoxLayout(action_group)
        primary_row = QHBoxLayout()
        primary_row.addWidget(self.guided_button)
        primary_row.addWidget(self.scan_button)
        primary_row.addWidget(self.prepare_compress_button)
        primary_row.addWidget(self.reset_button)
        action_layout.addLayout(primary_row)
        action_layout.addWidget(self.setup_summary_label)
        action_layout.addWidget(self.setup_hint_label)
        action_layout.addWidget(self.overwrite_warning_label)
        layout.addWidget(action_group)
        layout.addStretch(1)
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

        layout.addWidget(QLabel("Discovered Items"))
        layout.addWidget(self.review_table, stretch=2)
        layout.addWidget(QLabel("Candidate Matches"))
        layout.addWidget(self.candidate_table, stretch=1)

        search_row = QHBoxLayout()
        search_row.addWidget(self.search_input)
        search_row.addWidget(self.search_button)
        search_row.addWidget(self.manual_button)
        layout.addLayout(search_row)

        action_row = QHBoxLayout()
        for button in [
            self.prev_item_button,
            self.next_item_button,
            self.accept_button,
            self.skip_button,
            self.next_page_button,
            self.auto_accept_button,
            self.switch_button,
            self.folder_button,
            self.title_group_button,
        ]:
            action_row.addWidget(button)
        layout.addLayout(action_row)

        footer_row = QHBoxLayout()
        footer_row.addWidget(self.preview_button)
        footer_row.addWidget(self.apply_button)
        layout.addLayout(footer_row)

        detail_row = QHBoxLayout()
        detail_group = QGroupBox("Selected Item Details")
        detail_layout = QVBoxLayout(detail_group)
        detail_layout.addWidget(self.details_log)
        preview_group = QGroupBox("Organisation Preview")
        preview_layout = QVBoxLayout(preview_group)
        preview_layout.addWidget(self.preview_log)
        detail_row.addWidget(detail_group, stretch=1)
        detail_row.addWidget(preview_group, stretch=1)
        layout.addLayout(detail_row, stretch=1)
        return panel

    def _build_compress_tab(self) -> QWidget:
        panel = QWidget()
        layout = QVBoxLayout(panel)
        layout.setSpacing(10)
        layout.addWidget(self.compress_summary_label)
        layout.addWidget(self.compress_hint_label)
        layout.addWidget(self.start_compress_button)
        layout.addWidget(QLabel("Preparation progress"))
        layout.addWidget(self.prepare_progress)
        layout.addWidget(QLabel("Current file"))
        layout.addWidget(self.file_progress)
        layout.addWidget(QLabel("Overall encode progress"))
        layout.addWidget(self.overall_progress)
        layout.addWidget(QLabel("Compression Plan"))
        layout.addWidget(self.compression_table, stretch=2)
        layout.addWidget(QLabel("Compression Status"))
        layout.addWidget(self.compress_status_log, stretch=1)
        return panel

    def _build_summary_tab(self) -> QWidget:
        panel = QWidget()
        layout = QVBoxLayout(panel)
        layout.addWidget(self.summary_overview_label)
        layout.addWidget(self.summary_log, stretch=1)
        return panel

    def _connect_signals(self) -> None:
        self.organise_enabled.toggled.connect(self._on_config_edited)
        self.compress_enabled.toggled.connect(self._on_config_edited)
        self.apply_mode.toggled.connect(self._on_config_edited)
        self.copy_mode.toggled.connect(self._on_config_edited)
        self.use_cache.toggled.connect(self._on_config_edited)
        self.offline.toggled.connect(self._on_config_edited)
        self.overwrite.toggled.connect(self._on_config_edited)
        self.recursive.toggled.connect(self._on_config_edited)
        self.no_skip.toggled.connect(self._on_config_edited)
        self.use_calibration.toggled.connect(self._on_config_edited)
        self.min_confidence.valueChanged.connect(self._on_config_edited)
        self.source_input.textChanged.connect(self._on_config_edited)
        self.library_input.textChanged.connect(self._on_config_edited)
        self.extensions_input.textChanged.connect(self._on_config_edited)
        self.conflict_mode.currentTextChanged.connect(self._on_config_edited)
        self.policy.currentTextChanged.connect(self._on_config_edited)
        self.on_file_failure.currentTextChanged.connect(self._on_config_edited)
        self.duplicate_policy.currentTextChanged.connect(self._on_config_edited)

        self.scan_button.clicked.connect(self._start_scan)
        self.guided_button.clicked.connect(self._start_guided_pipeline)
        self.prepare_compress_button.clicked.connect(self._prepare_compression_from_setup)
        self.reset_button.clicked.connect(lambda: self._reset_runtime_state("Cleared runtime state."))

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

    def _on_config_edited(self, *_args) -> None:
        if self._loading_state:
            return
        if self.controller is not None or self.preview_state is not None or self.apply_result is not None or self.encode_preparation is not None or self.encode_results:
            self._config_dirty = True
        self._set_state(self.workflow_state)

    def _current_config(self) -> PipelineConfig:
        return build_pipeline_config(
            source=self.source_input.text().strip(),
            library=self.library_input.text().strip(),
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
            self._set_state(WorkflowState.FAILED)
            self._show_error(compatibility_error_text(issues))
            return False
        self._compatibility_checked = True
        return True

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
        self.details_log.clear()
        self.preview_log.clear()
        self.compress_status_log.clear()
        self.summary_log.clear()
        self.prepare_progress.setRange(0, 100)
        self.prepare_progress.setValue(0)
        self.file_progress.setValue(0)
        self.overall_progress.setValue(0)
        self._update_review_summary()
        self._update_compress_summary()
        self._refresh_pipeline_summary()
        if status_message:
            self._append_status(status_message)
        self._set_state(WorkflowState.SETUP)

    def _set_state(self, state: WorkflowState) -> None:
        self.workflow_state = state
        presentation = describe_workflow_state(state)
        self.step_label.setText(presentation.step_title)
        self.headline_label.setText(presentation.headline)
        guidance = presentation.guidance
        if self._config_dirty and state not in {WorkflowState.SCANNING, WorkflowState.APPLYING, WorkflowState.PREPARING_COMPRESSION, WorkflowState.COMPRESSING}:
            guidance += "\nSettings have changed since the last scan or compression plan. Start that stage again before continuing."
        self.guidance_label.setText(guidance)

        warnings: list[str] = []
        if self.overwrite.isChecked() and self.compress_enabled.isChecked():
            warnings.append("Overwrite is enabled. Successful compression will replace originals.")
        if self._active_worker_count > 0:
            warnings.append("A background task is currently running.")
        self.warning_label.setText("\n".join(warnings))
        self._update_ui()

    def _switch_tab(self, name: str) -> None:
        mapping = {"setup": 0, "review": 1, "compress": 2, "summary": 3}
        self.tabs.setCurrentIndex(mapping[name])

    def _update_ui(self) -> None:
        busy = self._active_worker_count > 0
        has_controller = self.controller is not None and bool(self.controller.items)
        review_index = self._current_review_index()
        has_review_selection = has_controller and review_index is not None
        can_preview = has_controller and not busy and not self._config_dirty
        can_apply = (
            self.preview_state is not None
            and self.preview_state.can_apply
            and not busy
            and not self._config_dirty
        )
        can_start_compression = (
            self.encode_preparation is not None
            and bool(self.encode_preparation.jobs)
            and self.workflow_state == WorkflowState.READY_TO_COMPRESS
            and not busy
            and not self._config_dirty
        )
        current_has_more = False
        if has_review_selection and self.controller is not None:
            current_has_more = self.controller.items[review_index or 0].has_more

        self.tabs.setTabEnabled(0, True)
        self.tabs.setTabEnabled(1, has_controller or busy or self.workflow_state in {WorkflowState.REVIEW, WorkflowState.REVIEW_BLOCKED, WorkflowState.READY_TO_APPLY, WorkflowState.SCANNING, WorkflowState.APPLYING})
        self.tabs.setTabEnabled(2, self.encode_preparation is not None or busy or self.workflow_state in {WorkflowState.PREPARING_COMPRESSION, WorkflowState.READY_TO_COMPRESS, WorkflowState.COMPRESSING})
        self.tabs.setTabEnabled(3, True)

        self.scan_button.setEnabled(self.organise_enabled.isChecked() and not busy)
        self.guided_button.setEnabled(not busy)
        self.prepare_compress_button.setEnabled(self.compress_enabled.isChecked() and not busy)
        self.reset_button.setEnabled(not busy)

        review_actions_enabled = has_review_selection and not busy and not self._config_dirty
        self.prev_item_button.setEnabled(review_actions_enabled and review_index not in {None, 0})
        self.next_item_button.setEnabled(
            review_actions_enabled
            and review_index is not None
            and review_index < self.review_table.rowCount() - 1
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

        self.setup_hint_label.setText(self._setup_hint_text())
        self.review_hint_label.setText(self._review_hint_text())
        self.compress_hint_label.setText(self._compress_hint_text())
        self.overwrite_warning_label.setText(
            "Compression overwrite is currently enabled. Review the compression plan carefully before starting."
            if self.overwrite.isChecked() and self.compress_enabled.isChecked()
            else ""
        )
        self._update_setup_summary()
        self._update_review_summary()
        self._update_compress_summary()

    def _setup_hint_text(self) -> str:
        if self._config_dirty and (self.controller is not None or self.encode_preparation is not None):
            return "Settings have changed. Start a new organise scan or compression plan before continuing."
        if self.workflow_state == WorkflowState.SETUP:
            return "Recommended: start with the guided pipeline, then review the suggested organise matches."
        if self.workflow_state == WorkflowState.FAILED:
            return "The last operation failed. Adjust settings if needed, then start the relevant stage again."
        return "Use this tab to adjust settings, then move through the workflow one stage at a time."

    def _review_hint_text(self) -> str:
        if self.controller is None:
            return "No organise scan results yet. Start an organise scan from the Setup tab."
        if self._config_dirty:
            return "Current review data is stale because settings changed. Re-scan before applying."
        if self.preview_state is None:
            return "Review matches, then click Build Preview when you are satisfied."
        if self.preview_state.can_apply:
            return "Preview is resolved. Apply organisation to continue."
        return "Preview found unresolved items. Accept or skip them before applying."

    def _compress_hint_text(self) -> str:
        if self.encode_preparation is None:
            return "No compression plan yet. Prepare one from Setup or after applying organisation."
        if self._config_dirty:
            return "Compression plan is stale because settings changed. Prepare a new plan."
        if not self.encode_preparation.jobs:
            return "No compressible files were selected for this run."
        if self.workflow_state == WorkflowState.COMPRESSING:
            return "Encoding is in progress."
        return "Review the compression plan, then click Start Compression."

    def _update_setup_summary(self) -> None:
        lines = [
            f"Source: {self.source_input.text().strip() or '(not set)'}",
            f"Library: {self.library_input.text().strip() or '(not set)'}",
            f"Organise enabled: {'yes' if self.organise_enabled.isChecked() else 'no'}",
            f"Compress enabled: {'yes' if self.compress_enabled.isChecked() else 'no'}",
        ]
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
        auto_safe = sum(1 for item in self.controller.items if item.auto_selectable)
        self.review_summary_label.setText(
            f"Items: {total} | Accepted: {accepted} | Manual: {manual} | "
            f"Skipped: {skipped} | Unresolved: {unresolved} | Auto-safe: {auto_safe}"
        )

    def _update_compress_summary(self) -> None:
        if self.encode_preparation is None:
            self.compress_summary_label.setText("No compression plan prepared.")
            return
        lines = [
            f"Library root: {self.encode_preparation.directory}",
            f"Recommended: {self.encode_preparation.recommended_count}",
            f"Maybe: {self.encode_preparation.maybe_count}",
            f"Skip: {self.encode_preparation.skip_count}",
            f"Selected: {self.encode_preparation.selected_count}",
        ]
        if self.encode_preparation.profile is not None:
            lines.append(
                f"Profile: {self.encode_preparation.profile.name} "
                f"({self.encode_preparation.profile.encoder_key}, CRF {self.encode_preparation.profile.crf})"
            )
        self.compress_summary_label.setText("\n".join(lines))

    def _append_status(self, text: str) -> None:
        self.compress_status_log.appendPlainText(text)

    def _append_summary(self, text: str) -> None:
        self.summary_log.appendPlainText(text)

    def _set_summary_text(self, text: str) -> None:
        self.summary_log.setPlainText(text)

    def _show_error(self, message: str) -> None:
        if self._shutting_down:
            return
        self._set_state(WorkflowState.FAILED)
        summary, technical_detail = self._summarise_error(message)
        summary_lines = ["Last operation failed.", "", summary]
        if technical_detail:
            summary_lines.extend(["", "Technical details are available in the error dialog."])
        self._set_summary_text("\n".join(summary_lines))
        self._switch_tab("summary")
        dialog = QMessageBox(self)
        dialog.setIcon(QMessageBox.Critical)
        dialog.setWindowTitle("mediaflow")
        dialog.setText(summary)
        if technical_detail:
            dialog.setInformativeText("Technical details are available below.")
            dialog.setDetailedText(technical_detail)
        dialog.exec()

    def _summarise_error(self, message: str) -> tuple[str, str | None]:
        text = message.strip()
        if "Traceback (most recent call last):" not in text:
            return text, None
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        final_line = next(
            (line for line in reversed(lines) if not line.startswith("^")),
            "Unexpected error while running the selected stage.",
        )
        if ":" in final_line:
            _, detail = final_line.split(":", 1)
            summary = detail.strip() or final_line
        else:
            summary = final_line
        return summary, text

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
            self._set_state(WorkflowState.SCANNING)
            self._switch_tab("review")
            self._append_status("Starting guided pipeline with organise scan.")
            worker = FunctionWorker(scan_controller, build_video_controller(config))
            self._start_worker(worker, self._scan_complete)
        else:
            self._append_status("Guided pipeline skipping organise stage and preparing compression.")
            self._prepare_compression_from_setup()

    def _scan_complete(self, controller: VideoUIController) -> None:
        self.controller = controller
        self.preview_state = None
        self.apply_result = None
        self._config_dirty = False
        self._populate_review_table()
        self._switch_tab("review")
        if not controller.items:
            self._set_state(WorkflowState.REVIEW)
            self._append_status("No organise candidates were discovered in the source folder.")
            return
        if self._guided_mode:
            accepted = self._auto_accept_safe_matches()
            self._append_status(f"Auto-accepted {accepted} safe match(es).")
            self._preview_plan()
            if self.preview_state is not None and self.preview_state.can_apply:
                self._set_state(WorkflowState.READY_TO_APPLY)
            else:
                self._set_state(WorkflowState.REVIEW_BLOCKED)
        else:
            self._set_state(WorkflowState.REVIEW)
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
        indexes = self.review_table.selectionModel().selectedRows()
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
            values = [
                candidate.title,
                candidate.year or "",
                candidate.source,
                f"{candidate.confidence:.2f}",
            ]
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
        if item.item.season is not None:
            lines.append(f"Season: {item.item.season}")
        if item.item.episode is not None:
            lines.append(f"Episode: {item.item.episode}")
        if item.item.episode_title:
            lines.append(f"Episode title: {item.item.episode_title}")
        if item.warning:
            lines.append(f"Warning: {item.warning}")
        if item.unresolved_reason:
            lines.append(f"Unresolved: {item.unresolved_reason}")
        self.details_log.setPlainText("\n".join(lines))

    def _selected_candidate_index(self) -> int:
        indexes = self.candidate_table.selectionModel().selectedRows()
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
        self._append_status(f"Applied current decision to the folder for item {index + 1}.")
        self._refresh_review()

    def _apply_choice_to_title_group(self) -> None:
        if self.controller is None:
            return
        index = self._current_review_index()
        if index is None:
            return
        self.controller.apply_choice_to_title_group(index)
        self._append_status(f"Applied current decision to the title group for item {index + 1}.")
        self._refresh_review()

    def _render_preview_summary(self) -> None:
        if self.preview_state is None:
            self.preview_log.clear()
            return
        lines = list(self.preview_state.summary_lines)
        if self.preview_state.unresolved_items:
            lines.append("")
            lines.append("Unresolved:")
            lines.extend(self.preview_state.unresolved_items[:10])
        if self.preview_state.warnings:
            lines.append("")
            lines.extend(f"Warning: {warning}" for warning in self.preview_state.warnings[:10])
        self.preview_log.setPlainText("\n".join(lines))

    def _preview_plan(self, rebuild_only: bool = False) -> None:
        if self.controller is None:
            return
        self.preview_state = build_preview(self.controller)
        self._render_preview_summary()
        if self.preview_state.can_apply:
            self._set_state(WorkflowState.READY_TO_APPLY)
        else:
            self._set_state(WorkflowState.REVIEW_BLOCKED)
        if not rebuild_only:
            self._append_status("Built organisation preview.")

    def _apply_plan(self) -> None:
        if self.controller is None:
            self._show_error("No organise review is loaded.")
            return
        if self._config_dirty:
            self._show_error("Settings changed after the scan. Start a new organise scan before applying.")
            return
        if self.preview_state is None:
            self._preview_plan(rebuild_only=True)
        if self.preview_state is None or not self.preview_state.can_apply:
            self._show_error("Resolve all unresolved items before applying organisation.")
            return
        reply = QMessageBox.question(
            self,
            "mediaflow",
            "Apply the current organisation plan to disk?",
        )
        if reply != QMessageBox.Yes:
            return
        self._set_state(WorkflowState.APPLYING)
        self._append_status("Applying organisation plan...")
        worker = FunctionWorker(self.controller.apply_preview, self.preview_state)
        self._start_worker(worker, self._apply_complete)

    def _apply_complete(self, result: ApplyResultState) -> None:
        self.apply_result = result
        self._append_status("Organisation stage complete.")
        self._refresh_pipeline_summary()
        self._switch_tab("summary")
        if self._guided_mode and self._continue_to_compress:
            self._append_status("Preparing compression plan after organisation.")
            self._prepare_compression_after_apply()
        else:
            self._set_state(WorkflowState.COMPLETED)

    def _prepare_compression_from_setup(self) -> None:
        self._guided_mode = False
        self._continue_to_compress = False
        self._start_compression_preparation("Preparing compression plan from Setup.")

    def _prepare_compression_after_apply(self) -> None:
        self._start_compression_preparation("Preparing compression plan for organised output.")

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
        self._set_state(WorkflowState.PREPARING_COMPRESSION)
        self._switch_tab("compress")
        self._append_status(status_message)
        worker = FunctionWorker(prepare_compression, config)
        self._start_worker(worker, self._compression_prepared, self._preparation_progress)

    def _preparation_progress(self, payload: object) -> None:
        if not isinstance(payload, tuple) or len(payload) != 3:
            return
        completed, total, path = payload
        self.prepare_progress.setRange(0, 100)
        if total:
            self.prepare_progress.setValue(int((completed / total) * 100))
        self._append_status(f"Analyzed {completed}/{total}: {Path(path).name}")

    def _compression_prepared(self, preparation: EncodePreparation) -> None:
        self.encode_preparation = preparation
        self.prepare_progress.setRange(0, 100)
        self.prepare_progress.setValue(100)
        self._populate_compression_table(preparation)
        self._refresh_pipeline_summary()
        self._switch_tab("compress")
        if not preparation.items:
            self._append_status("No supported video files found in the library.")
            self._config_dirty = False
            self._set_state(WorkflowState.READY_TO_COMPRESS)
            return
        if not preparation.jobs:
            self._append_status("Compression plan contains no selected jobs.")
            self._config_dirty = False
            self._set_state(WorkflowState.READY_TO_COMPRESS)
            return
        if preparation.stage_messages:
            for line in preparation.stage_messages:
                self._append_status(line)
        if preparation.duplicate_warnings:
            for warning in preparation.duplicate_warnings[:10]:
                self._append_status(f"Duplicate warning: {warning}")
        self._append_status(
            f"Prepared compression plan for {preparation.selected_count} file(s) with "
            f"{preparation.profile.name if preparation.profile else 'no profile'}."
        )
        self._config_dirty = False
        self._set_state(WorkflowState.READY_TO_COMPRESS)

    def _populate_compression_table(self, preparation: EncodePreparation | None) -> None:
        self.compression_table.setRowCount(0)
        if preparation is None:
            return
        selected_sources = {job.source for job in preparation.jobs}
        for row, item in enumerate(preparation.items):
            self.compression_table.insertRow(row)
            values = [
                item.source.name,
                item.codec or "",
                item.recommendation,
                item.reason_text,
                str(item.estimated_output_bytes) if item.estimated_output_bytes else "",
                str(item.estimated_savings_bytes) if item.estimated_savings_bytes else "",
                "yes" if item.source in selected_sources else "no",
            ]
            for column, value in enumerate(values):
                self.compression_table.setItem(row, column, QTableWidgetItem(value))

    def _start_compression(self) -> None:
        if self.encode_preparation is None:
            self._show_error("Prepare a compression plan before starting compression.")
            return
        if self._config_dirty:
            self._show_error("Settings changed after preparing compression. Prepare the plan again first.")
            return
        if not self.encode_preparation.jobs:
            self._show_error("There are no jobs selected in the current compression plan.")
            return
        reply = QMessageBox.question(
            self,
            "mediaflow",
            "Start compression with the current plan?",
        )
        if reply != QMessageBox.Yes:
            return
        self.file_progress.setValue(0)
        self.overall_progress.setValue(0)
        self._set_state(WorkflowState.COMPRESSING)
        self._append_status("Starting compression.")
        worker = FunctionWorker(run_compression, self.encode_preparation)
        self._start_worker(worker, self._compression_complete, self._encode_progress)

    def _encode_progress(self, progress: object) -> None:
        if not isinstance(progress, EncodeProgress):
            return
        self.file_progress.setValue(int(progress.current_file_progress * 100))
        self.overall_progress.setValue(int(progress.overall_progress * 100))
        self._append_status(
            f"{progress.current_file} | completed {progress.completed_files} | "
            f"remaining {progress.remaining_files} | state {progress.heartbeat_state}"
        )

    def _compression_complete(self, results: list) -> None:
        self.encode_results = list(results)
        self._append_status("Compression stage complete.")
        self._refresh_pipeline_summary()
        self._switch_tab("summary")
        self._set_state(WorkflowState.COMPLETED)

    def _refresh_pipeline_summary(self) -> None:
        summary = build_pipeline_summary(self.apply_result, self.encode_results)
        lines = [
            "Pipeline Summary",
            f"Organised plans: {summary.organised_plans}",
            f"Organise errors: {summary.organised_errors}",
            f"Encoded files: {summary.encoded_files}",
            f"Skipped files: {summary.skipped_files}",
            f"Failed files: {summary.failed_files}",
            f"Bytes saved: {summary.bytes_saved}",
        ]
        if summary.organise_report_path:
            lines.append(f"Organise report: {summary.organise_report_path}")
        if summary.organise_apply_report_path:
            lines.append(f"Organise apply report: {summary.organise_apply_report_path}")
        self.summary_overview_label.setText("\n".join(lines))

        details: list[str] = []
        if self.preview_state is not None:
            details.append("Organisation preview")
            details.extend(self.preview_state.summary_lines)
        if self.apply_result is not None:
            details.append("")
            details.append("Organisation apply")
            details.extend(self.apply_result.summary_lines)
        if self.encode_preparation is not None:
            details.append("")
            details.append("Compression plan")
            details.append(self.compress_summary_label.text())
            if self.encode_preparation.recommendation_reason:
                details.append(f"Reason: {self.encode_preparation.recommendation_reason}")
            if self.encode_preparation.size_confidence:
                details.append(f"Size confidence: {self.encode_preparation.size_confidence}")
            if self.encode_preparation.time_confidence:
                details.append(f"Time confidence: {self.encode_preparation.time_confidence}")
            if self.encode_preparation.grouped_incompatibilities:
                details.append("Likely follow-up incompatibilities:")
                details.extend(
                    f"- {name}: {count}"
                    for name, count in sorted(self.encode_preparation.grouped_incompatibilities.items())
                )
        if self.encode_results:
            details.append("")
            details.append("Compression results")
            for result in self.encode_results:
                status = "skipped" if result.skipped else "ok" if result.success else "failed"
                details.append(f"{result.job.source.name}: {status}")
                if result.error_message:
                    details.append(f"  {result.error_message}")
        self._set_summary_text("\n".join(details).strip())
