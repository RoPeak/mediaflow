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
    QSplitter,
    QTableWidget,
    QTableWidgetItem,
    QTabWidget,
    QVBoxLayout,
    QWidget,
)

from mediashrink.gui_api import EncodePreparation, EncodeProgress
from plexify.ui_controller import ApplyResultState, PreviewState, VideoUIController

from .config import PipelineConfig, PlexifySettings, ShrinkSettings, build_pipeline_config
from .mediashrink_adapter import prepare_compression, run_compression
from .pipeline import build_pipeline_summary, target_compression_root
from .plexify_adapter import build_preview, build_video_controller, scan_controller
from .settings import load_ui_state, save_ui_state
from .workers import FunctionWorker


class MainWindow(QMainWindow):
    def __init__(self, *, default_source: Path | None = None, default_library: Path | None = None) -> None:
        super().__init__()
        self.setWindowTitle("mediaflow")
        self.resize(1400, 900)
        self.thread_pool = QThreadPool.globalInstance()
        self.controller: VideoUIController | None = None
        self.preview_state: PreviewState | None = None
        self.apply_result: ApplyResultState | None = None
        self.encode_preparation: EncodePreparation | None = None
        self.encode_results: list = []
        self._active_worker_count = 0
        self._loading_state = True
        self._pipeline_requested = False
        self._pipeline_should_compress_after_apply = False

        self.source_input = QLineEdit(str(default_source) if default_source else "")
        self.library_input = QLineEdit(str(default_library) if default_library else "")
        self.organise_enabled = QCheckBox("Enable organise stage")
        self.organise_enabled.setChecked(True)
        self.compress_enabled = QCheckBox("Enable compress stage")
        self.compress_enabled.setChecked(True)
        self.apply_mode = QCheckBox("Apply organisation")
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

        self.scan_button = QPushButton("Scan Organise Stage")
        self.run_pipeline_button = QPushButton("Run Full Pipeline")
        self.preview_button = QPushButton("Preview Organisation")
        self.apply_button = QPushButton("Apply Organisation")
        self.compress_button = QPushButton("Run Compression")
        self.cancel_button = QPushButton("Cancel")
        self.prev_item_button = QPushButton("Prev Item")
        self.next_item_button = QPushButton("Next Item")
        self.next_page_button = QPushButton("More Candidates")
        self.auto_accept_button = QPushButton("Auto-Accept Safe Matches")

        self.review_table = QTableWidget(0, 7)
        self.review_table.setHorizontalHeaderLabels(
            ["Source", "Type", "Title", "Season/Episode", "Selected", "Status", "Warning"]
        )
        self.review_table.setSelectionBehavior(QTableWidget.SelectRows)
        self.review_table.setSelectionMode(QTableWidget.SingleSelection)
        self.review_table.setEditTriggers(QTableWidget.NoEditTriggers)

        self.candidate_table = QTableWidget(0, 4)
        self.candidate_table.setHorizontalHeaderLabels(["Title", "Year", "Source", "Confidence"])
        self.candidate_table.setSelectionBehavior(QTableWidget.SelectRows)
        self.candidate_table.setSelectionMode(QTableWidget.SingleSelection)
        self.candidate_table.setEditTriggers(QTableWidget.NoEditTriggers)

        self.search_input = QLineEdit()
        self.search_input.setPlaceholderText("Search query or manual title")
        self.details_log = QPlainTextEdit()
        self.details_log.setReadOnly(True)
        self.accept_button = QPushButton("Accept Candidate")
        self.skip_button = QPushButton("Skip Item")
        self.search_button = QPushButton("Search")
        self.switch_button = QPushButton("Switch TV/Movie")
        self.manual_button = QPushButton("Manual Title")
        self.folder_button = QPushButton("Apply To Folder")
        self.title_group_button = QPushButton("Apply To Title Group")

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
        self.status_log = QPlainTextEdit()
        self.status_log.setReadOnly(True)
        self.summary_log = QPlainTextEdit()
        self.summary_log.setReadOnly(True)

        self._build_ui()
        self._connect_signals()
        self._restore_ui_state(default_source=default_source, default_library=default_library)
        self._loading_state = False
        self._update_stage_controls()
        self._update_action_state()

    def _build_ui(self) -> None:
        root = QWidget()
        self.setCentralWidget(root)
        layout = QVBoxLayout(root)

        top_split = QSplitter(Qt.Horizontal)
        top_split.addWidget(self._build_config_panel())
        top_split.addWidget(self._build_review_panel())
        top_split.setStretchFactor(0, 0)
        top_split.setStretchFactor(1, 1)
        layout.addWidget(top_split, stretch=3)

        tabs = QTabWidget()
        tabs.addTab(self._build_run_panel(), "Run")
        tabs.addTab(self._build_summary_panel(), "Summary")
        layout.addWidget(tabs, stretch=2)

    def _build_config_panel(self) -> QWidget:
        panel = QWidget()
        layout = QVBoxLayout(panel)

        path_group = QGroupBox("Paths")
        path_form = QGridLayout(path_group)
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

        organise_group = QGroupBox("Organise Settings")
        self.organise_group = organise_group
        organise_form = QFormLayout(organise_group)
        organise_form.addRow(self.apply_mode)
        organise_form.addRow(self.copy_mode)
        organise_form.addRow(self.use_cache)
        organise_form.addRow(self.offline)
        organise_form.addRow("Minimum confidence", self.min_confidence)
        organise_form.addRow("Extensions", self.extensions_input)
        organise_form.addRow("Conflict handling", self.conflict_mode)
        layout.addWidget(organise_group)

        compress_group = QGroupBox("Compress Settings")
        self.compress_group = compress_group
        compress_form = QFormLayout(compress_group)
        compress_form.addRow(self.overwrite)
        compress_form.addRow(self.recursive)
        compress_form.addRow(self.no_skip)
        compress_form.addRow("Recommendation policy", self.policy)
        compress_form.addRow("On file failure", self.on_file_failure)
        compress_form.addRow(self.use_calibration)
        compress_form.addRow("Duplicate policy", self.duplicate_policy)
        layout.addWidget(compress_group)

        button_row = QHBoxLayout()
        button_row.addWidget(self.scan_button)
        button_row.addWidget(self.run_pipeline_button)
        button_row.addWidget(self.preview_button)
        button_row.addWidget(self.apply_button)
        button_row.addWidget(self.compress_button)
        button_row.addWidget(self.cancel_button)
        layout.addLayout(button_row)
        layout.addStretch(1)
        return panel

    def _build_review_panel(self) -> QWidget:
        panel = QWidget()
        layout = QVBoxLayout(panel)
        layout.addWidget(QLabel("Plexify Review"))
        layout.addWidget(self.review_table, stretch=2)
        layout.addWidget(QLabel("Candidates"))
        layout.addWidget(self.candidate_table, stretch=1)
        layout.addWidget(QLabel("Selected Item Details"))
        layout.addWidget(self.details_log, stretch=1)

        action_row = QHBoxLayout()
        action_row.addWidget(self.search_input)
        action_row.addWidget(self.search_button)
        action_row.addWidget(self.manual_button)
        layout.addLayout(action_row)

        button_row = QHBoxLayout()
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
            button_row.addWidget(button)
        layout.addLayout(button_row)
        return panel

    def _build_run_panel(self) -> QWidget:
        panel = QWidget()
        layout = QVBoxLayout(panel)
        layout.addWidget(QLabel("Preparation"))
        layout.addWidget(self.prepare_progress)
        layout.addWidget(QLabel("Current file"))
        layout.addWidget(self.file_progress)
        layout.addWidget(QLabel("Overall"))
        layout.addWidget(self.overall_progress)
        layout.addWidget(QLabel("Compression Plan"))
        self.compression_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        layout.addWidget(self.compression_table)
        layout.addWidget(QLabel("Status"))
        layout.addWidget(self.status_log)
        return panel

    def _build_summary_panel(self) -> QWidget:
        panel = QWidget()
        layout = QVBoxLayout(panel)
        layout.addWidget(QLabel("Summary"))
        layout.addWidget(self.summary_log)
        return panel

    def _connect_signals(self) -> None:
        self.organise_enabled.toggled.connect(self._update_stage_controls)
        self.compress_enabled.toggled.connect(self._update_stage_controls)
        self.scan_button.clicked.connect(self._start_scan)
        self.run_pipeline_button.clicked.connect(self._start_pipeline)
        self.preview_button.clicked.connect(self._preview_plan)
        self.apply_button.clicked.connect(self._apply_plan)
        self.compress_button.clicked.connect(self._start_compression)
        self.cancel_button.clicked.connect(self._cancel_requested)
        self.review_table.itemSelectionChanged.connect(self._review_selection_changed)
        self.candidate_table.itemDoubleClicked.connect(lambda *_: self._accept_selected_candidate())
        self.accept_button.clicked.connect(self._accept_selected_candidate)
        self.skip_button.clicked.connect(self._skip_selected_item)
        self.prev_item_button.clicked.connect(lambda: self._move_review_selection(-1))
        self.next_item_button.clicked.connect(lambda: self._move_review_selection(1))
        self.next_page_button.clicked.connect(self._load_next_candidate_page)
        self.auto_accept_button.clicked.connect(self._auto_accept_safe_matches)
        self.search_button.clicked.connect(self._search_current_item)
        self.switch_button.clicked.connect(self._switch_current_item)
        self.manual_button.clicked.connect(self._manual_select_current_item)
        self.folder_button.clicked.connect(self._apply_choice_to_folder)
        self.title_group_button.clicked.connect(self._apply_choice_to_title_group)
        self._connect_state_change_signals()

    def _connect_state_change_signals(self) -> None:
        for widget in [
            self.source_input,
            self.library_input,
            self.extensions_input,
        ]:
            widget.textChanged.connect(self._on_config_edited)
        for widget in [
            self.organise_enabled,
            self.compress_enabled,
            self.apply_mode,
            self.copy_mode,
            self.use_cache,
            self.offline,
            self.overwrite,
            self.recursive,
            self.no_skip,
            self.use_calibration,
        ]:
            widget.toggled.connect(self._on_config_edited)
        self.min_confidence.valueChanged.connect(self._on_config_edited)
        for widget in [self.conflict_mode, self.policy, self.on_file_failure, self.duplicate_policy]:
            widget.currentTextChanged.connect(self._on_config_edited)

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

    def _on_config_edited(self, *_args) -> None:
        if self._loading_state:
            return
        if self.controller is not None or self.preview_state is not None or self.encode_preparation is not None:
            self._invalidate_runtime_state("Configuration changed. Cleared previous review and run state.")

    def _invalidate_runtime_state(self, status_message: str | None = None) -> None:
        self.controller = None
        self.preview_state = None
        self.apply_result = None
        self.encode_preparation = None
        self.encode_results = []
        self._pipeline_requested = False
        self._pipeline_should_compress_after_apply = False
        self.review_table.setRowCount(0)
        self.candidate_table.setRowCount(0)
        self.compression_table.setRowCount(0)
        self.details_log.clear()
        self.summary_log.clear()
        self.prepare_progress.setValue(0)
        self.file_progress.setValue(0)
        self.overall_progress.setValue(0)
        if status_message:
            self._append_status(status_message)
        self._update_action_state()

    def closeEvent(self, event: QCloseEvent) -> None:  # pragma: no cover - GUI runtime path
        self._persist_ui_state()
        super().closeEvent(event)

    def _browse_into(self, target: QLineEdit) -> None:
        selected = QFileDialog.getExistingDirectory(self, "Select directory", target.text() or str(Path.home()))
        if selected:
            target.setText(selected)

    def _update_stage_controls(self) -> None:
        self.organise_group.setEnabled(self.organise_enabled.isChecked())
        self.compress_group.setEnabled(self.compress_enabled.isChecked())
        self._update_action_state()

    def _update_action_state(self) -> None:
        has_controller = self.controller is not None and bool(self.controller.items)
        has_review_selection = has_controller and self._current_review_index() is not None
        has_preview = self.preview_state is not None
        has_more_candidates = False
        if has_review_selection and self.controller is not None:
            current = self.controller.items[self._current_review_index() or 0]
            has_more_candidates = current.has_more

        self.run_pipeline_button.setEnabled(self._active_worker_count == 0)
        self.preview_button.setEnabled(self.organise_enabled.isChecked() and has_controller)
        self.apply_button.setEnabled(
            self.organise_enabled.isChecked() and has_controller and has_preview
        )
        self.prev_item_button.setEnabled(self.organise_enabled.isChecked() and has_review_selection)
        self.next_item_button.setEnabled(self.organise_enabled.isChecked() and has_review_selection)
        self.accept_button.setEnabled(self.organise_enabled.isChecked() and has_review_selection)
        self.skip_button.setEnabled(self.organise_enabled.isChecked() and has_review_selection)
        self.next_page_button.setEnabled(
            self.organise_enabled.isChecked() and has_review_selection and has_more_candidates
        )
        self.auto_accept_button.setEnabled(self.organise_enabled.isChecked() and has_controller)
        self.search_button.setEnabled(self.organise_enabled.isChecked() and has_review_selection)
        self.switch_button.setEnabled(self.organise_enabled.isChecked() and has_review_selection)
        self.manual_button.setEnabled(self.organise_enabled.isChecked() and has_review_selection)
        self.folder_button.setEnabled(self.organise_enabled.isChecked() and has_review_selection)
        self.title_group_button.setEnabled(
            self.organise_enabled.isChecked() and has_review_selection
        )
        self.candidate_table.setEnabled(self.organise_enabled.isChecked() and has_review_selection)
        self.review_table.setEnabled(self.organise_enabled.isChecked())
        self.compress_button.setEnabled(self.compress_enabled.isChecked())

    def _set_busy(self, busy: bool) -> None:
        self.scan_button.setEnabled(not busy and self.organise_enabled.isChecked())
        self.compress_button.setEnabled(not busy and self.compress_enabled.isChecked())
        self.run_pipeline_button.setEnabled(not busy)
        self.cancel_button.setEnabled(True)
        if busy:
            self.preview_button.setEnabled(False)
            self.apply_button.setEnabled(False)
            self.prev_item_button.setEnabled(False)
            self.next_item_button.setEnabled(False)
            self.accept_button.setEnabled(False)
            self.skip_button.setEnabled(False)
            self.next_page_button.setEnabled(False)
            self.auto_accept_button.setEnabled(False)
            self.search_button.setEnabled(False)
            self.switch_button.setEnabled(False)
            self.manual_button.setEnabled(False)
            self.folder_button.setEnabled(False)
            self.title_group_button.setEnabled(False)
        else:
            self._update_action_state()

    def _append_status(self, text: str) -> None:
        self.status_log.appendPlainText(text)

    def _append_summary(self, text: str) -> None:
        self.summary_log.appendPlainText(text)

    def _set_summary_text(self, text: str) -> None:
        self.summary_log.setPlainText(text)

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

    def _show_error(self, message: str) -> None:
        QMessageBox.critical(self, "mediaflow", message)

    def _start_worker(self, worker: FunctionWorker, on_result, on_progress=None) -> None:
        self._active_worker_count += 1
        self._set_busy(True)
        worker.signals.result.connect(on_result)
        worker.signals.error.connect(self._show_error)
        if on_progress is not None:
            worker.signals.progress.connect(on_progress)
        worker.signals.finished.connect(self._worker_finished)
        self.thread_pool.start(worker)

    def _worker_finished(self) -> None:
        self._active_worker_count = max(0, self._active_worker_count - 1)
        if self._active_worker_count == 0:
            self._set_busy(False)

    def _start_scan(self) -> None:
        try:
            config = self._current_config()
        except ValueError as exc:
            self._show_error(str(exc))
            return
        self._persist_ui_state()
        if not config.plexify.enabled:
            self._show_error("Organise stage is disabled.")
            return
        controller = build_video_controller(config)
        self._append_status("Scanning source with plexify...")
        worker = FunctionWorker(scan_controller, controller)
        self._start_worker(worker, self._scan_complete)

    def _start_pipeline(self) -> None:
        try:
            config = self._current_config()
        except ValueError as exc:
            self._show_error(str(exc))
            return
        self._persist_ui_state()
        self._pipeline_requested = True
        self._pipeline_should_compress_after_apply = config.shrink.enabled
        if config.plexify.enabled:
            self._append_status("Starting full pipeline with organise scan.")
            controller = build_video_controller(config)
            worker = FunctionWorker(scan_controller, controller)
            self._start_worker(worker, self._scan_complete)
            return
        self._append_status("Organise stage disabled. Starting compression stage directly.")
        self._start_compression()

    def _scan_complete(self, controller: VideoUIController) -> None:
        self.controller = controller
        self.preview_state = None
        self.apply_result = None
        self._populate_review_table()
        self._append_status(f"Loaded {len(controller.items)} item(s) for review.")
        if self._pipeline_requested:
            accepted = self._auto_accept_safe_matches()
            self._append_status(f"Auto-accepted {accepted} high-confidence match(es).")
            self._preview_plan()
            if self.preview_state is not None and self.preview_state.can_apply:
                reply = QMessageBox.question(
                    self,
                    "mediaflow",
                    "Organise preview is fully resolved. Apply it and continue the pipeline?",
                )
                if reply == QMessageBox.Yes:
                    self._apply_plan()
                else:
                    self._pipeline_requested = False
                    self._pipeline_should_compress_after_apply = False
            else:
                self._append_status(
                    "Pipeline paused for manual review. Resolve remaining items, then apply to continue."
                )
        self._update_action_state()

    def _populate_review_table(self) -> None:
        self.review_table.setRowCount(0)
        if self.controller is None:
            self._update_action_state()
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
        self._update_action_state()

    def _review_selection_changed(self) -> None:
        index = self._current_review_index()
        if index is not None:
            self._populate_candidate_table(index)
            self._populate_detail_view(index)
        else:
            self.details_log.clear()
        self._update_action_state()

    def _current_review_index(self) -> int | None:
        indexes = self.review_table.selectionModel().selectedRows()
        if not indexes:
            return None
        return indexes[0].row()

    def _populate_candidate_table(self, review_index: int) -> None:
        self.candidate_table.setRowCount(0)
        if self.controller is None:
            self._update_action_state()
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
        self._update_action_state()

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
        if item.manual_candidate is not None:
            lines.append(f"Manual candidate: {item.manual_candidate.title}")
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

    def _refresh_review(self) -> None:
        index = self._current_review_index()
        self._populate_review_table()
        if index is not None and self.review_table.rowCount() > 0:
            self.review_table.selectRow(min(index, self.review_table.rowCount() - 1))

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

    def _move_review_selection(self, delta: int) -> None:
        if self.review_table.rowCount() == 0:
            return
        index = self._current_review_index()
        current = index if index is not None else 0
        target = max(0, min(self.review_table.rowCount() - 1, current + delta))
        self.review_table.selectRow(target)

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
        self._refresh_review()

    def _apply_choice_to_folder(self) -> None:
        if self.controller is None:
            return
        index = self._current_review_index()
        if index is None:
            return
        self.controller.apply_choice_to_folder(index)
        self._refresh_review()

    def _apply_choice_to_title_group(self) -> None:
        if self.controller is None:
            return
        index = self._current_review_index()
        if index is None:
            return
        self.controller.apply_choice_to_title_group(index)
        self._refresh_review()

    def _preview_plan(self) -> None:
        if self.controller is None:
            self._show_error("Scan the organise stage first.")
            return
        self.preview_state = build_preview(self.controller)
        lines = list(self.preview_state.summary_lines)
        if self.preview_state.unresolved_items:
            lines.append("")
            lines.append("Unresolved:")
            lines.extend(self.preview_state.unresolved_items[:10])
        if self.preview_state.warnings:
            lines.append("")
            lines.extend(f"Warning: {warning}" for warning in self.preview_state.warnings[:10])
        self.summary_log.setPlainText("\n".join(lines))
        self._append_status("Built organisation preview.")
        self._update_action_state()

    def _apply_plan(self) -> None:
        if self.controller is None:
            self._show_error("Scan the organise stage first.")
            return
        if self.preview_state is None:
            self.preview_state = build_preview(self.controller)
        if not self.preview_state.can_apply:
            self._show_error("Resolve all unresolved items before applying.")
            return
        reply = QMessageBox.question(
            self,
            "mediaflow",
            "Apply the current organisation plan to disk?",
        )
        if reply != QMessageBox.Yes:
            return
        self._append_status("Applying organisation plan...")
        worker = FunctionWorker(self.controller.apply_preview, self.preview_state)
        self._start_worker(worker, self._apply_complete)

    def _apply_complete(self, result: ApplyResultState) -> None:
        self.apply_result = result
        lines = list(result.summary_lines)
        if result.warnings:
            lines.append("")
            lines.extend(f"Warning: {warning}" for warning in result.warnings[:10])
        if result.result.errors:
            lines.append("")
            lines.extend(f"Error: {error}" for error in result.result.errors[:10])
        self._set_summary_text("\n".join(lines))
        self._append_status("Organisation stage complete.")
        self._refresh_pipeline_summary()
        self._update_action_state()
        if self._pipeline_requested and self._pipeline_should_compress_after_apply:
            self._append_status("Continuing full pipeline into compression.")
            self._start_compression()
            self._pipeline_requested = False

    def _start_compression(self) -> None:
        try:
            config = self._current_config()
        except ValueError as exc:
            self._show_error(str(exc))
            return
        self._persist_ui_state()
        if not config.shrink.enabled:
            self._show_error("Compress stage is disabled.")
            return
        self.prepare_progress.setValue(0)
        self.file_progress.setValue(0)
        self.overall_progress.setValue(0)
        self._append_status(f"Preparing compression run in {target_compression_root(config)}...")
        worker = FunctionWorker(prepare_compression, config)
        self._start_worker(worker, self._compression_prepared, self._preparation_progress)

    def _preparation_progress(self, payload: object) -> None:
        if not isinstance(payload, tuple) or len(payload) != 3:
            return
        completed, total, path = payload
        if total:
            self.prepare_progress.setValue(int((completed / total) * 100))
        self._append_status(f"Analyzed {completed}/{total}: {Path(path).name}")

    def _compression_prepared(self, preparation: EncodePreparation) -> None:
        self.encode_preparation = preparation
        self._populate_compression_table(preparation)
        if not preparation.items:
            self._append_status("No supported video files found for compression.")
            self._append_summary("Compression: no supported files found.")
            return
        if preparation.profile is None or not preparation.jobs:
            self._append_status("No compressible files selected by mediashrink.")
            self._append_summary("Compression: no recommended or maybe files available to run.")
            return
        summary_lines = [
            f"Compression root: {preparation.directory}",
            f"Recommended: {preparation.recommended_count}",
            f"Maybe: {preparation.maybe_count}",
            f"Skip: {preparation.skip_count}",
            f"Selected for encode: {preparation.selected_count}",
            (
                f"Selected profile: {preparation.profile.name} "
                f"({preparation.profile.encoder_key}, CRF {preparation.profile.crf})"
            ),
            f"Total bytes scanned: {preparation.total_input_bytes}",
            f"Selected input bytes: {preparation.selected_input_bytes}",
            f"Selected est. output bytes: {preparation.selected_estimated_output_bytes}",
        ]
        if preparation.estimated_total_seconds:
            summary_lines.append(f"Estimated encode seconds: {int(preparation.estimated_total_seconds)}")
        self._set_summary_text("\n".join(summary_lines))
        self._append_status(
            f"Selected profile {preparation.profile.name} "
            f"({preparation.profile.encoder_key}, CRF {preparation.profile.crf})."
        )
        if preparation.duplicate_warnings:
            for warning in preparation.duplicate_warnings:
                self._append_status(f"Duplicate policy: {warning}")
        reply = QMessageBox.question(
            self,
            "mediaflow",
            "Compression plan is ready. Start encoding now?",
        )
        if reply != QMessageBox.Yes:
            self._append_status("Compression prepared but not started.")
            self._refresh_pipeline_summary()
            self._pipeline_requested = False
            self._pipeline_should_compress_after_apply = False
            return
        worker = FunctionWorker(run_compression, preparation)
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
        encoded = sum(1 for result in results if result.success and not result.skipped)
        skipped = sum(1 for result in results if result.skipped)
        failed = sum(1 for result in results if not result.success and not result.skipped)
        saved_bytes = sum(max(result.input_size_bytes - result.output_size_bytes, 0) for result in results)
        lines = [
            f"Compression complete for {len(results)} file(s).",
            f"Encoded: {encoded}",
            f"Skipped: {skipped}",
            f"Failed: {failed}",
            f"Bytes saved: {saved_bytes}",
        ]
        self._append_summary("\n".join(lines))
        for result in results:
            if result.error_message:
                self._append_summary(f"{result.job.source.name}: {result.error_message}")
        self._append_status("Compression stage complete.")
        self._refresh_pipeline_summary()
        self._pipeline_requested = False
        self._pipeline_should_compress_after_apply = False

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
        existing = self.summary_log.toPlainText().strip()
        if existing:
            lines.append("")
            lines.append(existing)
        self._set_summary_text("\n".join(lines))

    def _cancel_requested(self) -> None:
        if self._active_worker_count:
            self._show_error(
                "Cancellation is not wired into the current worker pipeline yet. "
                "Wait for the active task to finish, or stop the process externally if needed."
            )
            return
        self._pipeline_requested = False
        self._pipeline_should_compress_after_apply = False
        self.status_log.clear()
        self.summary_log.clear()
