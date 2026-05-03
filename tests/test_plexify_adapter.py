from __future__ import annotations

import importlib
import sys
from types import ModuleType


def _load_adapter_with_stubbed_plexify():
    plexify_module = ModuleType("plexify")
    paths_module = ModuleType("plexify.paths")
    ui_controller_module = ModuleType("plexify.ui_controller")
    paths_module.PathOverlapError = RuntimeError
    paths_module.ensure_non_overlapping_paths = lambda *args, **kwargs: None
    ui_controller_module.PreviewState = object
    ui_controller_module.VideoUIConfig = object
    ui_controller_module.VideoUIController = object
    plexify_module.paths = paths_module
    plexify_module.ui_controller = ui_controller_module

    original_plexify = sys.modules.get("plexify")
    original_paths = sys.modules.get("plexify.paths")
    original_ui_controller = sys.modules.get("plexify.ui_controller")
    original_adapter = sys.modules.get("mediaflow.plexify_adapter")

    sys.modules["plexify"] = plexify_module
    sys.modules["plexify.paths"] = paths_module
    sys.modules["plexify.ui_controller"] = ui_controller_module
    sys.modules.pop("mediaflow.plexify_adapter", None)

    try:
        adapter = importlib.import_module("mediaflow.plexify_adapter")
    finally:
        sys.modules.pop("mediaflow.plexify_adapter", None)
        if original_adapter is not None:
            sys.modules["mediaflow.plexify_adapter"] = original_adapter
        if original_plexify is not None:
            sys.modules["plexify"] = original_plexify
        else:
            sys.modules.pop("plexify", None)
        if original_paths is not None:
            sys.modules["plexify.paths"] = original_paths
        else:
            sys.modules.pop("plexify.paths", None)
        if original_ui_controller is not None:
            sys.modules["plexify.ui_controller"] = original_ui_controller
        else:
            sys.modules.pop("plexify.ui_controller", None)

    return adapter


class _LegacyController:
    def __init__(self) -> None:
        self.called_without_progress = False

    def scan(self) -> None:
        self.called_without_progress = True


class _ProgressController:
    def __init__(self) -> None:
        self.progress_callback = None
        self.cancel_callback = None

    def scan(self, *, progress_callback=None) -> None:
        self.progress_callback = progress_callback

    def apply_preview(self, preview, *, progress_callback=None, cancel_callback=None):
        self.progress_callback = progress_callback
        self.cancel_callback = cancel_callback
        return preview


class _LegacyApplyController:
    def __init__(self) -> None:
        self.preview = None

    def apply_preview(self, preview):
        self.preview = preview
        return preview


def test_scan_controller_supports_legacy_plexify_scan_signature() -> None:
    adapter = _load_adapter_with_stubbed_plexify()
    controller = _LegacyController()

    result = adapter.scan_controller(controller, progress_callback=lambda _payload: None)

    assert result is controller
    assert controller.called_without_progress is True


def test_scan_controller_passes_progress_callback_when_supported() -> None:
    adapter = _load_adapter_with_stubbed_plexify()
    controller = _ProgressController()
    callback = lambda _payload: None

    result = adapter.scan_controller(controller, progress_callback=callback)

    assert result is controller
    assert controller.progress_callback is callback


def test_apply_preview_controller_supports_legacy_signature() -> None:
    adapter = _load_adapter_with_stubbed_plexify()
    controller = _LegacyApplyController()
    preview = object()

    result = adapter.apply_preview_controller(controller, preview, progress_callback=lambda _payload: None)

    assert result is preview
    assert controller.preview is preview


def test_apply_preview_controller_converts_apply_progress_payload() -> None:
    adapter = _load_adapter_with_stubbed_plexify()
    controller = _ProgressController()
    captured = []

    adapter.apply_preview_controller(controller, object(), progress_callback=captured.append)
    controller.progress_callback(
        {
            "phase": "copying",
            "completed": 1,
            "total": 3,
            "current_source": "/tmp/source.mp4",
            "current_destination": "/tmp/dest.mp4",
            "source_size_bytes": 100,
            "bytes_copied": 50,
            "operation": "copying",
            "report_path": "/tmp/report.json",
            "conflict_action": "overwrite",
            "cancel_requested": True,
            "message": "Copying source.mp4",
        }
    )

    assert captured[0].phase == "copying"
    assert captured[0].completed == 1
    assert captured[0].current_source == "/tmp/source.mp4"
    assert captured[0].source_size_bytes == 100
    assert captured[0].bytes_copied == 50
    assert captured[0].operation == "copying"
    assert captured[0].report_path == "/tmp/report.json"
    assert captured[0].conflict_action == "overwrite"
    assert captured[0].cancel_requested is True


def test_apply_preview_controller_passes_cancel_callback_when_supported() -> None:
    adapter = _load_adapter_with_stubbed_plexify()
    controller = _ProgressController()
    cancel = lambda: False

    adapter.apply_preview_controller(controller, object(), progress_callback=lambda _payload: None, cancel_callback=cancel)

    assert controller.cancel_callback is cancel
