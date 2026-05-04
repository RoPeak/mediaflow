from __future__ import annotations

import inspect
from pathlib import Path
from typing import Callable

from plexify.ui_controller import PreviewState, VideoUIConfig, VideoUIController

from .callback_types import ApplyProgress
from .config import PipelineConfig


def build_video_controller(config: PipelineConfig) -> VideoUIController:
    return VideoUIController(
        VideoUIConfig(
            incoming=config.source,
            library=config.library,
            mode="apply" if config.plexify.apply else "dry-run",
            copy_mode=config.plexify.copy_mode,
            copy_workers=config.plexify.copy_workers,
            extensions=config.plexify.extensions,
            min_confidence=config.plexify.min_confidence,
            use_cache=config.plexify.use_cache,
            offline=config.plexify.offline,
            on_conflict=config.plexify.on_conflict,
        )
    )


def scan_controller(
    controller: VideoUIController,
    *,
    progress_callback: Callable[[object], None] | None = None,
) -> VideoUIController:
    if _supports_scan_progress_callback(controller):
        controller.scan(progress_callback=progress_callback)
    else:
        controller.scan()
    return controller


def build_preview(controller: VideoUIController) -> PreviewState:
    return controller.build_preview()


def apply_preview_controller(
    controller: VideoUIController,
    preview: PreviewState,
    *,
    progress_callback: Callable[[object], None] | None = None,
    cancel_callback: Callable[[], bool] | None = None,
):
    if _supports_apply_progress_callback(controller):
        kwargs = {
            "progress_callback": (
                (lambda payload: progress_callback(_convert_apply_progress(payload)))
                if progress_callback is not None
                else None
            )
        }
        if _supports_apply_cancel_callback(controller):
            kwargs["cancel_callback"] = cancel_callback
        return controller.apply_preview(preview, **kwargs)
    return controller.apply_preview(preview)


def _supports_scan_progress_callback(controller: VideoUIController) -> bool:
    try:
        signature = inspect.signature(controller.scan)
    except (TypeError, ValueError):
        return False
    return "progress_callback" in signature.parameters


def _supports_apply_progress_callback(controller: VideoUIController) -> bool:
    try:
        signature = inspect.signature(controller.apply_preview)
    except (TypeError, ValueError):
        return False
    return "progress_callback" in signature.parameters


def _supports_apply_cancel_callback(controller: VideoUIController) -> bool:
    try:
        signature = inspect.signature(controller.apply_preview)
    except (TypeError, ValueError):
        return False
    return "cancel_callback" in signature.parameters


def _convert_apply_progress(payload: object) -> object:
    if not isinstance(payload, dict):
        return payload
    return ApplyProgress(
        phase=str(payload.get("phase", "")),
        current_source=_path_text(payload.get("current_source")),
        current_destination=_path_text(payload.get("current_destination")),
        completed=int(payload.get("completed", 0) or 0),
        total=int(payload.get("total", 0) or 0),
        last_applied_source=_path_text(payload.get("last_applied_source")),
        message=str(payload.get("message") or "").strip() or None,
        operation=str(payload.get("operation") or "").strip() or None,
        source_size_bytes=_optional_int(payload.get("source_size_bytes")),
        bytes_copied=_optional_int(payload.get("bytes_copied")),
        current_file_bytes_copied=_optional_int(payload.get("current_file_bytes_copied")),
        completed_bytes=_optional_int(payload.get("completed_bytes")),
        total_bytes=_optional_int(payload.get("total_bytes")),
        active_files=_optional_int(payload.get("active_files")),
        parallel_workers=_optional_int(payload.get("parallel_workers")),
        progress_capability=str(payload.get("progress_capability") or "").strip() or None,
        started_at=str(payload.get("started_at") or "").strip() or None,
        completed_at=str(payload.get("completed_at") or "").strip() or None,
        report_path=_path_text(payload.get("report_path")),
        conflict_action=str(payload.get("conflict_action") or "").strip() or None,
        error=str(payload.get("error") or "").strip() or None,
        cancel_requested=bool(payload.get("cancel_requested", False)),
    )


def _path_text(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, Path):
        return str(value)
    text = str(value).strip()
    return text or None


def _optional_int(value: object) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
