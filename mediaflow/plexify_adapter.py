from __future__ import annotations

from typing import Callable

from plexify.ui_controller import PreviewState, VideoUIConfig, VideoUIController

from .config import PipelineConfig


def build_video_controller(config: PipelineConfig) -> VideoUIController:
    return VideoUIController(
        VideoUIConfig(
            incoming=config.source,
            library=config.library,
            mode="apply" if config.plexify.apply else "dry-run",
            copy_mode=config.plexify.copy_mode,
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
    controller.scan(progress_callback=progress_callback)
    return controller


def build_preview(controller: VideoUIController) -> PreviewState:
    return controller.build_preview()
