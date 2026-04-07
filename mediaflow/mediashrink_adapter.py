from __future__ import annotations

from mediashrink.gui_api import EncodePreparation, EncodeProgress, prepare_encode_run, run_encode_plan

from .config import PipelineConfig


def prepare_compression(config: PipelineConfig) -> EncodePreparation:
    return prepare_encode_run(
        directory=config.library,
        recursive=config.shrink.recursive,
        overwrite=config.shrink.overwrite,
        no_skip=config.shrink.no_skip,
        policy=config.shrink.policy,
        on_file_failure=config.shrink.on_file_failure,
        use_calibration=config.shrink.use_calibration,
        duplicate_policy=config.shrink.duplicate_policy,
    )


def run_compression(
    preparation: EncodePreparation,
    progress_callback: callable | None = None,
) -> list:
    return run_encode_plan(
        preparation,
        on_progress=progress_callback,
        on_file_failure=preparation.on_file_failure,
        use_calibration=preparation.use_calibration,
    )


__all__ = ["EncodePreparation", "EncodeProgress", "prepare_compression", "run_compression"]
