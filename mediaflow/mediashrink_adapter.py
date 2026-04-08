from __future__ import annotations

from dataclasses import replace
from typing import Callable

from mediashrink.gui_api import (
    EncodePreparation,
    EncodeProgress,
    prepare_encode_run,
    prepare_tools,
    run_encode_plan,
)
from mediashrink.models import EncodeAttempt, EncodeJob, EncodeResult

from .config import PipelineConfig


def prepare_compression(config: PipelineConfig) -> EncodePreparation:
    return prepare_encode_run(
        directory=config.compression_root,
        recursive=config.shrink.recursive,
        overwrite=config.shrink.overwrite,
        no_skip=config.shrink.no_skip,
        policy=config.shrink.policy,
        on_file_failure=config.shrink.on_file_failure,
        use_calibration=config.shrink.use_calibration,
        duplicate_policy=config.shrink.duplicate_policy,
    )


def missing_job_sources(preparation: EncodePreparation) -> list:
    return [job.source for job in preparation.jobs if not job.source.exists()]


def run_compression(
    preparation: EncodePreparation,
    progress_callback: Callable[[object], None] | None = None,
) -> list[EncodeResult]:
    missing_results: list[EncodeResult] = []
    runnable_jobs: list[EncodeJob] = []
    for job in preparation.jobs:
        if job.source.exists():
            runnable_jobs.append(job)
        else:
            missing_results.append(_missing_result(job))

    if not runnable_jobs:
        return missing_results

    active_preparation = replace(preparation, jobs=runnable_jobs)
    results = run_encode_plan(
        active_preparation,
        on_progress=progress_callback,
        on_file_failure=preparation.on_file_failure,
        use_calibration=preparation.use_calibration,
    )
    return missing_results + list(results)


def _missing_result(job: EncodeJob) -> EncodeResult:
    reason = (
        "Source file was missing when compression started. "
        "The compression root likely changed after planning."
    )
    return EncodeResult(
        job=job,
        skipped=False,
        skip_reason=None,
        success=False,
        input_size_bytes=0,
        output_size_bytes=0,
        duration_seconds=0.0,
        error_message=reason,
        raw_error_message=reason,
        attempts=[
            EncodeAttempt(
                preset=job.preset,
                crf=job.crf,
                success=False,
                duration_seconds=0.0,
                progress_pct=0.0,
                error_message=reason,
                retry_kind="missing-source",
            )
        ],
    )


__all__ = [
    "EncodePreparation",
    "EncodeProgress",
    "missing_job_sources",
    "prepare_compression",
    "prepare_tools",
    "run_compression",
]
