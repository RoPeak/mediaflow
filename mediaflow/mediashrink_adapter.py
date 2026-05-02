from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from typing import Callable

from mediashrink.analysis import (
    estimate_analysis_encode_seconds,
    estimate_size_confidence,
    estimate_time_confidence,
)
from mediashrink.gui_api import (
    EncodePreparation,
    EncodeProgress,
    prepare_encode_run,
    prepare_tools,
    run_encode_plan,
)
from mediashrink.models import EncodeAttempt, EncodeJob, EncodeResult
from mediashrink.scanner import build_jobs
from mediashrink.wizard import prepare_profile_planning

from .config import PipelineConfig
from .callback_types import PreparationProgress, PreparationStageUpdate


def prepare_compression(
    config: PipelineConfig,
    progress_callback: Callable[[object], None] | None = None,
) -> EncodePreparation:
    preparation = prepare_encode_run(
        directory=config.compression_root,
        recursive=config.shrink.recursive,
        overwrite=config.shrink.overwrite,
        no_skip=config.shrink.no_skip,
        policy=config.shrink.policy,
        on_file_failure=config.shrink.on_file_failure,
        use_calibration=config.shrink.use_calibration,
        duplicate_policy=config.shrink.duplicate_policy,
        progress_callback=(
            (lambda payload: progress_callback(_convert_preparation_payload(payload)))
            if progress_callback is not None
            else None
        ),
    )
    return _recover_zero_job_preparation(preparation, config)


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


def prepare_retry_compression(
    config: PipelineConfig,
    retry_sources: set[Path],
    progress_callback: Callable[[object], None] | None = None,
) -> EncodePreparation:
    retry_config = replace(
        config,
        shrink=replace(
            config.shrink,
            policy="highest-confidence",
            on_file_failure="skip",
            no_skip=True,
            duplicate_policy="prefer-mkv",
        ),
    )
    preparation = prepare_compression(retry_config, progress_callback=progress_callback)
    filtered = _filter_preparation_to_sources(preparation, retry_sources)
    extra_messages = list(filtered.stage_messages or [])
    extra_messages.append(
        "Retry plan uses compatibility-first defaults: highest-confidence policy, HEVC re-evaluation, and skip-on-failure."
    )
    extra_messages.append(
        "Review this retry plan carefully. It focuses on failed or compatibility-risk files only."
    )
    return replace(
        filtered,
        stage_messages=extra_messages,
        recommendation_reason=(
            filtered.recommendation_reason
            or "Compatibility-first retry plan prepared for failed or risky files."
        ),
    )


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


def _filter_preparation_to_sources(
    preparation: EncodePreparation,
    retry_sources: set[Path],
) -> EncodePreparation:
    if not retry_sources:
        return replace(
            preparation,
            items=[],
            jobs=[],
            recommended_count=0,
            maybe_count=0,
            skip_count=0,
            selected_count=0,
            total_input_bytes=0,
            selected_input_bytes=0,
            selected_estimated_output_bytes=0,
        )

    items = [item for item in preparation.items if item.source in retry_sources]
    jobs = [job for job in preparation.jobs if job.source in retry_sources]
    selected_sources = {job.source for job in jobs}
    recommended_count = sum(1 for item in items if item.recommendation == "recommended")
    maybe_count = sum(1 for item in items if item.recommendation == "maybe")
    skip_count = sum(1 for item in items if item.recommendation == "skip")
    total_input_bytes = sum(int(getattr(item, "size_bytes", 0) or 0) for item in items)
    selected_input_bytes = sum(
        int(getattr(item, "size_bytes", 0) or 0)
        for item in items
        if item.source in selected_sources
    )
    selected_estimated_output_bytes = sum(
        int(getattr(item, "estimated_output_bytes", 0) or 0)
        for item in items
        if item.source in selected_sources
    )
    return replace(
        preparation,
        items=items,
        jobs=jobs,
        recommended_count=recommended_count,
        maybe_count=maybe_count,
        skip_count=skip_count,
        selected_count=len(jobs),
        total_input_bytes=total_input_bytes,
        selected_input_bytes=selected_input_bytes,
        selected_estimated_output_bytes=selected_estimated_output_bytes,
    )


def _recover_zero_job_preparation(
    preparation: EncodePreparation,
    config: PipelineConfig,
) -> EncodePreparation:
    if preparation.jobs or not preparation.items:
        return preparation

    selected_items = [item for item in preparation.items if item.recommendation == "recommended"]
    if not selected_items:
        selected_items = [item for item in preparation.items if item.recommendation == "maybe"]
    if not selected_items:
        return preparation

    planning = prepare_profile_planning(
        analysis_items=preparation.items,
        ffmpeg=preparation.ffmpeg,
        ffprobe=preparation.ffprobe,
        policy=config.shrink.policy,
        use_calibration=preparation.use_calibration,
        console=None,
    )
    profiles = list(planning.profiles) if planning is not None else []
    profile = next((candidate for candidate in profiles if candidate.is_recommended), None)
    if profile is None and profiles:
        profile = profiles[0]
    if profile is None:
        messages = list(preparation.stage_messages or [])
        messages.append(
            "Compression analysis completed, but no encoder profile could be auto-selected. "
            "Review the plan details or rebuild the plan with different settings."
        )
        return replace(preparation, stage_messages=messages)

    jobs = build_jobs(
        files=[item.source for item in selected_items],
        output_dir=None,
        overwrite=config.shrink.overwrite,
        crf=profile.crf,
        preset=profile.encoder_key,
        dry_run=False,
        ffprobe=preparation.ffprobe,
        no_skip=config.shrink.no_skip,
    )
    messages = list(preparation.stage_messages or [])
    if preparation.profile is None:
        messages.append(
            "No encoder profile was auto-selected, so mediaflow chose the first available profile "
            f"({profile.name}) to keep the recommended plan runnable."
        )
    selected_input_bytes = sum(int(getattr(item, "size_bytes", 0) or 0) for item in selected_items)
    selected_estimated_output_bytes = sum(
        int(getattr(item, "estimated_output_bytes", 0) or 0)
        for item in selected_items
        if int(getattr(item, "estimated_output_bytes", 0) or 0) > 0
    )
    estimated_total_seconds = estimate_analysis_encode_seconds(
        selected_items,
        preset=profile.encoder_key,
        crf=profile.crf,
        ffmpeg=preparation.ffmpeg,
        known_speed=None,
        use_calibration=preparation.use_calibration,
        calibration_store=planning.active_calibration if planning is not None else None,
    )
    return replace(
        preparation,
        profile=profile,
        jobs=jobs,
        selected_count=len(selected_items),
        selected_input_bytes=selected_input_bytes,
        selected_estimated_output_bytes=selected_estimated_output_bytes,
        estimated_total_seconds=estimated_total_seconds,
        size_confidence=estimate_size_confidence(
            selected_items,
            preset=profile.encoder_key,
            use_calibration=preparation.use_calibration,
        ),
        time_confidence=estimate_time_confidence(
            selected_items,
            benchmarked_files=1 if planning is not None and planning.benchmark_speeds else 0,
            preset=profile.encoder_key,
            use_calibration=preparation.use_calibration,
        ),
        compatible_count=profile.compatible_count,
        incompatible_count=profile.incompatible_count,
        grouped_incompatibilities=profile.grouped_incompatibilities,
        recommendation_reason=preparation.recommendation_reason or profile.why_choose,
        stage_messages=messages,
    )


def _convert_preparation_payload(payload: object) -> object:
    if isinstance(payload, tuple) and len(payload) == 3:
        return PreparationProgress(*payload)
    if isinstance(payload, tuple) and len(payload) == 6 and payload[0] == "stage":
        _, stage, message, completed, total, _path = payload
        return PreparationStageUpdate(
            stage=str(stage),
            message=str(message),
            completed=int(completed) if completed is not None else None,
            total=int(total) if total is not None else None,
        )
    return payload


__all__ = [
    "EncodePreparation",
    "EncodeProgress",
    "missing_job_sources",
    "prepare_compression",
    "prepare_retry_compression",
    "prepare_tools",
    "run_compression",
]
