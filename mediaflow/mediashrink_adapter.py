from __future__ import annotations

from collections.abc import Collection
from dataclasses import replace
import inspect
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
from mediashrink.session import find_resumable_session, get_session_path, load_session
from mediashrink.wizard import prepare_profile_planning

from .callback_types import PreparationProgress, PreparationStageUpdate
from .config import PipelineConfig

try:
    from mediashrink.gui_api import EncodeRunResults as _NativeEncodeRunResults

    _HAS_NATIVE_ENCODE_RUN_RESULTS = True
except ImportError:
    _NativeEncodeRunResults = None
    _HAS_NATIVE_ENCODE_RUN_RESULTS = False


class EncodeRunResults(list):
    def __init__(
        self,
        results,
        *,
        session_path=None,
        resumed_from_session=False,
        session_status=None,
        stopped_early=False,
        interrupted=False,
    ):
        super().__init__(results)
        self.session_path = session_path
        self.resumed_from_session = resumed_from_session
        self.session_status = session_status or {}
        self.stopped_early = stopped_early
        self.interrupted = interrupted


def prepare_compression(
    config: PipelineConfig,
    progress_callback: Callable[[object], None] | None = None,
    source_paths: Collection[Path] | None = None,
    cancel_callback: Callable[[], bool] | None = None,
) -> EncodePreparation:
    kwargs = {
        "directory": config.compression_root,
        "recursive": config.shrink.recursive,
        "overwrite": config.shrink.overwrite,
        "no_skip": config.shrink.no_skip,
        "policy": config.shrink.policy,
        "on_file_failure": config.shrink.on_file_failure,
        "use_calibration": config.shrink.use_calibration,
        "duplicate_policy": config.shrink.duplicate_policy,
        "progress_callback": (
            (lambda payload: progress_callback(_convert_preparation_payload(payload)))
            if progress_callback is not None
            else None
        ),
    }
    if source_paths is not None and _supports_prepare_source_paths():
        kwargs["source_paths"] = source_paths
    if cancel_callback is not None and _supports_prepare_cancel_callback():
        kwargs["cancel_callback"] = cancel_callback
    try:
        preparation = prepare_encode_run(**kwargs)
    except TypeError as exc:
        unsupported = {
            name
            for name in ("source_paths", "cancel_callback")
            if name in kwargs and name in str(exc)
        }
        if not unsupported:
            raise
        for name in unsupported:
            kwargs.pop(name, None)
        preparation = prepare_encode_run(**kwargs)
    if source_paths is not None and "source_paths" not in kwargs:
        preparation = _filter_preparation_to_sources(preparation, set(source_paths))
    return _stabilize_preparation(preparation, config)


def prepare_safer_compression(
    config: PipelineConfig,
    progress_callback: Callable[[object], None] | None = None,
    source_paths: Collection[Path] | None = None,
    cancel_callback: Callable[[], bool] | None = None,
) -> EncodePreparation:
    safer_config = replace(
        config,
        shrink=replace(
            config.shrink,
            policy="highest-confidence",
            on_file_failure="skip",
            no_skip=True,
        ),
    )
    preparation = prepare_compression(
        safer_config,
        progress_callback=progress_callback,
        source_paths=source_paths,
        cancel_callback=cancel_callback,
    )
    extra_messages = list(preparation.stage_messages or [])
    extra_messages.append(
        "Safer rebuild uses compatibility-first defaults to prefer the most reliable runnable profile."
    )
    return replace(preparation, stage_messages=extra_messages)


def missing_job_sources(preparation: EncodePreparation) -> list:
    return [job.source for job in preparation.jobs if not job.source.exists()]


def resumable_session_status(preparation: EncodePreparation) -> dict[str, object] | None:
    jobs = list(preparation.jobs)
    if not jobs:
        return None
    session = find_resumable_session(
        preparation.directory,
        None,
        jobs[0].preset,
        jobs[0].crf,
    )
    if session is None:
        return None
    job_sources = {str(job.source) for job in jobs}
    session_sources = {entry.source for entry in session.entries}
    if not job_sources.issubset(session_sources):
        return None
    counts: dict[str, int] = {}
    for entry in session.entries:
        counts[entry.status] = counts.get(entry.status, 0) + 1
    return {
        "path": str(get_session_path(preparation.directory, None)),
        "counts": counts,
        "completed": counts.get("success", 0),
        "pending": counts.get("pending", 0) + counts.get("failed", 0) + counts.get("in_progress", 0),
    }


def session_path_for_preparation(preparation: EncodePreparation) -> Path:
    return get_session_path(preparation.directory, None)


def incomplete_session_status(
    directory: Path,
    source_paths: Collection[Path] | None = None,
) -> dict[str, object] | None:
    path = get_session_path(directory, None)
    session = load_session(path)
    if session is None:
        return None
    requested_sources = (
        {str(Path(path).resolve(strict=False)) for path in source_paths}
        if source_paths is not None
        else None
    )
    relevant_entries = [
        entry
        for entry in session.entries
        if requested_sources is None or str(Path(entry.source).resolve(strict=False)) in requested_sources
    ]
    if not relevant_entries:
        return None
    counts: dict[str, int] = {}
    for entry in relevant_entries:
        counts[entry.status] = counts.get(entry.status, 0) + 1
    pending = counts.get("pending", 0) + counts.get("failed", 0) + counts.get("in_progress", 0)
    if pending <= 0:
        return None
    current = next((entry for entry in relevant_entries if entry.status in {"failed", "in_progress"}), None)
    if current is None:
        current = next((entry for entry in relevant_entries if entry.status == "pending"), None)
    return {
        "path": str(path),
        "counts": counts,
        "completed": counts.get("success", 0),
        "pending": pending,
        "source_paths": [entry.source for entry in relevant_entries],
        "current_file": current.source if current is not None else None,
        "last_progress_pct": getattr(current, "last_progress_pct", None) if current is not None else None,
        "last_progress_at": getattr(current, "last_progress_at", None) if current is not None else None,
        "compatible_with_source_paths": requested_sources is not None,
    }


def run_compression(
    preparation: EncodePreparation,
    progress_callback: Callable[[object], None] | None = None,
    session_path: Path | None = None,
    resume: bool = False,
    cancel_callback: Callable[[], bool] | None = None,
    quarantine_originals: bool = False,
    quarantine_dir: Path | None = None,
    quarantine_retention_days: int | None = None,
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
        session_path=session_path,
        resume=resume,
        cancel_callback=cancel_callback,
        stop_mode="graceful",
        quarantine_originals=quarantine_originals,
        quarantine_dir=quarantine_dir,
        quarantine_retention_days=quarantine_retention_days,
    )
    return EncodeRunResults(
        missing_results + list(results),
        session_path=getattr(results, "session_path", session_path),
        resumed_from_session=bool(getattr(results, "resumed_from_session", False)),
        session_status=dict(getattr(results, "session_status", {}) or {}),
        stopped_early=bool(getattr(results, "stopped_early", False)),
        interrupted=bool(getattr(results, "interrupted", False)),
    )


def prepare_retry_compression(
    config: PipelineConfig,
    retry_sources: set[Path],
    progress_callback: Callable[[object], None] | None = None,
    cancel_callback: Callable[[], bool] | None = None,
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
    preparation = prepare_compression(
        retry_config,
        progress_callback=progress_callback,
        source_paths=retry_sources,
        cancel_callback=cancel_callback,
    )
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

    normalized_sources = {Path(path).resolve(strict=False) for path in retry_sources}
    items = [
        item for item in preparation.items
        if Path(item.source).resolve(strict=False) in normalized_sources
    ]
    jobs = [
        job for job in preparation.jobs
        if _job_source(job).resolve(strict=False) in normalized_sources
    ]
    selected_sources = {_job_source(job) for job in jobs}
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


def _job_source(job: object) -> Path:
    return Path(getattr(job, "source", job))


def _supports_prepare_source_paths() -> bool:
    try:
        signature = inspect.signature(prepare_encode_run)
    except (TypeError, ValueError):
        return False
    return any(
        parameter.kind == inspect.Parameter.VAR_KEYWORD or name == "source_paths"
        for name, parameter in signature.parameters.items()
    )


def _supports_prepare_cancel_callback() -> bool:
    try:
        signature = inspect.signature(prepare_encode_run)
    except (TypeError, ValueError):
        return False
    return any(
        parameter.kind == inspect.Parameter.VAR_KEYWORD or name == "cancel_callback"
        for name, parameter in signature.parameters.items()
    )


def supports_prepare_source_paths() -> bool:
    return _supports_prepare_source_paths()


def supports_prepare_cancel_callback() -> bool:
    return _supports_prepare_cancel_callback()


def supports_encode_run_results() -> bool:
    return _HAS_NATIVE_ENCODE_RUN_RESULTS


def _stabilize_preparation(
    preparation: EncodePreparation,
    config: PipelineConfig,
) -> EncodePreparation:
    if not preparation.items:
        return preparation

    selected_items = [item for item in preparation.items if item.recommendation == "recommended"]
    if not selected_items:
        selected_items = [item for item in preparation.items if item.recommendation == "maybe"]
    if not selected_items:
        return preparation

    current_profile = preparation.profile
    current_usable = bool(
        current_profile is not None
        and getattr(current_profile, "compatible_count", 0) > 0
        and preparation.jobs
        and not _profile_has_blocking_risk(current_profile)
    )
    if current_usable:
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
    profile = _choose_safe_profile(profiles)
    messages = list(preparation.stage_messages or [])
    if profile is None:
        messages.append(
            "Compression analysis completed, but no safe runnable profile could be selected automatically. "
            "Review the plan details or rebuild the plan with safer settings."
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
    if not jobs:
        messages.append(
            f"Profile {profile.name} was selected as the safest available fallback, but no runnable jobs were produced."
        )
        return replace(
            preparation,
            profile=profile,
            compatible_count=profile.compatible_count,
            incompatible_count=profile.incompatible_count,
            grouped_incompatibilities=profile.grouped_incompatibilities,
            recommendation_reason=preparation.recommendation_reason or profile.why_choose,
            stage_messages=messages,
        )
    if current_profile is None:
        messages.append(
            "No encoder profile was auto-selected, so mediaflow chose the safest runnable fallback "
            f"({profile.name}) to keep the recommended plan runnable."
        )
    elif getattr(current_profile, "compatible_count", 0) <= 0:
        messages.append(
            f"Selected profile {current_profile.name} was predicted to work for 0 file(s), so mediaflow switched "
            f"to the safer runnable fallback {profile.name}."
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
        selected_count=len(jobs),
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


def _choose_safe_profile(profiles: list[object]) -> object | None:
    compatible = [
        profile
        for profile in profiles
        if int(getattr(profile, "compatible_count", 0) or 0) > 0
        and not _profile_has_blocking_risk(profile)
    ]
    if not compatible:
        return None

    def _rank(profile: object) -> tuple[int, int, int, int]:
        encoder_key = str(getattr(profile, "encoder_key", "") or "").lower()
        software_bias = 0 if encoder_key in {"fast", "faster"} else 1
        return (
            -int(getattr(profile, "compatible_count", 0) or 0),
            int(getattr(profile, "incompatible_count", 0) or 0),
            software_bias,
            int(getattr(profile, "crf", 0) or 0),
        )

    return min(compatible, key=_rank)


def _profile_has_blocking_risk(profile: object) -> bool:
    grouped = getattr(profile, "grouped_incompatibilities", {}) or {}
    return any(
        token in str(reason).lower()
        for reason in grouped
        for token in ("hardware encoder startup", "output header failure", "container/header")
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
    "prepare_safer_compression",
    "prepare_retry_compression",
    "prepare_tools",
    "resumable_session_status",
    "run_compression",
    "supports_encode_run_results",
    "supports_prepare_cancel_callback",
    "supports_prepare_source_paths",
]
