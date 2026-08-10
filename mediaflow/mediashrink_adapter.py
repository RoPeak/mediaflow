from __future__ import annotations

from collections.abc import Collection
from dataclasses import replace
import inspect
from pathlib import Path
from types import SimpleNamespace
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
try:
    from mediashrink.gui_api import profile_id_for as _native_profile_id_for
except ImportError:
    _native_profile_id_for = None
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
    selected_profile_id: str | None = None,
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
    if selected_profile_id is not None and _supports_prepare_selected_profile():
        kwargs["selected_profile_id"] = selected_profile_id
    if cancel_callback is not None and _supports_prepare_cancel_callback():
        kwargs["cancel_callback"] = cancel_callback
    while True:
        try:
            preparation = prepare_encode_run(**kwargs)
            break
        except TypeError as exc:
            unsupported = next(
                (
                    name
                    for name in ("selected_profile_id", "source_paths", "cancel_callback")
                    if name in kwargs and name in str(exc)
                ),
                None,
            )
            if unsupported is not None:
                kwargs.pop(unsupported, None)
                continue
            raise
    if source_paths is not None and "source_paths" not in kwargs:
        preparation = _filter_preparation_to_sources(preparation, set(source_paths))
    preparation = _with_laptop_speed_option(preparation, config)
    if selected_profile_id is not None:
        preparation = _apply_profile_id(preparation, config, selected_profile_id, selection_method="manual")
    else:
        preparation = _apply_quality_default_if_needed(preparation, config)
    return _with_laptop_speed_option(_stabilize_preparation(preparation, config), config)


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


def prepare_speed_compression(
    config: PipelineConfig,
    progress_callback: Callable[[object], None] | None = None,
    source_paths: Collection[Path] | None = None,
) -> EncodePreparation:
    preparation = prepare_compression(
        config,
        progress_callback=progress_callback,
        source_paths=source_paths,
    )
    return _apply_laptop_speed_profile(preparation, config)


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
    kwargs = {
        "on_progress": progress_callback,
        "on_file_failure": preparation.on_file_failure,
        "use_calibration": preparation.use_calibration,
        "session_path": session_path,
        "resume": resume,
    }
    if cancel_callback is not None and _supports_run_encode_kwarg("cancel_callback"):
        kwargs["cancel_callback"] = cancel_callback
    if _supports_run_encode_kwarg("stop_mode"):
        kwargs["stop_mode"] = "graceful"
    if _supports_run_encode_kwarg("quarantine_originals"):
        kwargs["quarantine_originals"] = quarantine_originals
    if _supports_run_encode_kwarg("quarantine_dir"):
        kwargs["quarantine_dir"] = quarantine_dir
    if _supports_run_encode_kwarg("quarantine_retention_days"):
        kwargs["quarantine_retention_days"] = quarantine_retention_days
    results = run_encode_plan(active_preparation, **kwargs)
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


def profile_id_for(profile: object | None) -> str | None:
    if _native_profile_id_for is not None:
        return _native_profile_id_for(profile)
    if profile is None:
        return None
    existing = getattr(profile, "profile_id", None)
    if existing:
        return str(existing)
    parts = [
        str(getattr(profile, "name", "") or ""),
        str(getattr(profile, "encoder_key", "") or ""),
        str(getattr(profile, "sw_preset", "") or ""),
        str(getattr(profile, "crf", "") or ""),
    ]
    return "::".join(part.replace("::", "/") for part in parts)


def _selected_analysis_items(preparation: EncodePreparation) -> list:
    selected_items = [item for item in preparation.items if item.recommendation == "recommended"]
    if not selected_items:
        selected_items = [item for item in preparation.items if item.recommendation == "maybe"]
    return selected_items


def _profile_options(preparation: EncodePreparation) -> list[object]:
    return list(getattr(preparation, "profile_options", None) or [])


def _replace_preparation(preparation: EncodePreparation, **changes) -> EncodePreparation:
    dataclass_fields = getattr(preparation, "__dataclass_fields__", {}) or {}
    init_field_names = {
        name
        for name, field in dataclass_fields.items()
        if getattr(field, "init", False)
    }
    constructor_changes = {
        key: value for key, value in changes.items() if key in init_field_names
    }
    result = replace(preparation, **constructor_changes)
    dynamic_attrs = {
        key: value
        for key, value in vars(preparation).items()
        if key not in dataclass_fields
    }
    dynamic_attrs.update(
        {key: value for key, value in changes.items() if key not in init_field_names}
    )
    for key, value in dynamic_attrs.items():
        object.__setattr__(result, key, value)
    return result


def _estimated_output_for_profile(profile: object, selected_items: list[object]) -> int:
    profile_estimate = int(getattr(profile, "estimated_output_bytes", 0) or 0)
    if profile_estimate > 0:
        return profile_estimate
    return sum(
        int(getattr(item, "estimated_output_bytes", 0) or 0)
        for item in selected_items
        if int(getattr(item, "estimated_output_bytes", 0) or 0) > 0
    )


def _item_size_bytes(item: object) -> int:
    size = int(getattr(item, "size_bytes", 0) or 0)
    if size > 0:
        return size
    source = getattr(item, "source", None)
    try:
        return Path(source).stat().st_size if source is not None else 0
    except OSError:
        return 0


def _estimate_seconds_for_items(
    items: list[object],
    *,
    preset: str,
    crf: int,
    ffmpeg: Path,
    use_calibration: bool,
) -> float | None:
    try:
        return estimate_analysis_encode_seconds(
            items,
            preset=preset,
            crf=crf,
            ffmpeg=ffmpeg,
            known_speed=None,
            use_calibration=use_calibration,
            calibration_store=None,
        )
    except AttributeError:
        return None


def _estimate_size_confidence_for_items(
    items: list[object],
    *,
    preset: str,
    use_calibration: bool,
) -> str | None:
    try:
        return estimate_size_confidence(
            items,
            preset=preset,
            use_calibration=use_calibration,
        )
    except AttributeError:
        return None


def _estimate_time_confidence_for_items(
    items: list[object],
    *,
    preset: str,
    use_calibration: bool,
) -> str | None:
    try:
        return estimate_time_confidence(
            items,
            benchmarked_files=0,
            preset=preset,
            use_calibration=use_calibration,
        )
    except AttributeError:
        return None


def _apply_profile(
    preparation: EncodePreparation,
    config: PipelineConfig,
    profile: object,
    *,
    selection_method: str,
    message: str | None = None,
) -> EncodePreparation:
    selected_items = _selected_analysis_items(preparation)
    if not selected_items:
        return preparation
    crf = int(getattr(profile, "crf", 22) or 22)
    preset = str(getattr(profile, "encoder_key", "faster") or "faster")
    jobs = build_jobs(
        files=[item.source for item in selected_items],
        output_dir=None,
        overwrite=config.shrink.overwrite,
        crf=crf,
        preset=preset,
        dry_run=False,
        ffprobe=preparation.ffprobe,
        no_skip=config.shrink.no_skip,
    )
    selected_sources = {_job_source(job) for job in jobs}
    effective_items = [item for item in selected_items if item.source in selected_sources]
    selected_input_bytes = sum(_item_size_bytes(item) for item in effective_items)
    selected_estimated_output_bytes = _estimated_output_for_profile(profile, effective_items)
    estimated_total_seconds = _estimate_seconds_for_items(
        effective_items,
        preset=preset,
        crf=crf,
        ffmpeg=preparation.ffmpeg,
        use_calibration=preparation.use_calibration,
    )
    messages = list(preparation.stage_messages or [])
    if message:
        messages.append(message)
    profile_id = profile_id_for(profile)
    return _replace_preparation(
        preparation,
        profile=profile,
        jobs=jobs,
        selected_count=len(jobs),
        selected_input_bytes=selected_input_bytes,
        selected_estimated_output_bytes=selected_estimated_output_bytes,
        estimated_total_seconds=estimated_total_seconds,
        size_confidence=_estimate_size_confidence_for_items(
            effective_items,
            preset=preset,
            use_calibration=preparation.use_calibration,
        ),
        time_confidence=_estimate_time_confidence_for_items(
            effective_items,
            preset=preset,
            use_calibration=preparation.use_calibration,
        ),
        compatible_count=int(getattr(profile, "compatible_count", len(jobs)) or 0),
        incompatible_count=int(getattr(profile, "incompatible_count", 0) or 0),
        grouped_incompatibilities=getattr(profile, "grouped_incompatibilities", {}) or {},
        recommendation_reason=getattr(profile, "why_choose", None) or preparation.recommendation_reason,
        stage_messages=messages,
        selected_profile_id=profile_id,
        profile_selection_method=selection_method,
    )


def _apply_profile_id(
    preparation: EncodePreparation,
    config: PipelineConfig,
    selected_profile_id: str,
    *,
    selection_method: str,
) -> EncodePreparation:
    current_profile_id = profile_id_for(getattr(preparation, "profile", None))
    if current_profile_id == selected_profile_id:
        return _apply_profile(
            preparation,
            config,
            getattr(preparation, "profile", None),
            selection_method=selection_method,
        )
    for profile in _profile_options(preparation):
        if profile_id_for(profile) == selected_profile_id:
            return _apply_profile(
                preparation,
                config,
                profile,
                selection_method=selection_method,
                message=f"Compression plan rebuilt with profile {getattr(profile, 'name', selected_profile_id)}.",
            )
    return preparation


def _laptop_speed_profile(preparation: EncodePreparation, config: PipelineConfig) -> object | None:
    selected_items = _selected_analysis_items(preparation)
    if not selected_items:
        return None
    selected_input_bytes = sum(_item_size_bytes(item) for item in selected_items)
    selected_estimated_output_bytes = sum(
        int(getattr(item, "estimated_output_bytes", 0) or 0)
        for item in selected_items
        if int(getattr(item, "estimated_output_bytes", 0) or 0) > 0
    )
    estimated_seconds = _estimate_seconds_for_items(
        selected_items,
        preset="ultrafast",
        crf=22,
        ffmpeg=preparation.ffmpeg,
        use_calibration=preparation.use_calibration,
    )
    profile = SimpleNamespace(
        name="Laptop Speed",
        intent_label="Speed first",
        encoder_key="ultrafast",
        sw_preset="ultrafast",
        crf=22,
        estimated_output_bytes=selected_estimated_output_bytes,
        estimated_encode_seconds=estimated_seconds,
        quality_label="Good",
        compatible_count=len(selected_items),
        incompatible_count=0,
        grouped_incompatibilities={},
        why_choose=(
            "Fastest CPU-friendly option for constrained laptops. It trades compression efficiency and some "
            "detail retention for much shorter wall-clock time."
        ),
        profile_id="Laptop Speed::ultrafast::ultrafast::22",
        effective_input_bytes=selected_input_bytes,
    )
    return profile


def _with_laptop_speed_option(
    preparation: EncodePreparation,
    config: PipelineConfig,
) -> EncodePreparation:
    profile = _laptop_speed_profile(preparation, config)
    if profile is None:
        return preparation
    options = _profile_options(preparation)
    if any(profile_id_for(option) == profile.profile_id for option in options):
        return preparation
    return _replace_preparation(preparation, profile_options=[*options, profile])


def _movie_heavy_overwrite_batch(preparation: EncodePreparation, config: PipelineConfig) -> bool:
    if not config.shrink.overwrite:
        return False
    selected_items = _selected_analysis_items(preparation)
    if not selected_items:
        return False
    movie_like = 0
    for item in selected_items:
        path_text = str(getattr(item, "source", "")).lower()
        if "/movies/" in path_text or "\\movies\\" in path_text:
            movie_like += 1
        elif not any(token in path_text for token in ("tv shows", "s01e", "s02e", "s03e", "season ")):
            movie_like += 1
    return movie_like / len(selected_items) >= 0.6


def _apply_quality_default_if_needed(
    preparation: EncodePreparation,
    config: PipelineConfig,
) -> EncodePreparation:
    if config.shrink.policy != "fastest-wall-clock":
        return preparation
    if not _movie_heavy_overwrite_batch(preparation, config):
        return preparation
    current = getattr(preparation, "profile", None)
    current_name = str(getattr(current, "name", "") or "").lower()
    if current_name not in {"fast", "fast batch"}:
        return preparation
    options = _profile_options(preparation)
    balanced = next((profile for profile in options if str(getattr(profile, "name", "")).lower() == "balanced"), None)
    if balanced is None:
        return preparation
    compatible = int(getattr(balanced, "compatible_count", 0) or 0)
    selected_count = len(_selected_analysis_items(preparation))
    if compatible and compatible < selected_count:
        return preparation
    return _apply_profile(
        preparation,
        config,
        balanced,
        selection_method="quality-aware-default",
        message=(
            "Quality-aware default selected Balanced for this movie-heavy overwrite batch. "
            "Review the profile table before starting compression."
        ),
    )


def _apply_laptop_speed_profile(
    preparation: EncodePreparation,
    config: PipelineConfig,
) -> EncodePreparation:
    profile = _laptop_speed_profile(preparation, config)
    if profile is None:
        return preparation
    return _apply_profile(
        _with_laptop_speed_option(preparation, config),
        config,
        profile,
        selection_method="speed-shortcut",
        message=(
            "Laptop Speed rebuild selected x265 ultrafast at CRF 22. Expect faster encodes, but larger outputs "
            "and less reliable size estimates than the default Fast profile."
        ),
    )


def _supports_prepare_source_paths() -> bool:
    try:
        signature = inspect.signature(prepare_encode_run)
    except (TypeError, ValueError):
        return False
    return any(
        parameter.kind == inspect.Parameter.VAR_KEYWORD or name == "source_paths"
        for name, parameter in signature.parameters.items()
    )


def _supports_prepare_selected_profile() -> bool:
    try:
        signature = inspect.signature(prepare_encode_run)
    except (TypeError, ValueError):
        return False
    return any(
        parameter.kind == inspect.Parameter.VAR_KEYWORD or name == "selected_profile_id"
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


def _supports_run_encode_kwarg(name: str) -> bool:
    try:
        signature = inspect.signature(run_encode_plan)
    except (TypeError, ValueError):
        return False
    return any(
        parameter.kind == inspect.Parameter.VAR_KEYWORD or parameter_name == name
        for parameter_name, parameter in signature.parameters.items()
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
    "profile_id_for",
    "resumable_session_status",
    "run_compression",
    "supports_encode_run_results",
    "supports_prepare_cancel_callback",
    "supports_prepare_source_paths",
]
