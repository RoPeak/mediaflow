from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from mediashrink.gui_api import EncodePreparation
from mediashrink.models import EncodeJob

from mediaflow.callback_types import PreparationProgress, PreparationStageUpdate
from mediaflow.mediashrink_adapter import (
    _convert_preparation_payload,
    incomplete_session_status,
    missing_job_sources,
    prepare_compression,
    prepare_retry_compression,
    prepare_safer_compression,
    prepare_speed_compression,
    profile_id_for,
    run_compression,
    supports_encode_run_results,
    supports_prepare_source_paths,
)
from mediaflow.config import PipelineConfig, ShrinkSettings


def _job(tmp_path: Path, name: str) -> EncodeJob:
    source = tmp_path / name
    return EncodeJob(
        source=source,
        output=source.with_suffix(".out.mkv"),
        tmp_output=source.with_suffix(".tmp.mkv"),
        crf=22,
        preset="faster",
        dry_run=False,
    )


def _preparation(tmp_path: Path, jobs: list[EncodeJob]) -> EncodePreparation:
    return EncodePreparation(
        directory=tmp_path,
        ffmpeg=tmp_path / "ffmpeg",
        ffprobe=tmp_path / "ffprobe",
        items=[],
        duplicate_warnings=[],
        profile=SimpleNamespace(name="Fast", encoder_key="faster", crf=22),
        jobs=jobs,
        recommended_count=0,
        maybe_count=0,
        skip_count=0,
        selected_count=len(jobs),
        total_input_bytes=0,
        selected_input_bytes=0,
        selected_estimated_output_bytes=0,
        estimated_total_seconds=0.0,
        on_file_failure="retry",
        use_calibration=True,
    )


def test_missing_job_sources_reports_jobs_that_disappeared(tmp_path: Path) -> None:
    existing = _job(tmp_path, "existing.mkv")
    existing.source.write_bytes(b"x")
    missing = _job(tmp_path, "missing.mkv")

    missing_sources = missing_job_sources(_preparation(tmp_path, [existing, missing]))

    assert missing_sources == [missing.source]


def test_run_compression_returns_missing_result_without_crashing(tmp_path: Path) -> None:
    existing = _job(tmp_path, "existing.mkv")
    existing.source.write_bytes(b"x")
    missing = _job(tmp_path, "missing.mkv")
    prep = _preparation(tmp_path, [existing, missing])
    fake_result = SimpleNamespace(
        job=existing,
        skipped=False,
        success=True,
        input_size_bytes=100,
        output_size_bytes=50,
        error_message=None,
    )

    with patch("mediaflow.mediashrink_adapter.run_encode_plan", return_value=[fake_result]):
        results = run_compression(prep)

    assert len(results) == 2
    assert any(result.success for result in results)
    missing_result = next(result for result in results if result.job.source == missing.source)
    assert missing_result.success is False
    assert "missing" in (missing_result.error_message or "").lower()


def test_run_compression_preserves_session_metadata(tmp_path: Path) -> None:
    existing = _job(tmp_path, "existing.mkv")
    existing.source.write_bytes(b"x")
    prep = _preparation(tmp_path, [existing])
    fake_result = SimpleNamespace(
        job=existing,
        skipped=False,
        success=True,
        input_size_bytes=100,
        output_size_bytes=50,
        error_message=None,
    )
    from mediaflow.mediashrink_adapter import EncodeRunResults

    with patch(
        "mediaflow.mediashrink_adapter.run_encode_plan",
        return_value=EncodeRunResults(
            [fake_result],
            session_path=tmp_path / ".mediashrink-session.json",
            resumed_from_session=True,
            session_status={"success": 1},
            stopped_early=True,
            interrupted=True,
        ),
    ) as run_encode_plan:
        def cancel_callback() -> bool:
            return False

        results = run_compression(prep, resume=True, cancel_callback=cancel_callback)

    assert getattr(results, "session_path") == tmp_path / ".mediashrink-session.json"
    assert getattr(results, "resumed_from_session") is True
    assert getattr(results, "session_status") == {"success": 1}
    assert getattr(results, "stopped_early") is True
    assert getattr(results, "interrupted") is True
    assert run_encode_plan.call_args.kwargs["cancel_callback"] is cancel_callback


def test_incomplete_session_status_reports_retryable_counts(tmp_path: Path) -> None:
    from mediashrink.session import build_session, get_session_path, save_session, update_session_entry

    first = _job(tmp_path, "done.mkv")
    second = _job(tmp_path, "retry.mkv")
    for job in (first, second):
        job.source.write_bytes(b"x")
    session = build_session(
        tmp_path,
        "faster",
        22,
        overwrite=True,
        output_dir=None,
        jobs=[first, second],
    )
    update_session_entry(session, first.source, status="success", output=first.output)
    update_session_entry(
        session,
        second.source,
        status="failed",
        error="boom",
        last_progress_pct=12.5,
        last_progress_at="2026-05-07T09:00:00",
    )
    save_session(session, get_session_path(tmp_path, None))

    status = incomplete_session_status(tmp_path)

    assert status is not None
    assert status["completed"] == 1
    assert status["pending"] == 1
    assert status["current_file"] == str(second.source)
    assert status["source_paths"] == [str(first.source), str(second.source)]


def test_incomplete_session_status_filters_to_prepared_source_set(tmp_path: Path) -> None:
    from mediashrink.session import build_session, get_session_path, save_session, update_session_entry

    first = _job(tmp_path, "first.mkv")
    second = _job(tmp_path, "second.mkv")
    for job in (first, second):
        job.source.write_bytes(b"x")
    session = build_session(
        tmp_path,
        "faster",
        22,
        overwrite=True,
        output_dir=None,
        jobs=[first, second],
    )
    update_session_entry(session, first.source, status="failed", error="old root run")
    update_session_entry(session, second.source, status="pending")
    save_session(session, get_session_path(tmp_path, None))

    status = incomplete_session_status(tmp_path, source_paths={second.source})

    assert status is not None
    assert status["pending"] == 1
    assert status["current_file"] == str(second.source)
    assert status["source_paths"] == [str(second.source)]


def test_incomplete_session_status_ignores_unrelated_prepared_plan(tmp_path: Path) -> None:
    from mediashrink.session import build_session, get_session_path, save_session, update_session_entry

    session_job = _job(tmp_path, "session.mkv")
    planned_job = _job(tmp_path, "planned.mkv")
    for job in (session_job, planned_job):
        job.source.write_bytes(b"x")
    session = build_session(
        tmp_path,
        "faster",
        22,
        overwrite=True,
        output_dir=None,
        jobs=[session_job],
    )
    update_session_entry(session, session_job.source, status="failed", error="old root run")
    save_session(session, get_session_path(tmp_path, None))

    status = incomplete_session_status(tmp_path, source_paths={planned_job.source})

    assert status is None


def test_convert_preparation_payload_maps_stage_updates() -> None:
    payload = ("stage", "benchmarking", "Benchmarking profiles...", 1, 3, "")

    converted = _convert_preparation_payload(payload)

    assert isinstance(converted, PreparationStageUpdate)
    assert converted.stage == "benchmarking"
    assert converted.completed == 1


def test_convert_preparation_payload_maps_analysis_updates(tmp_path: Path) -> None:
    payload = (1, 2, str(tmp_path / "movie.mkv"))

    converted = _convert_preparation_payload(payload)

    assert isinstance(converted, PreparationProgress)
    assert converted.completed == 1


def test_prepare_retry_compression_filters_to_requested_sources(tmp_path: Path) -> None:
    first = _job(tmp_path, "first.mkv")
    second = _job(tmp_path, "second.mkv")
    for job in (first, second):
        job.source.write_bytes(b"x")

    item_one = SimpleNamespace(
        source=first.source,
        codec="h264",
        size_bytes=100,
        estimated_output_bytes=50,
        estimated_savings_bytes=50,
        recommendation="recommended",
        reason_text="Needs retry",
    )
    item_two = SimpleNamespace(
        source=second.source,
        codec="h264",
        size_bytes=120,
        estimated_output_bytes=60,
        estimated_savings_bytes=60,
        recommendation="recommended",
        reason_text="Needs retry",
    )
    prep = _preparation(tmp_path, [first, second])
    prep = prep.__class__(**{**prep.__dict__, "items": [item_one, item_two]})
    config = PipelineConfig(
        source=tmp_path,
        library=tmp_path,
        compression_root=tmp_path,
        shrink=ShrinkSettings(),
    )

    with patch("mediaflow.mediashrink_adapter.prepare_compression", return_value=prep):
        retry = prepare_retry_compression(config, {first.source})

    assert [item.source for item in retry.items] == [first.source]
    assert [job.source for job in retry.jobs] == [first.source]
    assert retry.selected_count == 1
    assert retry.stage_messages is not None


def test_prepare_compression_passes_source_allowlist_to_mediashrink(tmp_path: Path) -> None:
    config = PipelineConfig(
        source=tmp_path,
        library=tmp_path,
        compression_root=tmp_path,
        shrink=ShrinkSettings(),
    )
    selected = {tmp_path / "movie.mkv"}
    prep = _preparation(tmp_path, [])
    captured: dict[str, object] = {}

    def fake_prepare_encode_run(**kwargs):
        captured.update(kwargs)
        return prep

    with patch("mediaflow.mediashrink_adapter.prepare_encode_run", side_effect=fake_prepare_encode_run):
        result = prepare_compression(config, source_paths=selected)

    assert result is prep
    assert captured["source_paths"] == selected


def test_mediashrink_capability_helpers_reflect_adapter_dependencies() -> None:
    assert supports_prepare_source_paths() in {True, False}
    assert supports_encode_run_results() in {True, False}


def test_prepare_compression_forwards_selected_profile_id(tmp_path: Path) -> None:
    config = PipelineConfig(
        source=tmp_path,
        library=tmp_path,
        compression_root=tmp_path,
        shrink=ShrinkSettings(),
    )
    selected = "Balanced::slow::slow::20"
    profile = SimpleNamespace(
        name="Balanced",
        encoder_key="slow",
        sw_preset="slow",
        crf=20,
        estimated_output_bytes=40,
        compatible_count=1,
        incompatible_count=0,
        grouped_incompatibilities={},
        why_choose="Quality-focused option.",
    )
    source = tmp_path / "movie.mkv"
    source.write_bytes(b"x" * 100)
    item = SimpleNamespace(
        source=source,
        codec="mpeg2video",
        size_bytes=100,
        estimated_output_bytes=50,
        estimated_savings_bytes=50,
        recommendation="recommended",
        reason_text="Strong savings",
    )
    prep = EncodePreparation(
        directory=tmp_path,
        ffmpeg=tmp_path / "ffmpeg",
        ffprobe=tmp_path / "ffprobe",
        items=[item],
        duplicate_warnings=[],
        profile=profile,
        jobs=[_job(tmp_path, "movie.mkv")],
        recommended_count=1,
        maybe_count=0,
        skip_count=0,
        selected_count=1,
        total_input_bytes=100,
        selected_input_bytes=100,
        selected_estimated_output_bytes=40,
        estimated_total_seconds=20.0,
        on_file_failure="retry",
        use_calibration=True,
        profile_options=[profile],
        selected_profile_id=selected,
    )
    captured: dict[str, object] = {}

    def fake_prepare_encode_run(**kwargs):
        captured.update(kwargs)
        return prep

    with patch("mediaflow.mediashrink_adapter.prepare_encode_run", side_effect=fake_prepare_encode_run), patch(
        "mediaflow.mediashrink_adapter.build_jobs", return_value=prep.jobs
    ), patch("mediaflow.mediashrink_adapter.estimate_analysis_encode_seconds", return_value=20.0):
        result = prepare_compression(config, selected_profile_id=selected)

    assert captured["selected_profile_id"] == selected
    assert result.selected_profile_id == selected
    assert result.profile_selection_method == "manual"


def test_prepare_compression_uses_quality_aware_default_for_movie_overwrite(tmp_path: Path) -> None:
    movie_dir = tmp_path / "Movies" / "Movie (2000)"
    movie_dir.mkdir(parents=True)
    source = movie_dir / "Movie (2000).mkv"
    source.write_bytes(b"x" * 100)
    item = SimpleNamespace(
        source=source,
        codec="mpeg2video",
        size_bytes=100,
        estimated_output_bytes=50,
        estimated_savings_bytes=50,
        recommendation="recommended",
        reason_text="Strong savings",
    )
    fast = SimpleNamespace(
        name="Fast",
        encoder_key="faster",
        sw_preset="faster",
        crf=22,
        estimated_output_bytes=40,
        estimated_encode_seconds=10.0,
        compatible_count=1,
        incompatible_count=0,
        grouped_incompatibilities={},
        why_choose="Fast default.",
    )
    balanced = SimpleNamespace(
        name="Balanced",
        encoder_key="slow",
        sw_preset="slow",
        crf=20,
        estimated_output_bytes=60,
        estimated_encode_seconds=20.0,
        compatible_count=1,
        incompatible_count=0,
        grouped_incompatibilities={},
        why_choose="Better quality for movies.",
    )
    job = EncodeJob(
        source=source,
        output=source.with_suffix(".out.mkv"),
        tmp_output=source.with_suffix(".tmp.mkv"),
        crf=20,
        preset="slow",
        dry_run=False,
    )
    prep = EncodePreparation(
        directory=tmp_path,
        ffmpeg=tmp_path / "ffmpeg",
        ffprobe=tmp_path / "ffprobe",
        items=[item],
        duplicate_warnings=[],
        profile=fast,
        jobs=[job],
        recommended_count=1,
        maybe_count=0,
        skip_count=0,
        selected_count=1,
        total_input_bytes=100,
        selected_input_bytes=100,
        selected_estimated_output_bytes=40,
        estimated_total_seconds=10.0,
        on_file_failure="retry",
        use_calibration=True,
        profile_options=[fast, balanced],
        recommended_profile_id=profile_id_for(fast),
        selected_profile_id=profile_id_for(fast),
    )
    config = PipelineConfig(
        source=tmp_path,
        library=tmp_path,
        compression_root=tmp_path,
        shrink=ShrinkSettings(policy="fastest-wall-clock", overwrite=True),
    )

    with patch("mediaflow.mediashrink_adapter.prepare_encode_run", return_value=prep), patch(
        "mediaflow.mediashrink_adapter.build_jobs", return_value=[job]
    ), patch("mediaflow.mediashrink_adapter.estimate_analysis_encode_seconds", return_value=20.0):
        result = prepare_compression(config)

    assert result.profile.name == "Balanced"
    assert result.selected_profile_id == profile_id_for(balanced)
    assert result.profile_selection_method == "quality-aware-default"
    assert result.selected_estimated_output_bytes == 60


def test_prepare_compression_filters_sources_when_mediashrink_lacks_allowlist(tmp_path: Path) -> None:
    first = tmp_path / "first.mkv"
    second = tmp_path / "second.mkv"
    first.write_bytes(b"x")
    second.write_bytes(b"x")
    config = PipelineConfig(
        source=tmp_path,
        library=tmp_path,
        compression_root=tmp_path,
        shrink=ShrinkSettings(),
    )
    first_job = _job(tmp_path, "first.mkv")
    second_job = _job(tmp_path, "second.mkv")
    prep = _preparation(tmp_path, [first_job, second_job])

    def fake_prepare_encode_run(
        *,
        directory,
        recursive=True,
        overwrite=True,
        no_skip=False,
        policy="fastest-wall-clock",
        on_file_failure="retry",
        use_calibration=True,
        duplicate_policy="prefer-mkv",
        progress_callback=None,
    ):
        return prep

    with patch("mediaflow.mediashrink_adapter.prepare_encode_run", side_effect=fake_prepare_encode_run):
        result = prepare_compression(config, source_paths={second})

    assert [job.source for job in result.jobs] == [second]
    assert result.selected_count == 1


def test_prepare_compression_falls_back_to_safest_runnable_profile(tmp_path: Path) -> None:
    source = tmp_path / "movie.mkv"
    source.write_bytes(b"x")
    item = SimpleNamespace(
        source=source,
        codec="h264",
        size_bytes=100,
        estimated_output_bytes=50,
        estimated_savings_bytes=50,
        recommendation="recommended",
        reason_text="Strong projected savings",
    )
    prep = EncodePreparation(
        directory=tmp_path,
        ffmpeg=tmp_path / "ffmpeg",
        ffprobe=tmp_path / "ffprobe",
        items=[item],
        duplicate_warnings=[],
        profile=None,
        jobs=[],
        recommended_count=1,
        maybe_count=0,
        skip_count=0,
        selected_count=1,
        total_input_bytes=100,
        selected_input_bytes=0,
        selected_estimated_output_bytes=0,
        estimated_total_seconds=0.0,
        on_file_failure="retry",
        use_calibration=True,
        stage_messages=[],
    )
    planning = SimpleNamespace(
        profiles=[
            SimpleNamespace(
                name="Fastest hardware",
                encoder_key="amf",
                crf=20,
                is_recommended=False,
                compatible_count=0,
                incompatible_count=1,
                grouped_incompatibilities={"hardware encoder startup failure": 1},
                why_choose="Fastest profile on this device.",
            ),
            SimpleNamespace(
                name="Safer software",
                encoder_key="fast",
                crf=22,
                is_recommended=False,
                compatible_count=1,
                incompatible_count=0,
                grouped_incompatibilities={},
                why_choose="Best available fallback profile.",
            ),
        ],
        benchmark_speeds={"fast": 1.0},
        active_calibration=None,
    )
    config = PipelineConfig(
        source=tmp_path,
        library=tmp_path,
        compression_root=tmp_path,
        shrink=ShrinkSettings(),
    )
    fake_jobs = [_job(tmp_path, "movie.mkv")]

    with patch("mediaflow.mediashrink_adapter.prepare_encode_run", return_value=prep), patch(
        "mediaflow.mediashrink_adapter.prepare_profile_planning", return_value=planning
    ), patch("mediaflow.mediashrink_adapter.build_jobs", return_value=fake_jobs), patch(
        "mediaflow.mediashrink_adapter.estimate_analysis_encode_seconds", return_value=12.0
    ), patch("mediaflow.mediashrink_adapter.estimate_size_confidence", return_value="High"), patch(
        "mediaflow.mediashrink_adapter.estimate_time_confidence", return_value="Medium"
    ):
        recovered = prepare_compression(config)

    assert recovered.profile is not None
    assert recovered.profile.name == "Safer software"
    assert recovered.jobs == fake_jobs
    assert recovered.selected_input_bytes == 100
    assert recovered.selected_estimated_output_bytes == 50
    assert recovered.stage_messages is not None
    assert any("safest runnable fallback" in line for line in recovered.stage_messages)


def test_prepare_compression_reports_when_no_safe_profile_exists(tmp_path: Path) -> None:
    source = tmp_path / "movie.mkv"
    source.write_bytes(b"x")
    item = SimpleNamespace(
        source=source,
        codec="h264",
        size_bytes=100,
        estimated_output_bytes=50,
        estimated_savings_bytes=50,
        recommendation="recommended",
        reason_text="Strong projected savings",
    )
    prep = EncodePreparation(
        directory=tmp_path,
        ffmpeg=tmp_path / "ffmpeg",
        ffprobe=tmp_path / "ffprobe",
        items=[item],
        duplicate_warnings=[],
        profile=SimpleNamespace(name="AMF", encoder_key="amf", crf=20, compatible_count=0, incompatible_count=1),
        jobs=[],
        recommended_count=1,
        maybe_count=0,
        skip_count=0,
        selected_count=1,
        total_input_bytes=100,
        selected_input_bytes=0,
        selected_estimated_output_bytes=0,
        estimated_total_seconds=0.0,
        on_file_failure="retry",
        use_calibration=True,
        stage_messages=[],
        compatible_count=0,
        incompatible_count=1,
        grouped_incompatibilities={"hardware encoder startup failure": 1},
        recommendation_reason="Likely works for 0 files.",
    )
    planning = SimpleNamespace(
        profiles=[
            SimpleNamespace(
                name="Still bad",
                encoder_key="amf",
                crf=20,
                compatible_count=0,
                incompatible_count=1,
                grouped_incompatibilities={"hardware encoder startup failure": 1},
                why_choose="Not safe",
            )
        ],
        benchmark_speeds={},
        active_calibration=None,
    )
    config = PipelineConfig(
        source=tmp_path,
        library=tmp_path,
        compression_root=tmp_path,
        shrink=ShrinkSettings(),
    )

    with patch("mediaflow.mediashrink_adapter.prepare_encode_run", return_value=prep), patch(
        "mediaflow.mediashrink_adapter.prepare_profile_planning", return_value=planning
    ):
        recovered = prepare_compression(config)

    assert recovered.jobs == []
    assert recovered.stage_messages is not None
    assert any("no safe runnable profile" in line.lower() for line in recovered.stage_messages)


def test_prepare_safer_compression_adds_compatibility_first_note(tmp_path: Path) -> None:
    prep = _preparation(tmp_path, [])
    config = PipelineConfig(
        source=tmp_path,
        library=tmp_path,
        compression_root=tmp_path,
        shrink=ShrinkSettings(),
    )

    with patch("mediaflow.mediashrink_adapter.prepare_compression", return_value=prep):
        safer = prepare_safer_compression(config)

    assert safer.stage_messages is not None
    assert any("compatibility-first defaults" in line for line in safer.stage_messages)


def test_prepare_speed_compression_rebuilds_with_ultrafast_profile(tmp_path: Path) -> None:
    source = tmp_path / "movie.mkv"
    source.write_bytes(b"x")
    item = SimpleNamespace(
        source=source,
        codec="mpeg2video",
        size_bytes=100,
        estimated_output_bytes=40,
        estimated_savings_bytes=60,
        recommendation="recommended",
        reason_text="Strong projected savings",
    )
    prep = _preparation(tmp_path, [_job(tmp_path, "movie.mkv")])
    prep = prep.__class__(
        **{
            **prep.__dict__,
            "items": [item],
            "recommended_count": 1,
            "selected_input_bytes": 100,
            "selected_estimated_output_bytes": 40,
        }
    )
    config = PipelineConfig(
        source=tmp_path,
        library=tmp_path,
        compression_root=tmp_path,
        shrink=ShrinkSettings(),
    )
    fake_job = EncodeJob(
        source=source,
        output=source,
        tmp_output=source.with_name(".tmp_movie.mkv"),
        crf=22,
        preset="ultrafast",
        dry_run=False,
    )

    with patch("mediaflow.mediashrink_adapter.prepare_compression", return_value=prep), patch(
        "mediaflow.mediashrink_adapter.build_jobs", return_value=[fake_job]
    ), patch("mediaflow.mediashrink_adapter.estimate_analysis_encode_seconds", return_value=30.0), patch(
        "mediaflow.mediashrink_adapter.estimate_size_confidence", return_value="Medium"
    ), patch("mediaflow.mediashrink_adapter.estimate_time_confidence", return_value="Low"):
        speed = prepare_speed_compression(config, source_paths={source})

    assert speed.profile is not None
    assert speed.profile.name == "Laptop Speed"
    assert speed.profile.encoder_key == "ultrafast"
    assert speed.profile.crf == 22
    assert speed.jobs == [fake_job]
    assert speed.selected_input_bytes == 100
    assert speed.selected_estimated_output_bytes == 40
    assert speed.stage_messages is not None
    assert any("Laptop Speed" in line for line in speed.stage_messages)
