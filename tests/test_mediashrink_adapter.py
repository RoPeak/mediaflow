from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from mediashrink.gui_api import EncodePreparation
from mediashrink.models import EncodeJob

from mediaflow.callback_types import PreparationProgress, PreparationStageUpdate
from mediaflow.mediashrink_adapter import (
    _convert_preparation_payload,
    missing_job_sources,
    prepare_compression,
    prepare_retry_compression,
    prepare_safer_compression,
    run_compression,
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
