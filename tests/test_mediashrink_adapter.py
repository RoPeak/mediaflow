from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from mediashrink.gui_api import EncodePreparation
from mediashrink.models import EncodeJob

from mediaflow.mediashrink_adapter import missing_job_sources, run_compression


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
