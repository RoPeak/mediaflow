from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from mediaflow.compression_preview import (
    CompressionPreviewRequest,
    build_preview_request,
    cleanup_preview_result,
    preview_start_seconds,
    run_compression_preview,
)


def test_preview_start_seconds_uses_middle_with_safe_clamp() -> None:
    assert preview_start_seconds(3600, 90) == 1620
    assert preview_start_seconds(100, 90) == 5
    assert preview_start_seconds(0, 90) == 0


def test_build_preview_request_probes_duration_for_middle_start(tmp_path: Path) -> None:
    source = tmp_path / "movie.mkv"
    source.write_bytes(b"x")

    with patch("mediaflow.compression_preview.get_duration_seconds", return_value=1000):
        request = build_preview_request(
            source=source,
            ffmpeg=tmp_path / "ffmpeg",
            ffprobe=tmp_path / "ffprobe",
            preset="faster",
            crf=22,
            duration_seconds=60,
            profile_id="profile",
            profile_label="Fast",
        )

    assert request.start_seconds == 450
    assert request.profile_id == "profile"
    assert request.profile_label == "Fast"


def test_run_compression_preview_cleans_temp_dir_on_failure(tmp_path: Path) -> None:
    source = tmp_path / "movie.mkv"
    source.write_bytes(b"source")

    class FakeProcess:
        stdout = iter(["out_time_ms=1000000\n"])
        stderr = SimpleNamespace(read=lambda: "encode failed\n")
        returncode = 1

        def wait(self):
            return self.returncode

        def kill(self):
            self.returncode = -9

    with (
        patch("mediaflow.compression_preview.get_duration_seconds", return_value=120),
        patch("mediaflow.compression_preview.build_ffmpeg_command", return_value=["ffmpeg", "-i", str(source), str(tmp_path / "out.mkv")]),
        patch("mediaflow.compression_preview.subprocess.Popen", return_value=FakeProcess()),
    ):
        result = run_compression_preview(
            CompressionPreviewRequest(
                source=source,
                ffmpeg=tmp_path / "ffmpeg",
                ffprobe=tmp_path / "ffprobe",
                preset="faster",
                crf=22,
                duration_seconds=30,
                start_seconds=50,
            )
        )

    assert result.success is False
    assert result.temp_dir is None
    assert result.output_path is None
    assert "encode failed" in result.error_message


def test_cleanup_preview_result_removes_success_temp_dir(tmp_path: Path) -> None:
    temp_dir = tmp_path / "preview"
    temp_dir.mkdir()
    output = temp_dir / "movie_preview.mkv"
    output.write_bytes(b"x")
    result = SimpleNamespace(temp_dir=temp_dir)

    assert cleanup_preview_result(result) is True
    assert not temp_dir.exists()
