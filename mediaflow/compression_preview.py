from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import shutil
import subprocess
import tempfile
import time
from collections.abc import Callable

from mediashrink.encoder import build_ffmpeg_command, get_duration_seconds
from mediashrink.models import EncodeJob


@dataclass(frozen=True)
class CompressionPreviewRequest:
    source: Path
    ffmpeg: Path
    ffprobe: Path
    preset: str
    crf: int
    duration_seconds: float
    start_seconds: float
    profile_id: str | None = None
    profile_label: str = ""


@dataclass(frozen=True)
class CompressionPreviewProgress:
    source: Path
    progress: float
    elapsed_seconds: float
    message: str


@dataclass(frozen=True)
class CompressionPreviewResult:
    request: CompressionPreviewRequest
    success: bool
    cancelled: bool
    output_path: Path | None
    temp_dir: Path | None
    input_size_bytes: int
    output_size_bytes: int
    source_duration_seconds: float
    encoded_duration_seconds: float
    elapsed_seconds: float
    estimated_full_output_bytes: int
    error_message: str = ""


def preview_start_seconds(duration_seconds: float, clip_seconds: float) -> float:
    if duration_seconds <= 0 or clip_seconds <= 0:
        return 0.0
    latest_start = max(duration_seconds - clip_seconds - 5.0, 0.0)
    midpointish = duration_seconds * 0.45
    return max(0.0, min(midpointish, latest_start))


def build_preview_request(
    *,
    source: Path,
    ffmpeg: Path,
    ffprobe: Path,
    preset: str,
    crf: int,
    duration_seconds: float,
    profile_id: str | None = None,
    profile_label: str = "",
) -> CompressionPreviewRequest:
    source_duration = get_duration_seconds(source, ffprobe)
    return CompressionPreviewRequest(
        source=source,
        ffmpeg=ffmpeg,
        ffprobe=ffprobe,
        preset=preset,
        crf=crf,
        duration_seconds=max(float(duration_seconds), 1.0),
        start_seconds=preview_start_seconds(source_duration, float(duration_seconds)),
        profile_id=profile_id,
        profile_label=profile_label,
    )


def run_compression_preview(
    request: CompressionPreviewRequest,
    progress_callback: Callable[[object], None] | None = None,
    cancel_callback: Callable[[], bool] | None = None,
) -> CompressionPreviewResult:
    temp_dir = Path(tempfile.mkdtemp(prefix="mediaflow-preview-"))
    output = temp_dir / f"{request.source.stem}_preview{request.source.suffix}"
    tmp_output = temp_dir / f".tmp_{request.source.stem}_preview{request.source.suffix}"
    started = time.monotonic()
    source_duration = get_duration_seconds(request.source, request.ffprobe)
    input_size = _path_size(request.source)
    stderr_tail: list[str] = []
    cancelled = False

    job = EncodeJob(
        source=request.source,
        output=output,
        tmp_output=tmp_output,
        crf=request.crf,
        preset=request.preset,
        dry_run=False,
        skip=False,
    )
    cmd = build_ffmpeg_command(job, request.ffmpeg, request.duration_seconds)
    cmd = _with_seek(cmd, request.start_seconds)

    _emit(
        progress_callback,
        CompressionPreviewProgress(request.source, 0.0, 0.0, "Starting preview encode..."),
    )
    try:
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        assert process.stdout is not None
        for raw_line in process.stdout:
            if _cancel_requested(cancel_callback):
                cancelled = True
                process.kill()
                break
            progress = _progress_from_line(raw_line, request.duration_seconds)
            if progress is not None:
                _emit(
                    progress_callback,
                    CompressionPreviewProgress(
                        request.source,
                        progress,
                        time.monotonic() - started,
                        f"Encoding preview clip... {int(progress * 100)}%",
                    ),
                )
        stderr = ""
        if process.stderr is not None:
            stderr = process.stderr.read()
        if stderr:
            stderr_tail = [line.strip() for line in stderr.splitlines() if line.strip()][-5:]
        process.wait()
    except Exception as exc:
        try:
            process.kill()
            process.wait()
        except Exception:
            pass
        cleanup_preview_path(temp_dir)
        raise RuntimeError(f"Preview encode failed: {exc}") from exc

    elapsed = time.monotonic() - started
    if cancelled or _cancel_requested(cancel_callback):
        cleanup_preview_path(temp_dir)
        return CompressionPreviewResult(
            request=request,
            success=False,
            cancelled=True,
            output_path=None,
            temp_dir=None,
            input_size_bytes=input_size,
            output_size_bytes=0,
            source_duration_seconds=source_duration,
            encoded_duration_seconds=request.duration_seconds,
            elapsed_seconds=elapsed,
            estimated_full_output_bytes=0,
            error_message="Preview cancelled.",
        )

    if process.returncode != 0 or not output.exists():
        cleanup_preview_path(temp_dir)
        return CompressionPreviewResult(
            request=request,
            success=False,
            cancelled=False,
            output_path=None,
            temp_dir=None,
            input_size_bytes=input_size,
            output_size_bytes=0,
            source_duration_seconds=source_duration,
            encoded_duration_seconds=request.duration_seconds,
            elapsed_seconds=elapsed,
            estimated_full_output_bytes=0,
            error_message="\n".join(stderr_tail) or f"FFmpeg exited with code {process.returncode}",
        )

    output_size = _path_size(output)
    estimated_full = 0
    if source_duration > 0 and request.duration_seconds > 0 and output_size > 0:
        estimated_full = int(output_size * (source_duration / request.duration_seconds))
    _emit(
        progress_callback,
        CompressionPreviewProgress(request.source, 1.0, elapsed, "Preview encode complete."),
    )
    return CompressionPreviewResult(
        request=request,
        success=True,
        cancelled=False,
        output_path=output,
        temp_dir=temp_dir,
        input_size_bytes=input_size,
        output_size_bytes=output_size,
        source_duration_seconds=source_duration,
        encoded_duration_seconds=request.duration_seconds,
        elapsed_seconds=elapsed,
        estimated_full_output_bytes=estimated_full,
    )


def cleanup_preview_result(result: CompressionPreviewResult | None) -> bool:
    if result is None:
        return True
    return cleanup_preview_path(result.temp_dir)


def cleanup_preview_path(path: Path | None) -> bool:
    if path is None:
        return True
    try:
        shutil.rmtree(path, ignore_errors=True)
    except OSError:
        return False
    return not path.exists()


def _with_seek(cmd: list[str], start_seconds: float) -> list[str]:
    if start_seconds <= 0 or len(cmd) < 2:
        return cmd
    return [cmd[0], "-ss", f"{start_seconds:.3f}", *cmd[1:]]


def _progress_from_line(line: str, duration_seconds: float) -> float | None:
    if duration_seconds <= 0 or "=" not in line:
        return None
    key, _, value = line.strip().partition("=")
    seconds: float | None = None
    if key in {"out_time_ms", "out_time_us"}:
        try:
            seconds = float(value) / 1_000_000.0
        except ValueError:
            return None
    elif key == "out_time":
        seconds = _timestamp_seconds(value)
    if seconds is None:
        return None
    return max(0.0, min(seconds / duration_seconds, 1.0))


def _timestamp_seconds(value: str) -> float | None:
    parts = value.strip().split(":")
    if len(parts) != 3:
        return None
    try:
        return float(parts[0]) * 3600 + float(parts[1]) * 60 + float(parts[2])
    except ValueError:
        return None


def _cancel_requested(cancel_callback: Callable[[], bool] | None) -> bool:
    if cancel_callback is None:
        return False
    try:
        return bool(cancel_callback())
    except Exception:
        return False


def _emit(callback: Callable[[object], None] | None, payload: object) -> None:
    if callback is not None:
        callback(payload)


def _path_size(path: Path) -> int:
    try:
        return path.stat().st_size
    except OSError:
        return 0
