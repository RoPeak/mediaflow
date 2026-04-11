from __future__ import annotations

from dataclasses import dataclass

from .config import PipelineConfig
from .integrations import summarise_apply_result


def _safe_path(value: object) -> str | None:
    return str(value) if value else None


@dataclass(frozen=True)
class PipelineSummary:
    organised_files: int = 0
    organise_skipped: int = 0
    organised_errors: int = 0
    encoded_files: int = 0
    skipped_files: int = 0
    failed_files: int = 0
    bytes_saved: int = 0
    organise_report_path: str | None = None
    organise_apply_report_path: str | None = None


def build_pipeline_summary(
    apply_result: object | None,
    encode_results: list[object] | None,
) -> PipelineSummary:
    apply_stats = summarise_apply_result(apply_result)

    encoded_files = 0
    skipped_files = 0
    failed_files = 0
    bytes_saved = 0
    for result in encode_results or []:
        skipped = bool(getattr(result, "skipped", False))
        success = bool(getattr(result, "success", False))
        if skipped:
            skipped_files += 1
        elif success:
            encoded_files += 1
            input_size = int(getattr(result, "input_size_bytes", 0) or 0)
            output_size = int(getattr(result, "output_size_bytes", 0) or 0)
            bytes_saved += max(input_size - output_size, 0)
        else:
            failed_files += 1

    return PipelineSummary(
        organised_files=apply_stats.moved_count,
        organise_skipped=apply_stats.skipped_count,
        organised_errors=apply_stats.error_count,
        encoded_files=encoded_files,
        skipped_files=skipped_files,
        failed_files=failed_files,
        bytes_saved=bytes_saved,
        organise_report_path=apply_stats.report_path,
        organise_apply_report_path=apply_stats.apply_report_path,
    )


def should_run_plexify(config: PipelineConfig) -> bool:
    return config.plexify.enabled


def should_run_mediashrink(config: PipelineConfig) -> bool:
    return config.shrink.enabled


def target_compression_root(config: PipelineConfig) -> str:
    return str(config.compression_root)
