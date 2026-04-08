from __future__ import annotations

from dataclasses import dataclass

from .config import PipelineConfig


def _safe_path(value: object) -> str | None:
    return str(value) if value else None


@dataclass(frozen=True)
class PipelineSummary:
    organised_plans: int = 0
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
    organised_plans = 0
    organised_errors = 0
    organise_report_path = None
    organise_apply_report_path = None
    if apply_result is not None:
        organised_plans = int(getattr(getattr(apply_result, "result", None), "planned", 0) or 0)
        organised_errors = len(getattr(getattr(apply_result, "result", None), "errors", []) or [])
        organise_report_path = _safe_path(getattr(apply_result, "report_path", None))
        organise_apply_report_path = _safe_path(getattr(apply_result, "apply_report_path", None))

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
        organised_plans=organised_plans,
        organised_errors=organised_errors,
        encoded_files=encoded_files,
        skipped_files=skipped_files,
        failed_files=failed_files,
        bytes_saved=bytes_saved,
        organise_report_path=organise_report_path,
        organise_apply_report_path=organise_apply_report_path,
    )


def should_run_plexify(config: PipelineConfig) -> bool:
    return config.plexify.enabled


def should_run_mediashrink(config: PipelineConfig) -> bool:
    return config.shrink.enabled


def target_compression_root(config: PipelineConfig) -> str:
    return str(config.compression_root)
