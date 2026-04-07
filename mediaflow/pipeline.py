from __future__ import annotations

from dataclasses import dataclass

from .config import PipelineConfig


@dataclass(frozen=True)
class PipelineSummary:
    organised_plans: int = 0
    organised_errors: int = 0
    encoded_files: int = 0
    skipped_files: int = 0
    failed_files: int = 0


def should_run_plexify(config: PipelineConfig) -> bool:
    return config.plexify.enabled


def should_run_mediashrink(config: PipelineConfig) -> bool:
    return config.shrink.enabled


def target_compression_root(config: PipelineConfig) -> str:
    return str(config.library)
