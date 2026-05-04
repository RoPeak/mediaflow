"""Type aliases for worker callback payloads.

These types describe the shapes emitted by progress_callback during background
operations. They are used for isinstance narrowing in MainWindow callback slots.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import NamedTuple


class PreparationProgress(NamedTuple):
    """Progress payload emitted during file-by-file preparation analysis."""

    completed: int
    total: int
    path: str


@dataclass(frozen=True)
class PreparationStageUpdate:
    """Progress payload emitted when preparation moves between major phases."""

    stage: str
    message: str
    completed: int | None = None
    total: int | None = None


@dataclass(frozen=True)
class ApplyProgress:
    """Progress payload emitted while organisation is being applied to disk."""

    phase: str
    current_source: str | None = None
    current_destination: str | None = None
    completed: int = 0
    total: int = 0
    last_applied_source: str | None = None
    message: str | None = None
    operation: str | None = None
    source_size_bytes: int | None = None
    bytes_copied: int | None = None
    current_file_bytes_copied: int | None = None
    completed_bytes: int | None = None
    total_bytes: int | None = None
    active_files: int | None = None
    parallel_workers: int | None = None
    progress_capability: str | None = None
    started_at: str | None = None
    completed_at: str | None = None
    report_path: str | None = None
    conflict_action: str | None = None
    error: str | None = None
    cancel_requested: bool = False
