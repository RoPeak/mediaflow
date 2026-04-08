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
