"""Type aliases for worker callback payloads.

These types describe the shapes emitted by progress_callback during background
operations. They are used for isinstance narrowing in MainWindow callback slots.
"""
from __future__ import annotations

from typing import NamedTuple


class PreparationProgress(NamedTuple):
    """Progress payload emitted by prepare_compression during file analysis."""

    completed: int
    total: int
    path: str
