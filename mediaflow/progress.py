from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field


PREPARATION_STAGE_SPECS: tuple[tuple[str, str, float, float], ...] = (
    ("discovering", "Discovering files", 0.00, 0.10),
    ("analysing", "Analysing files", 0.10, 0.55),
    ("benchmarking", "Benchmarking profiles", 0.55, 0.75),
    ("smoke", "Smoke probing", 0.75, 0.92),
    ("scoring", "Scoring recommendations", 0.92, 0.99),
    ("plan-ready", "Plan ready", 0.99, 1.00),
)


def normalize_preparation_stage(stage: str) -> str:
    lowered = stage.lower()
    if "discover" in lowered:
        return "discovering"
    if "analys" in lowered:
        return "analysing"
    if "benchmark" in lowered or "provisional profile" in lowered:
        return "benchmarking"
    if "smoke" in lowered:
        return "smoke"
    if "scoring" in lowered:
        return "scoring"
    if "ready" in lowered:
        return "plan-ready"
    return "analysing"


def preparation_stage_title(stage: str) -> str:
    key = normalize_preparation_stage(stage)
    for candidate, title, _start, _end in PREPARATION_STAGE_SPECS:
        if candidate == key:
            return title
    return stage.replace("_", " ").replace("-", " ").title()


def preparation_timeline_text(active_stage: str) -> str:
    current_key = normalize_preparation_stage(active_stage)
    stages = [(key, title) for key, title, _start, _end in PREPARATION_STAGE_SPECS]
    current_index = next((index for index, (key, _title) in enumerate(stages) if key == current_key), 0)
    parts: list[str] = []
    for index, (_key, title) in enumerate(stages):
        if index < current_index:
            prefix = "✓"
        elif index == current_index:
            prefix = "▶"
        else:
            prefix = "○"
        parts.append(f"{prefix} {title}")
    return "  •  ".join(parts)


@dataclass
class PreparationProgressModel:
    stage_key: str = "discovering"
    stage_message: str = "Preparing compression plan..."
    discovered_files: int = 0
    discovered_bytes: int = 0
    current_file_name: str = ""
    progress_ratio: float = 0.0
    raw_events: list[str] = field(default_factory=list)

    def update_stage(self, stage: str, message: str, completed: int | None = None, total: int | None = None) -> None:
        self.stage_key = normalize_preparation_stage(stage)
        self.stage_message = message
        if completed is not None and total:
            self.progress_ratio = self._stage_progress_ratio(self.stage_key, completed / total)
        elif self.stage_key == "plan-ready":
            self.progress_ratio = 1.0
        else:
            self.progress_ratio = max(self.progress_ratio, self._stage_progress_ratio(self.stage_key, 0.0))
        self.raw_events.append(message)

    def update_analysis(self, completed: int, total: int, file_name: str, file_size: int) -> None:
        self.stage_key = "analysing"
        self.current_file_name = file_name
        self.discovered_files = max(self.discovered_files, completed)
        self.discovered_bytes += max(file_size, 0)
        if total:
            self.progress_ratio = self._stage_progress_ratio("analysing", completed / total)
        self.stage_message = f"Analysing {completed} of {total} file(s)"
        self.raw_events.append(f"[{completed}/{total}] {file_name}")
        if total and completed >= total:
            self.progress_ratio = max(self.progress_ratio, self._stage_progress_ratio("benchmarking", 0.0))

    def mark_ready(self) -> None:
        self.stage_key = "plan-ready"
        self.stage_message = "Compression plan ready."
        self.progress_ratio = 1.0

    @staticmethod
    def _stage_progress_ratio(stage_key: str, ratio: float) -> float:
        clipped = max(0.0, min(ratio, 1.0))
        for candidate, _title, start, end in PREPARATION_STAGE_SPECS:
            if candidate == stage_key:
                return start + (end - start) * clipped
        return clipped


@dataclass
class EncodeProgressModel:
    current_file_name: str = ""
    phase: str = "Ready"
    current_file_progress: float = 0.0
    overall_progress: float = 0.0
    displayed_file_progress: float = 0.0
    completed_files: int = 0
    remaining_files: int = 0
    bytes_processed: int = 0
    total_bytes: int = 0
    elapsed_seconds: float = 0.0
    eta_seconds: float | None = None
    speed_mbps: float | None = None
    eta_confident: bool = False
    _history: deque[tuple[float, int]] = field(default_factory=lambda: deque(maxlen=24))

    def reset(self) -> None:
        self.current_file_name = ""
        self.phase = "Ready"
        self.current_file_progress = 0.0
        self.overall_progress = 0.0
        self.displayed_file_progress = 0.0
        self.completed_files = 0
        self.remaining_files = 0
        self.bytes_processed = 0
        self.total_bytes = 0
        self.elapsed_seconds = 0.0
        self.eta_seconds = None
        self.speed_mbps = None
        self.eta_confident = False
        self._history.clear()

    def update_from_progress(
        self,
        *,
        current_file_name: str,
        phase: str,
        current_file_progress: float,
        overall_progress: float,
        completed_files: int,
        remaining_files: int,
        bytes_processed: int,
        total_bytes: int,
        now: float,
    ) -> None:
        new_file = current_file_name != self.current_file_name
        if new_file:
            self.current_file_name = current_file_name
            self.current_file_progress = max(0.0, min(current_file_progress, 1.0))
            self.displayed_file_progress = self.current_file_progress
        else:
            self.current_file_progress = max(
                self.current_file_progress,
                max(0.0, min(current_file_progress, 1.0)),
            )
        self.phase = phase
        self.overall_progress = max(self.overall_progress, max(0.0, min(overall_progress, 1.0)))
        self.completed_files = completed_files
        self.remaining_files = remaining_files
        self.bytes_processed = max(self.bytes_processed, bytes_processed)
        self.total_bytes = max(total_bytes, self.total_bytes)
        self._history.append((now, self.bytes_processed))

    def tick(self, now: float, elapsed_seconds: float) -> None:
        self.elapsed_seconds = max(0.0, elapsed_seconds)
        gap = self.current_file_progress - self.displayed_file_progress
        if gap > 0:
            step = max(0.01, min(0.08, gap * 0.6))
            self.displayed_file_progress = min(self.current_file_progress, self.displayed_file_progress + step)
        else:
            self.displayed_file_progress = self.current_file_progress

        self.speed_mbps = self._rolling_speed_mbps()
        self.eta_confident = self._eta_has_confidence()
        if self.eta_confident and self.speed_mbps and self.total_bytes > self.bytes_processed:
            remaining_bytes = self.total_bytes - self.bytes_processed
            target_eta = remaining_bytes / (self.speed_mbps * 1_048_576)
            self.eta_seconds = self._smooth_eta(target_eta)
        elif self.eta_confident and self.overall_progress >= 0.15 and self.elapsed_seconds >= 60:
            target_eta = self.elapsed_seconds / self.overall_progress * (1 - self.overall_progress)
            self.eta_seconds = self._smooth_eta(target_eta)
        else:
            self.eta_seconds = None

    def _rolling_speed_mbps(self) -> float | None:
        if len(self._history) < 2:
            return None
        start_time, start_bytes = self._history[0]
        end_time, end_bytes = self._history[-1]
        elapsed = end_time - start_time
        if elapsed <= 0:
            return None
        delta = max(0, end_bytes - start_bytes)
        if delta <= 0:
            return None
        return delta / elapsed / 1_048_576

    def _eta_has_confidence(self) -> bool:
        if len(self._history) < 4:
            return False
        if self.elapsed_seconds < 45:
            return False
        if self.overall_progress < 0.05 and self.completed_files <= 0:
            return False
        return self.speed_mbps is not None or self.overall_progress >= 0.15

    def _smooth_eta(self, target_eta: float) -> float:
        target_eta = max(0.0, target_eta)
        if self.eta_seconds is None:
            return target_eta
        return (self.eta_seconds * 0.7) + (target_eta * 0.3)
