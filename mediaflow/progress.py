from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from pathlib import Path


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
class ApplyProgressModel:
    phase: str = "starting"
    current_source: str = ""
    current_destination: str = ""
    current_item_index: int = 0
    completed_items: int = 0
    total_items: int = 0
    current_file_bytes: int = 0
    total_bytes: int = 0
    bytes_copied: int = 0
    current_file_bytes_copied: int = 0
    completed_bytes: int = 0
    active_files: int = 0
    parallel_workers: int = 1
    progress_capability: str = ""
    speed_mbps: float | None = None
    eta_seconds: float | None = None
    elapsed_seconds: float = 0.0
    stalled_seconds: float = 0.0
    report_path: str = ""
    cancel_requested: bool = False
    event_log: deque[str] = field(default_factory=lambda: deque(maxlen=200))

    def reset(self) -> None:
        self.phase = "starting"
        self.current_source = ""
        self.current_destination = ""
        self.current_item_index = 0
        self.completed_items = 0
        self.total_items = 0
        self.current_file_bytes = 0
        self.total_bytes = 0
        self.bytes_copied = 0
        self.current_file_bytes_copied = 0
        self.completed_bytes = 0
        self.active_files = 0
        self.parallel_workers = 1
        self.progress_capability = ""
        self.speed_mbps = None
        self.eta_seconds = None
        self.elapsed_seconds = 0.0
        self.stalled_seconds = 0.0
        self.report_path = ""
        self.cancel_requested = False
        self.event_log.clear()

    def update_from_progress(self, payload: object, *, now: float) -> None:
        phase = str(getattr(payload, "phase", "") or "working")
        completed = max(0, int(getattr(payload, "completed", 0) or 0))
        total = max(0, int(getattr(payload, "total", 0) or 0))
        current_source = str(getattr(payload, "current_source", "") or "")
        current_destination = str(getattr(payload, "current_destination", "") or "")
        current_size = int(getattr(payload, "source_size_bytes", 0) or 0)
        bytes_copied = int(getattr(payload, "bytes_copied", 0) or 0)
        current_file_bytes_copied = int(getattr(payload, "current_file_bytes_copied", 0) or 0)
        completed_bytes = int(getattr(payload, "completed_bytes", 0) or 0)
        total_bytes = int(getattr(payload, "total_bytes", 0) or 0)
        report_path = str(getattr(payload, "report_path", "") or "")
        new_source = bool(current_source and current_source != self.current_source)

        self.phase = phase
        self.completed_items = min(completed, total) if total else completed
        self.total_items = max(self.total_items, total)
        self.current_source = current_source or self.current_source
        self.current_destination = current_destination or self.current_destination
        self.current_file_bytes = current_size or self.current_file_bytes
        self.bytes_copied = max(self.bytes_copied, bytes_copied)
        self.current_file_bytes_copied = max(0 if new_source else self.current_file_bytes_copied, current_file_bytes_copied)
        self.completed_bytes = max(self.completed_bytes, completed_bytes, self.bytes_copied)
        self.total_bytes = max(self.total_bytes, total_bytes)
        self.active_files = max(0, int(getattr(payload, "active_files", 0) or 0))
        self.parallel_workers = max(1, int(getattr(payload, "parallel_workers", 1) or 1))
        self.progress_capability = str(getattr(payload, "progress_capability", "") or self.progress_capability)
        self.report_path = report_path or self.report_path
        self.cancel_requested = self.cancel_requested or bool(getattr(payload, "cancel_requested", False))
        self.current_item_index = self._current_index(phase, self.completed_items, self.total_items)
        self.elapsed_seconds = max(0.0, now)
        if self.elapsed_seconds > 0 and self.completed_bytes > 0:
            self.speed_mbps = self.completed_bytes / self.elapsed_seconds / 1_048_576
            if self.total_bytes > self.completed_bytes and self.speed_mbps > 0:
                self.eta_seconds = (self.total_bytes - self.completed_bytes) / (self.speed_mbps * 1_048_576)

        message = str(getattr(payload, "message", "") or "").strip()
        if not message:
            label = Path(self.current_source).name if self.current_source else "file operation"
            message = f"{phase.replace('-', ' ').title()}: {label}"
        if not self.event_log or self.event_log[-1] != message:
            self.event_log.append(message)

    def update_stall(self, *, elapsed_seconds: float, stalled_seconds: float) -> None:
        self.elapsed_seconds = max(0.0, elapsed_seconds)
        self.stalled_seconds = max(0.0, stalled_seconds)

    @staticmethod
    def _current_index(phase: str, completed: int, total: int) -> int:
        if total <= 0:
            return 0
        normalized = phase.replace("_", "-").strip().lower()
        if normalized in {"done", "finalizing-report"}:
            return total
        if normalized == "completed-item":
            return min(total, max(1, completed))
        return min(total, completed + 1)


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
