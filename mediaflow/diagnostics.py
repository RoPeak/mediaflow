from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field, is_dataclass
from datetime import datetime, timezone
from pathlib import Path

from .settings import get_config_dir


def diagnostics_dir(base_dir: Path | None = None) -> Path:
    root = base_dir or (get_config_dir() / "runs")
    root.mkdir(parents=True, exist_ok=True)
    return root


def diagnostics_path(base_dir: Path | None = None, *, started_at: datetime | None = None) -> Path:
    timestamp = (started_at or datetime.now(timezone.utc)).strftime("%Y%m%dT%H%M%SZ")
    return diagnostics_dir(base_dir) / f"mediaflow-run-{timestamp}.json"


@dataclass
class DiagnosticsRecorder:
    effective_config: dict[str, object] | None = None
    events: list[dict[str, object]] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    written_path: Path | None = None
    _last_event_signatures: dict[str, tuple[tuple[str, object], ...]] = field(default_factory=dict)

    def set_config(self, payload: dict[str, object]) -> None:
        self.effective_config = payload

    def record_event(self, kind: str, **payload: object) -> None:
        serialized_payload = {key: _serialize(value) for key, value in payload.items()}
        signature = tuple(sorted(serialized_payload.items()))
        if self._last_event_signatures.get(kind) == signature:
            return
        self._last_event_signatures[kind] = signature
        event = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "kind": kind,
        }
        event.update(serialized_payload)
        self.events.append(event)

    def record_warning(self, text: str) -> None:
        if text not in self.warnings:
            self.warnings.append(text)
        self.record_event("warning", text=text)

    def write(
        self,
        *,
        base_dir: Path | None = None,
        summary: dict[str, object] | None = None,
        failure: dict[str, object] | None = None,
    ) -> Path:
        path = diagnostics_path(base_dir, started_at=self.started_at)
        payload = {
            "started_at": self.started_at.isoformat(),
            "effective_config": _serialize(self.effective_config),
            "warnings": list(self.warnings),
            "events": list(self.events),
            "summary": _serialize(summary),
            "failure": _serialize(failure),
        }
        path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        self.written_path = path
        return path


def _serialize(value: object) -> object:
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {str(key): _serialize(inner) for key, inner in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_serialize(inner) for inner in value]
    if is_dataclass(value):
        return _serialize(asdict(value))
    if hasattr(value, "__dict__"):
        return _serialize(vars(value))
    return str(value)
