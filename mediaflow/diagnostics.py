from __future__ import annotations

import json
import tempfile
from dataclasses import asdict, dataclass, field, is_dataclass
from datetime import datetime, timezone
from pathlib import Path

from .settings import get_config_dir


def default_diagnostics_candidates(library: Path | None = None) -> list[Path]:
    candidates = [get_config_dir() / "runs"]
    if library is not None and str(library).strip():
        candidates.append(library / ".mediaflow" / "runs")
    candidates.append(Path.home() / "mediaflow-runs")
    return candidates


def select_diagnostics_dir(candidates: list[Path]) -> tuple[Path, str | None]:
    failures: list[str] = []
    for candidate in candidates:
        try:
            candidate.mkdir(parents=True, exist_ok=True)
            probe = candidate / ".mediaflow-diagnostics-probe"
            probe.write_text("ok", encoding="utf-8")
            probe.unlink(missing_ok=True)
            if failures:
                return candidate, "Primary diagnostics location unavailable; using fallback. " + " | ".join(failures)
            return candidate, None
        except OSError as exc:
            failures.append(f"{candidate}: {exc}")
    fallback = candidates[-1] if candidates else Path.home() / "mediaflow-runs"
    return fallback, "Unable to verify diagnostics directory. " + " | ".join(failures)


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
    provenance: dict[str, object] | None = None
    events: list[dict[str, object]] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    written_path: Path | None = None
    _last_event_signatures: dict[str, tuple[tuple[str, object], ...]] = field(default_factory=dict)

    def set_config(self, payload: dict[str, object]) -> None:
        self.effective_config = payload

    def set_provenance(self, payload: dict[str, object]) -> None:
        self.provenance = payload

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
            "provenance": _serialize(self.provenance),
            "warnings": list(self.warnings),
            "events": list(self.events),
            "summary": _serialize(summary),
            "failure": _serialize(failure),
        }
        _atomic_write_text(path, json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        _atomic_write_text(path.with_suffix(".log"), _human_log(payload), encoding="utf-8")
        self.written_path = path
        return path


def _atomic_write_text(path: Path, text: str, *, encoding: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", encoding=encoding, dir=path.parent, delete=False) as handle:
        handle.write(text)
        temp_path = Path(handle.name)
    try:
        temp_path.replace(path)
    except Exception:
        temp_path.unlink(missing_ok=True)
        raise


def _human_log(payload: dict[str, object]) -> str:
    lines = [f"Started: {payload.get('started_at', '')}", ""]
    provenance = payload.get("provenance")
    if isinstance(provenance, dict):
        lines.append("Provenance")
        for key in ("app_version", "python_executable", "python_version", "platform", "config_dir", "diagnostics_dir"):
            if key in provenance:
                lines.append(f"{key}: {provenance[key]}")
        lines.append("")
    warnings = payload.get("warnings")
    if isinstance(warnings, list) and warnings:
        lines.append("Warnings")
        lines.extend(f"- {warning}" for warning in warnings)
        lines.append("")
    events = payload.get("events")
    if isinstance(events, list):
        lines.append("Events")
        for event in events:
            if not isinstance(event, dict):
                continue
            timestamp = event.get("timestamp", "")
            kind = event.get("kind", "")
            details = ", ".join(
                f"{key}={value}"
                for key, value in event.items()
                if key not in {"timestamp", "kind"} and value not in (None, "")
            )
            lines.append(f"{timestamp} {kind}" + (f" | {details}" if details else ""))
        lines.append("")
    summary = payload.get("summary")
    if isinstance(summary, dict):
        lines.append("Summary")
        for key, value in summary.items():
            lines.append(f"{key}: {value}")
    failure = payload.get("failure")
    if failure:
        lines.extend(["", f"Failure: {failure}"])
    return "\n".join(lines).rstrip() + "\n"


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
