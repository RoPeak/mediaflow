from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


def _safe_text(value: object | None) -> str:
    return str(value or "").strip()


def _safe_path(value: object | None) -> str | None:
    text = _safe_text(value)
    return text or None


@dataclass(frozen=True)
class OrganiseApplyStats:
    moved_count: int = 0
    skipped_count: int = 0
    error_count: int = 0
    report_path: str | None = None
    apply_report_path: str | None = None
    warnings: tuple[str, ...] = ()


@dataclass(frozen=True)
class CompressionPlanRow:
    source: Path
    display_name: str
    codec: str
    recommendation: str
    reason: str
    plain_reason: str
    estimated_output_bytes: int
    estimated_savings_bytes: int
    selected: bool
    exists: bool
    issue: str
    classification: str
    risk_reason: str


@dataclass(frozen=True)
class EncodeResultRow:
    source: Path
    display_name: str
    status: str
    original_bytes: int
    final_bytes: int
    saved_bytes: int
    reason: str
    raw_reason: str
    is_encoded: bool
    is_failed: bool
    is_skipped: bool
    retry_ready: bool


@dataclass(frozen=True)
class CompressionPlanClassification:
    safe_selected: tuple[CompressionPlanRow, ...] = ()
    risky_follow_up: tuple[CompressionPlanRow, ...] = ()
    informational_skips: tuple[CompressionPlanRow, ...] = ()
    missing_items: tuple[CompressionPlanRow, ...] = ()


@dataclass(frozen=True)
class FailureSummaryGroup:
    summary: str
    guidance: str
    raw_reasons: tuple[str, ...]
    count: int


def summarise_apply_result(apply_result: object | None) -> OrganiseApplyStats:
    if apply_result is None:
        return OrganiseApplyStats()

    result = getattr(apply_result, "result", None)
    moved = tuple(getattr(result, "moved", []) or [])
    skipped = tuple(getattr(result, "skipped", []) or [])
    errors = tuple(getattr(result, "errors", []) or [])
    warnings = tuple(getattr(apply_result, "warnings", []) or [])
    return OrganiseApplyStats(
        moved_count=len(moved),
        skipped_count=len(skipped),
        error_count=len(errors),
        report_path=_safe_path(getattr(apply_result, "report_path", None)),
        apply_report_path=_safe_path(getattr(apply_result, "apply_report_path", None)),
        warnings=warnings,
    )


def build_compression_plan_rows(preparation: object | None) -> list[CompressionPlanRow]:
    if preparation is None:
        return []

    selected_sources = {getattr(job, "source", None) for job in getattr(preparation, "jobs", []) or []}
    rows: list[CompressionPlanRow] = []
    for item in getattr(preparation, "items", []) or []:
        source = getattr(item, "source", None)
        if source is None:
            continue
        exists = bool(getattr(source, "exists", lambda: False)())
        selected = source in selected_sources
        issue = ""
        reason = _safe_text(getattr(item, "reason_text", None))
        if selected and not exists:
            issue = "Missing from compression root"
        elif getattr(item, "recommendation", "") == "skip":
            issue = _problem_issue_text(reason)
        classification = classify_plan_item(
            codec=_safe_text(getattr(item, "codec", None)),
            recommendation=_safe_text(getattr(item, "recommendation", None)),
            reason=reason,
            selected=selected,
            exists=exists,
        )
        risk_reason = ""
        if classification == "risky-follow-up":
            risk_reason = "Deferred by default because output/container compatibility looks risky."
        rows.append(
            CompressionPlanRow(
                source=source,
                display_name=display_name_for_ui(source.name),
                codec=_safe_text(getattr(item, "codec", None)),
                recommendation=_safe_text(getattr(item, "recommendation", None)),
                reason=reason,
                plain_reason=translate_plan_reason(reason),
                estimated_output_bytes=int(getattr(item, "estimated_output_bytes", 0) or 0),
                estimated_savings_bytes=int(getattr(item, "estimated_savings_bytes", 0) or 0),
                selected=selected,
                exists=exists,
                issue=issue,
                classification=classification,
                risk_reason=risk_reason,
            )
        )
    return rows


def build_encode_result_rows(results: Iterable[object] | None) -> list[EncodeResultRow]:
    rows: list[EncodeResultRow] = []
    for result in results or []:
        job = getattr(result, "job", None)
        source = getattr(job, "source", None)
        if source is None:
            continue
        skipped = bool(getattr(result, "skipped", False))
        success = bool(getattr(result, "success", False))
        original_bytes = int(getattr(result, "input_size_bytes", 0) or 0)
        final_bytes = int(getattr(result, "output_size_bytes", 0) or 0)
        saved_bytes = max(original_bytes - final_bytes, 0) if success else 0
        if skipped:
            status = "Skipped"
            raw_reason = _safe_text(getattr(result, "skip_reason", None)) or "Skipped by plan"
            reason = translate_result_reason(raw_reason)
        elif success:
            status = "Encoded"
            raw_reason = "Encoded successfully"
            reason = "Encoded successfully"
        else:
            status = "Failed"
            raw_reason = _safe_text(getattr(result, "error_message", None)) or "Encoding failed"
            reason = translate_result_reason(raw_reason)
        rows.append(
            EncodeResultRow(
                source=source,
                display_name=display_name_for_ui(source.name),
                status=status,
                original_bytes=original_bytes,
                final_bytes=final_bytes,
                saved_bytes=saved_bytes,
                reason=reason,
                raw_reason=raw_reason,
                is_encoded=success,
                is_failed=not skipped and not success,
                is_skipped=skipped,
                retry_ready=(not skipped and not success),
            )
        )
    return rows


def collect_retry_sources(preparation: object | None, results: Iterable[object] | None) -> set[Path]:
    sources: set[Path] = set()
    for row in build_encode_result_rows(results):
        if row.retry_ready:
            sources.add(row.source)

    for row in build_compression_plan_rows(preparation):
        if row.selected and not row.exists:
            sources.add(row.source)
            continue
        if row.classification == "risky-follow-up":
            sources.add(row.source)
    return sources


def classify_compression_plan(rows: Iterable[CompressionPlanRow]) -> CompressionPlanClassification:
    safe_selected: list[CompressionPlanRow] = []
    risky_follow_up: list[CompressionPlanRow] = []
    informational_skips: list[CompressionPlanRow] = []
    missing_items: list[CompressionPlanRow] = []

    for row in rows:
        if row.classification == "missing":
            missing_items.append(row)
        elif row.classification == "risky-follow-up":
            risky_follow_up.append(row)
        elif row.classification == "informational-skip":
            informational_skips.append(row)
        elif row.classification == "safe-selected":
            safe_selected.append(row)

    return CompressionPlanClassification(
        safe_selected=tuple(safe_selected),
        risky_follow_up=tuple(risky_follow_up),
        informational_skips=tuple(informational_skips),
        missing_items=tuple(missing_items),
    )


def classify_plan_item(
    *,
    codec: str,
    recommendation: str,
    reason: str,
    selected: bool,
    exists: bool,
) -> str:
    if selected and not exists:
        return "missing"
    lowered_reason = reason.lower()
    lowered_codec = codec.lower()
    if selected and _looks_retryable_problem(reason):
        return "risky-follow-up"
    if recommendation == "skip" and (
        "already h.265" in lowered_reason
        or "already hevc" in lowered_reason
        or lowered_codec == "hevc"
    ):
        return "informational-skip"
    if selected:
        return "safe-selected"
    return "other"


def display_name_for_ui(name: str) -> str:
    cleaned = _safe_text(name)
    for prefix in ("In progress:", "Ready:", "Encoding:", "Completed:"):
        if cleaned.startswith(prefix):
            cleaned = cleaned.removeprefix(prefix).strip()
    cleaned = cleaned.replace("(Unknown Year)", "").replace("  ", " ").strip()
    cleaned = re_sub_space_before_suffix(cleaned)
    cleaned = cleaned.replace(" ()", "")
    return cleaned or name


def re_sub_space_before_suffix(value: str) -> str:
    for suffix in (".mkv", ".mp4", ".avi", ".mov", ".m4v", ".ts"):
        value = value.replace(f" {suffix}", suffix)
    return value


def translate_plan_reason(reason: str) -> str:
    if not reason:
        return ""
    lowered = reason.lower()
    if "output header" in lowered:
        return "Output/container compatibility risk"
    if "copied stream" in lowered or "container" in lowered:
        return "Container compatibility follow-up needed"
    if "already h.265" in lowered or "already hevc" in lowered:
        return "Already efficient codec"
    return reason


def translate_result_reason(reason: str) -> str:
    if not reason:
        return ""
    lowered = reason.lower()
    if "nothing was written into output file" in lowered:
        return "Output container/profile combination produced no valid file; retry with compatibility-first settings."
    if "output header failure" in lowered:
        return "Output/container compatibility blocked encoding; retry with compatibility-first settings."
    if "container" in lowered and "incompat" in lowered:
        return "Container compatibility blocked encoding; retry with compatibility-first settings."
    if "missing" in lowered:
        return "Planned source file was missing when compression ran."
    return reason


def group_failure_rows(rows: Iterable[EncodeResultRow]) -> list[FailureSummaryGroup]:
    grouped: dict[tuple[str, str], list[str]] = {}
    for row in rows:
        if not row.is_failed:
            continue
        guidance = "Prepare a compatibility-first retry plan for the affected files."
        key = (row.reason, guidance)
        grouped.setdefault(key, [])
        if row.raw_reason:
            grouped[key].append(row.raw_reason)
    results: list[FailureSummaryGroup] = []
    for (summary, guidance), raw_reasons in grouped.items():
        results.append(
            FailureSummaryGroup(
                summary=summary,
                guidance=guidance,
                raw_reasons=tuple(raw_reasons),
                count=len(raw_reasons) or 1,
            )
        )
    return sorted(results, key=lambda item: (-item.count, item.summary))


def recommended_headroom_bytes(preparation: object | None) -> int:
    if preparation is None:
        return 0

    largest_input = 0
    largest_estimated_output = 0
    selected_sources = {getattr(job, "source", None) for job in getattr(preparation, "jobs", []) or []}
    for item in getattr(preparation, "items", []) or []:
        source = getattr(item, "source", None)
        if source not in selected_sources:
            continue
        largest_input = max(largest_input, int(getattr(item, "size_bytes", 0) or 0))
        largest_estimated_output = max(
            largest_estimated_output,
            int(getattr(item, "estimated_output_bytes", 0) or 0),
        )
    largest_known_output = max(
        (int(getattr(job, "estimated_output_bytes", 0) or 0) for job in getattr(preparation, "jobs", []) or []),
        default=0,
    )
    working_set = max(largest_input, largest_estimated_output, largest_known_output)
    safety_padding = 512 * 1024 * 1024
    minimum = 1024 * 1024 * 1024
    return max(minimum, working_set + safety_padding)


def _problem_issue_text(reason: str) -> str:
    if not reason:
        return ""
    lowered = reason.lower()
    if "output header" in lowered:
        return "Container/header compatibility risk"
    if "container" in lowered:
        return "Container compatibility risk"
    if "copied stream" in lowered:
        return "Copied-stream compatibility risk"
    if "already h.265" in lowered or "already hevc" in lowered:
        return "Already efficient codec"
    return "Needs compatibility follow-up"


def _looks_retryable_problem(reason: str) -> bool:
    lowered = reason.lower()
    return any(
        token in lowered
        for token in (
            "output header",
            "container",
            "copied stream",
            "incompatib",
            "follow-up",
        )
    )
