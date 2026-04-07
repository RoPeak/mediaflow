from __future__ import annotations

from dataclasses import dataclass
from importlib import import_module


@dataclass(frozen=True)
class CompatibilityIssue:
    area: str
    message: str
    technical_detail: str | None = None


def check_runtime_compatibility() -> list[CompatibilityIssue]:
    issues: list[CompatibilityIssue] = []

    try:
        ui_services = import_module("plexify.ui_services")
    except Exception as exc:
        issues.append(
            CompatibilityIssue(
                area="plexify",
                message="Could not import plexify integration modules.",
                technical_detail=str(exc),
            )
        )
    else:
        candidate_page = getattr(ui_services, "UICandidatePage", None)
        fields = getattr(candidate_page, "__dataclass_fields__", {})
        if "attempted_queries" not in fields:
            issues.append(
                CompatibilityIssue(
                    area="plexify",
                    message="Installed plexify build is missing expected candidate page fields.",
                    technical_detail="UICandidatePage.attempted_queries is required by current video search flow.",
                )
            )

    try:
        gui_api = import_module("mediashrink.gui_api")
    except Exception as exc:
        issues.append(
            CompatibilityIssue(
                area="mediashrink",
                message="Could not import mediashrink GUI integration modules.",
                technical_detail=str(exc),
            )
        )
    else:
        for name in ("prepare_encode_run", "run_encode_plan", "EncodePreparation", "EncodeProgress"):
            if not hasattr(gui_api, name):
                issues.append(
                    CompatibilityIssue(
                        area="mediashrink",
                        message=f"Installed mediashrink build is missing `{name}`.",
                    )
                )

    return issues


def compatibility_error_text(issues: list[CompatibilityIssue]) -> str:
    lines = ["Runtime compatibility check failed:"]
    for issue in issues:
        lines.append(f"- {issue.area}: {issue.message}")
        if issue.technical_detail:
            lines.append(f"  {issue.technical_detail}")
    return "\n".join(lines)
