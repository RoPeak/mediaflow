from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class WorkflowState(str, Enum):
    SETUP = "setup"
    SCANNING = "scanning"
    REVIEW = "review"
    REVIEW_BLOCKED = "review_blocked"
    READY_TO_APPLY = "ready_to_apply"
    APPLYING = "applying"
    PREPARING_COMPRESSION = "preparing_compression"
    READY_TO_COMPRESS = "ready_to_compress"
    COMPRESSING = "compressing"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass(frozen=True)
class WorkflowPresentation:
    step_title: str
    headline: str
    guidance: str


def describe_workflow_state(state: WorkflowState) -> WorkflowPresentation:
    mapping = {
        WorkflowState.SETUP: WorkflowPresentation(
            step_title="Step 1 of 4: Setup",
            headline="Configure source, library, and stages",
            guidance="Choose your folders, confirm stage toggles, then start a scan or guided pipeline.",
        ),
        WorkflowState.SCANNING: WorkflowPresentation(
            step_title="Step 2 of 4: Scanning",
            headline="Scanning source with plexify",
            guidance="Wait for scan results. Review actions are temporarily disabled while plexify loads candidates.",
        ),
        WorkflowState.REVIEW: WorkflowPresentation(
            step_title="Step 2 of 4: Review",
            headline="Review proposed plexify matches",
            guidance="Accept, skip, search again, or use manual matching. Build a preview before applying organisation.",
        ),
        WorkflowState.REVIEW_BLOCKED: WorkflowPresentation(
            step_title="Step 2 of 4: Review required",
            headline="Manual review is still required",
            guidance="Some items are unresolved. Resolve or skip them, then preview and apply organisation.",
        ),
        WorkflowState.READY_TO_APPLY: WorkflowPresentation(
            step_title="Step 3 of 4: Apply organisation",
            headline="Organisation preview is ready",
            guidance="Check the preview summary, then apply organisation to continue.",
        ),
        WorkflowState.APPLYING: WorkflowPresentation(
            step_title="Step 3 of 4: Applying",
            headline="Applying organisation to disk",
            guidance="Please wait while plexify moves or copies files.",
        ),
        WorkflowState.PREPARING_COMPRESSION: WorkflowPresentation(
            step_title="Step 4 of 4: Prepare compression",
            headline="Preparing mediashrink plan",
            guidance="Scanning the library and building a compression plan.",
        ),
        WorkflowState.READY_TO_COMPRESS: WorkflowPresentation(
            step_title="Step 4 of 4: Compression ready",
            headline="Compression plan is ready",
            guidance="Review the compression plan and start encoding when you are ready.",
        ),
        WorkflowState.COMPRESSING: WorkflowPresentation(
            step_title="Step 4 of 4: Compressing",
            headline="Encoding in progress",
            guidance="Progress bars show the current file and overall encode progress.",
        ),
        WorkflowState.COMPLETED: WorkflowPresentation(
            step_title="Completed",
            headline="Pipeline finished",
            guidance="Review the final summary and reports before closing the app.",
        ),
        WorkflowState.FAILED: WorkflowPresentation(
            step_title="Attention required",
            headline="The last operation failed",
            guidance="Read the error summary, adjust settings if needed, and try again.",
        ),
    }
    return mapping[state]
