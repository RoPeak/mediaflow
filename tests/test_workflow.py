from __future__ import annotations

from mediaflow.workflow import WorkflowState, describe_workflow_state


def test_workflow_descriptions_are_human_readable() -> None:
    presentation = describe_workflow_state(WorkflowState.REVIEW_BLOCKED)

    assert presentation.step_title == "Step 2 of 4: Review required"
    assert "Manual review" in presentation.headline
    assert "Resolve or skip" in presentation.guidance
