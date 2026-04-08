from __future__ import annotations

from mediaflow.workflow import WorkflowState, describe_workflow_state


def test_workflow_descriptions_are_human_readable() -> None:
    presentation = describe_workflow_state(WorkflowState.REVIEW_BLOCKED)

    assert presentation.step_title == "Step 2 of 4: Review required"
    assert "Manual review" in presentation.headline
    assert "Resolve or skip" in presentation.guidance


def test_compress_only_step_numbering() -> None:
    setup = describe_workflow_state(WorkflowState.SETUP, organise_enabled=False)
    ready = describe_workflow_state(WorkflowState.READY_TO_COMPRESS, organise_enabled=False)
    preparing = describe_workflow_state(WorkflowState.PREPARING_COMPRESSION, organise_enabled=False)

    assert setup.step_title == "Step 1 of 2: Setup"
    assert ready.step_title == "Step 2 of 2: Compression ready"
    assert preparing.step_title == "Step 2 of 2: Prepare compression"


def test_full_pipeline_step_numbering_unchanged() -> None:
    setup = describe_workflow_state(WorkflowState.SETUP, organise_enabled=True)
    ready = describe_workflow_state(WorkflowState.READY_TO_COMPRESS, organise_enabled=True)

    assert setup.step_title == "Step 1 of 4: Setup"
    assert ready.step_title == "Step 4 of 4: Compression ready"
