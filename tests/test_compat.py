from __future__ import annotations

from mediaflow.compat import CompatibilityIssue, compatibility_error_text


def test_compatibility_error_text_includes_area_message_and_details() -> None:
    text = compatibility_error_text(
        [
            CompatibilityIssue(
                area="plexify",
                message="Installed build is missing a required field.",
                technical_detail="UICandidatePage.attempted_queries is required.",
            )
        ]
    )

    assert "Runtime compatibility check failed:" in text
    assert "- plexify: Installed build is missing a required field." in text
    assert "UICandidatePage.attempted_queries is required." in text
