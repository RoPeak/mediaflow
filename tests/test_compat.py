from __future__ import annotations

from types import SimpleNamespace

import pytest

from mediaflow.compat import CompatibilityIssue, compatibility_error_text
from mediaflow.compat import check_runtime_compatibility


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


def test_runtime_compatibility_accepts_legacy_scan_signature(monkeypatch: pytest.MonkeyPatch) -> None:
    class CandidatePage:
        __dataclass_fields__ = {"attempted_queries": object()}

    class VideoUIController:
        def scan(self) -> None:
            return None

    def fake_import_module(name: str):
        if name == "plexify.ui_services":
            return SimpleNamespace(UICandidatePage=CandidatePage)
        if name == "plexify.ui_controller":
            return SimpleNamespace(VideoUIController=VideoUIController)
        if name == "mediashrink.gui_api":
            return SimpleNamespace(
                prepare_encode_run=object(),
                run_encode_plan=object(),
                EncodePreparation=object(),
                EncodeProgress=object(),
            )
        raise AssertionError(f"Unexpected import: {name}")

    monkeypatch.setattr("mediaflow.compat.import_module", fake_import_module)

    issues = check_runtime_compatibility()

    assert issues == []


def test_runtime_compatibility_rejects_unsupported_scan_signature(monkeypatch: pytest.MonkeyPatch) -> None:
    class CandidatePage:
        __dataclass_fields__ = {"attempted_queries": object()}

    class VideoUIController:
        def scan(self, unexpected, other) -> None:
            return None

    def fake_import_module(name: str):
        if name == "plexify.ui_services":
            return SimpleNamespace(UICandidatePage=CandidatePage)
        if name == "plexify.ui_controller":
            return SimpleNamespace(VideoUIController=VideoUIController)
        if name == "mediashrink.gui_api":
            return SimpleNamespace(
                prepare_encode_run=object(),
                run_encode_plan=object(),
                EncodePreparation=object(),
                EncodeProgress=object(),
            )
        raise AssertionError(f"Unexpected import: {name}")

    monkeypatch.setattr("mediaflow.compat.import_module", fake_import_module)

    issues = check_runtime_compatibility()

    assert any("unsupported scan interface" in issue.message for issue in issues)
