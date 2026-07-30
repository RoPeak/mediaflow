from __future__ import annotations

from types import SimpleNamespace
from typing import ClassVar

import pytest

from mediaflow.compat import (
    CompatibilityIssue,
    check_runtime_compatibility,
    compatibility_error_text,
)


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
        __dataclass_fields__: ClassVar[dict[str, object]] = {"attempted_queries": object()}

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
        if name == "guessit.api":
            return SimpleNamespace(guessit=lambda _filename: {})
        raise AssertionError(f"Unexpected import: {name}")

    monkeypatch.setattr("mediaflow.compat.import_module", fake_import_module)

    issues = check_runtime_compatibility()

    assert issues == []


def test_runtime_compatibility_rejects_unsupported_scan_signature(monkeypatch: pytest.MonkeyPatch) -> None:
    class CandidatePage:
        __dataclass_fields__: ClassVar[dict[str, object]] = {"attempted_queries": object()}

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
        if name == "guessit.api":
            return SimpleNamespace(guessit=lambda _filename: {})
        raise AssertionError(f"Unexpected import: {name}")

    monkeypatch.setattr("mediaflow.compat.import_module", fake_import_module)

    issues = check_runtime_compatibility()

    assert any("unsupported scan interface" in issue.message for issue in issues)

def test_runtime_compatibility_reports_guessit_parse_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    class CandidatePage:
        __dataclass_fields__: ClassVar[dict[str, object]] = {"attempted_queries": object()}

    class VideoUIController:
        def scan(self) -> None:
            return None

    def broken_guessit(_filename: str) -> None:
        raise RuntimeError("missing guessit config")

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
        if name == "guessit.api":
            return SimpleNamespace(guessit=broken_guessit)
        raise AssertionError(f"Unexpected import: {name}")

    monkeypatch.setattr("mediaflow.compat.import_module", fake_import_module)

    issues = check_runtime_compatibility()

    assert any(issue.area == "guessit" for issue in issues)

