from __future__ import annotations

import pytest


def test_mediashrink_gui_api_contract() -> None:
    from mediashrink import gui_api

    assert hasattr(gui_api, "EncodePreparation")
    assert hasattr(gui_api, "EncodeProgress")
    assert hasattr(gui_api, "prepare_encode_run")
    assert hasattr(gui_api, "run_encode_plan")


def test_plexify_ui_controller_contract() -> None:
    pytest.importorskip("requests")
    from plexify import ui_controller

    assert hasattr(ui_controller, "PreviewState")
    assert hasattr(ui_controller, "ApplyResultState")
    assert hasattr(ui_controller, "VideoUIConfig")
    assert hasattr(ui_controller, "VideoUIController")
