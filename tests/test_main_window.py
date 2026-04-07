from __future__ import annotations

import pytest

pytest.importorskip("PySide6")

from PySide6.QtWidgets import QApplication

from mediaflow.main_window import MainWindow


def test_stage_toggles_enable_and_disable_group_boxes() -> None:
    app = QApplication.instance() or QApplication([])
    window = MainWindow()

    window.organise_enabled.setChecked(False)
    window.compress_enabled.setChecked(True)
    window._update_stage_controls()

    assert window.organise_group.isEnabled() is False
    assert window.compress_group.isEnabled() is True

    window.compress_enabled.setChecked(False)
    window._update_stage_controls()

    assert window.compress_group.isEnabled() is False


def test_initial_action_state_is_practical() -> None:
    app = QApplication.instance() or QApplication([])
    window = MainWindow()

    assert window.scan_button.isEnabled() is True
    assert window.preview_button.isEnabled() is False
    assert window.apply_button.isEnabled() is False
    assert window.accept_button.isEnabled() is False
    assert window.compress_button.isEnabled() is True


def test_disabling_compress_stage_disables_run_button() -> None:
    app = QApplication.instance() or QApplication([])
    window = MainWindow()

    window.compress_enabled.setChecked(False)
    window._update_stage_controls()

    assert window.compress_button.isEnabled() is False
