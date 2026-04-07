from __future__ import annotations

from mediaflow.workers import _supports_progress_callback


def test_supports_progress_callback_detects_named_parameter() -> None:
    def worker(progress_callback=None):
        return progress_callback

    assert _supports_progress_callback(worker) is True


def test_supports_progress_callback_rejects_functions_without_named_parameter() -> None:
    def worker(value):
        return value

    assert _supports_progress_callback(worker) is False
