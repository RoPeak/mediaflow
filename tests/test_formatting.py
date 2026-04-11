"""Tests for pure static utility methods in MainWindow.

These run without instantiating a QApplication — static methods are callable
directly on the class. Both mediashrink and PySide6 must be importable for
the module-level imports in main_window.py to succeed, so we guard on both.
"""
import pytest

pytest.importorskip("mediashrink")
pytest.importorskip("PySide6")

from mediaflow.main_window import MainWindow  # noqa: E402


class TestStripRich:
    def test_removes_simple_tag(self):
        assert MainWindow._strip_rich("[dim]hello[/dim]") == "hello"

    def test_removes_colour_tags(self):
        assert MainWindow._strip_rich("[white]text[/white]") == "text"

    def test_passthrough_plain(self):
        assert MainWindow._strip_rich("no markup") == "no markup"

    def test_mixed(self):
        result = MainWindow._strip_rich("[dim]In progress:[/dim] [white]The Dark Knight[/white]")
        assert result == "In progress: The Dark Knight"


class TestFormatBytes:
    def test_bytes(self):
        assert MainWindow._format_bytes(512) == "512.0 B"

    def test_kilobytes(self):
        assert MainWindow._format_bytes(1536) == "1.5 KB"

    def test_megabytes(self):
        assert "MB" in MainWindow._format_bytes(2 * 1024 * 1024)

    def test_gigabytes(self):
        result = MainWindow._format_bytes(4 * 1024 ** 3)
        assert "GB" in result and "4" in result


class TestFormatElapsed:
    def test_under_60(self):
        assert MainWindow._format_elapsed(45.9) == "45s"

    def test_exactly_60(self):
        assert MainWindow._format_elapsed(60.0) == "1m 0s"

    def test_minutes_and_seconds(self):
        assert MainWindow._format_elapsed(195.0) == "3m 15s"


class TestNormalizeHeartbeatState:
    def test_maps_active_to_encoding(self):
        assert MainWindow._normalize_heartbeat_state("active") == "Encoding"

    def test_strips_rich_markup_before_mapping(self):
        assert MainWindow._normalize_heartbeat_state("[dim]muxing[/dim]") == "Muxing"


class TestProgressBucket:
    def test_zero_bucket(self):
        assert MainWindow._progress_bucket(0.0) == 0

    def test_mid_bucket(self):
        assert MainWindow._progress_bucket(0.51) == 10

    def test_clamps_high_values(self):
        assert MainWindow._progress_bucket(9.9) == 20
