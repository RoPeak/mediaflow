from __future__ import annotations

import time
from pathlib import Path


def launch(*, source: Path | None = None, library: Path | None = None) -> None:
    try:
        from PySide6.QtWidgets import QApplication
    except ImportError as exc:
        raise RuntimeError(
            "PySide6 is required to run mediaflow. Install project dependencies first."
        ) from exc

    from .main_window import MainWindow

    app = QApplication.instance() or QApplication([])
    startup_started = time.monotonic()
    window = MainWindow(default_source=source, default_library=library)
    window.show()
    app.processEvents()
    window.note_startup_complete(startup_started)
    app.exec()
