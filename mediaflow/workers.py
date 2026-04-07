from __future__ import annotations

import inspect
import traceback
from collections.abc import Callable


try:
    from PySide6.QtCore import QObject, QRunnable, Signal
except ImportError:  # pragma: no cover - handled at runtime when GUI is launched
    QObject = object
    QRunnable = object

    class Signal:  # type: ignore[override]
        def __init__(self, *args, **kwargs) -> None:
            pass


class WorkerSignals(QObject):
    result = Signal(object)
    error = Signal(str)
    progress = Signal(object)
    finished = Signal()


class FunctionWorker(QRunnable):
    def __init__(self, fn: Callable[..., object], /, *args, **kwargs) -> None:
        super().__init__()
        self.fn = fn
        self.args = args
        self.kwargs = kwargs
        self.signals = WorkerSignals()

    def run(self) -> None:
        try:
            if _supports_progress_callback(self.fn):
                result = self.fn(
                    *self.args,
                    progress_callback=self.signals.progress.emit,
                    **self.kwargs,
                )
            else:
                result = self.fn(*self.args, **self.kwargs)
        except Exception:  # pragma: no cover - GUI runtime behavior
            self.signals.error.emit(traceback.format_exc())
        else:
            self.signals.result.emit(result)
        finally:
            self.signals.finished.emit()


def _supports_progress_callback(fn: Callable[..., object]) -> bool:
    try:
        signature = inspect.signature(fn)
    except (TypeError, ValueError):
        return False
    return "progress_callback" in signature.parameters
