from __future__ import annotations

from types import SimpleNamespace

from mediaflow.progress import ApplyProgressModel, EncodeProgressModel, PreparationProgressModel


def test_encode_progress_model_keeps_file_progress_monotonic_within_file() -> None:
    model = EncodeProgressModel()

    model.update_from_progress(
        current_file_name="movie.mkv",
        phase="Encoding",
        current_file_progress=0.40,
        overall_progress=0.10,
        completed_files=0,
        remaining_files=4,
        bytes_processed=100,
        total_bytes=1000,
        now=1.0,
    )
    model.update_from_progress(
        current_file_name="movie.mkv",
        phase="Encoding",
        current_file_progress=0.35,
        overall_progress=0.12,
        completed_files=0,
        remaining_files=4,
        bytes_processed=110,
        total_bytes=1000,
        now=2.0,
    )

    assert model.current_file_progress == 0.40
    assert model.overall_progress == 0.12


def test_encode_progress_model_resets_displayed_progress_when_file_changes() -> None:
    model = EncodeProgressModel()
    model.update_from_progress(
        current_file_name="first.mkv",
        phase="Encoding",
        current_file_progress=0.60,
        overall_progress=0.30,
        completed_files=0,
        remaining_files=2,
        bytes_processed=300,
        total_bytes=1000,
        now=1.0,
    )
    model.tick(2.0, 1.0)
    model.update_from_progress(
        current_file_name="second.mkv",
        phase="Encoding",
        current_file_progress=0.05,
        overall_progress=0.32,
        completed_files=1,
        remaining_files=1,
        bytes_processed=320,
        total_bytes=1000,
        now=3.0,
    )

    assert model.current_file_name == "second.mkv"
    assert model.displayed_file_progress == 0.05


def test_preparation_progress_model_tracks_stage_weighting() -> None:
    model = PreparationProgressModel()

    model.update_stage("discovering", "Discovering files", completed=1, total=4)
    discovering = model.progress_ratio
    model.update_analysis(2, 4, "movie.mkv", 100)
    analysing = model.progress_ratio

    assert analysing > discovering
    assert model.discovered_files == 2
    assert model.discovered_bytes == 100


def test_encode_progress_model_holds_eta_until_confident() -> None:
    model = EncodeProgressModel()

    model.update_from_progress(
        current_file_name="movie.mkv",
        phase="Encoding",
        current_file_progress=0.02,
        overall_progress=0.01,
        completed_files=0,
        remaining_files=4,
        bytes_processed=10,
        total_bytes=1000,
        now=1.0,
    )
    model.update_from_progress(
        current_file_name="movie.mkv",
        phase="Encoding",
        current_file_progress=0.03,
        overall_progress=0.02,
        completed_files=0,
        remaining_files=4,
        bytes_processed=20,
        total_bytes=1000,
        now=10.0,
    )
    model.tick(10.0, 9.0)

    assert model.eta_seconds is None
    assert model.eta_confident is False


def test_encode_progress_model_produces_eta_once_history_is_stable() -> None:
    model = EncodeProgressModel()
    for now, processed, current, overall in [
        (1.0, 100, 0.10, 0.06),
        (20.0, 250, 0.20, 0.12),
        (50.0, 450, 0.35, 0.20),
        (80.0, 650, 0.50, 0.28),
    ]:
        model.update_from_progress(
            current_file_name="movie.mkv",
            phase="Encoding",
            current_file_progress=current,
            overall_progress=overall,
            completed_files=0,
            remaining_files=4,
            bytes_processed=processed,
            total_bytes=1000,
            now=now,
        )
    model.tick(80.0, 79.0)

    assert model.eta_seconds is not None
    assert model.eta_confident is True


def test_apply_progress_model_tracks_current_and_completed_counts() -> None:
    model = ApplyProgressModel()

    model.update_from_progress(
        SimpleNamespace(
            phase="copying",
            completed=0,
            total=7,
            current_source="/tmp/source.mp4",
            current_destination="/tmp/dest.mp4",
            source_size_bytes=100,
            message="Copying source.mp4",
        ),
        now=12.0,
    )

    assert model.current_item_index == 1
    assert model.completed_items == 0
    assert model.total_items == 7
    assert model.current_file_bytes == 100


def test_apply_progress_model_tracks_byte_progress_and_eta() -> None:
    model = ApplyProgressModel()

    model.update_from_progress(
        SimpleNamespace(
            phase="copying",
            completed=0,
            total=2,
            current_source="/tmp/source.mp4",
            current_destination="/tmp/dest.mp4",
            source_size_bytes=100,
            current_file_bytes_copied=50,
            completed_bytes=50,
            total_bytes=200,
            parallel_workers=2,
            progress_capability="byte-copy",
            message="Copying source.mp4",
        ),
        now=10.0,
    )

    assert model.current_file_bytes_copied == 50
    assert model.completed_bytes == 50
    assert model.total_bytes == 200
    assert model.parallel_workers == 2
    assert model.progress_capability == "byte-copy"
    assert model.speed_mbps is not None
    assert model.eta_seconds is not None
