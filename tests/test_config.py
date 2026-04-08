from __future__ import annotations

from pathlib import Path

import pytest

from mediaflow.config import PlexifySettings, ShrinkSettings, build_pipeline_config
from mediaflow.pipeline import (
    build_pipeline_summary,
    should_run_mediashrink,
    should_run_plexify,
    target_compression_root,
)


def test_build_pipeline_config_requires_one_stage(tmp_path: Path) -> None:
    library = tmp_path / "library"
    library.mkdir()
    with pytest.raises(ValueError, match="Enable at least one stage"):
        build_pipeline_config(
            source=str(tmp_path / "source"),
            library=str(library),
            plexify=PlexifySettings(enabled=False),
            shrink=ShrinkSettings(enabled=False),
        )


def test_build_pipeline_config_allows_compress_only(tmp_path: Path) -> None:
    library = tmp_path / "library"
    library.mkdir()

    config = build_pipeline_config(
        source="",
        library=str(library),
        plexify=PlexifySettings(enabled=False),
        shrink=ShrinkSettings(enabled=True),
    )

    assert should_run_plexify(config) is False
    assert should_run_mediashrink(config) is True
    assert target_compression_root(config) == str(library)


def test_build_pipeline_config_defaults_compression_root_to_library(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    library = tmp_path / "library"
    library.mkdir()

    config = build_pipeline_config(
        source=str(source),
        library=str(library),
    )

    assert config.compression_root == library


def test_build_pipeline_config_accepts_custom_compression_root(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    library = tmp_path / "library"
    library.mkdir()
    compression_root = tmp_path / "compress"
    compression_root.mkdir()

    config = build_pipeline_config(
        source=str(source),
        library=str(library),
        compression_root=str(compression_root),
    )

    assert config.compression_root == compression_root


def test_build_pipeline_config_requires_existing_compression_root_when_enabled(tmp_path: Path) -> None:
    library = tmp_path / "library"
    library.mkdir()

    with pytest.raises(ValueError, match="Compression root must exist"):
        build_pipeline_config(
            source="",
            library=str(library),
            compression_root=str(tmp_path / "missing-root"),
            plexify=PlexifySettings(enabled=False),
            shrink=ShrinkSettings(enabled=True),
        )


def test_build_pipeline_config_requires_existing_source_when_plexify_enabled(tmp_path: Path) -> None:
    library = tmp_path / "library"
    library.mkdir()

    with pytest.raises(ValueError, match="Source folder must exist"):
        build_pipeline_config(
            source=str(tmp_path / "missing"),
            library=str(library),
        )


def test_build_pipeline_summary_aggregates_stage_results() -> None:
    class ResultState:
        planned = 4
        errors = ["one"]

    class ApplyState:
        result = ResultState()
        report_path = "/tmp/report.json"
        apply_report_path = "/tmp/apply-report.json"

    class EncodeState:
        def __init__(self, *, skipped: bool, success: bool, input_size: int, output_size: int) -> None:
            self.skipped = skipped
            self.success = success
            self.input_size_bytes = input_size
            self.output_size_bytes = output_size

    summary = build_pipeline_summary(
        ApplyState(),
        [
            EncodeState(skipped=False, success=True, input_size=1000, output_size=400),
            EncodeState(skipped=True, success=False, input_size=1000, output_size=0),
            EncodeState(skipped=False, success=False, input_size=500, output_size=0),
        ],
    )

    assert summary.organised_plans == 4
    assert summary.organised_errors == 1
    assert summary.encoded_files == 1
    assert summary.skipped_files == 1
    assert summary.failed_files == 1
    assert summary.bytes_saved == 600
