from __future__ import annotations

from pathlib import Path

import pytest

from mediaflow.config import PlexifySettings, ShrinkSettings, build_pipeline_config
from mediaflow.pipeline import should_run_mediashrink, should_run_plexify, target_compression_root


def test_build_pipeline_config_requires_one_stage(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="Enable at least one stage"):
        build_pipeline_config(
            source=str(tmp_path / "source"),
            library=str(tmp_path / "library"),
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


def test_build_pipeline_config_requires_existing_source_when_plexify_enabled(tmp_path: Path) -> None:
    library = tmp_path / "library"
    library.mkdir()

    with pytest.raises(ValueError, match="Source folder must exist"):
        build_pipeline_config(
            source=str(tmp_path / "missing"),
            library=str(library),
        )
