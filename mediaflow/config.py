from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from plexify.paths import PathOverlapError, ensure_non_overlapping_paths


@dataclass(frozen=True)
class PlexifySettings:
    enabled: bool = True
    apply: bool = True
    copy_mode: bool = True
    use_cache: bool = True
    offline: bool = False
    min_confidence: float = 0.90
    extensions: str = ".mkv,.mp4,.avi,.m4v,.mov,.ts"
    on_conflict: str = "rename"


@dataclass(frozen=True)
class ShrinkSettings:
    enabled: bool = True
    overwrite: bool = True
    recursive: bool = True
    no_skip: bool = False
    policy: str = "fastest-wall-clock"
    on_file_failure: str = "retry"
    use_calibration: bool = True
    duplicate_policy: str = "prefer-mkv"


@dataclass(frozen=True)
class PipelineConfig:
    source: Path
    library: Path
    compression_root: Path
    plexify: PlexifySettings = PlexifySettings()
    shrink: ShrinkSettings = ShrinkSettings()

    def validate(self, *, allow_missing_compression_root: bool = False) -> None:
        if self.plexify.enabled:
            if not str(self.library).strip():
                raise ValueError("A library folder is required when organise is enabled.")
            if self.library.exists() and self.library.is_file():
                raise ValueError("Library path must be a folder.")
        if not self.plexify.enabled and not self.shrink.enabled:
            raise ValueError("Enable at least one stage.")
        if self.plexify.enabled:
            if not str(self.source).strip():
                raise ValueError("A source folder is required when organise is enabled.")
            if not self.source.exists() or not self.source.is_dir():
                raise ValueError("Source folder must exist.")
        if self.shrink.enabled:
            if not str(self.compression_root).strip():
                raise ValueError("A compression root is required when compress is enabled.")
            if self.compression_root.exists() and not self.compression_root.is_dir():
                raise ValueError("Compression root must be a folder.")
            if not self.compression_root.exists() and not allow_missing_compression_root:
                raise ValueError("Compression root must exist.")
        if self.plexify.min_confidence < 0 or self.plexify.min_confidence > 1:
            raise ValueError("Minimum confidence must be between 0 and 1.")
        if self.plexify.enabled:
            try:
                ensure_non_overlapping_paths(
                    self.source,
                    self.library,
                    label_source="Source",
                    label_library="Library",
                )
            except PathOverlapError as exc:
                raise ValueError(exc.issue.reason) from exc


def build_pipeline_config(
    *,
    source: str,
    library: str,
    compression_root: str | None = None,
    plexify: PlexifySettings | None = None,
    shrink: ShrinkSettings | None = None,
    allow_missing_compression_root: bool = False,
) -> PipelineConfig:
    raw_library = Path(library).expanduser()
    raw_source = Path(source).expanduser() if source.strip() else raw_library
    raw_compression_root = (
        Path(compression_root).expanduser()
        if compression_root and compression_root.strip()
        else raw_library
    )
    config = PipelineConfig(
        source=raw_source,
        library=raw_library,
        compression_root=raw_compression_root,
        plexify=plexify or PlexifySettings(),
        shrink=shrink or ShrinkSettings(),
    )
    config.validate(allow_missing_compression_root=allow_missing_compression_root)
    return config
