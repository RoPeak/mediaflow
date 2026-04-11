from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from mediaflow.integrations import (
    build_compression_plan_rows,
    build_encode_result_rows,
    classify_compression_plan,
    display_name_for_ui,
    group_failure_rows,
)


def test_display_name_for_ui_strips_progress_prefixes_and_unknown_year() -> None:
    assert display_name_for_ui("In progress: Ghost (1990) (Unknown Year).mkv") == "Ghost (1990).mkv"


def test_build_compression_plan_rows_marks_risky_follow_up_items(tmp_path: Path) -> None:
    source = tmp_path / "movie.mp4"
    source.write_bytes(b"x")
    preparation = SimpleNamespace(
        jobs=[SimpleNamespace(source=source)],
        items=[
            SimpleNamespace(
                source=source,
                codec="h264",
                recommendation="recommended",
                reason_text="output header failure: 6",
                estimated_output_bytes=100,
                estimated_savings_bytes=50,
            )
        ],
    )

    rows = build_compression_plan_rows(preparation)

    assert rows[0].classification == "risky-follow-up"
    assert "compatibility" in rows[0].plain_reason.lower()


def test_classify_compression_plan_splits_safe_risky_and_informational(tmp_path: Path) -> None:
    safe = tmp_path / "safe.mkv"
    safe.write_bytes(b"x")
    risky = tmp_path / "risky.mp4"
    risky.write_bytes(b"x")
    skip = tmp_path / "skip.mkv"
    skip.write_bytes(b"x")
    preparation = SimpleNamespace(
        jobs=[SimpleNamespace(source=safe), SimpleNamespace(source=risky)],
        items=[
            SimpleNamespace(
                source=safe,
                codec="mpeg2video",
                recommendation="recommended",
                reason_text="legacy codec with strong projected space savings",
                estimated_output_bytes=100,
                estimated_savings_bytes=200,
            ),
            SimpleNamespace(
                source=risky,
                codec="h264",
                recommendation="recommended",
                reason_text="container compatibility issue",
                estimated_output_bytes=100,
                estimated_savings_bytes=100,
            ),
            SimpleNamespace(
                source=skip,
                codec="hevc",
                recommendation="skip",
                reason_text="video stream is already H.265/HEVC",
                estimated_output_bytes=0,
                estimated_savings_bytes=0,
            ),
        ],
    )

    groups = classify_compression_plan(build_compression_plan_rows(preparation))

    assert [row.source for row in groups.safe_selected] == [safe]
    assert [row.source for row in groups.risky_follow_up] == [risky]
    assert [row.source for row in groups.informational_skips] == [skip]


def test_build_encode_result_rows_translates_container_failures() -> None:
    source = Path("/tmp/movie.mp4")
    rows = build_encode_result_rows(
        [
            SimpleNamespace(
                job=SimpleNamespace(source=source),
                skipped=False,
                success=False,
                input_size_bytes=100,
                output_size_bytes=0,
                error_message="[out#0/mp4] Nothing was written into output file",
            )
        ]
    )

    assert rows[0].retry_ready is True
    assert "compatibility-first" in rows[0].reason
    assert rows[0].raw_reason.startswith("[out#0/mp4]")


def test_group_failure_rows_groups_shared_reasons() -> None:
    rows = [
        SimpleNamespace(is_failed=True, reason="Container compatibility blocked encoding", raw_reason="raw one"),
        SimpleNamespace(is_failed=True, reason="Container compatibility blocked encoding", raw_reason="raw two"),
    ]

    grouped = group_failure_rows(rows)

    assert len(grouped) == 1
    assert grouped[0].count == 2
