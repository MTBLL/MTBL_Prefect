"""Smoke test: full_pipeline executes all expected CLI invocations in DAG order.

Mocks `run_uv_cli` (not `subprocess.run`) so Prefect's own internal subprocess
calls are unaffected. Verifies extract -> transform -> load ordering and that
each pipe is invoked with the expected key arguments.
"""

from __future__ import annotations

from mtbl_prefect.flows.full_pipeline import full_pipeline
from mtbl_prefect.tasks import shell


def test_full_pipeline_preseason_dispatches_all_pipes(monkeypatch):
    calls: list[tuple[str, str]] = []

    def fake_run_uv_cli(project_dir, *args, allow_exit_code_1=False):
        calls.append((project_dir, " ".join(str(a) for a in args)))

    monkeypatch.setattr(shell, "run_uv_cli", fake_run_uv_cli)

    full_pipeline(year=2025, preseason=True)

    def first_idx(project_substr: str, args_substr: str = "") -> int:
        return next(
            i for i, (p, a) in enumerate(calls)
            if project_substr in p and args_substr in a
        )

    # All extractors invoked
    assert any("ESPN_API_Extractor" in p and "players-extract" in a for p, a in calls)
    assert any("ESPN_API_Extractor" in p and "league-extract" in a for p, a in calls)
    # Fangraphs gets --winter-meetings in preseason mode
    assert any(
        "Fangraphs_API_Extractor" in p and "--winter-meetings" in a
        for p, a in calls
    )
    # Savant uses year-1 in preseason
    assert any("Savant_API_Extractor" in p and "2024" in a for p, a in calls)

    # Transforms invoked
    assert any("Player_Universe_Trx" in p for p, _ in calls)
    assert any("MTBL_Valuations" in p for p, _ in calls)

    # Load invoked with the original year (not Savant's year-1)
    assert any("Player_Universe_Load" in p and "2025" in a for p, a in calls)

    # DAG ordering: every extractor finishes before any transform begins;
    # every transform finishes before load begins.
    extract_last = max(
        first_idx("ESPN_API_Extractor", "league-extract"),
        first_idx("Fangraphs_API_Extractor"),
        first_idx("Savant_API_Extractor"),
    )
    transform_first = min(first_idx("Player_Universe_Trx"), first_idx("MTBL_Valuations"))
    transform_last = max(first_idx("Player_Universe_Trx"), first_idx("MTBL_Valuations"))
    load_first = first_idx("Player_Universe_Load")

    assert extract_last < transform_first, "extract must finish before transform starts"
    assert transform_last < load_first, "transform must finish before load starts"


def test_full_pipeline_in_season_uses_ros_flag_for_fangraphs(monkeypatch):
    """In-season run (no --preseason) should pass -ros to Fangraphs, not --preseason."""
    calls: list[tuple[str, str]] = []

    def fake_run_uv_cli(project_dir, *args, allow_exit_code_1=False):
        calls.append((project_dir, " ".join(str(a) for a in args)))

    monkeypatch.setattr(shell, "run_uv_cli", fake_run_uv_cli)

    full_pipeline(year=2026, preseason=False)

    # Fangraphs gets no mode flag in-season (default mix handles rest-of-season behavior)
    assert any(
        "Fangraphs_API_Extractor" in p and "--winter-meetings" not in a
        for p, a in calls
    )
    # Savant uses --season equal to --year when not in preseason
    assert any("Savant_API_Extractor" in p and "2026" in a for p, a in calls)
