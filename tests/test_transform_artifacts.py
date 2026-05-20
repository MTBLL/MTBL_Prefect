"""Tests for publish_valuations_logs — the iteration-log artifact task."""

from __future__ import annotations

import os

import pytest

from mtbl_prefect.flows import transform


# A minimal but format-faithful summary log: header, CONVERGENCE table,
# WARNINGS table. Whitespace alignment matches the real sub-project output.
_SUMMARY = """\
=== source: {source} ===
ts: 2026-05-20T21:31:45

CONVERGENCE
-----------
 source          phase  pos  iters_run  converged  oscillating  best_iter
{source}   phase3b-iter    C          5      False         True          2
{source}   phase6d-iter   RP          4       True        False          4

WARNINGS (1)
------------
 source          phase  pos  iter                   kind                  msg
{source}   phase3b-iter    C     5   oscillation_resolved   period-2+ oscillation here
"""


@pytest.fixture
def fake_tables(monkeypatch):
    """Capture create_table_artifact calls instead of hitting Prefect."""
    calls: list[dict] = []
    monkeypatch.setattr(
        transform, "create_table_artifact", lambda **kwargs: calls.append(kwargs)
    )
    return calls


def _make_run_dir(logs_root, name, sources, mtime):
    run_dir = logs_root / name
    run_dir.mkdir(parents=True)
    for source in sources:
        (run_dir / f"{source}_summary.log").write_text(_SUMMARY.format(source=source))
    os.utime(run_dir, (mtime, mtime))
    return run_dir


def test_publishes_convergence_and_warnings_tables(tmp_path, monkeypatch, fake_tables):
    monkeypatch.setattr(transform, "VALUATIONS_LOGS_DIR", tmp_path)
    # An older run and a newer run — only the newest should be published.
    _make_run_dir(tmp_path, "20260101-000000", ["current"], mtime=1000)
    _make_run_dir(tmp_path, "20260520-213144", ["current", "ros"], mtime=2000)

    transform.publish_valuations_logs.fn()

    by_key = {c["key"]: c for c in fake_tables}
    assert set(by_key) == {"mtbl-valuations-convergence", "mtbl-valuations-warnings"}

    # Two sources x two convergence rows each = 4 rows; the stale run is ignored.
    conv = by_key["mtbl-valuations-convergence"]["table"]
    assert len(conv) == 4
    assert {r["source"] for r in conv} == {"current", "ros"}
    first = conv[0]
    assert first["pos"] == "C" and first["converged"] == "False" and first["best_iter"] == "2"

    # Warnings: one per source; the msg column keeps its internal spaces.
    warn = by_key["mtbl-valuations-warnings"]["table"]
    assert len(warn) == 2
    assert warn[0]["kind"] == "oscillation_resolved"
    assert warn[0]["msg"] == "period-2+ oscillation here"

    assert "20260520-213144" in by_key["mtbl-valuations-convergence"]["description"]


def test_no_logs_dir_is_noop(tmp_path, monkeypatch, fake_tables):
    """A missing logs directory is logged and swallowed — never raises."""
    monkeypatch.setattr(transform, "VALUATIONS_LOGS_DIR", tmp_path / "missing")
    transform.publish_valuations_logs.fn()
    assert fake_tables == []


def test_empty_run_dir_is_noop(tmp_path, monkeypatch, fake_tables):
    """A run directory with no *_summary.log files publishes nothing."""
    monkeypatch.setattr(transform, "VALUATIONS_LOGS_DIR", tmp_path)
    (tmp_path / "20260520-213144").mkdir()
    transform.publish_valuations_logs.fn()
    assert fake_tables == []


def test_rows_under_returns_empty_for_absent_marker():
    """Parser tolerates a section that isn't present in the log."""
    assert transform._rows_under(["just one line"], "CONVERGENCE", 7) == []
