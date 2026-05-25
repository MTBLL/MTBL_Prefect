"""Tests for publish_valuations_logs — the iteration-log artifact task."""

from __future__ import annotations

import os

import pytest

from mtbl_prefect.flows import transform


_SUMMARY = """\
=== source: {source} ===
ts: 2026-05-20T21:31:45

CONVERGENCE
-----------
 source          phase  pos  iters_run  converged  oscillating  best_iter
{source}   phase3b-iter    C          5      False         True          2

WARNINGS (1)
------------
 source          phase  pos  iter                   kind                  msg
{source}   phase3b-iter    C     5   oscillation_resolved   period-2+ oscillation here
"""

# A per-position detail log: one iteration block with both tables. The
# rostered table deliberately includes a multi-word player name.
_POSITION_LOG = """\
================================================================================
PHASE: phase3b-iter   |   POS: C   |   ITER: 1   |   SOURCE: current
================================================================================
ts: 2026-05-20T21:53:22
pool_size: 47  rostered: 11  replacement: 3  below: 33
composition_hash: 2f5a9f2245

RLP / scale (rostered tier basis, replacement-tier baseline):
 cat  rostered_mean  rostered_stdev  rlp_raw_avg
   R         25.636           8.920       19.667
  HR          8.818           4.045        7.333

rostered + replacement:
 rank             name         tier  total_z  R_raw   R_z
    1         Ben Rice     ROSTERED   17.325 36.000 2.466
    2  William Contreras REPLACEMENT    4.268 23.000 1.009

below_replacement: 33 (truncated)
"""


@pytest.fixture
def fake_artifacts(monkeypatch):
    """Capture create_markdown_artifact calls instead of hitting Prefect."""
    calls: list[dict] = []
    monkeypatch.setattr(
        transform, "create_markdown_artifact", lambda **kwargs: calls.append(kwargs)
    )
    return calls


def _make_run_dir(logs_root, name, sources, mtime, *, with_detail=True):
    run_dir = logs_root / name
    run_dir.mkdir(parents=True)
    for source in sources:
        (run_dir / f"{source}_summary.log").write_text(_SUMMARY.format(source=source))
        if with_detail:
            detail = run_dir / source
            detail.mkdir()
            (detail / "C.log").write_text(_POSITION_LOG)
    os.utime(run_dir, (mtime, mtime))
    return run_dir


def test_run_dir_names_snapshots(tmp_path, monkeypatch):
    monkeypatch.setattr(transform, "VALUATIONS_LOGS_DIR", tmp_path)
    (tmp_path / "20260101-000000").mkdir()
    (tmp_path / "20260520-215321").mkdir()
    assert transform._run_dir_names() == {"20260101-000000", "20260520-215321"}


def test_run_dir_names_missing_root_is_empty(tmp_path, monkeypatch):
    monkeypatch.setattr(transform, "VALUATIONS_LOGS_DIR", tmp_path / "missing")
    assert transform._run_dir_names() == set()


def test_aligned_table_reassembles_multiword_names():
    """The rostered table's multi-word name column survives the parse."""
    header = " rank             name         tier  total_z  R_raw   R_z"
    rows = [
        "    1         Ben Rice     ROSTERED   17.325 36.000 2.466",
        "    2  William Contreras REPLACEMENT    4.268 23.000 1.009",
    ]
    md = transform._aligned_table(header, rows)
    assert "| Ben Rice |" in md
    assert "| William Contreras |" in md
    assert "| rank | name | tier | total_z | R_raw | R_z |" in md


def test_shards_artifacts_per_source_and_position(
    tmp_path, monkeypatch, fake_artifacts
):
    monkeypatch.setattr(transform, "VALUATIONS_LOGS_DIR", tmp_path)
    _make_run_dir(tmp_path, "20260520-215321", ["current", "ros"], mtime=2000)

    transform.publish_valuations_logs.fn(run_dir_names=["20260520-215321"])

    by_key = {c["key"]: c for c in fake_artifacts}
    # Per source: one summary shard + one shard per position (here just C).
    assert set(by_key) == {
        "mtbl-valuations-logs-current-summary",
        "mtbl-valuations-logs-current-c",
        "mtbl-valuations-logs-ros-summary",
        "mtbl-valuations-logs-ros-c",
    }

    summary = by_key["mtbl-valuations-logs-current-summary"]["markdown"]
    assert "### Convergence" in summary and "### Warnings" in summary
    assert "| source | phase | pos | iters_run | converged | oscillating | best_iter |" in summary
    assert "#### phase3b-iter" not in summary  # summary shard carries no detail

    pos = by_key["mtbl-valuations-logs-current-c"]["markdown"]
    assert "# mtbl-valuations — current / C" in pos
    assert "#### phase3b-iter · C · iter 1" in pos
    assert "**RLP / scale**" in pos and "**rostered + replacement**" in pos
    assert "| Ben Rice |" in pos

    # No code fences anywhere — that was the broken render path.
    for art in fake_artifacts:
        assert "```" not in art["markdown"] and "<pre" not in art["markdown"]


def test_ignores_dirs_not_from_this_run(tmp_path, monkeypatch, fake_artifacts):
    """A newer directory from an overlapping run must not be published."""
    monkeypatch.setattr(transform, "VALUATIONS_LOGS_DIR", tmp_path)
    # This run's dir (older) and a concurrent run's dir (newer by mtime).
    _make_run_dir(tmp_path, "20260520-100000", ["current"], mtime=1000)
    _make_run_dir(tmp_path, "20260520-200000", ["current"], mtime=2000)

    # Only this run's directory name is passed in.
    transform.publish_valuations_logs.fn(run_dir_names=["20260520-100000"])

    # Despite the other dir being newer, artifacts come from this run's dir.
    assert fake_artifacts
    for art in fake_artifacts:
        assert "20260520-100000" in art["description"]
        assert "20260520-200000" not in art["description"]


def test_no_run_dir_is_noop(tmp_path, monkeypatch, fake_artifacts):
    """Naming a directory that does not exist publishes nothing — never raises."""
    monkeypatch.setattr(transform, "VALUATIONS_LOGS_DIR", tmp_path)
    transform.publish_valuations_logs.fn(run_dir_names=["20260520-999999"])
    assert fake_artifacts == []


def test_empty_run_dir_is_noop(tmp_path, monkeypatch, fake_artifacts):
    """A run directory with no *_summary.log files publishes nothing."""
    monkeypatch.setattr(transform, "VALUATIONS_LOGS_DIR", tmp_path)
    (tmp_path / "20260520-215321").mkdir()
    transform.publish_valuations_logs.fn(run_dir_names=["20260520-215321"])
    assert fake_artifacts == []


def test_summary_without_detail_dir_still_publishes(tmp_path, monkeypatch, fake_artifacts):
    """A source with no per-position detail still gets its summary shard."""
    monkeypatch.setattr(transform, "VALUATIONS_LOGS_DIR", tmp_path)
    _make_run_dir(tmp_path, "20260520-215321", ["current"], mtime=2000, with_detail=False)
    transform.publish_valuations_logs.fn(run_dir_names=["20260520-215321"])
    assert len(fake_artifacts) == 1
    assert fake_artifacts[0]["key"] == "mtbl-valuations-logs-current-summary"
    assert "### Convergence" in fake_artifacts[0]["markdown"]


def test_multi_candidate_disambiguates_keys(tmp_path, monkeypatch, fake_artifacts):
    """Two run dirs in the window → publish both with run-name-suffixed keys.

    Filesystem state alone cannot tell which dir belongs to this run when
    an overlapping mtbl-valuations run also created a dir in the same
    before/after window. Picking newest-by-mtime would silently overwrite
    this run's artifacts with the other run's content. Instead publish
    every candidate under disambiguated keys (``-<rundirname>``).
    """
    monkeypatch.setattr(transform, "VALUATIONS_LOGS_DIR", tmp_path)
    # Two run dirs, both within this run's snapshot window. Newer mtime on
    # the second one is what the old max-by-mtime code would have picked.
    _make_run_dir(tmp_path, "20260520-100000", ["current"], mtime=1000)
    _make_run_dir(tmp_path, "20260520-200000", ["current"], mtime=2000)

    transform.publish_valuations_logs.fn(
        run_dir_names=["20260520-100000", "20260520-200000"]
    )

    by_key = {c["key"]: c for c in fake_artifacts}
    # Both dirs publish full artifact sets with disambiguated keys — no
    # collision between the two runs' artifacts.
    assert set(by_key) == {
        "mtbl-valuations-logs-current-summary-20260520-100000",
        "mtbl-valuations-logs-current-c-20260520-100000",
        "mtbl-valuations-logs-current-summary-20260520-200000",
        "mtbl-valuations-logs-current-c-20260520-200000",
    }
    # Stable (un-suffixed) keys are NOT used when there is ambiguity — they
    # would silently pin one run's content under the expected name.
    assert "mtbl-valuations-logs-current-summary" not in by_key
    assert "mtbl-valuations-logs-current-c" not in by_key

    # Each artifact's body refers to its own run dir, not the other one.
    art100 = by_key["mtbl-valuations-logs-current-summary-20260520-100000"]
    art200 = by_key["mtbl-valuations-logs-current-summary-20260520-200000"]
    assert "20260520-100000" in art100["markdown"]
    assert "20260520-200000" not in art100["markdown"]
    assert "20260520-200000" in art200["markdown"]
    assert "20260520-100000" not in art200["markdown"]
