"""Tests for publish_valuations_logs — the iteration-log artifact task."""

from __future__ import annotations

import os

import pytest

from mtbl_prefect.flows import transform


@pytest.fixture
def fake_artifacts(monkeypatch):
    """Capture create_markdown_artifact calls instead of hitting Prefect."""
    calls: list[dict] = []
    monkeypatch.setattr(
        transform, "create_markdown_artifact", lambda **kwargs: calls.append(kwargs)
    )
    return calls


def _make_run_dir(logs_root, name, sources, mtime, *, with_detail=True):
    """Create a run dir: one <source>_summary.log + a <source>/ detail dir."""
    run_dir = logs_root / name
    run_dir.mkdir(parents=True)
    for source in sources:
        (run_dir / f"{source}_summary.log").write_text(f"{source} summary body")
        if with_detail:
            detail = run_dir / source
            detail.mkdir()
            (detail / "C.log").write_text(f"{source} C iteration detail")
            (detail / "OF.log").write_text(f"{source} OF iteration detail")
    os.utime(run_dir, (mtime, mtime))
    return run_dir


def test_publishes_one_artifact_per_source(tmp_path, monkeypatch, fake_artifacts):
    monkeypatch.setattr(transform, "VALUATIONS_LOGS_DIR", tmp_path)
    # An older run and a newer run — only the newest is published.
    _make_run_dir(tmp_path, "20260101-000000", ["current"], mtime=1000)
    _make_run_dir(tmp_path, "20260520-215321", ["current", "ros"], mtime=2000)

    transform.publish_valuations_logs.fn()

    by_key = {c["key"]: c for c in fake_artifacts}
    assert set(by_key) == {
        "mtbl-valuations-logs-current",
        "mtbl-valuations-logs-ros",
    }

    cur = by_key["mtbl-valuations-logs-current"]["markdown"]
    # Summary + per-position detail are all present, each in a <pre> block.
    assert "current summary body" in cur
    assert "current C iteration detail" in cur
    assert "current OF iteration detail" in cur
    assert "<pre style=" in cur
    assert "## C" in cur and "## OF" in cur
    assert "20260520-215321" in by_key["mtbl-valuations-logs-current"]["description"]


def test_log_content_is_html_escaped(tmp_path, monkeypatch, fake_artifacts):
    """Raw `<`, `>`, `&` in logs must be escaped so they can't break markup."""
    monkeypatch.setattr(transform, "VALUATIONS_LOGS_DIR", tmp_path)
    run = tmp_path / "20260520-215321"
    run.mkdir()
    (run / "current_summary.log").write_text("top RLP $12 > rostered $11 & <b>bold</b>")
    os.utime(run, (2000, 2000))

    transform.publish_valuations_logs.fn()

    md = fake_artifacts[0]["markdown"]
    assert "&gt;" in md and "&amp;" in md and "&lt;b&gt;" in md
    assert "<b>bold</b>" not in md  # the literal tag must not survive


def test_no_logs_dir_is_noop(tmp_path, monkeypatch, fake_artifacts):
    """A missing logs directory is logged and swallowed — never raises."""
    monkeypatch.setattr(transform, "VALUATIONS_LOGS_DIR", tmp_path / "missing")
    transform.publish_valuations_logs.fn()
    assert fake_artifacts == []


def test_empty_run_dir_is_noop(tmp_path, monkeypatch, fake_artifacts):
    """A run directory with no *_summary.log files publishes nothing."""
    monkeypatch.setattr(transform, "VALUATIONS_LOGS_DIR", tmp_path)
    (tmp_path / "20260520-215321").mkdir()
    transform.publish_valuations_logs.fn()
    assert fake_artifacts == []


def test_summary_without_detail_dir_still_publishes(tmp_path, monkeypatch, fake_artifacts):
    """A source with a summary but no per-position detail dir is fine."""
    monkeypatch.setattr(transform, "VALUATIONS_LOGS_DIR", tmp_path)
    _make_run_dir(tmp_path, "20260520-215321", ["current"], mtime=2000, with_detail=False)
    transform.publish_valuations_logs.fn()
    assert len(fake_artifacts) == 1
    assert "current summary body" in fake_artifacts[0]["markdown"]
