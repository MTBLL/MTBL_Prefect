"""Tests for publish_valuations_logs — the iteration-log artifact task."""

from __future__ import annotations

import os

import pytest

from mtbl_prefect.flows import transform


@pytest.fixture
def fake_artifact(monkeypatch):
    """Capture create_markdown_artifact calls instead of hitting Prefect."""
    calls: list[dict] = []
    monkeypatch.setattr(
        transform, "create_markdown_artifact", lambda **kwargs: calls.append(kwargs)
    )
    return calls


def _make_run_dir(logs_root, name, summaries, mtime):
    run_dir = logs_root / name
    run_dir.mkdir(parents=True)
    for source, text in summaries.items():
        (run_dir / f"{source}_summary.log").write_text(text)
    os.utime(run_dir, (mtime, mtime))
    return run_dir


def test_publishes_latest_run_summaries(tmp_path, monkeypatch, fake_artifact):
    monkeypatch.setattr(transform, "VALUATIONS_LOGS_DIR", tmp_path)
    # An older run and a newer run — only the newest should be published.
    _make_run_dir(tmp_path, "20260101-000000", {"current": "STALE"}, mtime=1000)
    _make_run_dir(
        tmp_path,
        "20260520-061646",
        {"current": "current convergence trace", "ros": "ros convergence trace"},
        mtime=2000,
    )

    transform.publish_valuations_logs.fn()

    assert len(fake_artifact) == 1
    art = fake_artifact[0]
    assert art["key"] == "mtbl-valuations-iteration-logs"
    # Fences are tagged `text` so the UI renderer does not syntax-highlight them.
    assert "```text" in art["markdown"]
    assert "current convergence trace" in art["markdown"]
    assert "ros convergence trace" in art["markdown"]
    assert "STALE" not in art["markdown"]
    assert "20260520-061646" in art["markdown"]


def test_no_logs_dir_is_noop(tmp_path, monkeypatch, fake_artifact):
    """A missing logs directory is logged and swallowed — never raises."""
    monkeypatch.setattr(transform, "VALUATIONS_LOGS_DIR", tmp_path / "missing")
    transform.publish_valuations_logs.fn()
    assert fake_artifact == []


def test_empty_run_dir_is_noop(tmp_path, monkeypatch, fake_artifact):
    """A run directory with no *_summary.log files publishes nothing."""
    monkeypatch.setattr(transform, "VALUATIONS_LOGS_DIR", tmp_path)
    (tmp_path / "20260520-061646").mkdir()
    transform.publish_valuations_logs.fn()
    assert fake_artifact == []
