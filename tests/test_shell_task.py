"""Unit tests for the cli_task factory and run_uv_cli helper."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from mtbl_prefect.tasks import shell


def _fake_run(returncode: int):
    def _run(cmd, **kwargs):
        _run.last_cmd = cmd
        return SimpleNamespace(returncode=returncode)
    _run.last_cmd = None
    return _run


def test_run_uv_cli_success(monkeypatch):
    fake = _fake_run(0)
    monkeypatch.setattr(shell.subprocess, "run", fake)
    shell.run_uv_cli("_extract/X", "fakecli", "--year", "2026")
    assert fake.last_cmd[:4] == ["uv", "run", "--directory", str(shell.REPO_ROOT / "_extract/X")]
    assert "--year" in fake.last_cmd


def test_run_uv_cli_raises_on_nonzero(monkeypatch):
    monkeypatch.setattr(shell.subprocess, "run", _fake_run(2))
    with pytest.raises(RuntimeError, match="exit code 2"):
        shell.run_uv_cli("_extract/X", "fakecli")


def test_run_uv_cli_allows_exit_1_when_whitelisted(monkeypatch):
    monkeypatch.setattr(shell.subprocess, "run", _fake_run(1))
    shell.run_uv_cli("_transform/Player_Universe_Trx", "universe-trx", allow_exit_code_1=True)


def test_run_uv_cli_rejects_exit_1_by_default(monkeypatch):
    monkeypatch.setattr(shell.subprocess, "run", _fake_run(1))
    with pytest.raises(RuntimeError, match="exit code 1"):
        shell.run_uv_cli("_extract/X", "fakecli")


def test_cli_task_interpolates_kwargs(monkeypatch):
    fake = _fake_run(0)
    monkeypatch.setattr(shell.subprocess, "run", fake)
    t = shell.cli_task(
        "test",
        project_dir="_extract/X",
        command=["fakecli", "--year", "{year}"],
    )
    t.fn(year=2026)
    assert "2026" in fake.last_cmd


def test_cli_task_passes_through_allow_exit_code_1(monkeypatch):
    monkeypatch.setattr(shell.subprocess, "run", _fake_run(1))
    t = shell.cli_task(
        "test",
        project_dir="_transform/Player_Universe_Trx",
        command=["universe-trx"],
        allow_exit_code_1=True,
    )
    t.fn()


def test_cli_task_rejects_other_failures_when_whitelisted(monkeypatch):
    monkeypatch.setattr(shell.subprocess, "run", _fake_run(2))
    t = shell.cli_task(
        "test",
        project_dir="_extract/X",
        command=["fakecli"],
        allow_exit_code_1=True,
    )
    with pytest.raises(RuntimeError, match="exit code 2"):
        t.fn()
