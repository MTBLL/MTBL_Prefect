"""Unit tests for the cli_task factory and run_uv_cli helper."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from mtbl_prefect.tasks import shell


def _fake_popen(returncode: int, output: str = ""):
    """Build a fake `subprocess.Popen` class.

    The fake streams `output` line by line via `.stdout` and exits with
    `returncode` from `.wait()`. Construction args are recorded on the class
    so tests can assert the command and environment passed to Popen.
    """

    class _FakePopen:
        last_cmd: list[str] | None = None
        last_kwargs: dict | None = None

        def __init__(self, cmd, **kwargs):
            _FakePopen.last_cmd = cmd
            _FakePopen.last_kwargs = kwargs
            self.stdout = iter(output.splitlines(keepends=True))

        def wait(self):
            return returncode

    return _FakePopen


@pytest.fixture(autouse=True)
def _isolate_env(monkeypatch):
    """Default to host mode for every test; container-mode tests opt in."""
    monkeypatch.delenv("MTBL_VENV_ROOT", raising=False)
    monkeypatch.delenv("VIRTUAL_ENV", raising=False)


def test_run_uv_cli_success(monkeypatch):
    fake = _fake_popen(0)
    monkeypatch.setattr(shell.subprocess, "Popen", fake)
    shell.run_uv_cli("_extract/X", "fakecli", "--year", "2026")
    assert fake.last_cmd[:4] == ["uv", "run", "--directory", str(shell.REPO_ROOT / "_extract/X")]
    assert "--year" in fake.last_cmd


def test_run_uv_cli_streams_subprocess_output(monkeypatch, capsys):
    """Subprocess stdout is re-printed line by line so log_prints captures it."""
    monkeypatch.setattr(
        shell.subprocess, "Popen", _fake_popen(0, output="hello\nworld\n")
    )
    shell.run_uv_cli("_extract/X", "fakecli")
    captured = capsys.readouterr().out
    assert "hello" in captured and "world" in captured


def test_run_uv_cli_raises_on_nonzero(monkeypatch):
    monkeypatch.setattr(shell.subprocess, "Popen", _fake_popen(2))
    with pytest.raises(RuntimeError, match="exit code 2"):
        shell.run_uv_cli("_extract/X", "fakecli")


def test_run_uv_cli_rejects_exit_1(monkeypatch):
    """Exit code 1 is a real failure — no more universe-trx whitelist (MTBL-153)."""
    monkeypatch.setattr(shell.subprocess, "Popen", _fake_popen(1))
    with pytest.raises(RuntimeError, match="exit code 1"):
        shell.run_uv_cli("_extract/X", "fakecli")


def test_cli_task_interpolates_kwargs(monkeypatch):
    fake = _fake_popen(0)
    monkeypatch.setattr(shell.subprocess, "Popen", fake)
    t = shell.cli_task(
        "test",
        project_dir="_extract/X",
        command=["fakecli", "--year", "{year}"],
    )
    t.fn(year=2026)
    assert "2026" in fake.last_cmd


# Container-mode behavior

def test_container_mode_inserts_frozen_and_sets_uv_project_environment(monkeypatch):
    monkeypatch.setenv("MTBL_VENV_ROOT", "/opt/uv-envs")
    fake = _fake_popen(0)
    monkeypatch.setattr(shell.subprocess, "Popen", fake)
    shell.run_uv_cli("_extract/Savant_API_Extractor", "savant-extract", "--season", "2026")
    assert fake.last_cmd[:5] == [
        "uv",
        "run",
        "--frozen",
        "--directory",
        str(shell.REPO_ROOT / "_extract/Savant_API_Extractor"),
    ]
    assert fake.last_kwargs["env"]["UV_PROJECT_ENVIRONMENT"] == "/opt/uv-envs/_extract_Savant_API_Extractor"


def test_container_mode_strips_leaked_virtual_env(monkeypatch):
    monkeypatch.setenv("MTBL_VENV_ROOT", "/opt/uv-envs")
    monkeypatch.setenv("VIRTUAL_ENV", "/app/.venv")
    fake = _fake_popen(0)
    monkeypatch.setattr(shell.subprocess, "Popen", fake)
    shell.run_uv_cli("_extract/X", "fakecli")
    assert "VIRTUAL_ENV" not in fake.last_kwargs["env"]


def test_host_mode_strips_virtual_env_too(monkeypatch):
    monkeypatch.setenv("VIRTUAL_ENV", "/some/host/venv")
    fake = _fake_popen(0)
    monkeypatch.setattr(shell.subprocess, "Popen", fake)
    shell.run_uv_cli("_extract/X", "fakecli")
    assert "VIRTUAL_ENV" not in fake.last_kwargs["env"]


def test_host_mode_does_not_add_frozen_or_uv_project_environment(monkeypatch):
    fake = _fake_popen(0)
    monkeypatch.setattr(shell.subprocess, "Popen", fake)
    shell.run_uv_cli("_extract/X", "fakecli")
    assert "--frozen" not in fake.last_cmd
    assert "UV_PROJECT_ENVIRONMENT" not in fake.last_kwargs["env"]


# Retry classification: transient vs permanent
# Output is the merged stdout+stderr stream — the classifier scans all of it.

@pytest.mark.parametrize("output_excerpt", [
    "httpx.HTTPError: 503 Service Unavailable",
    "ConnectionError: Connection refused by host",
    "TimeoutError: Read timed out",
    "Got 429 Too Many Requests, retrying",
    "RemoteDisconnected: Remote end closed connection without response",
    # psql / libpq disconnect signatures — exercised by the Neon sync step.
    '❌ psql upload failed: psql: error: connection to server at "ep-dawn-leaf-aew9d2fm.c-2.us-east-2.aws.neon.tech" (3.137.42.68), port 5432 failed: SSL error: unexpected eof while reading',
    "psql: error: SSL SYSCALL error: EOF detected",
    "psql: error: server closed the connection unexpectedly\n\tThis probably means the server terminated abnormally",
    "psql: error: could not connect to server: Connection timed out",
])
def test_transient_output_produces_retryable_marker(monkeypatch, output_excerpt):
    monkeypatch.setattr(shell.subprocess, "Popen", _fake_popen(2, output=output_excerpt))
    with pytest.raises(RuntimeError, match=r"\[retryable\]"):
        shell.run_uv_cli("_extract/X", "fakecli")


@pytest.mark.parametrize("output_excerpt", [
    "Failed to build `player-universe-load`",
    "Invalid value for '--batters-file': Path does not exist",
    "ValueError: Resources path does not exist",
    "ModuleNotFoundError: No module named 'foo'",
    "",
])
def test_permanent_output_omits_retryable_marker(monkeypatch, output_excerpt):
    monkeypatch.setattr(shell.subprocess, "Popen", _fake_popen(2, output=output_excerpt))
    with pytest.raises(RuntimeError) as exc_info:
        shell.run_uv_cli("_extract/X", "fakecli")
    assert "[retryable]" not in str(exc_info.value)


def test_retry_only_transient_returns_true_for_marked_message():
    state = SimpleNamespace(message="[retryable] command failed with exit code 2: ...")
    assert shell._retry_only_transient(None, None, state) is True


def test_retry_only_transient_returns_false_for_unmarked_message():
    state = SimpleNamespace(message="command failed with exit code 2: ...")
    assert shell._retry_only_transient(None, None, state) is False


def test_retry_only_transient_handles_none_message():
    state = SimpleNamespace(message=None)
    assert shell._retry_only_transient(None, None, state) is False
