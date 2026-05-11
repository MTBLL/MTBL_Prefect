"""Shell-task factory for wrapping `uv run --directory <project> <cli>` invocations as Prefect tasks."""

from __future__ import annotations

import os
import subprocess
import sys
from collections.abc import Callable
from typing import Any

from prefect import task

from mtbl_prefect.config import REPO_ROOT


# Substrings in stderr that suggest the failure is transient (network / API /
# rate-limit) and worth retrying with backoff. Build failures, validation
# errors, and Python tracebacks are deliberately not on this list — backoff
# does not heal a missing dependency or a broken arg.
TRANSIENT_PATTERNS: tuple[str, ...] = (
    "Connection refused",
    "Connection reset",
    "Connection timed out",
    "Read timed out",
    "ReadTimeoutError",
    "ConnectionError",
    "RemoteDisconnected",
    "ProtocolError",
    "HTTPError",
    "httpx.HTTPError",
    "429 Too Many Requests",
    "502 Bad Gateway",
    "503 Service Unavailable",
    "504 Gateway Timeout",
    "rate limit",
    "Rate limit",
    "TimeoutError",
    "TemporaryFailure",
)

# Marker injected into RuntimeError messages so retry_condition_fn can
# differentiate transient (worth retrying) from permanent (don't retry).
_RETRYABLE_MARKER = "[retryable]"


def _build_env(project_dir: str) -> dict[str, str]:
    env = os.environ.copy()
    env.pop("VIRTUAL_ENV", None)
    venv_root = env.get("MTBL_VENV_ROOT")
    if venv_root:
        project_name = project_dir.strip("/").replace("/", "_")
        env["UV_PROJECT_ENVIRONMENT"] = f"{venv_root}/{project_name}"
    return env


def _build_cmd(project_dir: str, args: tuple[str, ...]) -> list[str]:
    extra: list[str] = []
    if os.environ.get("MTBL_VENV_ROOT"):
        extra.append("--frozen")
    return ["uv", "run", *extra, "--directory", str(REPO_ROOT / project_dir), *args]


def _looks_transient(stderr: str) -> bool:
    return any(p in stderr for p in TRANSIENT_PATTERNS)


def _retry_only_transient(task, task_run, state) -> bool:
    """Prefect retry_condition_fn: retry only when the failure looked transient.

    See TRANSIENT_PATTERNS for what qualifies. Build failures, validation
    errors, missing files, etc. fail immediately without burning the retry
    budget on causes that won't self-heal.
    """
    return _RETRYABLE_MARKER in (state.message or "")


def run_uv_cli(project_dir: str, *args: str, allow_exit_code_1: bool = False) -> None:
    """Run `uv run --directory <REPO_ROOT/project_dir> <args>` as a subprocess.

    Stderr is captured (then echoed) so we can classify the failure mode.
    Stdout streams through to the parent so progress is visible in real time.
    """
    full_cmd = _build_cmd(project_dir, args)
    env = _build_env(project_dir)
    print(f"$ {' '.join(full_cmd)}")
    result = subprocess.run(
        full_cmd, env=env, check=False, stderr=subprocess.PIPE, text=True
    )
    if result.stderr:
        print(result.stderr, end="", file=sys.stderr)

    if result.returncode == 0:
        return
    if allow_exit_code_1 and result.returncode == 1:
        print("(treating exit code 1 as success — known CLI quirk)")
        return

    cmd_str = " ".join(full_cmd)
    if _looks_transient(result.stderr or ""):
        raise RuntimeError(
            f"{_RETRYABLE_MARKER} command failed with exit code "
            f"{result.returncode}: {cmd_str}"
        )
    raise RuntimeError(
        f"command failed with exit code {result.returncode}: {cmd_str}"
    )


def cli_task(
    name: str,
    *,
    project_dir: str,
    command: list[str],
    retries: int = 0,
    retry_delay_seconds: list[int] | int = 0,
    allow_exit_code_1: bool = False,
) -> Callable[..., None]:
    """Build a Prefect @task wrapping a single uv-run CLI invocation.

    Placeholders in `command` use Python str.format syntax and are interpolated
    from kwargs at call time, e.g. command=["--year", "{year}"] then task(year=2026).

    Retries fire only for transient (network/HTTP) failures — see
    _retry_only_transient. Permanent failures (build errors, validation,
    missing files) skip retries and fail the task immediately.

    When allow_exit_code_1=True, exit code 1 is treated as success — workaround for
    the universe-trx CLI bug; see Notion TDD §6.6.
    """

    @task(
        name=name,
        retries=retries,
        retry_delay_seconds=retry_delay_seconds,
        retry_condition_fn=_retry_only_transient,
        log_prints=True,
    )
    def _run(**kwargs: Any) -> None:
        resolved = [arg.format(**kwargs) if "{" in arg else arg for arg in command]
        run_uv_cli(project_dir, *resolved, allow_exit_code_1=allow_exit_code_1)

    return _run
