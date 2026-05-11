"""Shell-task factory for wrapping `uv run --directory <project> <cli>` invocations as Prefect tasks."""

from __future__ import annotations

import subprocess
from collections.abc import Callable
from pathlib import Path
from typing import Any

from prefect import task

from mtbl_prefect.config import REPO_ROOT


def run_uv_cli(project_dir: str, *args: str, allow_exit_code_1: bool = False) -> None:
    """Run `uv run --directory <REPO_ROOT/project_dir> <args>`.

    Public helper for tasks that need more than a single CLI invocation
    (e.g. the ESPN extractor runs two subcommands sequentially within one task).
    """
    full_cmd = ["uv", "run", "--directory", str(REPO_ROOT / project_dir), *args]
    print(f"$ {' '.join(full_cmd)}")
    result = subprocess.run(full_cmd, check=False)
    if result.returncode == 0:
        return
    if allow_exit_code_1 and result.returncode == 1:
        print(f"(treating exit code 1 as success — known CLI quirk)")
        return
    raise RuntimeError(
        f"command failed with exit code {result.returncode}: {' '.join(full_cmd)}"
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

    When allow_exit_code_1=True, exit code 1 is treated as success — workaround for
    the universe-trx CLI bug; see Notion TDD §6.6.
    """

    @task(
        name=name,
        retries=retries,
        retry_delay_seconds=retry_delay_seconds,
        log_prints=True,
    )
    def _run(**kwargs: Any) -> None:
        resolved = [arg.format(**kwargs) if "{" in arg else arg for arg in command]
        run_uv_cli(project_dir, *resolved, allow_exit_code_1=allow_exit_code_1)

    return _run
