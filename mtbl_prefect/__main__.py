"""CLI dispatcher for `python -m mtbl_prefect <command>`."""

from __future__ import annotations

import _thread
import sys
import threading
from datetime import datetime

import click

from mtbl_prefect.flows.full_pipeline import full_pipeline


def _watch_stdin_for_eof() -> None:
    """Daemon thread: on stdin EOF (Ctrl+D from attached TTY), interrupt main.

    `_thread.interrupt_main()` raises KeyboardInterrupt in the main thread,
    which Prefect catches and transitions the flow to Crashed cleanly — hooks
    fire, container exits gracefully. Compare with os._exit() which kills the
    process so abruptly Prefect's run state is left orphaned in the DB.

    Only useful for interactive `docker compose run` invocations. When the
    runner is fired by launchd (no TTY attached), this thread is never started.
    """
    try:
        while sys.stdin.readline():
            pass
    except Exception:
        pass
    print(
        "\n[mtbl-prefect] stdin EOF (Ctrl+D) received — interrupting flow",
        file=sys.stderr,
    )
    _thread.interrupt_main()


@click.group()
def cli() -> None:
    """MTBL Prefect orchestrator."""


@cli.command("full-pipeline")
@click.option("--year", type=int, default=None, help="League year (defaults to current)")
@click.option(
    "--preseason",
    is_flag=True,
    default=False,
    help="Preseason mode: Savant uses year-1 while ESPN/Fangraphs use --year",
)
def run_full_pipeline(year: int | None, preseason: bool) -> None:
    """Run the full ETL pipeline: extract -> transform -> load."""
    if year is None:
        year = datetime.now().year
    full_pipeline(year=year, preseason=preseason)


def main() -> None:
    if sys.stdin.isatty():
        threading.Thread(target=_watch_stdin_for_eof, daemon=True).start()
    cli()


if __name__ == "__main__":
    main()
