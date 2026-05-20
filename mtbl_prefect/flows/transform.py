"""Transform subflow: universe-trx then mtbl-valuations, sequential.

After mtbl-valuations runs, its per-run iteration summary logs are attached
to the flow run as a Prefect artifact so each run's convergence / warnings
trace is reviewable in the UI alongside the logs.
"""

from __future__ import annotations

from prefect import flow, task
from prefect.artifacts import create_markdown_artifact

from mtbl_prefect.config import REPO_ROOT
from mtbl_prefect.tasks.shell import cli_task


# mtbl-valuations writes a timestamped logs/<YYYYMMDD-HHMMSS>/ directory per
# run, each holding one <source>_summary.log per valuation source (current,
# preseason, ros, synthetic, updated). Path is hardcoded — it is a fixed,
# known location inside the sub-project repo.
VALUATIONS_LOGS_DIR = REPO_ROOT / "_transform/MTBL_Valuations/logs"


player_universe_trx = cli_task(
    "player-universe-trx",
    project_dir="_transform/Player_Universe_Trx",
    command=["universe-trx", "--year", "{year}"],
    retries=1,
    retry_delay_seconds=30,
)


mtbl_valuations = cli_task(
    "mtbl-valuations",
    project_dir="_transform/MTBL_Valuations",
    command=["mtbl-valuations", "hydrate"],
    retries=1,
    retry_delay_seconds=30,
)


@task(name="publish-valuations-iteration-logs", log_prints=True)
def publish_valuations_logs() -> None:
    """Attach the most recent mtbl-valuations iteration summaries as an artifact.

    Reads every `*_summary.log` from the newest `logs/<timestamp>/` directory
    and pins them to the flow run as one markdown artifact, keyed so each run
    forms a reviewable history in the UI.

    Best-effort: a missing directory or read error is logged and swallowed —
    a logging hiccup must never fail the transform.
    """
    try:
        run_dirs = [d for d in VALUATIONS_LOGS_DIR.iterdir() if d.is_dir()]
        if not run_dirs:
            print(f"No mtbl-valuations log runs under {VALUATIONS_LOGS_DIR}")
            return

        latest = max(run_dirs, key=lambda d: d.stat().st_mtime)
        summaries = sorted(latest.glob("*_summary.log"))
        if not summaries:
            print(f"No *_summary.log files in {latest}")
            return

        sections = [
            f"# mtbl-valuations iteration logs\n\n_Run directory: `{latest.name}`_\n"
        ]
        for summary in summaries:
            sections.append(f"## {summary.stem}\n\n```\n{summary.read_text()}\n```\n")

        create_markdown_artifact(
            key="mtbl-valuations-iteration-logs",
            markdown="\n".join(sections),
            description=f"Convergence / warnings summaries — run {latest.name}",
        )
        print(
            f"Published {len(summaries)} valuation summary log(s) "
            f"from {latest.name} as a Prefect artifact"
        )
    except Exception as exc:  # noqa: BLE001 — logging is strictly best-effort
        print(f"WARNING: could not publish valuations iteration logs: {exc}")


@flow(name="transform", log_prints=True)
def transform(year: int) -> None:
    player_universe_trx(year=year)
    mtbl_valuations()
    publish_valuations_logs()
