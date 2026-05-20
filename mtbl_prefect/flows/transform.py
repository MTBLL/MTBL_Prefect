"""Transform subflow: universe-trx then mtbl-valuations, sequential.

After mtbl-valuations runs, its per-run iteration summary logs are parsed and
attached to the flow run as native Prefect table artifacts (convergence +
warnings) so each run's trace is reviewable in the UI.
"""

from __future__ import annotations

from prefect import flow, task
from prefect.artifacts import create_table_artifact

from mtbl_prefect.config import REPO_ROOT
from mtbl_prefect.tasks.shell import cli_task


# mtbl-valuations writes a timestamped logs/<YYYYMMDD-HHMMSS>/ directory per
# run, each holding one <source>_summary.log per valuation source (current,
# preseason, ros, synthetic, updated). Path is hardcoded — it is a fixed,
# known location inside the sub-project repo.
VALUATIONS_LOGS_DIR = REPO_ROOT / "_transform/MTBL_Valuations/logs"

# Column layout of the two whitespace-aligned tables in every *_summary.log.
# The last warnings column (msg) is free text, so it is split off last.
CONVERGENCE_COLS = (
    "source", "phase", "pos", "iters_run", "converged", "oscillating", "best_iter"
)
WARNINGS_COLS = ("source", "phase", "pos", "iter", "kind", "msg")


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


def _rows_under(lines: list[str], marker: str, ncols: int) -> list[list[str]]:
    """Parse the whitespace-aligned table under `marker` in a summary log.

    Layout is fixed: a marker line (e.g. ``CONVERGENCE`` or ``WARNINGS (44)``),
    a dashed rule, a column-header line, then data rows up to the first blank
    line. Each row is split into exactly `ncols` fields — the final field
    keeps any internal spaces (the warnings ``msg`` column is free text).
    Returns [] if the marker is absent.
    """
    start = None
    for i, line in enumerate(lines):
        text = line.strip()
        if text == marker or text.startswith(marker + " "):
            start = i
            break
    if start is None:
        return []

    rows: list[list[str]] = []
    # +3 skips the marker line, the dashed rule, and the column header.
    for line in lines[start + 3:]:
        if not line.strip():
            break
        fields = line.split(maxsplit=ncols - 1)
        if len(fields) < ncols:
            fields += [""] * (ncols - len(fields))
        rows.append(fields)
    return rows


@task(name="publish-valuations-iteration-logs", log_prints=True)
def publish_valuations_logs() -> None:
    """Publish the latest mtbl-valuations iteration logs as table artifacts.

    Parses the newest `logs/<timestamp>/` directory's `*_summary.log` files
    into two native Prefect table artifacts — convergence and warnings —
    pooled across all valuation sources (the `source` column distinguishes
    them). Native tables sidestep the UI's markdown code-block rendering.

    Best-effort: a missing directory or parse error is logged and swallowed —
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

        convergence: list[dict[str, str]] = []
        warnings: list[dict[str, str]] = []
        for summary in summaries:
            lines = summary.read_text().splitlines()
            for fields in _rows_under(lines, "CONVERGENCE", len(CONVERGENCE_COLS)):
                convergence.append(dict(zip(CONVERGENCE_COLS, fields)))
            for fields in _rows_under(lines, "WARNINGS", len(WARNINGS_COLS)):
                warnings.append(dict(zip(WARNINGS_COLS, fields)))

        if convergence:
            create_table_artifact(
                key="mtbl-valuations-convergence",
                table=convergence,
                description=f"Per-position convergence — run {latest.name}",
            )
        if warnings:
            create_table_artifact(
                key="mtbl-valuations-warnings",
                table=warnings,
                description=f"Iteration warnings — run {latest.name}",
            )
        print(
            f"Published convergence ({len(convergence)} rows) + "
            f"warnings ({len(warnings)} rows) from {latest.name}"
        )
    except Exception as exc:  # noqa: BLE001 — logging is strictly best-effort
        print(f"WARNING: could not publish valuations iteration logs: {exc}")


@flow(name="transform", log_prints=True)
def transform(year: int) -> None:
    player_universe_trx(year=year)
    mtbl_valuations()
    publish_valuations_logs()
