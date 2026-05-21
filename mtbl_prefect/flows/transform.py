"""Transform subflow: universe-trx then mtbl-valuations, sequential.

After mtbl-valuations runs, its per-run iteration logs (the source summaries
plus the full per-position iteration detail) are attached to the flow run as
one markdown artifact per valuation source, so each run's complete trace is
reviewable in the UI.
"""

from __future__ import annotations

import html

from prefect import flow, task
from prefect.artifacts import create_markdown_artifact

from mtbl_prefect.config import REPO_ROOT
from mtbl_prefect.tasks.shell import cli_task


# mtbl-valuations writes a timestamped logs/<YYYYMMDD-HHMMSS>/ directory per
# run: one <source>_summary.log per valuation source (current, preseason, ros,
# synthetic, updated) plus a <source>/ subdirectory of per-position iteration
# detail logs (C.log, 1B.log, ... SP.log, RP.log). Path is hardcoded — it is a
# fixed, known location inside the sub-project repo.
VALUATIONS_LOGS_DIR = REPO_ROOT / "_transform/MTBL_Valuations/logs"

# Inline-styled <pre>. A raw HTML <pre> element renders with browser-default
# (or these) styles — it does NOT pick up Prefect UI v2's markdown code-block
# CSS, which renders light-on-light. Markdown ``` fences hit that broken CSS;
# a real <pre> element sidesteps it.
_PRE_STYLE = (
    "background:#1e1e2e;color:#e4e4e7;padding:12px;border-radius:6px;"
    "overflow-x:auto;font-size:12px;line-height:1.45;"
)


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


def _pre_block(text: str) -> str:
    """Wrap raw log text in an inline-styled HTML <pre>.

    Content is HTML-escaped — the warning logs contain `>` and player names
    that would otherwise break the markup. Using a real <pre> element rather
    than a markdown ``` fence avoids Prefect UI v2's broken code-block theme.
    """
    return f'<pre style="{_PRE_STYLE}">{html.escape(text)}</pre>'


@task(name="publish-valuations-iteration-logs", log_prints=True)
def publish_valuations_logs() -> None:
    """Publish the latest mtbl-valuations run's logs as per-source artifacts.

    For each valuation source, one markdown artifact carries the source
    summary plus every per-position iteration detail log, each embedded in a
    raw <pre> block. One artifact per source keeps each well under any size
    limit and groups them in the UI.

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

        published = 0
        for summary in summaries:
            source = summary.stem.removesuffix("_summary")
            sections = [
                f"# mtbl-valuations logs — {source}\n",
                f"_Run: `{latest.name}`_\n",
                "## Summary\n",
                _pre_block(summary.read_text()),
            ]
            # Per-position iteration detail lives in a sibling directory named
            # for the source (e.g. logs/<run>/current/C.log).
            detail_dir = latest / source
            if detail_dir.is_dir():
                for pos_log in sorted(detail_dir.glob("*.log")):
                    sections.append(f"\n## {pos_log.stem}\n")
                    sections.append(_pre_block(pos_log.read_text()))

            create_markdown_artifact(
                key=f"mtbl-valuations-logs-{source}",
                markdown="\n".join(sections),
                description=f"{source} iteration logs — run {latest.name}",
            )
            published += 1

        print(
            f"Published {published} per-source iteration-log artifact(s) "
            f"from {latest.name}"
        )
    except Exception as exc:  # noqa: BLE001 — logging is strictly best-effort
        print(f"WARNING: could not publish valuations iteration logs: {exc}")


@flow(name="transform", log_prints=True)
def transform(year: int) -> None:
    player_universe_trx(year=year)
    mtbl_valuations()
    publish_valuations_logs()
