"""Extract subflow: three sources fanned out in parallel.

ESPN runs two CLI subcommands sequentially within one task; Fangraphs and
Savant are each a single CLI call. Savant's --season may differ from --year
in preseason mode (handled at the full_pipeline level).
"""

from __future__ import annotations

from prefect import flow, task

from mtbl_prefect.config import EXTRACT_OUTPUT_DIR
from mtbl_prefect.tasks import shell
from mtbl_prefect.tasks.shell import cli_task


@task(
    name="espn-api-extractor",
    retries=3,
    retry_delay_seconds=[30, 120, 600],
    log_prints=True,
)
def espn_api_extractor(year: int) -> None:
    shell.run_uv_cli(
        "_extract/ESPN_API_Extractor",
        "espn",
        "players-extract",
        "--year",
        str(year),
        "--output-dir",
        str(EXTRACT_OUTPUT_DIR),
        "--force-full-extraction",
    )
    shell.run_uv_cli(
        "_extract/ESPN_API_Extractor",
        "espn",
        "league-extract",
        "--year",
        str(year),
        "--output-dir",
        str(EXTRACT_OUTPUT_DIR),
    )


# Fangraphs CLI no longer has mode flags. Every invocation pulls all three
# projection slots (projections, projs_updated, ros) — see commit c7cfa2e
# in the Fangraphs_API_Extractor repo. Mode-conditional flag plumbing
# (--winter-meetings, --predraft, --ros) has been removed; year-round
# default invocation is correct.
fangraphs_extractor = cli_task(
    "fangraphs-api-extractor",
    project_dir="_extract/Fangraphs_API_Extractor",
    command=[
        "fangraphs-api-extractor",
        "--year",
        "{year}",
        "--output-dir",
        str(EXTRACT_OUTPUT_DIR),
    ],
    retries=3,
    retry_delay_seconds=[30, 120, 600],
)


savant_extractor = cli_task(
    "savant-api-extractor",
    project_dir="_extract/Savant_API_Extractor",
    command=[
        "savant-extract",
        "--season",
        "{season}",
        "--output-dir",
        str(EXTRACT_OUTPUT_DIR),
    ],
    retries=3,
    retry_delay_seconds=[30, 120, 600],
)


@flow(name="extract", log_prints=True)
def extract(year: int, season: int) -> None:
    """Run all three extractors concurrently. Any failure fails the subflow.

    `season` differs from `year` only in preseason mode (year - 1 for Savant);
    full_pipeline does that derivation upstream. Fangraphs no longer needs to
    know about preseason mode — its CLI handles slot selection internally.
    """
    futures = [
        espn_api_extractor.submit(year),
        fangraphs_extractor.submit(year=year),
        savant_extractor.submit(season=season),
    ]
    for f in futures:
        f.result()
