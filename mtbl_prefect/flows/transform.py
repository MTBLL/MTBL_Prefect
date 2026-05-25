"""Transform subflow: universe-trx then mtbl-valuations, sequential.

After mtbl-valuations runs, its per-run iteration logs are rendered into
markdown artifacts — one per (source, position) shard plus a per-source
summary. The logs' whitespace-aligned tables are converted to real markdown
pipe tables and the section structure to headings — markdown tables and
headings render correctly in the Prefect UI, unlike fenced code blocks.
"""

from __future__ import annotations

from pathlib import Path

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

# Tier labels in the per-position "rostered + replacement" table. They anchor
# the row parse: player names span multiple tokens, so a plain split breaks —
# the single ALL-CAPS tier token marks where the name ends.
_TIERS = {"ROSTERED", "REPLACEMENT", "BELOW_REPLACEMENT"}

# Column layouts of the two tables in every *_summary.log. The warnings `msg`
# column is free text, so it is the only one allowed to keep internal spaces.
_CONVERGENCE_COLS = (
    "source", "phase", "pos", "iters_run", "converged", "oscillating", "best_iter"
)
_WARNINGS_COLS = ("source", "phase", "pos", "iter", "kind", "msg")


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


# --------------------------------------------------------------------------
# markdown table helpers
# --------------------------------------------------------------------------

def _cell(value: str) -> str:
    """Escape a value for a markdown table cell (a literal | breaks the row)."""
    return value.replace("|", "\\|").strip()


def _pipe_table(headers: list[str], rows: list[list[str]]) -> str:
    """Render headers + rows as a markdown pipe table."""
    head = "| " + " | ".join(_cell(h) for h in headers) + " |"
    rule = "| " + " | ".join("---" for _ in headers) + " |"
    body = "\n".join(
        "| " + " | ".join(_cell(c) for c in row) + " |" for row in rows
    )
    return f"{head}\n{rule}\n{body}" if rows else f"{head}\n{rule}"


def _aligned_table(header_line: str, data_lines: list[str]) -> str:
    """Convert a whitespace-aligned text table to a markdown pipe table.

    Most rows split cleanly on whitespace into the header's column count. The
    "rostered + replacement" table is the exception — its `name` column holds
    multi-word player names — so rows that over-split are reassembled around
    the tier token. Rows that still don't fit the column count are dropped.
    """
    headers = header_line.split()
    ncols = len(headers)
    rows: list[list[str]] = []
    for line in data_lines:
        tokens = line.split()
        if len(tokens) == ncols:
            rows.append(tokens)
            continue
        # Reassemble a multi-word name around the tier token.
        tier_idx = next(
            (i for i, t in enumerate(tokens) if t in _TIERS), None
        )
        if tier_idx is not None and tier_idx >= 2:
            row = [tokens[0], " ".join(tokens[1:tier_idx]), *tokens[tier_idx:]]
            if len(row) == ncols:
                rows.append(row)
    return _pipe_table(headers, rows)


# --------------------------------------------------------------------------
# log parsers
# --------------------------------------------------------------------------

def _summary_section(lines: list[str], marker: str, cols: tuple[str, ...]) -> str:
    """Render the table under `marker` (CONVERGENCE / WARNINGS) as markdown.

    Layout is fixed: marker line, a dashed rule, a column header, then data
    rows up to the first blank line. Returns "" if the marker is absent.
    """
    start = next(
        (i for i, line in enumerate(lines)
         if line.strip() == marker or line.strip().startswith(marker + " ")),
        None,
    )
    if start is None:
        return ""
    ncols = len(cols)
    rows: list[list[str]] = []
    for line in lines[start + 3:]:  # skip marker, dashed rule, column header
        if not line.strip():
            break
        fields = line.split(maxsplit=ncols - 1)
        if len(fields) < ncols:
            fields += [""] * (ncols - len(fields))
        rows.append(fields)
    return _pipe_table(list(cols), rows)


def _render_summary(text: str) -> str:
    """Render a *_summary.log into markdown — convergence + warnings tables."""
    lines = text.splitlines()
    out = []
    convergence = _summary_section(lines, "CONVERGENCE", _CONVERGENCE_COLS)
    warnings = _summary_section(lines, "WARNINGS", _WARNINGS_COLS)
    if convergence:
        out.append("### Convergence\n\n" + convergence)
    if warnings:
        out.append("### Warnings\n\n" + warnings)
    return "\n\n".join(out)


def _render_position_log(text: str) -> str:
    """Render one per-position detail log into markdown.

    Each iteration block becomes an `####` heading with a meta line and the
    RLP/scale + rostered tables as markdown pipe tables. Blocks without those
    tables (e.g. budget phases) render heading + meta only.
    """
    lines = text.splitlines()
    out: list[str] = []
    i, n = 0, len(lines)
    while i < n:
        line = lines[i]
        if line.startswith("PHASE:"):
            kv = {}
            for part in line.split("|"):
                if ":" in part:
                    k, v = part.split(":", 1)
                    kv[k.strip()] = v.strip()
            out.append(
                f"#### {kv.get('PHASE', '?')} · {kv.get('POS', '?')} "
                f"· iter {kv.get('ITER', '?')}"
            )
        elif line.startswith(("pool_size:", "composition_hash:")):
            out.append(f"`{line.strip()}`")
        elif line.startswith("below_replacement:"):
            out.append(f"_{line.strip()}_")
        elif line.startswith(("RLP / scale", "rostered + replacement")):
            label = line.strip().rstrip(":").split("(")[0].strip()
            # Header is the next non-blank line; rows run until the next blank.
            j = i + 1
            while j < n and not lines[j].strip():
                j += 1
            if j < n:
                header_line = lines[j]
                data: list[str] = []
                j += 1
                while j < n and lines[j].strip():
                    data.append(lines[j])
                    j += 1
                out.append(f"**{label}**\n\n{_aligned_table(header_line, data)}")
                i = j
                continue
        i += 1
    return "\n\n".join(out)


def _run_dir_names() -> set[str]:
    """Snapshot the run-directory names currently under the logs root.

    The transform flow takes this snapshot before and after mtbl-valuations;
    the difference is the directory this run produced — which scopes the
    artifact publish to this run rather than a global newest-by-mtime pick.
    """
    try:
        return {d.name for d in VALUATIONS_LOGS_DIR.iterdir() if d.is_dir()}
    except FileNotFoundError:
        return set()


def _publish_for_run_dir(run_dir: Path, key_suffix: str) -> int:
    """Publish summary + per-position artifacts for one run directory.

    `key_suffix` is appended to every artifact key. Empty in the normal
    single-candidate case (stable keys keep a source's shards clustered in
    the UI). Set to ``-<rundirname>`` when multiple candidates force
    disambiguation — see publish_valuations_logs.
    """
    summaries = sorted(run_dir.glob("*_summary.log"))
    if not summaries:
        print(f"No *_summary.log files in {run_dir}")
        return 0

    published = 0
    for summary in summaries:
        source = summary.stem.removesuffix("_summary")

        # One artifact for the source summary (convergence + warnings).
        create_markdown_artifact(
            key=f"mtbl-valuations-logs-{source}-summary{key_suffix}",
            markdown="\n\n".join([
                f"# mtbl-valuations — {source} / summary",
                f"_Run: `{run_dir.name}`_",
                _render_summary(summary.read_text()),
            ]),
            description=f"{source} convergence + warnings — run {run_dir.name}",
        )
        published += 1

        # One artifact per position — sharded so each renders fast.
        detail_dir = run_dir / source
        if detail_dir.is_dir():
            for pos_log in sorted(detail_dir.glob("*.log")):
                position = pos_log.stem
                create_markdown_artifact(
                    key=f"mtbl-valuations-logs-{source}-{position.lower()}{key_suffix}",
                    markdown="\n\n".join([
                        f"# mtbl-valuations — {source} / {position}",
                        f"_Run: `{run_dir.name}`_",
                        _render_position_log(pos_log.read_text()),
                    ]),
                    description=(
                        f"{source} / {position} iteration detail "
                        f"— run {run_dir.name}"
                    ),
                )
                published += 1
    return published


@task(name="publish-valuations-iteration-logs", log_prints=True)
def publish_valuations_logs(run_dir_names: list[str] | None = None) -> None:
    """Publish this run's mtbl-valuations logs, sharded per position.

    `run_dir_names` is the run-directory name(s) that appeared while
    mtbl-valuations ran — the flow resolves this by diffing before/after
    snapshots of the logs root around the mtbl-valuations call. Scoping to
    that set (rather than a global newest-by-mtime) keeps an overlapping run
    from misdirecting the artifacts.

    Prefect artifacts are flat — there are no directories — so the key encodes
    the hierarchy: ``mtbl-valuations-logs-<source>-<position>``. Keys sort
    alphabetically in the UI, so a source's shards cluster together.

    Per valuation source: one summary artifact (convergence + warnings) plus
    one artifact per fielding position. Sharding keeps each artifact small so
    it renders fast — a single combined per-source document was too large.

    Multi-candidate handling: in the normal case the before/after window
    yields one new directory and stable keys are used. If an overlapping
    run created a second directory in the same window we cannot tell which
    is ours from filesystem state — picking newest-by-mtime could publish
    the other run's content under our keys and silently overwrite the
    expected artifacts. Instead, publish every candidate with the run-dir
    name appended to each key (``-<rundirname>``). Worst case: two complete
    artifact sets, each clearly tagged, no data lost or mis-attributed.

    Best-effort: a missing directory or parse error is logged and swallowed —
    a logging hiccup must never fail the transform.
    """
    try:
        # Sort by dir name (YYYYMMDD-HHMMSS, lex order == chrono order) for
        # a deterministic publish order — not for tie-breaking, since we
        # publish every candidate, not just one.
        candidates = sorted(
            (
                VALUATIONS_LOGS_DIR / name
                for name in (run_dir_names or [])
                if (VALUATIONS_LOGS_DIR / name).is_dir()
            ),
            key=lambda d: d.name,
        )
        if not candidates:
            print(
                "No mtbl-valuations log directory for this run under "
                f"{VALUATIONS_LOGS_DIR}"
            )
            return

        disambiguate = len(candidates) > 1
        if disambiguate:
            print(
                f"WARNING: {len(candidates)} mtbl-valuations log dirs "
                f"appeared in this run's window — cannot attribute to "
                f"this run from filesystem state. Publishing all with "
                f"run-name-suffixed keys: {[d.name for d in candidates]}"
            )

        published = 0
        for run_dir in candidates:
            suffix = f"-{run_dir.name}" if disambiguate else ""
            published += _publish_for_run_dir(run_dir, suffix)

        print(
            f"Published {published} sharded iteration-log artifact(s) "
            f"from {[d.name for d in candidates]}"
        )
    except Exception as exc:  # noqa: BLE001 — logging is strictly best-effort
        print(f"WARNING: could not publish valuations iteration logs: {exc}")


@flow(name="transform", log_prints=True)
def transform(year: int) -> None:
    player_universe_trx(year=year)
    # Bracket mtbl-valuations with run-dir snapshots: the difference is the
    # directory this run produced, which scopes the artifact publish to this
    # run rather than whatever happens to be newest in a shared logs root.
    before = _run_dir_names()
    mtbl_valuations()
    publish_valuations_logs(sorted(_run_dir_names() - before))
