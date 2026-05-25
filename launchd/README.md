# Pipeline scheduling — LaunchAgent + pmset wake

This directory contains the macOS LaunchAgent that fires the MTBL Prefect pipeline **twice a day, both America/Denver wall-clock**, plus install/uninstall helpers.

| Run | Why |
|---|---|
| **00:16** | Captures the prior day's completed games. Surfaces who to target on waivers before claims process. |
| **09:05** | Captures that day's waiver activity (league claims clear at 09:00). Post-waiver roster state is loaded in time to set lineups for the day. |

Both fire times live in a single `StartCalendarInterval` array on one agent — one job, two triggers, shared `ProgramArguments` and log files. launchd fires on the system clock (TZ `America/Denver`), so both runs track the MDT/MST shift automatically and hold their local wall-clock year-round.

> The agent's `Label`, plist filename, and log files still read `nightly` — a historical name from when there was only the midnight run. Harmless; just don't read it as "runs once."

Why LaunchAgent over `cron`: macOS `cron` does not fire missed jobs when the Mac is asleep at the scheduled time — the run is silently skipped. `launchd` instead fires on wake: a job whose scheduled time passed while the Mac slept runs as soon as it wakes. Paired with `pmset` we also wake the Mac just before the **00:16** fire, so the overnight run is reliable even when the laptop is closed. The 09:05 run relies on the Mac being awake (typical for a workday morning) or on launchd's fire-on-wake catch-up — see the `pmset` note below.

## Install

From this directory:

```
./install.sh
```

The script:
- Lints the plist with `plutil`
- Copies `com.mtbl.prefect.nightly.plist` to `~/Library/LaunchAgents/`
- Bootstraps it into your user's `gui/<uid>` launchd domain (idempotent)
- Creates the log directory at `/Users/Shared/BaseballHQ/logs/` if missing

Then run the one-time `pmset` command (needs sudo):

```
sudo pmset repeat wakeorpoweron MTWRFSU 00:14:00
```

`MTWRFSU` = Mon, Tue, Wed, Thu, Fri, Sat, Sun (macOS's weekday letter codes). `wakeorpoweron` wakes from sleep AND turns on from soft-off. The 2-minute lead time gives the kernel + filesystem a moment to settle before the LaunchAgent fires at 00:16; Docker Desktop itself is started by the plist's bash preflight (`open -ga Docker` + poll `docker info` for up to 120s) — `pmset` does not relaunch userspace daemons on wake.

This `pmset` wake covers the **00:16** run only — `pmset repeat` supports a single repeating wake event. The **09:05** run is not wake-scheduled: if the Mac is awake it fires on time; if asleep, launchd runs it on the next wake (open the lid → the run fires → fresh data lands ~5–15 min later, still in time to set lineups). No second `pmset` entry is needed for the morning run.

## Verify install

```
# Confirm the agent is loaded in your user's GUI launchd domain
launchctl print gui/$(id -u)/com.mtbl.prefect.nightly | head -30

# Confirm pmset wake schedule is active
pmset -g sched
```

If `launchctl print` returns `Could not find service`, the agent isn't loaded — re-run `./install.sh`. If `pmset -g sched` shows no schedule, run the `sudo pmset repeat ...` command from the Install section.

## Trigger a run on demand (kickstart)

Use this to fire the pipeline immediately instead of waiting for the next scheduled run (00:16 or 09:05). It's the single most useful command in this whole setup — for verification after install, dry-running code changes, and re-running after a fixed failure.

### The command

```
launchctl kickstart -k gui/$(id -u)/com.mtbl.prefect.nightly
```

What each piece does:

| Piece | Purpose |
|---|---|
| `launchctl kickstart` | Fire the agent NOW, regardless of its schedule |
| `-k` | Kill any already-running instance first — makes re-triggering safe and idempotent |
| `gui/$(id -u)` | The launchd domain where `./install.sh` registered the agent (your user's GUI session) |
| `com.mtbl.prefect.nightly` | The agent's `Label` (from the plist) |

The command returns immediately. The flow itself runs in the background via Docker — typical run takes 5–15 minutes depending on cache state and external API responsiveness.

### Watch progress live

```
tail -f /Users/Shared/BaseballHQ/logs/mtbl-prefect-nightly.log
```

Exit tail with `Ctrl+C`. The flow keeps running regardless of whether you're tailing — tail just stops following new lines on your terminal.

### What you should see in the log (in order)

1. `$ docker compose --profile runner run --rm runner full-pipeline --year YYYY`
2. Extract tasks running in parallel — three sources fanning out:
    - `espn-api-extractor` (runs both `players-extract` and `league-extract`)
    - `fangraphs-api-extractor`
    - `savant-api-extractor`
3. Transform tasks running sequentially:
    - `player-universe-trx`
    - `mtbl-valuations`
4. Load task:
    - `player-universe-load` → loads to fantasy-pg → syncs to Neon
5. `:white_check_mark: MTBL pipeline ... completed successfully` lands in `#mtbll` (Slack)
6. healthchecks.io check flips green

On failure, you'll see a `:x: MTBL pipeline ... failed: ...` message in Slack and the healthcheck flips red instead.

### When to use kickstart

- **After install** — verify the LaunchAgent + Docker chain end-to-end without waiting overnight
- **After deploying code changes** — validate behavior before the next scheduled fire (especially after touching `mtbl_prefect/`, sub-project repos, or anything bind-mounted into the runner)
- **After a failed nightly run** — once you've identified and fixed the cause, re-run immediately rather than waiting another night
- **Anytime you want a flow run to happen NOW** — there is no other "run the pipeline now" command worth memorizing; this is the one

### What `kickstart -k` does NOT do

- **Does not change the schedule** — the next scheduled fire (00:16 / 09:05) still happens as configured
- **Does not bypass pmset** — kickstart only works if the Mac is awake (or the LaunchAgent is already loaded and idle)
- **Does not restart persistent Docker services** — `prefect-server`, `postgres`, and `fantasy-pg` keep running between fires regardless of kickstart activity
- **Does not require the previous run to have finished cleanly** — `-k` kills any in-progress run first

### If kickstart errors

| Error | Cause | Fix |
|---|---|---|
| `Could not find service` | Agent isn't loaded | `./install.sh` |
| `Service is disabled` | Agent was booted out | `./install.sh` to re-bootstrap |
| No errors but log shows no activity after ~30s | Docker Desktop may have suspended | Open Docker Desktop, wait for daemon to be responsive, retry kickstart |
| Log shows the runner starting but Fangraphs/Savant errors | Sub-project source on disk may be at a branch state your pipeline doesn't expect — check `cd _extract/<project> && git branch --show-current` | Roll the sub-project to a known-good branch or fix the integration |

## Logs

| Path | Purpose |
|---|---|
| `/Users/Shared/BaseballHQ/logs/mtbl-prefect-nightly.log` | stdout from each fire — Prefect run state, task progress |
| `/Users/Shared/BaseballHQ/logs/mtbl-prefect-nightly.err` | stderr from each fire — Python tracebacks, uv install errors |

Logs append indefinitely. Periodic rotation isn't wired here yet — see "Open questions" in [TDD §14](https://www.notion.so/35d8d8a04528815fb440ecc2daf39472). Manual rotation: `mv mtbl-prefect-nightly.log mtbl-prefect-nightly.log.$(date +%Y%m%d)`.

## Parallel-run verification (MTBL-151 acceptance, 1-week window)

The acceptance criterion requires running the new Prefect pipeline alongside the old `_etl_runners/run_full_pipeline.sh` for one week and comparing outputs daily, before retiring the bash runner.

Recommended setup:

1. **Keep Prefect on its schedule** — 00:16 + 09:05 (this LaunchAgent, already installed).
2. **Schedule the bash runner at a non-conflicting time**, e.g. 06:00 MST — between the two Prefect runs. The bash runner writes to the same Postgres + Neon target, so running it AFTER the 00:16 Prefect run lets you compare "did the bash version land the same data as Prefect did ~6 hours earlier".
3. **Manual or scripted daily diff** — quick options:
    - Compare row counts in Neon: `SELECT COUNT(*) FROM players;` (etc.) at 05:55 vs 06:30
    - Compare extract output file sizes / line counts in `/Users/Shared/BaseballHQ/resources/extract/`
    - Compare valuations table for one sample player

A second LaunchAgent for the bash runner is the cleanest way to schedule. Template (not committed; one-week use):

```xml
<!-- ~/Library/LaunchAgents/com.mtbl.bash.parallel.plist -->
<key>Label</key><string>com.mtbl.bash.parallel</string>
<key>ProgramArguments</key>
<array>
    <string>/bin/bash</string>
    <string>-lc</string>
    <string>/Users/Shared/BaseballHQ/tools/_etl_runners/run_full_pipeline.sh --year $(date +%Y) &gt;&gt; /Users/Shared/BaseballHQ/logs/bash-parallel.log 2&gt;&amp;1</string>
</array>
<key>StartCalendarInterval</key>
<dict><key>Hour</key><integer>6</integer><key>Minute</key><integer>0</integer></dict>
```

Bootstrap with the same `launchctl bootstrap gui/<uid> <path>` pattern. After 7 consecutive days of parity, boot it out (`launchctl bootout`) — it's not part of the long-term system.

## Uninstall

```
./uninstall.sh
```

Removes the plist from `~/Library/LaunchAgents/` and boots it out of launchd. Does NOT remove the pmset wake schedule (do that separately with `sudo pmset repeat cancel` if you want — though leaving it set is harmless: an unused wake just turns the screen on briefly).

## Troubleshooting

**Agent fires but `docker compose` not found** — the LaunchAgent's `PATH` doesn't include the Docker Desktop binary location. Default in the plist is `/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin` which covers both Apple Silicon and Intel Homebrew. If your Docker install lives elsewhere, edit the `<key>PATH</key>` value in the plist + re-install.

**00:16 run does not fire** — first check `pmset -g log | tail -50` to see whether the Mac actually woke at 00:14 or went deeper into sleep. If wake didn't happen, `pmset` wasn't set or got reset by a macOS update. Re-run the `sudo pmset repeat wakeorpoweron MTWRFSU 00:14:00` command.

**00:16 run fires but exits with status 2** — that's the Docker preflight giving up. The plist runs `open -ga Docker` and then polls `docker info` every 2s for 120s before falling through to the pipeline; exit 2 means Docker Desktop never came up in time. Common causes: (1) Docker Desktop is mid-update and the daemon is stuck; (2) "Use Rosetta for x86_64/amd64 emulation" got toggled and Docker is in a recovery loop; (3) the disk is full and Docker can't start its VM. Open Docker Desktop manually, watch it settle, then re-fire with `launchctl kickstart -k gui/$(id -u)/com.mtbl.prefect.nightly`. If exit 2 recurs, raise the poll budget (`seq 1 60` → `seq 1 120`) for a 240s ceiling.

**00:16 run fires but exits with status 1 and the err log ends in `dial unix … docker.sock: no such file or directory`** — the preflight succeeded but Docker died between the poll and the `docker compose run`. Race is rare on a stable host; if you see it repeatedly, the Mac is probably re-sleeping mid-run (System Settings → Battery / Energy → "Wake for network access" or "Prevent automatic sleeping when display is off").

**09:05 run does not fire** — this run has no `pmset` wake. If the Mac was asleep at 09:05, launchd runs the job on the next wake — check the log timestamp against when you opened the Mac. If it fired late but did fire, that's expected behavior, not a fault. If it never fired at all, confirm the agent is loaded (`launchctl print` — see Verify) and that `launchctl print` shows two `StartCalendarInterval` descriptors.

**Logs grow unbounded** — file a follow-up to add `logrotate` or a simple cron+find rotation. Not load-bearing for short-term operation.

**Need to change the schedule** — `StartCalendarInterval` in `com.mtbl.prefect.nightly.plist` is an `<array>` of `<dict>` entries, one per fire time. Edit a dict's `Hour`/`Minute`, add a dict for another run, or remove one, then re-run `./install.sh`. The bootout/bootstrap sequence picks up the new schedule. Verify with `launchctl print` — it should list one descriptor per fire time.

**Want to disable temporarily** — `launchctl bootout gui/$(id -u)/com.mtbl.prefect.nightly` disables until re-bootstrapped. Doesn't delete the plist; re-running `./install.sh` re-enables.
