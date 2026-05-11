# Nightly scheduling — LaunchAgent + pmset wake

This directory contains the macOS LaunchAgent that fires the MTBL Prefect pipeline nightly at **00:16 America/Denver**, plus install/uninstall helpers.

Why LaunchAgent over `cron`: macOS `cron` does not fire missed jobs when the Mac is asleep at the scheduled time — the run is silently skipped. `launchd` fires on wake; paired with `pmset` we wake the Mac just before the agent fires, so the run is reliable even when the laptop is closed overnight.

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

`MTWRFSU` = Mon, Tue, Wed, Thu, Fri, Sat, Sun (macOS's weekday letter codes). `wakeorpoweron` wakes from sleep AND turns on from soft-off. The 2-minute lead time gives Docker Desktop a moment to settle after wake before the LaunchAgent fires the run at 00:16.

## Verify

```
# Agent is loaded
launchctl print gui/$(id -u)/com.mtbl.prefect.nightly | head -30

# Wake schedule is active
pmset -g sched

# Manually trigger once to confirm the wiring (without waiting for 00:16)
launchctl kickstart -k gui/$(id -u)/com.mtbl.prefect.nightly
tail -f /Users/Shared/BaseballHQ/logs/mtbl-prefect-nightly.log
```

`launchctl kickstart -k` re-runs the agent immediately, regardless of schedule. Useful for sanity-checking installation without waiting overnight.

## Logs

| Path | Purpose |
|---|---|
| `/Users/Shared/BaseballHQ/logs/mtbl-prefect-nightly.log` | stdout from each fire — Prefect run state, task progress |
| `/Users/Shared/BaseballHQ/logs/mtbl-prefect-nightly.err` | stderr from each fire — Python tracebacks, uv install errors |

Logs append indefinitely. Periodic rotation isn't wired here yet — see "Open questions" in [TDD §14](https://www.notion.so/35d8d8a04528815fb440ecc2daf39472). Manual rotation: `mv mtbl-prefect-nightly.log mtbl-prefect-nightly.log.$(date +%Y%m%d)`.

## Parallel-run verification (MTBL-151 acceptance, 1-week window)

The acceptance criterion requires running the new Prefect pipeline alongside the old `_etl_runners/run_full_pipeline.sh` for one week and comparing outputs daily, before retiring the bash runner.

Recommended setup:

1. **Keep Prefect at 00:16 nightly** (this LaunchAgent, already installed).
2. **Schedule the bash runner at a non-conflicting time**, e.g. 06:00 MST. The bash runner writes to the same Postgres + Neon target, so running it AFTER the Prefect run lets you compare "did the bash version land the same data as Prefect did 6 hours earlier".
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

**Agent does not fire at 00:16** — first check `pmset -g log | tail -50` to see whether the Mac actually woke at 00:14 or went deeper into sleep. If wake didn't happen, `pmset` wasn't set or got reset by a macOS update. Re-run the `sudo pmset repeat wakeorpoweron MTWRFSU 00:14:00` command.

**Logs grow unbounded** — file a follow-up to add `logrotate` or a simple cron+find rotation. Not load-bearing for short-term operation.

**Need to change the schedule** — edit `com.mtbl.prefect.nightly.plist` `Hour`/`Minute` values, then re-run `./install.sh`. The bootout/bootstrap sequence picks up the new schedule.

**Want to disable temporarily** — `launchctl bootout gui/$(id -u)/com.mtbl.prefect.nightly` disables until re-bootstrapped. Doesn't delete the plist; re-running `./install.sh` re-enables.
