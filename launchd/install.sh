#!/usr/bin/env bash
# Install the MTBL Prefect nightly LaunchAgent on the current Mac.
# Safe to re-run: bootstraps idempotently by booting out any existing instance first.

set -euo pipefail

PLIST_NAME="com.mtbl.prefect.nightly"
PLIST_SRC="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/${PLIST_NAME}.plist"
PLIST_DST="${HOME}/Library/LaunchAgents/${PLIST_NAME}.plist"
LOG_DIR="/Users/Shared/BaseballHQ/logs"
UID_DOMAIN="gui/$(id -u)"

echo "==> Ensuring log directory: ${LOG_DIR}"
mkdir -p "${LOG_DIR}"

echo "==> Validating plist syntax"
plutil -lint "${PLIST_SRC}"

echo "==> Installing ${PLIST_NAME} → ${PLIST_DST}"
mkdir -p "${HOME}/Library/LaunchAgents"
cp "${PLIST_SRC}" "${PLIST_DST}"

echo "==> Loading into launchd (boot out any prior instance first)"
launchctl bootout "${UID_DOMAIN}/${PLIST_NAME}" 2>/dev/null || true
launchctl bootstrap "${UID_DOMAIN}" "${PLIST_DST}"

echo
echo "✓ LaunchAgent installed. Next fire: 05:45 America/Denver tomorrow (or 09:05 today if before 09:05)."
echo
echo "  Verify with:"
echo "      launchctl print ${UID_DOMAIN}/${PLIST_NAME} | head -30"
echo
echo "  Required follow-up — wake the Mac at 05:43 (one-time, needs sudo):"
echo "      sudo pmset repeat wakeorpoweron MTWRFSU 05:43:00"
echo
echo "  Confirm wake schedule:"
echo "      pmset -g sched"
echo
echo "  Tail nightly logs:"
echo "      tail -f ${LOG_DIR}/mtbl-prefect-nightly.log"
echo
