#!/usr/bin/env bash
# Remove the MTBL Prefect nightly LaunchAgent from the current Mac.

set -euo pipefail

PLIST_NAME="com.mtbl.prefect.nightly"
PLIST_DST="${HOME}/Library/LaunchAgents/${PLIST_NAME}.plist"
UID_DOMAIN="gui/$(id -u)"

echo "==> Booting out ${PLIST_NAME}"
launchctl bootout "${UID_DOMAIN}/${PLIST_NAME}" 2>/dev/null || echo "  (was not loaded)"

echo "==> Removing ${PLIST_DST}"
rm -f "${PLIST_DST}"

echo
echo "✓ LaunchAgent uninstalled."
echo
echo "  The pmset wake schedule is NOT removed automatically. To cancel it:"
echo "      sudo pmset repeat cancel"
echo
