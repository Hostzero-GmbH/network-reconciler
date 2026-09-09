#!/bin/bash
# preremove.sh — run before network-reconciler package removal.
set -euo pipefail

SERVICE=network-reconciler

# Stop the service if running to ensure rules are flushed via the shutdown handler.
if systemctl is-active --quiet "${SERVICE}.service"; then
    echo "Stopping ${SERVICE}..."
    systemctl stop "${SERVICE}.service" || true
fi

systemctl disable "${SERVICE}.service" 2>/dev/null || true

# Belt-and-suspenders: flush nftables table directly in case the service
# crashed and did not clean up on its own.
nft delete table inet network-reconciler 2>/dev/null || true

# Same for the BGP announcements, but ONLY for addresses this service owns.
#
# The loopback interface is shared: operators and other tooling put anycast and service
# addresses there too. Removing every /32 on lo would take those down with us, so the
# set to remove is derived from the per-VM config files this service wrote, not from
# whatever happens to be on the interface.
LO_IFACE="${NR_LOOPBACK_IFACE:-lo}"
BACKUP_DIR="${NR_BACKUP_DIR:-/etc/pve/network-reconciler}"

if command -v ip >/dev/null 2>&1 && [ -d "${BACKUP_DIR}" ]; then
    # `|| true` is load-bearing: with no *.config files the glob does not expand and
    # grep exits non-zero, which `set -o pipefail` plus `set -e` would turn into a prerm
    # failure on any node that never hosted a NAT-mapped VM.
    OWNED=$(grep -ho '"ExternalIP"[[:space:]]*:[[:space:]]*"[^"]*"' "${BACKUP_DIR}"/*.config 2>/dev/null \
        | sed 's/.*"\([^"]*\)"$/\1/' | sort -u || true)

    for addr in ${OWNED}; do
        if ip -o -4 addr show dev "${LO_IFACE}" 2>/dev/null | grep -qw "${addr}/32"; then
            echo "Removing loopback address ${addr}/32 from ${LO_IFACE}..."
            ip addr del "${addr}/32" dev "${LO_IFACE}" 2>/dev/null || true
        fi
    done
fi

echo "${SERVICE} removed."
