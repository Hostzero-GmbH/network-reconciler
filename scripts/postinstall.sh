#!/bin/bash
# postinstall.sh — run after network-reconciler package installation.
set -euo pipefail

SERVICE=network-reconciler

# ── Create system user ────────────────────────────────────────────────────────
if ! id -u "${SERVICE}" &>/dev/null; then
    useradd \
        --system \
        --shell /usr/sbin/nologin \
        --home-dir /var/lib/network-reconciler \
        --comment "Network Reconciler" \
        "${SERVICE}"
fi

# ── Fix ownership on directories that nfpm creates as root ───────────────────
chown -R "${SERVICE}:${SERVICE}" /var/lib/network-reconciler
chown -R "${SERVICE}:${SERVICE}" /etc/network-reconciler/certs
chmod 750 /etc/network-reconciler/certs

mkdir -p /etc/pve/network-reconciler
# /etc/pve is pmxcfs: chown is not supported there. Keep provisioning best-effort.
chmod 770 /etc/pve/network-reconciler 2>/dev/null || true

# Ensure service can read static config and environment overrides.
chown root:"${SERVICE}" /etc/network-reconciler
chmod 750 /etc/network-reconciler
if [ -f /etc/network-reconciler/config.yaml ]; then
    chown root:"${SERVICE}" /etc/network-reconciler/config.yaml
    chmod 640 /etc/network-reconciler/config.yaml
fi
if [ -f /etc/network-reconciler/environment ]; then
    chown root:"${SERVICE}" /etc/network-reconciler/environment
    chmod 640 /etc/network-reconciler/environment
fi

# /etc/pve/.members is usually group-readable by www-data.
# Add service user to that group so config.Load() can read cluster metadata.
if getent group www-data >/dev/null 2>&1; then
    usermod -a -G www-data "${SERVICE}" || true
fi

# ── Issue client certificate from PVE cluster CA ─────────────────────────────
if command -v proxmox-eventbus &>/dev/null; then
    CERT_DIR=/etc/network-reconciler/certs
    if [ ! -f "${CERT_DIR}/client.pem" ]; then
        echo "Issuing NATS client certificate..."
        proxmox-eventbus issue-client-cert \
            --cn "${SERVICE}" \
            --out "${CERT_DIR}" \
            && echo "Certificate issued to ${CERT_DIR}" \
            || echo "WARNING: Certificate issuance failed. Run manually:
  proxmox-eventbus issue-client-cert --cn ${SERVICE} --out ${CERT_DIR}" >&2

        chown -R "${SERVICE}:${SERVICE}" "${CERT_DIR}"
        chmod 640 "${CERT_DIR}/client.key"
    fi
else
    echo "WARNING: proxmox-eventbus not found." >&2
    echo "Install it first, then run:" >&2
    echo "  proxmox-eventbus issue-client-cert --cn ${SERVICE} --out /etc/network-reconciler/certs" >&2
fi

# ── Remind operator to configure the service ─────────────────────────────────
cat <<'EOF'

Next steps:
  1. Edit /etc/network-reconciler/config.yaml — set cluster, nats.servers, netbox.url
  2. Set the Netbox API token: echo "NR_NETBOX_TOKEN=<token>" >> /etc/network-reconciler/environment
  3. Configure FRR BGP — merge /usr/share/doc/network-reconciler/frr-bgp-example.conf
     into /etc/frr/frr.conf, then run: systemctl reload frr
     A node without `redistribute connected` announces nothing at all and its VMs
     are unreachable, with no error anywhere. Verify the RUNNING config with:
       vtysh -c "show running-config" | grep redistribute
  4. Run:  systemctl enable --now network-reconciler
  5. Check: journalctl -u network-reconciler -f

EOF

# ── Enable and start the service ─────────────────────────────────────────────
systemctl daemon-reload
systemctl enable "${SERVICE}.service"

# Only auto-start if config has been filled in (non-empty cluster field).
if grep -q '^cluster: ""' /etc/network-reconciler/config.yaml 2>/dev/null; then
    echo "Service NOT auto-started: edit /etc/network-reconciler/config.yaml first."
else
    systemctl start "${SERVICE}.service" || true
fi
