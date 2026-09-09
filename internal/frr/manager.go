// Package frr manages BGP advertisement of VM external IPs without talking to FRR.
//
// The external IP of each locally-running VM is added to the loopback interface as a
// /32, producing a *connected* route that bgpd redistributes with
// `redistribute connected`. Removing the address withdraws the announcement. There is
// no runtime interaction with FRR at all — `ip addr add/del` is the whole interface.
//
// Required FRR bgpd configuration is documented in packaging/frr-bgp-example.conf.
package frr

import (
	"context"
	"encoding/json"
	"fmt"
	"net/netip"
	"os/exec"
	"strings"
)

// Manager manages the loopback addresses that back BGP announcements.
type Manager struct {
	enabled bool
	loIface string
}

// New returns a new FRR Manager.
// If enabled is false all operations are no-ops (useful for environments without FRR).
func New(enabled bool, loIface string) *Manager {
	return &Manager{enabled: enabled, loIface: loIface}
}

// Advertise adds extIP/32 as an address on the loopback interface.
// Returns nil if the address already exists (idempotent).
func (m *Manager) Advertise(ctx context.Context, extIP netip.Addr) error {
	if !m.enabled {
		return nil
	}
	addr := extIP.String() + "/32"
	out, err := exec.CommandContext(ctx, "ip", "addr", "add", addr, "dev", m.loIface).CombinedOutput()
	if err != nil {
		if isAlreadyExists(out) {
			return nil
		}
		return fmt.Errorf("ip addr add %s dev %s: %w (output: %s)", addr, m.loIface, err, out)
	}
	return nil
}

// Withdraw removes extIP/32 from the loopback interface addresses.
// Returns nil if the address does not exist (idempotent).
func (m *Manager) Withdraw(ctx context.Context, extIP netip.Addr) error {
	if !m.enabled {
		return nil
	}
	addr := extIP.String() + "/32"
	out, err := exec.CommandContext(ctx, "ip", "addr", "del", addr, "dev", m.loIface).CombinedOutput()
	if err != nil {
		if isNoSuchProcess(out) || isAddrNotAssigned(out) {
			return nil
		}
		return fmt.Errorf("ip addr del %s dev %s: %w (output: %s)", addr, m.loIface, err, out)
	}
	return nil
}

// Redistribution describes how bgpd is currently configured to redistribute the routes
// this package installs.
type Redistribution struct {
	// Connected is true when connected routes are redistributed, i.e. VMs running on
	// this node are announced at all.
	Connected bool
}

// CheckRedistribution inspects the running bgpd configuration.
//
// frr.conf is operator-owned and this package never edits it, so a node missing
// `redistribute connected` announces nothing at all — silently. The reconciler's own
// logs would still report the loopback address being added, and VMs migrated to that
// node would simply be unreachable with no error anywhere.
func (m *Manager) CheckRedistribution(ctx context.Context) (Redistribution, error) {
	out, err := exec.CommandContext(ctx, "vtysh", "-c", "show running-config").Output()
	if err != nil {
		return Redistribution{}, fmt.Errorf("vtysh -c 'show running-config': %w", err)
	}
	return parseRedistribution(out), nil
}

// parseRedistribution extracts the redistribution sources from a bgpd running-config.
func parseRedistribution(config []byte) Redistribution {
	var r Redistribution
	for _, line := range strings.Split(string(config), "\n") {
		// Fields tolerates the leading indentation FRR emits and the optional
		// `route-map NAME` suffix.
		fields := strings.Fields(line)
		if len(fields) < 2 || fields[0] != "redistribute" {
			continue
		}
		if fields[1] == "connected" {
			r.Connected = true
		}
	}
	return r
}

// ── Kernel state enumeration ────────────────────────────────────────────────────

type ipAddrEntry struct {
	IFName   string `json:"ifname"`
	AddrInfo []struct {
		Family    string `json:"family"`
		Local     string `json:"local"`
		PrefixLen int    `json:"prefixlen"`
	} `json:"addr_info"`
}

// ActiveIPs returns the /32 addresses currently assigned to the loopback interface,
// excluding loopback-reserved addresses such as 127.0.0.0/8.
//
// The in-memory view of what has been advertised does not survive a restart, so
// without reading the kernel back a crash mid-migration would leave an address on lo
// forever — and two nodes announcing the same /32 means upstream ECMP and a
// persistent partial blackhole.
func (m *Manager) ActiveIPs(ctx context.Context) (map[netip.Addr]struct{}, error) {
	result := make(map[netip.Addr]struct{})
	if !m.enabled {
		return result, nil
	}

	out, err := exec.CommandContext(ctx, "ip", "-j", "-4", "addr", "show", "dev", m.loIface).Output()
	if err != nil {
		return nil, fmt.Errorf("ip -j -4 addr show dev %s: %w", m.loIface, err)
	}

	var entries []ipAddrEntry
	if err := json.Unmarshal(out, &entries); err != nil {
		return nil, fmt.Errorf("parsing ip addr JSON: %w", err)
	}

	for _, entry := range entries {
		for _, info := range entry.AddrInfo {
			if info.PrefixLen != 32 {
				continue
			}
			addr, err := netip.ParseAddr(info.Local)
			if err != nil || addr.IsLoopback() {
				continue
			}
			result[addr] = struct{}{}
		}
	}
	return result, nil
}

func isAlreadyExists(out []byte) bool {
	s := string(out)
	return strings.Contains(s, "File exists") || strings.Contains(s, "RTNETLINK answers: File exists")
}

func isNoSuchProcess(out []byte) bool {
	s := string(out)
	return strings.Contains(s, "No such process") || strings.Contains(s, "RTNETLINK answers: No such process")
}

func isAddrNotAssigned(out []byte) bool {
	s := string(out)
	return strings.Contains(s, "Cannot assign requested address") || strings.Contains(s, "RTNETLINK answers: Cannot assign requested address")
}
