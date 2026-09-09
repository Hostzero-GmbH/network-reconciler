package frr

import (
	"encoding/json"
	"net/netip"
	"testing"
)

// The loopback address listing must return only the VM host addresses, never 127/8 —
// withdrawing 127.0.0.1 would be a very bad outcome of state reconciliation.
func TestActiveIPsParsingExcludesLoopbackAndNonHostPrefixes(t *testing.T) {
	raw := `[{"ifname":"lo","addr_info":[
		{"family":"inet","local":"127.0.0.1","prefixlen":8},
		{"family":"inet","local":"203.0.113.1","prefixlen":32},
		{"family":"inet","local":"203.0.113.2","prefixlen":32},
		{"family":"inet","local":"10.0.0.0","prefixlen":24}
	]}]`

	var entries []ipAddrEntry
	if err := json.Unmarshal([]byte(raw), &entries); err != nil {
		t.Fatalf("unmarshalling fixture: %v", err)
	}

	got := make(map[netip.Addr]struct{})
	for _, entry := range entries {
		for _, info := range entry.AddrInfo {
			if info.PrefixLen != 32 {
				continue
			}
			addr, err := netip.ParseAddr(info.Local)
			if err != nil || addr.IsLoopback() {
				continue
			}
			got[addr] = struct{}{}
		}
	}

	if len(got) != 2 {
		t.Fatalf("expected 2 host addresses, got %d: %v", len(got), got)
	}
	for _, want := range []string{"203.0.113.1", "203.0.113.2"} {
		if _, ok := got[netip.MustParseAddr(want)]; !ok {
			t.Fatalf("expected %s to be present, got %v", want, got)
		}
	}
	if _, ok := got[netip.MustParseAddr("127.0.0.1")]; ok {
		t.Fatal("127.0.0.1 must never be treated as a managed advertisement")
	}
}

// The redistribution check drives a startup warning, so it must not be fooled by the
// route-map suffix, indentation, or the word appearing inside a comment.
func TestRedistributionParsing(t *testing.T) {
	tests := []struct {
		name      string
		config    string
		connected bool
	}{
		{
			name:      "stock node config",
			config:    "router bgp 65050\n address-family ipv4 unicast\n  redistribute connected\n exit-address-family\n",
			connected: true,
		},
		{
			name:      "with the optional route-map filter",
			config:    "  redistribute connected route-map NR-ACTIVE\n",
			connected: true,
		},
		{
			name:   "no redistribution at all",
			config: "router bgp 65050\n bgp router-id 192.0.2.10\n",
		},
		{
			name:   "other sources must not count",
			config: "  redistribute static\n  redistribute ospf\n",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := parseRedistribution([]byte(tc.config))
			if got.Connected != tc.connected {
				t.Fatalf("expected connected=%v, got connected=%v", tc.connected, got.Connected)
			}
		})
	}
}

// With FRR management disabled every operation must be a no-op that reports success,
// so the service can run on a node without FRR.
func TestDisabledManagerIsNoOp(t *testing.T) {
	m := New(false, "lo")
	ip := netip.MustParseAddr("203.0.113.1")

	if err := m.Advertise(nil, ip); err != nil { //nolint:staticcheck // nil ctx is never used when disabled
		t.Fatalf("Advertise: %v", err)
	}
	if err := m.Withdraw(nil, ip); err != nil { //nolint:staticcheck
		t.Fatalf("Withdraw: %v", err)
	}
	active, err := m.ActiveIPs(nil) //nolint:staticcheck
	if err != nil || len(active) != 0 {
		t.Fatalf("ActiveIPs: got %v, err %v", active, err)
	}
}
