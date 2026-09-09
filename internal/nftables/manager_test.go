package nftables_test

import (
	"net/netip"
	"strings"
	"testing"

	"github.com/hostzero/network-reconciler/internal/netbox"
	"github.com/hostzero/network-reconciler/internal/nftables"
)

func TestRenderEmptyMappings(t *testing.T) {
	m := nftables.New()
	rules, err := m.Render(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !strings.Contains(rules, "add table inet network-reconciler") {
		t.Error("expected table declaration in output")
	}
	if !strings.Contains(rules, "flush table inet network-reconciler") {
		t.Error("expected flush table in output")
	}
	if !strings.Contains(rules, "chain prerouting") {
		t.Error("expected prerouting chain in output")
	}
	if !strings.Contains(rules, "chain postrouting") {
		t.Error("expected postrouting chain in output")
	}
}

func TestRenderSingleMapping(t *testing.T) {
	m := nftables.New()

	extIP := netip.MustParseAddr("203.0.113.1")
	intIP := netip.MustParseAddr("10.0.1.100")

	mappings := []netbox.NATMapping{
		{ExternalIP: extIP, InternalIP: intIP, VMName: "web-01"},
	}

	rules, err := m.Render(mappings)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !strings.Contains(rules, "ip daddr 203.0.113.1 dnat to 10.0.1.100") {
		t.Errorf("expected DNAT rule in output, got:\n%s", rules)
	}
	if !strings.Contains(rules, "ip saddr 10.0.1.100 snat to 203.0.113.1") {
		t.Errorf("expected SNAT rule in output, got:\n%s", rules)
	}
}

func TestRenderMultipleMappings(t *testing.T) {
	m := nftables.New()

	mappings := []netbox.NATMapping{
		{
			ExternalIP: netip.MustParseAddr("203.0.113.1"),
			InternalIP: netip.MustParseAddr("10.0.1.100"),
			VMName:     "web-01",
		},
		{
			ExternalIP: netip.MustParseAddr("203.0.113.2"),
			InternalIP: netip.MustParseAddr("10.0.1.101"),
			VMName:     "web-02",
		},
	}

	rules, err := m.Render(mappings)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for _, want := range []string{
		"ip daddr 203.0.113.1 dnat to 10.0.1.100",
		"ip daddr 203.0.113.2 dnat to 10.0.1.101",
		"ip saddr 10.0.1.100 snat to 203.0.113.1",
		"ip saddr 10.0.1.101 snat to 203.0.113.2",
	} {
		if !strings.Contains(rules, want) {
			t.Errorf("expected %q in output", want)
		}
	}
}

func TestRenderNoDuplicateFlush(t *testing.T) {
	m := nftables.New()
	rules, _ := m.Render(nil)

	count := strings.Count(rules, "flush table")
	if count != 1 {
		t.Errorf("expected exactly 1 flush table, found %d", count)
	}
}
