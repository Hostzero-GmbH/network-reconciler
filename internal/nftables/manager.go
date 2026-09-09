// Package nftables manages an isolated nftables table for NAT rules.
// Rules are applied atomically using `nft -f` with a fully-rendered ruleset file.
package nftables

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"text/template"

	"github.com/hostzero/network-reconciler/internal/netbox"
)

const tableName = "network-reconciler"

// rulesTemplate renders a complete nftables ruleset for the managed table.
// The add+flush pattern ensures idempotency and atomicity:
//   - `add table` creates the table if it does not exist (idempotent).
//   - `flush table` removes all existing chains and rules.
//   - The table block re-creates desired chains and rules.
//
// nft applies the entire file as a single kernel transaction when invoked with -f.
const rulesTemplate = `add table inet {{ .TableName }}
flush table inet {{ .TableName }}
table inet {{ .TableName }} {
    chain prerouting {
        type nat hook prerouting priority dstnat; policy accept;
{{ range .Rules }}        ip daddr {{ .ExternalIP }} dnat to {{ .InternalIP }}
{{ end }}    }
    chain postrouting {
        type nat hook postrouting priority srcnat; policy accept;
{{ range .Rules }}        ip saddr {{ .InternalIP }} snat to {{ .ExternalIP }}
{{ end }}    }
}
`

var parsedTemplate = template.Must(template.New("rules").Parse(rulesTemplate))

// Manager manages the `inet network-reconciler` nftables table.
type Manager struct{}

// New returns a new nftables Manager.
func New() *Manager {
	return &Manager{}
}

// Apply atomically replaces all rules in the managed table with those derived
// from the given NAT mappings. An empty mappings slice flushes all rules while
// keeping the table and empty chains in place.
func (m *Manager) Apply(ctx context.Context, mappings []netbox.NATMapping) error {
	rules, err := m.render(mappings)
	if err != nil {
		return fmt.Errorf("rendering nft rules: %w", err)
	}

	tmp, err := os.CreateTemp("", "network-reconciler-*.nft")
	if err != nil {
		return fmt.Errorf("creating temp rules file: %w", err)
	}
	defer os.Remove(tmp.Name())

	if _, err := tmp.WriteString(rules); err != nil {
		tmp.Close()
		return fmt.Errorf("writing nft rules to temp file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("closing temp rules file: %w", err)
	}

	out, err := exec.CommandContext(ctx, "nft", "-f", tmp.Name()).CombinedOutput()
	if err != nil {
		return fmt.Errorf("nft -f failed: %w\noutput: %s\nruleset:\n%s", err, out, rules)
	}

	return nil
}

// Flush removes the entire managed table from the kernel. Safe to call when the
// table does not exist (no-op). Called on clean shutdown.
func (m *Manager) Flush(ctx context.Context) error {
	out, err := exec.CommandContext(ctx, "nft", "delete", "table", "inet", tableName).CombinedOutput()
	if err != nil {
		outStr := string(out)
		// "No such file or directory" or "No such table" → already absent, not an error.
		if bytes.Contains(out, []byte("No such file")) ||
			bytes.Contains(out, []byte("No such table")) ||
			bytes.Contains(out, []byte("No such base chain")) {
			return nil
		}
		return fmt.Errorf("nft delete table inet %s: %w (output: %s)", tableName, err, outStr)
	}
	return nil
}

// Render returns the nftables ruleset string for the given mappings.
// Exported for use in tests.
func (m *Manager) Render(mappings []netbox.NATMapping) (string, error) {
	return m.render(mappings)
}

type templateData struct {
	TableName string
	Rules     []ruleEntry
}

type ruleEntry struct {
	ExternalIP string
	InternalIP string
}

func (m *Manager) render(mappings []netbox.NATMapping) (string, error) {
	data := templateData{TableName: tableName}
	for _, mapping := range mappings {
		data.Rules = append(data.Rules, ruleEntry{
			ExternalIP: mapping.ExternalIP.String(),
			InternalIP: mapping.InternalIP.String(),
		})
	}

	var buf bytes.Buffer
	if err := parsedTemplate.Execute(&buf, data); err != nil {
		return "", fmt.Errorf("executing rules template: %w", err)
	}
	return buf.String(), nil
}
