package config

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestLoad_UsesMembersForClusterAndNode(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	membersPath := filepath.Join(dir, ".members")
	configPath := filepath.Join(dir, "config.yaml")

	members := `{
"nodename": "pve01",
"cluster": { "name": "NOV" },
"nodelist": {
  "pve01": { "id": 1 },
  "pve02": { "id": 2 }
  }
}`
	if err := os.WriteFile(membersPath, []byte(members), 0o600); err != nil {
		t.Fatalf("write members: %v", err)
	}

	cfgYAML := fmt.Sprintf(`pve_members_path: %s
nats:
  servers: []
  cert: /tmp/cert
  key: /tmp/key
  ca: /tmp/ca
netbox:
  url: https://netbox.local
  token: token
`, membersPath)
	if err := os.WriteFile(configPath, []byte(cfgYAML), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := Load(configPath)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.Cluster != "NOV" {
		t.Fatalf("cfg.Cluster = %q, want NOV", cfg.Cluster)
	}
	if cfg.Node != "pve01" {
		t.Fatalf("cfg.Node = %q, want pve01", cfg.Node)
	}

	wantServers := []string{"tls://pve01:4222", "tls://pve02:4222"}
	if !reflect.DeepEqual(cfg.NATS.Servers, wantServers) {
		t.Fatalf("cfg.NATS.Servers = %v, want %v", cfg.NATS.Servers, wantServers)
	}
}
