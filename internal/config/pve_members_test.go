package config

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestReadPVEMembers(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, ".members")
	content := `{
"nodename": "pve01",
"version": 7,
"cluster": { "name": "NOV", "version": 3, "nodes": 3, "quorate": 1 },
"nodelist": {
  "pve01": { "id": 1, "online": 1, "ip": "192.0.2.10"},
  "pve03": { "id": 2, "online": 1, "ip": "192.0.2.12"},
  "pve02": { "id": 3, "online": 1, "ip": "192.0.2.11"}
  }
}`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write members: %v", err)
	}

	members, err := readPVEMembers(path)
	if err != nil {
		t.Fatalf("readPVEMembers() error = %v", err)
	}
	if members.ClusterName != "NOV" {
		t.Fatalf("ClusterName = %q, want NOV", members.ClusterName)
	}
	if members.NodeName != "pve01" {
		t.Fatalf("NodeName = %q, want pve01", members.NodeName)
	}
	wantNodes := []string{"pve01", "pve03", "pve02"}
	if !reflect.DeepEqual(members.Nodes, wantNodes) {
		t.Fatalf("Nodes = %v, want %v", members.Nodes, wantNodes)
	}

	wantServers := []string{"tls://pve01:4222", "tls://pve03:4222", "tls://pve02:4222"}
	if !reflect.DeepEqual(members.DefaultNATSServers(), wantServers) {
		t.Fatalf("DefaultNATSServers() = %v, want %v", members.DefaultNATSServers(), wantServers)
	}
}
