package netbox

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// vmListHandler serves the batched virtual-machines lookup, recording each call's id
// filter (comma-joined) so tests can assert the resolution is batched and deduplicated.
//
// It mimics Netbox's strictness about the filter shape: ids must arrive as the `id`
// parameter repeated, and an unrecognised filter such as the pre-3.0 `id__in` is a
// hard 400 rather than something the API quietly ignores.
func vmListHandler(calls *[]string, vms []map[string]any) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query()
		for param := range query {
			if strings.Contains(param, "__in") {
				w.WriteHeader(http.StatusBadRequest)
				_ = json.NewEncoder(w).Encode(map[string]any{
					"detail": "Unknown filter field: " + param,
				})
				return
			}
		}

		ids := query["id"]
		if len(ids) == 0 {
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"detail": "no id filter supplied",
			})
			return
		}
		*calls = append(*calls, strings.Join(ids, ","))

		// Only return the VMs the query actually asked for, so a request carrying the
		// ids in a form Netbox does not understand cannot pass by accident.
		wanted := make(map[string]bool, len(ids))
		for _, id := range ids {
			wanted[id] = true
		}
		matched := make([]map[string]any, 0, len(vms))
		for _, vm := range vms {
			if wanted[fmt.Sprintf("%v", vm["id"])] {
				matched = append(matched, vm)
			}
		}

		_ = json.NewEncoder(w).Encode(map[string]any{
			"count":   len(matched),
			"next":    nil,
			"results": matched,
		})
	}
}

func TestFetchNATMappingsResolvesVMIDFromVMList(t *testing.T) {
	var vmListCalls []string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/api/ipam/ip-addresses/"):
			resp := map[string]any{
				"count": 1,
				"next":  nil,
				"results": []map[string]any{
					{
						"id":                   656,
						"address":              "192.0.2.51/32",
						"assigned_object_type": "virtualization.vminterface",
						"assigned_object": map[string]any{
							"id":   991,
							"name": "eth0",
							"virtual_machine": map[string]any{
								"id":   130,
								"name": "NAT TEST11",
							},
						},
						"nat_outside": []map[string]any{{"id": 1, "address": "172.16.1.1/32"}},
					},
				},
			}
			_ = json.NewEncoder(w).Encode(resp)
			return
		case r.Method == http.MethodGet && r.URL.Path == "/api/virtualization/virtual-machines/":
			vmListHandler(&vmListCalls, []map[string]any{
				{
					"id":            130,
					"name":          "NAT TEST11",
					"custom_fields": map[string]any{"proxmox_vmid": 999},
				},
			})(w, r)
			return
		default:
			http.NotFound(w, r)
			return
		}
	}))
	defer srv.Close()

	client := NewClient(srv.URL, "test-token")
	mappings, err := client.FetchNATMappings(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(vmListCalls) != 1 {
		t.Fatalf("expected one batched VM list call, got %d: %v", len(vmListCalls), vmListCalls)
	}
	if vmListCalls[0] != "130" {
		t.Fatalf("expected id=130, got %q", vmListCalls[0])
	}
	if len(mappings) != 1 {
		t.Fatalf("expected one mapping, got %d", len(mappings))
	}
	if got := mappings[0].VMProxmoxVMID; got != 999 {
		t.Fatalf("expected proxmox VMID 999, got %d", got)
	}
	if got := mappings[0].ExternalIP.String(); got != "172.16.1.1" {
		t.Fatalf("expected external IP 172.16.1.1, got %s", got)
	}
}

// Two IPs on the same VM must resolve with a single batched request, not one per IP.
func TestFetchNATMappingsBatchesResolvedVMIDPerVM(t *testing.T) {
	var vmListCalls []string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/api/ipam/ip-addresses/"):
			resp := map[string]any{
				"count": 2,
				"next":  nil,
				"results": []map[string]any{
					{
						"id":                   656,
						"address":              "192.0.2.51/32",
						"assigned_object_type": "virtualization.vminterface",
						"assigned_object": map[string]any{
							"id":   991,
							"name": "eth0",
							"virtual_machine": map[string]any{
								"id":   130,
								"name": "NAT TEST11",
							},
						},
						"nat_outside": []map[string]any{{"id": 1, "address": "172.16.1.1/32"}},
					},
					{
						"id":                   657,
						"address":              "192.0.2.52/32",
						"assigned_object_type": "virtualization.vminterface",
						"assigned_object": map[string]any{
							"id":   992,
							"name": "eth1",
							"virtual_machine": map[string]any{
								"id":   130,
								"name": "NAT TEST11",
							},
						},
						"nat_outside": []map[string]any{{"id": 2, "address": "172.16.1.2/32"}},
					},
				},
			}
			_ = json.NewEncoder(w).Encode(resp)
			return
		case r.Method == http.MethodGet && r.URL.Path == "/api/virtualization/virtual-machines/":
			vmListHandler(&vmListCalls, []map[string]any{
				{
					"id":            130,
					"name":          "NAT TEST11",
					"custom_fields": map[string]any{"proxmox_vmid": 999},
				},
			})(w, r)
			return
		default:
			http.NotFound(w, r)
			return
		}
	}))
	defer srv.Close()

	client := NewClient(srv.URL, "test-token")
	mappings, err := client.FetchNATMappings(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(vmListCalls) != 1 {
		t.Fatalf("expected one batched VM list call for two IPs on same VM, got %d: %v", len(vmListCalls), vmListCalls)
	}
	if vmListCalls[0] != "130" {
		t.Fatalf("expected deduplicated id=130, got %q", vmListCalls[0])
	}
	if len(mappings) != 2 {
		t.Fatalf("expected two mappings, got %d", len(mappings))
	}
	for i, m := range mappings {
		if m.VMProxmoxVMID != 999 {
			t.Fatalf("mapping %d expected proxmox VMID 999, got %d", i, m.VMProxmoxVMID)
		}
	}
}

// A VM with no proxmox_vmid must be skipped rather than failing the whole fetch.
func TestFetchNATMappingsSkipsVMWithoutProxmoxVMID(t *testing.T) {
	var vmListCalls []string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/api/ipam/ip-addresses/"):
			resp := map[string]any{
				"count": 1,
				"next":  nil,
				"results": []map[string]any{
					{
						"id":                   656,
						"address":              "192.0.2.51/32",
						"assigned_object_type": "virtualization.vminterface",
						"assigned_object": map[string]any{
							"id":   991,
							"name": "eth0",
							"virtual_machine": map[string]any{
								"id":   130,
								"name": "NAT TEST11",
							},
						},
						"nat_outside": []map[string]any{{"id": 1, "address": "172.16.1.1/32"}},
					},
				},
			}
			_ = json.NewEncoder(w).Encode(resp)
			return
		case r.Method == http.MethodGet && r.URL.Path == "/api/virtualization/virtual-machines/":
			vmListHandler(&vmListCalls, []map[string]any{
				{"id": 130, "name": "NAT TEST11", "custom_fields": map[string]any{}},
			})(w, r)
			return
		default:
			http.NotFound(w, r)
			return
		}
	}))
	defer srv.Close()

	client := NewClient(srv.URL, "test-token")
	mappings, err := client.FetchNATMappings(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(mappings) != 0 {
		t.Fatalf("expected no mappings for a VM without proxmox_vmid, got %d", len(mappings))
	}
}
