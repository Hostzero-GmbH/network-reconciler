package netbox_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/hostzero/network-reconciler/internal/netbox"
)

// mockNetbox stubs /api/extras/webhooks/ and /api/extras/event-rules/.
type mockNetbox struct {
	webhooks   []map[string]any
	eventRules []map[string]any
	nextID     int
	writes     int
	t          *testing.T
}

func newMockNetbox(t *testing.T) (*mockNetbox, *httptest.Server) {
	t.Helper()
	m := &mockNetbox{t: t, nextID: 1}
	srv := httptest.NewServer(m)
	return m, srv
}

func (m *mockNetbox) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p := r.URL.Path
	switch {
	case r.Method == http.MethodGet && strings.HasPrefix(p, "/api/extras/webhooks/"):
		m.handleList(w, r, &m.webhooks)
	case r.Method == http.MethodPost && p == "/api/extras/webhooks/":
		m.handleCreate(w, r, &m.webhooks)
	case r.Method == http.MethodPatch && strings.HasPrefix(p, "/api/extras/webhooks/"):
		m.handlePatch(w, r, &m.webhooks)

	case r.Method == http.MethodGet && strings.HasPrefix(p, "/api/extras/event-rules/"):
		m.handleList(w, r, &m.eventRules)
	case r.Method == http.MethodPost && p == "/api/extras/event-rules/":
		m.handleCreate(w, r, &m.eventRules)
	case r.Method == http.MethodPatch && strings.HasPrefix(p, "/api/extras/event-rules/"):
		m.handlePatch(w, r, &m.eventRules)

	default:
		http.Error(w, "not found", http.StatusNotFound)
	}
}

func (m *mockNetbox) handleList(w http.ResponseWriter, r *http.Request, store *[]map[string]any) {
	name := r.URL.Query().Get("name")
	var results []map[string]any
	for _, obj := range *store {
		if name == "" || obj["name"] == name {
			results = append(results, obj)
		}
	}
	if results == nil {
		results = []map[string]any{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"count": len(results), "next": nil, "results": results})
}

func (m *mockNetbox) handleCreate(w http.ResponseWriter, r *http.Request, store *[]map[string]any) {
	m.writes++
	var body map[string]any
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	body["id"] = m.nextID
	m.nextID++
	*store = append(*store, body)
	writeJSON(w, http.StatusCreated, body)
}

func (m *mockNetbox) handlePatch(w http.ResponseWriter, r *http.Request, store *[]map[string]any) {
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	idStr := parts[len(parts)-1]
	m.writes++
	var targetID int
	fmt.Sscanf(idStr, "%d", &targetID) //nolint:errcheck

	var body map[string]any
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	for i, obj := range *store {
		if toInt(obj["id"]) == targetID {
			body["id"] = targetID
			(*store)[i] = body
			writeJSON(w, http.StatusOK, body)
			return
		}
	}
	http.Error(w, "not found", http.StatusNotFound)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v) //nolint:errcheck
}

// toInt normalises numeric JSON values (float64 after decode, int from pre-set fixtures).
func toInt(v any) int {
	switch n := v.(type) {
	case int:
		return n
	case float64:
		return int(n)
	case int64:
		return int(n)
	}
	return -1
}

// ── Tests ─────────────────────────────────────────────────────────────────────

func TestEnsureWebhookCreatesWebhookAndEventRule(t *testing.T) {
	mock, srv := newMockNetbox(t)
	defer srv.Close()

	client := netbox.NewClient(srv.URL, "test-token")
	err := client.EnsureWebhook(context.Background(), "network-reconciler-pve01", "http://pve01:9095/webhook", "secret")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(mock.webhooks) != 1 {
		t.Fatalf("expected 1 webhook, got %d", len(mock.webhooks))
	}
	if mock.webhooks[0]["name"] != "network-reconciler-pve01" {
		t.Errorf("webhook name: got %v", mock.webhooks[0]["name"])
	}
	if mock.webhooks[0]["payload_url"] != "http://pve01:9095/webhook" {
		t.Errorf("webhook payload_url: got %v", mock.webhooks[0]["payload_url"])
	}
	bodyTemplate, ok := mock.webhooks[0]["body_template"].(string)
	if !ok || bodyTemplate == "" {
		t.Fatalf("expected webhook body_template to be set, got %v", mock.webhooks[0]["body_template"])
	}
	if !strings.Contains(bodyTemplate, "object_type.split('.')[-1]") {
		t.Errorf("webhook body_template should derive model from object_type, got: %s", bodyTemplate)
	}
	if !strings.Contains(bodyTemplate, "vm_proxmox_vmid") {
		t.Errorf("webhook body_template should include vm_proxmox_vmid, got: %s", bodyTemplate)
	}
	if !strings.Contains(bodyTemplate, "nat_outside") {
		t.Errorf("webhook body_template should include nat_outside, got: %s", bodyTemplate)
	}
	// content_types must NOT appear on the webhook in Netbox v4.
	if _, hasContentTypes := mock.webhooks[0]["content_types"]; hasContentTypes {
		t.Error("webhook must not have content_types field (Netbox v4 uses event rules)")
	}

	if len(mock.eventRules) != 1 {
		t.Fatalf("expected 1 event rule, got %d", len(mock.eventRules))
	}
	if mock.eventRules[0]["action_type"] != "webhook" {
		t.Errorf("event rule action_type: got %v", mock.eventRules[0]["action_type"])
	}
	if mock.eventRules[0]["action_object_type"] != "extras.webhook" {
		t.Errorf("event rule action_object_type: got %v", mock.eventRules[0]["action_object_type"])
	}
	if toInt(mock.eventRules[0]["action_object_id"]) != toInt(mock.webhooks[0]["id"]) {
		t.Errorf("event rule action_object_id %v != webhook id %v",
			mock.eventRules[0]["action_object_id"], mock.webhooks[0]["id"])
	}
}

func TestEnsureWebhookEventRuleObjectAndEventTypes(t *testing.T) {
	mock, srv := newMockNetbox(t)
	defer srv.Close()

	client := netbox.NewClient(srv.URL, "test-token")
	_ = client.EnsureWebhook(context.Background(), "network-reconciler-pve01", "http://pve01:9095/webhook", "")

	if len(mock.eventRules) == 0 {
		t.Fatal("no event rule created")
	}

	wantObjectTypes := map[string]bool{
		"ipam.ipaddress":                false,
		"virtualization.virtualmachine": false,
		"virtualization.vminterface":    false,
	}
	for _, ct := range mock.eventRules[0]["object_types"].([]interface{}) {
		wantObjectTypes[ct.(string)] = true
	}
	for name, found := range wantObjectTypes {
		if !found {
			t.Errorf("missing object_type %q in event rule", name)
		}
	}

	wantEventTypes := map[string]bool{
		"object_created": false,
		"object_updated": false,
		"object_deleted": false,
	}
	for _, et := range mock.eventRules[0]["event_types"].([]interface{}) {
		wantEventTypes[et.(string)] = true
	}
	for name, found := range wantEventTypes {
		if !found {
			t.Errorf("missing event_type %q in event rule", name)
		}
	}
}

func TestEnsureWebhookNoopWhenBothMatch(t *testing.T) {
	mock, srv := newMockNetbox(t)
	defer srv.Close()

	webhookID := 1
	mock.webhooks = []map[string]any{{
		"id":            webhookID,
		"name":          "network-reconciler-pve01",
		"payload_url":   "http://pve01:9095/webhook",
		"body_template": `{"event":"{{ event }}","model":"{{ object_type.split('.')[-1] }}","object_type":"{{ object_type }}","request_id":{% if request and request.id %}"{{ request.id }}"{% elif request_id %}"{{ request_id }}"{% else %}null{% endif %},"data":{"url":"{{ data.url }}"}}`,
	}}
	mock.eventRules = []map[string]any{{"id": 10, "name": "network-reconciler-pve01", "action_object_id": float64(webhookID)}}
	mock.nextID = 11

	client := netbox.NewClient(srv.URL, "test-token")
	err := client.EnsureWebhook(context.Background(), "network-reconciler-pve01", "http://pve01:9095/webhook", "secret")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(mock.webhooks) != 1 {
		t.Errorf("expected 1 webhook (no duplicate), got %d", len(mock.webhooks))
	}
	if len(mock.eventRules) != 1 {
		t.Errorf("expected 1 event rule (no duplicate), got %d", len(mock.eventRules))
	}
}

func TestEnsureWebhookPatchesWebhookWhenURLDiffers(t *testing.T) {
	mock, srv := newMockNetbox(t)
	defer srv.Close()

	webhookID := 1
	mock.webhooks = []map[string]any{{
		"id":            webhookID,
		"name":          "network-reconciler-pve01",
		"payload_url":   "http://10.0.0.5:9095/webhook",
		"body_template": `{"event":"{{ event }}","model":"{{ object_type.split('.')[-1] }}","object_type":"{{ object_type }}","request_id":{% if request and request.id %}"{{ request.id }}"{% elif request_id %}"{{ request_id }}"{% else %}null{% endif %},"data":{"url":"{{ data.url }}"}}`,
	}}
	mock.eventRules = []map[string]any{{"id": 10, "name": "network-reconciler-pve01", "action_object_id": float64(webhookID)}}
	mock.nextID = 11

	client := netbox.NewClient(srv.URL, "test-token")
	newURL := "http://pve01:9095/webhook"
	err := client.EnsureWebhook(context.Background(), "network-reconciler-pve01", newURL, "secret")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(mock.webhooks) != 1 {
		t.Fatalf("expected 1 webhook (patched, not duplicated), got %d", len(mock.webhooks))
	}
	if mock.webhooks[0]["payload_url"] != newURL {
		t.Errorf("expected updated payload_url %q, got %v", newURL, mock.webhooks[0]["payload_url"])
	}
	// Webhook ID unchanged → event rule still valid, should not be patched.
	if len(mock.eventRules) != 1 {
		t.Errorf("expected 1 event rule (unchanged), got %d", len(mock.eventRules))
	}
}

func TestEnsureWebhookPatchesEventRuleWhenWebhookRecreated(t *testing.T) {
	mock, srv := newMockNetbox(t)
	defer srv.Close()

	newWebhookID := 42
	mock.webhooks = []map[string]any{{
		"id":            newWebhookID,
		"name":          "network-reconciler-pve01",
		"payload_url":   "http://pve01:9095/webhook",
		"body_template": `{"event":"{{ event }}","model":"{{ object_type.split('.')[-1] }}","object_type":"{{ object_type }}","request_id":{% if request and request.id %}"{{ request.id }}"{% elif request_id %}"{{ request_id }}"{% else %}null{% endif %},"data":{"url":"{{ data.url }}"}}`,
	}}
	mock.eventRules = []map[string]any{{"id": 10, "name": "network-reconciler-pve01", "action_object_id": float64(99)}} // stale ID
	mock.nextID = 100

	client := netbox.NewClient(srv.URL, "test-token")
	err := client.EnsureWebhook(context.Background(), "network-reconciler-pve01", "http://pve01:9095/webhook", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(mock.eventRules) != 1 {
		t.Fatalf("expected 1 event rule (patched), got %d", len(mock.eventRules))
	}
	if toInt(mock.eventRules[0]["action_object_id"]) != newWebhookID {
		t.Errorf("expected event rule action_object_id %d, got %d", newWebhookID, toInt(mock.eventRules[0]["action_object_id"]))
	}
}

func TestEnsureWebhookCreatesEventRuleWhenMissing(t *testing.T) {
	mock, srv := newMockNetbox(t)
	defer srv.Close()

	webhookID := 7
	mock.webhooks = []map[string]any{{
		"id":            webhookID,
		"name":          "network-reconciler-pve01",
		"payload_url":   "http://pve01:9095/webhook",
		"body_template": `{"event":"{{ event }}","model":"{{ object_type.split('.')[-1] }}","object_type":"{{ object_type }}","request_id":{% if request and request.id %}"{{ request.id }}"{% elif request_id %}"{{ request_id }}"{% else %}null{% endif %},"data":{"url":"{{ data.url }}"}}`,
	}}
	mock.nextID = 20

	client := netbox.NewClient(srv.URL, "test-token")
	err := client.EnsureWebhook(context.Background(), "network-reconciler-pve01", "http://pve01:9095/webhook", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(mock.webhooks) != 1 {
		t.Errorf("expected 1 webhook (not recreated), got %d", len(mock.webhooks))
	}
	if len(mock.eventRules) != 1 {
		t.Fatalf("expected 1 event rule created, got %d", len(mock.eventRules))
	}
	if toInt(mock.eventRules[0]["action_object_id"]) != webhookID {
		t.Errorf("expected action_object_id %d, got %d", webhookID, toInt(mock.eventRules[0]["action_object_id"]))
	}
}

func TestEnsureWebhookPatchesWebhookWhenTemplateDiffers(t *testing.T) {
	mock, srv := newMockNetbox(t)
	defer srv.Close()

	webhookID := 5
	mock.webhooks = []map[string]any{{
		"id":            webhookID,
		"name":          "network-reconciler-pve01",
		"payload_url":   "http://pve01:9095/webhook",
		"body_template": `{"event":"{{ event }}","model":"","data":{{ data }}}`,
	}}
	mock.eventRules = []map[string]any{{"id": 12, "name": "network-reconciler-pve01", "action_object_id": float64(webhookID)}}

	client := netbox.NewClient(srv.URL, "test-token")
	err := client.EnsureWebhook(context.Background(), "network-reconciler-pve01", "http://pve01:9095/webhook", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	bodyTemplate, ok := mock.webhooks[0]["body_template"].(string)
	if !ok || bodyTemplate == "" {
		t.Fatalf("expected updated body_template, got %v", mock.webhooks[0]["body_template"])
	}
	if !strings.Contains(bodyTemplate, "object_type") {
		t.Errorf("expected template to include object_type, got: %s", bodyTemplate)
	}
}

func TestEnsureWebhookEnablesDisabledEventRule(t *testing.T) {
	mock, srv := newMockNetbox(t)
	defer srv.Close()

	webhookID := 3
	mock.webhooks = []map[string]any{{
		"id":                webhookID,
		"name":              "network-reconciler-pve01",
		"payload_url":       "http://10.0.0.5:9095/webhook",
		"body_template":     realBodyTemplate(t),
		"http_method":       "POST",
		"http_content_type": "application/json",
	}}
	// Rule points at the right webhook but somebody disabled it → nothing is delivered.
	mock.eventRules = []map[string]any{{
		"id":               10,
		"name":             "network-reconciler-pve01",
		"enabled":          false,
		"object_types":     []any{"ipam.ipaddress", "virtualization.virtualmachine", "virtualization.vminterface"},
		"event_types":      []any{"object_created", "object_updated", "object_deleted"},
		"action_object_id": float64(webhookID),
	}}
	mock.nextID = 11

	client := netbox.NewClient(srv.URL, "test-token")
	if err := client.EnsureWebhook(context.Background(), "network-reconciler-pve01", "http://10.0.0.5:9095/webhook", ""); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(mock.eventRules) != 1 {
		t.Fatalf("expected 1 event rule, got %d", len(mock.eventRules))
	}
	if enabled, _ := mock.eventRules[0]["enabled"].(bool); !enabled {
		t.Errorf("expected disabled event rule to be re-enabled, got %v", mock.eventRules[0]["enabled"])
	}
}

func TestEnsureWebhookRepairsIncompleteEventRuleTypes(t *testing.T) {
	mock, srv := newMockNetbox(t)
	defer srv.Close()

	webhookID := 4
	mock.webhooks = []map[string]any{{
		"id":                webhookID,
		"name":              "network-reconciler-pve01",
		"payload_url":       "http://10.0.0.5:9095/webhook",
		"body_template":     realBodyTemplate(t),
		"http_method":       "POST",
		"http_content_type": "application/json",
	}}
	// Missing vminterface and the delete event → IP re-assignments never arrive.
	mock.eventRules = []map[string]any{{
		"id":               10,
		"name":             "network-reconciler-pve01",
		"enabled":          true,
		"object_types":     []any{"ipam.ipaddress"},
		"event_types":      []any{"object_created", "object_updated"},
		"action_object_id": float64(webhookID),
	}}
	mock.nextID = 11

	client := netbox.NewClient(srv.URL, "test-token")
	if err := client.EnsureWebhook(context.Background(), "network-reconciler-pve01", "http://10.0.0.5:9095/webhook", ""); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	objectTypes := toStringSlice(mock.eventRules[0]["object_types"])
	for _, want := range []string{"ipam.ipaddress", "virtualization.virtualmachine", "virtualization.vminterface"} {
		if !contains(objectTypes, want) {
			t.Errorf("expected object_types to include %q, got %v", want, objectTypes)
		}
	}
	eventTypes := toStringSlice(mock.eventRules[0]["event_types"])
	if !contains(eventTypes, "object_deleted") {
		t.Errorf("expected event_types to include object_deleted, got %v", eventTypes)
	}
}

func TestEnsureWebhookPatchesSecretDrift(t *testing.T) {
	mock, srv := newMockNetbox(t)
	defer srv.Close()

	webhookID := 5
	mock.webhooks = []map[string]any{{
		"id":                webhookID,
		"name":              "network-reconciler-pve01",
		"payload_url":       "http://10.0.0.5:9095/webhook",
		"body_template":     realBodyTemplate(t),
		"http_method":       "POST",
		"http_content_type": "application/json",
		"secret":            "stale-secret",
	}}
	mock.eventRules = []map[string]any{{
		"id":               10,
		"name":             "network-reconciler-pve01",
		"enabled":          true,
		"object_types":     []any{"ipam.ipaddress", "virtualization.virtualmachine", "virtualization.vminterface"},
		"event_types":      []any{"object_created", "object_updated", "object_deleted"},
		"action_object_id": float64(webhookID),
	}}
	mock.nextID = 11

	client := netbox.NewClient(srv.URL, "test-token")
	if err := client.EnsureWebhook(context.Background(), "network-reconciler-pve01", "http://10.0.0.5:9095/webhook", "current-secret"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got := mock.webhooks[0]["secret"]; got != "current-secret" {
		t.Errorf("expected secret to be re-synced to the configured value, got %v", got)
	}
}

func TestEnsureWebhookLeavesMatchingObjectsUntouched(t *testing.T) {
	mock, srv := newMockNetbox(t)
	defer srv.Close()

	webhookID := 6
	mock.webhooks = []map[string]any{{
		"id":                webhookID,
		"name":              "network-reconciler-pve01",
		"payload_url":       "http://10.0.0.5:9095/webhook",
		"body_template":     realBodyTemplate(t),
		"http_method":       "POST",
		"http_content_type": "application/json",
		"secret":            "current-secret",
	}}
	mock.eventRules = []map[string]any{{
		"id":               10,
		"name":             "network-reconciler-pve01",
		"enabled":          true,
		"object_types":     []any{"ipam.ipaddress", "virtualization.virtualmachine", "virtualization.vminterface"},
		"event_types":      []any{"object_created", "object_updated", "object_deleted"},
		"action_object_id": float64(webhookID),
	}}

	client := netbox.NewClient(srv.URL, "test-token")
	if err := client.EnsureWebhook(context.Background(), "network-reconciler-pve01", "http://10.0.0.5:9095/webhook", "current-secret"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if mock.writes != 0 {
		t.Errorf("expected no writes when everything already matches, got %d", mock.writes)
	}
}

// realBodyTemplate returns the body template the client registers, so drift tests
// only exercise the field they intend to change.
func realBodyTemplate(t *testing.T) string {
	t.Helper()
	mock, srv := newMockNetbox(t)
	defer srv.Close()

	client := netbox.NewClient(srv.URL, "test-token")
	if err := client.EnsureWebhook(context.Background(), "tmpl-probe", "http://probe:9095/webhook", ""); err != nil {
		t.Fatalf("probing body template: %v", err)
	}
	return mock.webhooks[0]["body_template"].(string)
}

func toStringSlice(v any) []string {
	items, ok := v.([]any)
	if !ok {
		if strs, ok := v.([]string); ok {
			return strs
		}
		return nil
	}
	out := make([]string, 0, len(items))
	for _, item := range items {
		if s, ok := item.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

func contains(values []string, want string) bool {
	for _, v := range values {
		if v == want {
			return true
		}
	}
	return false
}
