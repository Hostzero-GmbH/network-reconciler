// Package netbox provides a minimal Netbox REST API client for fetching NAT mappings.
package netbox

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const vmInterfaceType = "virtualization.vminterface"

// Client is a thin Netbox REST API client.
type Client struct {
	baseURL string
	token   string
	http    *http.Client
}

// NewClient returns a new Netbox client for the given base URL and API token.
func NewClient(baseURL, token string) *Client {
	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		token:   token,
		http:    &http.Client{Timeout: 30 * time.Second},
	}
}

// FetchNATMappings returns all external→internal IP NAT mappings defined in Netbox.
//
// It queries IP addresses that have nat_outside populated (i.e. internal VM IPs
// that have a public NAT IP assigned). Pagination is handled transparently.
//
// Netbox data model assumed:
//   - Internal IP: assigned to a VM interface, has nat_outside pointing to external IP.
//   - External IP: has nat_inside pointing back to the internal IP.
func (c *Client) FetchNATMappings(ctx context.Context) ([]NATMapping, error) {
	// Collect the pages first, then resolve any missing proxmox_vmid values in one
	// batched query. Resolving inline used to issue a serial GET per VM, which is
	// what made a full fetch take seconds on a cluster of any size.
	type pendingIP struct {
		ipAddress
		vmNetboxID int
	}

	var (
		mappings   []NATMapping
		unresolved []pendingIP
		needVMIDs  []int
		seenVMID   = make(map[int]struct{})
	)

	url := c.baseURL + "/api/ipam/ip-addresses/?nat_outside__isnull=false&limit=200"
	for url != "" {
		var resp ipListResponse
		if err := c.get(ctx, url, &resp); err != nil {
			return nil, fmt.Errorf("fetching NAT mappings: %w", err)
		}

		for _, ip := range resp.Results {
			if !isVMInterface(ip) {
				continue
			}

			vmProxmoxVMID, ok := proxmoxVMIDFromVM(ip.AssignedObject.VirtualMachine)
			if ok {
				mappings = append(mappings, mappingsForIP(ip, vmProxmoxVMID)...)
				continue
			}

			vmNetboxID := ip.AssignedObject.VirtualMachine.ID
			if vmNetboxID <= 0 {
				continue // VMID is the sole identity key used by the reconciler
			}
			unresolved = append(unresolved, pendingIP{ipAddress: ip, vmNetboxID: vmNetboxID})
			if _, dup := seenVMID[vmNetboxID]; !dup {
				seenVMID[vmNetboxID] = struct{}{}
				needVMIDs = append(needVMIDs, vmNetboxID)
			}
		}

		if resp.Next != nil && *resp.Next != "" {
			url = *resp.Next
		} else {
			url = ""
		}
	}

	if len(needVMIDs) == 0 {
		return mappings, nil
	}

	resolved, err := c.fetchProxmoxVMIDs(ctx, needVMIDs)
	if err != nil {
		return nil, fmt.Errorf("resolving proxmox_vmid for %d VM(s): %w", len(needVMIDs), err)
	}

	for _, pending := range unresolved {
		vmProxmoxVMID, ok := resolved[pending.vmNetboxID]
		if !ok {
			continue
		}
		mappings = append(mappings, mappingsForIP(pending.ipAddress, vmProxmoxVMID)...)
	}

	return mappings, nil
}

// mappingsForIP expands one internal IP into a mapping per NAT-outside address.
func mappingsForIP(ip ipAddress, vmProxmoxVMID int) []NATMapping {
	internalIP, err := parseHostIP(ip.Address)
	if err != nil {
		return nil // skip malformed entries silently; Netbox data issue
	}

	vmName := ip.AssignedObject.VirtualMachine.Name
	out := make([]NATMapping, 0, len(ip.NATOutside))
	for _, ext := range ip.NATOutside {
		externalIP, err := parseHostIP(ext.Address)
		if err != nil {
			continue
		}
		out = append(out, NATMapping{
			ExternalIP:    externalIP,
			InternalIP:    internalIP,
			VMProxmoxVMID: vmProxmoxVMID,
			VMName:        vmName,
		})
	}
	return out
}

// fetchProxmoxVMIDs resolves Netbox VM ids to proxmox_vmid custom field values with
// one paginated batch query per 100 ids rather than one request per VM. VMs without
// the custom field set are simply absent from the result.
func (c *Client) fetchProxmoxVMIDs(ctx context.Context, netboxIDs []int) (map[int]int, error) {
	const batchSize = 100

	resolved := make(map[int]int, len(netboxIDs))
	for start := 0; start < len(netboxIDs); start += batchSize {
		end := start + batchSize
		if end > len(netboxIDs) {
			end = len(netboxIDs)
		}

		// An id set is expressed as the parameter repeated (?id=1&id=2). The `__in`
		// lookup this used to send was removed in Netbox 3.0, and unknown filters are
		// rejected outright with HTTP 400 rather than ignored, so `id__in=1,2` would
		// fail the whole fetch on any current Netbox.
		params := url.Values{}
		for _, id := range netboxIDs[start:end] {
			params.Add("id", strconv.Itoa(id))
		}
		params.Set("limit", strconv.Itoa(batchSize))

		endpoint := c.baseURL + "/api/virtualization/virtual-machines/?" + params.Encode()
		for endpoint != "" {
			var resp vmListResponse
			if err := c.get(ctx, endpoint, &resp); err != nil {
				return nil, err
			}
			for i := range resp.Results {
				vm := &resp.Results[i]
				if vmid, ok := proxmoxVMIDFromVM(vm); ok {
					resolved[vm.ID] = vmid
				}
			}
			if resp.Next != nil && *resp.Next != "" {
				endpoint = *resp.Next
			} else {
				endpoint = ""
			}
		}
	}
	return resolved, nil
}

func proxmoxVMIDFromVM(vm *virtualMachine) (int, bool) {
	if vm == nil || vm.CustomFields == nil {
		return 0, false
	}
	v, ok := vm.CustomFields["proxmox_vmid"]
	if !ok || v == nil {
		return 0, false
	}

	switch n := v.(type) {
	case float64:
		if n > 0 {
			return int(n), true
		}
	case int:
		if n > 0 {
			return n, true
		}
	case int64:
		if n > 0 {
			return int(n), true
		}
	case string:
		n = strings.TrimSpace(n)
		if n == "" {
			return 0, false
		}
		parsed, err := strconv.Atoi(n)
		if err == nil && parsed > 0 {
			return parsed, true
		}
	}

	return 0, false
}

// get performs a GET request authenticated with the API token and decodes the
// JSON response body into dest.
func (c *Client) get(ctx context.Context, url string, dest any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("creating request: %w", err)
	}
	req.Header.Set("Authorization", "Token "+c.token)
	req.Header.Set("Accept", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("executing request to %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("netbox API %s returned HTTP %d: %s", url, resp.StatusCode, errorBody(resp))
	}

	if err := json.NewDecoder(resp.Body).Decode(dest); err != nil {
		return fmt.Errorf("decoding response from %s: %w", url, err)
	}
	return nil
}

// post sends a JSON POST request and decodes the response into dest.
func (c *Client) post(ctx context.Context, endpoint string, body, dest any) error {
	return c.writeRequest(ctx, http.MethodPost, endpoint, body, dest, http.StatusCreated)
}

// patch sends a JSON PATCH request and decodes the response into dest.
func (c *Client) patch(ctx context.Context, endpoint string, body, dest any) error {
	return c.writeRequest(ctx, http.MethodPatch, endpoint, body, dest, http.StatusOK)
}

func (c *Client) writeRequest(ctx context.Context, method, endpoint string, body, dest any, expectStatus int) error {
	data, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("marshalling request body: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, method, endpoint, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("creating %s request: %w", method, err)
	}
	req.Header.Set("Authorization", "Token "+c.token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("executing %s to %s: %w", method, endpoint, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != expectStatus {
		// Netbox explains rejected fields in the body (e.g. an event_types value that
		// the running Netbox version does not know), so surface it in the error.
		return fmt.Errorf("netbox %s %s returned HTTP %d (expected %d): %s",
			method, endpoint, resp.StatusCode, expectStatus, errorBody(resp))
	}

	if dest != nil {
		if err := json.NewDecoder(resp.Body).Decode(dest); err != nil {
			return fmt.Errorf("decoding response from %s %s: %w", method, endpoint, err)
		}
	}
	return nil
}

// errorBody reads a bounded prefix of an error response body for inclusion in an
// error message. Returns "<empty>" when there is nothing to read.
func errorBody(resp *http.Response) string {
	data, err := io.ReadAll(io.LimitReader(resp.Body, 512))
	if err != nil || len(data) == 0 {
		return "<empty>"
	}
	return strings.TrimSpace(string(data))
}

// managedObjectTypes are the Netbox content types (app_label.model_name format)
// whose changes should trigger reconciliation. Set on the EventRule, not the Webhook.
var managedObjectTypes = []string{
	"ipam.ipaddress",
	"virtualization.virtualmachine",
	"virtualization.vminterface",
}

// managedEventTypes are the Netbox v4 event type strings that trigger reconciliation.
var managedEventTypes = []string{
	"object_created",
	"object_updated",
	"object_deleted",
}

const (
	eventRuleActionType       = "webhook"
	eventRuleActionObjectType = "extras.webhook"
	webhookBodyTemplate       = `{"event":"{{ event }}","model":"{{ object_type.split('.')[-1] }}","object_type":"{{ object_type }}","request_id":{% if request and request.id %}"{{ request.id }}"{% elif request_id %}"{{ request_id }}"{% else %}null{% endif %},"data":{"url":"{{ data.url }}","vm_name":"{% if data.assigned_object and data.assigned_object.virtual_machine %}{{ data.assigned_object.virtual_machine.name }}{% else %}{{ data.name | default('', true) }}{% endif %}","vm_netbox_id":{% if data.assigned_object and data.assigned_object.virtual_machine %}{{ data.assigned_object.virtual_machine.id }}{% elif object_type == "virtualization.virtualmachine" %}{{ data.id }}{% else %}null{% endif %},"vm_proxmox_vmid":{% if object_type == "virtualization.virtualmachine" and data.custom_fields and data.custom_fields.proxmox_vmid is not none %}{{ data.custom_fields.proxmox_vmid }}{% elif data.assigned_object and data.assigned_object.virtual_machine and data.assigned_object.virtual_machine.custom_fields and data.assigned_object.virtual_machine.custom_fields.proxmox_vmid is not none %}{{ data.assigned_object.virtual_machine.custom_fields.proxmox_vmid }}{% else %}null{% endif %},"internal_ip":"{{ data.address | default('', true) }}","primary_ip":"{% if data.primary_ip4 and data.primary_ip4.address %}{{ data.primary_ip4.address }}{% elif data.primary_ip and data.primary_ip.address %}{{ data.primary_ip.address }}{% else %}{% endif %}","nat_outside":[{% if data.nat_outside %}{% for nat in data.nat_outside %}"{{ nat.address }}"{% if not loop.last %},{% endif %}{% endfor %}{% endif %}]}}`
)

// EnsureWebhook ensures this node's webhook endpoint and its associated event rule
// exist in Netbox and are correctly configured.
//
// In Netbox v4 this requires two objects:
//  1. A Webhook (/api/extras/webhooks/) — defines the HTTP endpoint, method, and secret.
//  2. An EventRule (/api/extras/event-rules/) — links object types and event types to
//     the webhook action. The event rule is named identically to the webhook.
//
// The function is idempotent:
//   - Creates missing objects.
//   - PATCHes objects whose key fields have changed (e.g. node IP change → new payload URL).
//   - No-ops when everything already matches.
//
// secret may be empty if the webhook server is running without HMAC verification.
func (c *Client) EnsureWebhook(ctx context.Context, name, payloadURL, secret string) error {
	webhookID, err := c.ensureWebhookEndpoint(ctx, name, payloadURL, secret)
	if err != nil {
		return err
	}
	return c.ensureEventRule(ctx, name, webhookID)
}

// ensureWebhookEndpoint creates or updates the Webhook object and returns its ID.
func (c *Client) ensureWebhookEndpoint(ctx context.Context, name, payloadURL, secret string) (int, error) {
	listURL := c.baseURL + "/api/extras/webhooks/?name=" + url.QueryEscape(name) + "&limit=1"

	var listResp webhookListResponse
	if err := c.get(ctx, listURL, &listResp); err != nil {
		return 0, fmt.Errorf("listing webhooks: %w", err)
	}

	body := webhookRequest{
		Name:            name,
		PayloadURL:      payloadURL,
		HTTPMethod:      "POST",
		HTTPContentType: "application/json",
		BodyTemplate:    webhookBodyTemplate,
		Secret:          secret,
	}

	if listResp.Count == 0 {
		var created webhookObject
		endpoint := c.baseURL + "/api/extras/webhooks/"
		if err := c.post(ctx, endpoint, body, &created); err != nil {
			return 0, fmt.Errorf("creating webhook %q: %w", name, err)
		}
		return created.ID, nil
	}

	existing := listResp.Results[0]
	if !webhookNeedsUpdate(existing, body) {
		return existing.ID, nil
	}

	// Something drifted (node IP change, template change, secret rotation) — patch it.
	endpoint := fmt.Sprintf("%s/api/extras/webhooks/%d/", c.baseURL, existing.ID)
	var updated webhookObject
	if err := c.patch(ctx, endpoint, body, &updated); err != nil {
		return 0, fmt.Errorf("updating webhook %q (id=%d): %w", name, existing.ID, err)
	}
	return existing.ID, nil
}

// ensureEventRule creates or updates the EventRule that links managed object types
// and event types to the webhook identified by webhookID.
func (c *Client) ensureEventRule(ctx context.Context, name string, webhookID int) error {
	listURL := c.baseURL + "/api/extras/event-rules/?name=" + url.QueryEscape(name) + "&limit=1"

	var listResp eventRuleListResponse
	if err := c.get(ctx, listURL, &listResp); err != nil {
		return fmt.Errorf("listing event rules: %w", err)
	}

	body := eventRuleRequest{
		Name:             name,
		ObjectTypes:      managedObjectTypes,
		EventTypes:       managedEventTypes,
		ActionType:       eventRuleActionType,
		ActionObjectType: eventRuleActionObjectType,
		ActionObjectID:   webhookID,
		Enabled:          true,
	}

	if listResp.Count == 0 {
		endpoint := c.baseURL + "/api/extras/event-rules/"
		if err := c.post(ctx, endpoint, body, nil); err != nil {
			return fmt.Errorf("creating event rule %q: %w", name, err)
		}
		return nil
	}

	existing := listResp.Results[0]
	if !eventRuleNeedsUpdate(existing, body) {
		return nil // already enabled and wired to the correct webhook and types
	}

	// The rule is disabled, wired to a stale webhook, or missing object/event types.
	endpoint := fmt.Sprintf("%s/api/extras/event-rules/%d/", c.baseURL, existing.ID)
	if err := c.patch(ctx, endpoint, body, nil); err != nil {
		return fmt.Errorf("updating event rule %q (id=%d): %w", name, existing.ID, err)
	}
	return nil
}

// webhookNeedsUpdate reports whether the webhook stored in Netbox differs from the
// state this node wants. An empty desired secret is ignored: it means HMAC
// verification is disabled locally, so whatever Netbox holds is left alone.
func webhookNeedsUpdate(existing webhookObject, want webhookRequest) bool {
	switch {
	case existing.PayloadURL != want.PayloadURL,
		existing.BodyTemplate != want.BodyTemplate,
		existing.HTTPMethod != "" && !strings.EqualFold(existing.HTTPMethod, want.HTTPMethod),
		existing.HTTPContentType != "" && existing.HTTPContentType != want.HTTPContentType,
		want.Secret != "" && existing.Secret != want.Secret:
		return true
	}
	return false
}

// eventRuleNeedsUpdate reports whether the event rule stored in Netbox differs from
// the state this node wants. A rule that is disabled or no longer covers all managed
// object/event types delivers nothing, so it must be repaired even when it already
// points at the right webhook.
func eventRuleNeedsUpdate(existing eventRuleObject, want eventRuleRequest) bool {
	if existing.ActionObjectID == nil || int(*existing.ActionObjectID) != want.ActionObjectID {
		return true
	}
	if !existing.Enabled {
		return true
	}
	return !coversAll(existing.ObjectTypes, want.ObjectTypes) ||
		!coversAll(existing.EventTypes, want.EventTypes)
}

// coversAll reports whether have contains every element of want.
func coversAll(have lenientStrings, want []string) bool {
	set := make(map[string]struct{}, len(have))
	for _, v := range have {
		set[v] = struct{}{}
	}
	for _, v := range want {
		if _, ok := set[v]; !ok {
			return false
		}
	}
	return true
}

// isVMInterface returns true if the IP address is assigned to a VM interface.
func isVMInterface(ip ipAddress) bool {
	if ip.AssignedObjectType == nil || *ip.AssignedObjectType != vmInterfaceType {
		return false
	}
	if ip.AssignedObject == nil || ip.AssignedObject.VirtualMachine == nil {
		return false
	}
	if ip.AssignedObject.VirtualMachine.Name == "" {
		return false
	}
	return true
}

// parseHostIP parses a CIDR-notation address and returns the host part.
// E.g. "10.0.1.100/24" → 10.0.1.100.
func parseHostIP(cidr string) (netip.Addr, error) {
	prefix, err := netip.ParsePrefix(cidr)
	if err != nil {
		// Try as a bare IP address (no prefix).
		addr, err2 := netip.ParseAddr(cidr)
		if err2 != nil {
			return netip.Addr{}, fmt.Errorf("parsing IP %q: %w", cidr, err)
		}
		return addr, nil
	}
	return prefix.Addr(), nil
}
