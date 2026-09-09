package netbox

import (
	"encoding/json"
	"net/netip"
)

// NATMapping represents a single external-to-internal IP NAT mapping linked to a VM.
type NATMapping struct {
	ExternalIP    netip.Addr
	InternalIP    netip.Addr
	VMProxmoxVMID int
	VMName        string
}

// ipListResponse is the paginated Netbox REST API list envelope.
type ipListResponse struct {
	Count   int         `json:"count"`
	Next    *string     `json:"next"`
	Results []ipAddress `json:"results"`
}

// ipAddress represents a Netbox IP address object (subset of fields we use).
type ipAddress struct {
	ID                 int             `json:"id"`
	Address            string          `json:"address"` // CIDR notation, e.g. "10.0.1.100/24"
	AssignedObjectType *string         `json:"assigned_object_type"`
	AssignedObject     *assignedObject `json:"assigned_object"`
	NATOutside         []natIP         `json:"nat_outside"`
}

// assignedObject is the polymorphic interface/VM assignment on an IP address.
type assignedObject struct {
	ID             int             `json:"id"`
	Name           string          `json:"name"`
	VirtualMachine *virtualMachine `json:"virtual_machine"`
}

type virtualMachine struct {
	ID           int            `json:"id"`
	Name         string         `json:"name"`
	CustomFields map[string]any `json:"custom_fields"`
}

// vmListResponse is the paginated list envelope for GET
// /api/virtualization/virtual-machines/, used to batch-resolve proxmox_vmid.
type vmListResponse struct {
	Count   int              `json:"count"`
	Next    *string          `json:"next"`
	Results []virtualMachine `json:"results"`
}

// natIP is the abbreviated IP address representation used in nat_outside/nat_inside lists.
type natIP struct {
	ID      int    `json:"id"`
	Address string `json:"address"` // CIDR notation
}

// ── Webhook registration types ────────────────────────────────────────────────

// webhookListResponse is the paginated list response for GET /api/extras/webhooks/.
type webhookListResponse struct {
	Count   int             `json:"count"`
	Results []webhookObject `json:"results"`
}

// webhookObject is a Netbox webhook as returned by the API.
type webhookObject struct {
	ID              int    `json:"id"`
	Name            string `json:"name"`
	PayloadURL      string `json:"payload_url"`
	BodyTemplate    string `json:"body_template"`
	HTTPMethod      string `json:"http_method"`
	HTTPContentType string `json:"http_content_type"`
	// Secret is returned by the Netbox webhook serializer, so drift between the
	// local webhook.secret and Netbox can be detected and repaired.
	Secret string `json:"secret"`
}

// webhookRequest is the body sent for POST and PATCH /api/extras/webhooks/.
// In Netbox v4 the webhook object only defines the HTTP endpoint; content types
// and event types are configured on the associated EventRule instead.
type webhookRequest struct {
	Name            string `json:"name"`
	PayloadURL      string `json:"payload_url"`
	HTTPMethod      string `json:"http_method"`
	HTTPContentType string `json:"http_content_type"`
	BodyTemplate    string `json:"body_template"`
	Secret          string `json:"secret,omitempty"`
}

// ── Event rule types ─────────────────────────────────────────────────────────

// eventRuleListResponse is the paginated list response for GET /api/extras/event-rules/.
type eventRuleListResponse struct {
	Count   int               `json:"count"`
	Results []eventRuleObject `json:"results"`
}

// eventRuleObject is a Netbox event rule as returned by the API.
type eventRuleObject struct {
	ID             int            `json:"id"`
	Name           string         `json:"name"`
	Enabled        bool           `json:"enabled"`
	ObjectTypes    lenientStrings `json:"object_types"`
	EventTypes     lenientStrings `json:"event_types"`
	ActionObjectID *int64         `json:"action_object_id"` // nullable in schema
}

// lenientStrings decodes a JSON array whose items may be plain strings
// ("ipam.ipaddress"), choice objects ({"value": "object_created"}) or content-type
// objects ({"app_label": "ipam", "model": "ipaddress"}) — the Netbox API has used
// each of these shapes across v4 releases. Anything unrecognised decodes to an
// empty slice, which makes the caller PATCH the desired state instead of failing.
type lenientStrings []string

func (l *lenientStrings) UnmarshalJSON(b []byte) error {
	var raw []json.RawMessage
	if err := json.Unmarshal(b, &raw); err != nil {
		*l = nil
		return nil
	}

	out := make([]string, 0, len(raw))
	for _, item := range raw {
		var str string
		if err := json.Unmarshal(item, &str); err == nil {
			out = append(out, str)
			continue
		}
		var obj struct {
			Value    string `json:"value"`
			AppLabel string `json:"app_label"`
			Model    string `json:"model"`
		}
		if err := json.Unmarshal(item, &obj); err != nil {
			continue
		}
		switch {
		case obj.Value != "":
			out = append(out, obj.Value)
		case obj.AppLabel != "" && obj.Model != "":
			out = append(out, obj.AppLabel+"."+obj.Model)
		}
	}
	*l = out
	return nil
}

// eventRuleRequest is the body sent for POST and PATCH /api/extras/event-rules/.
type eventRuleRequest struct {
	Name             string   `json:"name"`
	ObjectTypes      []string `json:"object_types"`
	EventTypes       []string `json:"event_types"`
	ActionType       string   `json:"action_type"`
	ActionObjectType string   `json:"action_object_type"`
	ActionObjectID   int      `json:"action_object_id"`
	Enabled          bool     `json:"enabled"`
}
