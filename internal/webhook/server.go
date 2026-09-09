// Package webhook provides an HTTP server that receives Netbox webhook
// notifications and triggers an immediate reconcile when relevant objects change.
//
// Netbox configuration (Settings → Webhooks → Add webhook):
//   - URL: http://<node-ip>:9095/webhook
//   - HTTP method: POST
//   - Content type: application/json
//   - Secret: <same value as webhook.secret in config.yaml>
//   - Events: Created, Updated, Deleted
//   - Object types: IPAM > IP address, Virtualization > Virtual machine,
//     Virtualization > VM interface
package webhook

import (
	"context"
	"crypto/hmac"
	"crypto/sha512"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"

	"go.uber.org/zap"
)

const (
	maxBodyBytes    = 1 << 20 // 1 MiB — generous upper bound for a Netbox payload
	signatureHeader = "X-Hook-Signature"
)

// relevantModels is the set of Netbox object types that can affect NAT mappings.
// Changes to any of these trigger an immediate reconcile.
var relevantModels = map[string]bool{
	"ipaddress":      true,
	"virtualmachine": true,
	"vminterface":    true,
}

// payload is the Netbox webhook POST body schema.
type payload struct {
	Event      string          `json:"event"` // "created" | "updated" | "deleted"
	Timestamp  string          `json:"timestamp"`
	Model      string          `json:"model"`       // e.g. "ipaddress"
	ObjectType string          `json:"object_type"` // e.g. "virtualization.virtualmachine"
	Username   string          `json:"username"`
	RequestID  string          `json:"request_id"`
	Data       json.RawMessage `json:"data"`
}

// Delta carries webhook payload fields used for incremental reconcile updates.
type Delta struct {
	Event         string
	Model         string
	ObjectType    string
	RequestID     string
	VMName        string
	VMProxmoxVMID int
	HasVMID       bool
	InternalIP    string
	NATOutside    []string
}

// ApplyDeltaFunc receives parsed webhook delta data and should return true when
// the delta was accepted for incremental handling (so full reconcile trigger can be skipped).
type ApplyDeltaFunc func(delta Delta) bool

// TriggerFunc is a callback invoked when a relevant webhook arrives.
// The reconciler's TriggerReconcile method satisfies this type.
type TriggerFunc func()

// Server is an HTTP server that receives Netbox webhook POSTs.
type Server struct {
	secret     string
	trigger    TriggerFunc
	applyDelta ApplyDeltaFunc
	log        *zap.Logger
	mux        *http.ServeMux
	srv        *http.Server
}

// New creates a Server that listens on listenAddr, validates payloads against
// secret (if non-empty), and calls trigger on relevant model changes.
func New(listenAddr, secret string, trigger TriggerFunc, log *zap.Logger) *Server {
	return NewWithDelta(listenAddr, secret, trigger, nil, log)
}

// NewWithDelta is like New, but also allows passing webhook payload data to an
// incremental reconcile path.
func NewWithDelta(listenAddr, secret string, trigger TriggerFunc, applyDelta ApplyDeltaFunc, log *zap.Logger) *Server {
	s := &Server{
		secret:     secret,
		trigger:    trigger,
		applyDelta: applyDelta,
		log:        log,
		mux:        http.NewServeMux(),
	}

	if secret == "" {
		log.Warn("webhook.secret is not set — HMAC signature verification is disabled; " +
			"set webhook.secret (or NR_WEBHOOK_SECRET) to match the Netbox webhook secret")
	}

	s.mux.HandleFunc("/webhook", s.handleWebhook)

	s.srv = &http.Server{
		Addr:         listenAddr,
		Handler:      s.mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	return s
}

// ServeHTTP implements http.Handler, enabling use with httptest.ResponseRecorder in tests.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

// Start begins serving. It blocks until the server is stopped; call Shutdown
// from another goroutine to stop it gracefully. Returns http.ErrServerClosed
// on a clean shutdown.
func (s *Server) Start() error {
	s.log.Info("webhook server listening", zap.String("addr", s.srv.Addr))
	return s.srv.ListenAndServe()
}

// Shutdown gracefully stops the server within the deadline of ctx.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.srv.Shutdown(ctx)
}

// handleWebhook processes a single Netbox webhook POST.
func (s *Server) handleWebhook(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, maxBodyBytes))
	if err != nil {
		http.Error(w, "error reading body", http.StatusBadRequest)
		return
	}

	if !s.verifySignature(body, r.Header.Get(signatureHeader)) {
		s.log.Warn("webhook rejected: invalid signature — check that webhook.secret matches the secret on the Netbox webhook object",
			zap.String("remote_addr", r.RemoteAddr),
			zap.Bool("signature_header_present", r.Header.Get(signatureHeader) != ""),
		)
		http.Error(w, "invalid signature", http.StatusUnauthorized)
		return
	}

	var p payload
	if err := json.Unmarshal(body, &p); err != nil {
		http.Error(w, "invalid JSON payload", http.StatusBadRequest)
		return
	}

	model := normalizeModel(p.Model)
	if model == "" {
		model = normalizeModel(p.ObjectType)
	}
	if model == "" {
		model = modelFromDataURL(p.Data)
	}

	s.log.Info("netbox webhook received",
		zap.String("event", p.Event),
		zap.String("model", model),
		zap.String("model_raw", p.Model),
		zap.String("object_type_raw", p.ObjectType),
		zap.String("data", string(p.Data)),
		zap.String("request_id", p.RequestID),
	)

	if relevantModels[model] {
		handledByDelta := false
		if s.applyDelta != nil {
			handledByDelta = s.applyDelta(buildDelta(p, model))
		}

		if handledByDelta {
			s.log.Debug("applied webhook delta",
				zap.String("model", model),
				zap.String("event", p.Event),
			)
		} else {
			s.log.Debug("triggering full reconcile from webhook",
				zap.String("model", model),
				zap.String("event", p.Event),
			)
			s.trigger()
		}
	} else {
		s.log.Debug("ignoring webhook for irrelevant model", zap.String("model", model))
	}

	w.WriteHeader(http.StatusNoContent)
}

// normalizeModel maps known Netbox model/object-type formats to the compact
// model names used by this service.
func normalizeModel(model string) string {
	switch strings.ToLower(strings.TrimSpace(model)) {
	case "ipaddress", "ip_address", "ip-address", "ipam.ipaddress":
		return "ipaddress"
	case "virtualmachine", "virtual_machine", "virtual-machine", "virtualization.virtualmachine":
		return "virtualmachine"
	case "vminterface", "vm_interface", "vm-interface", "virtualization.vminterface":
		return "vminterface"
	default:
		return ""
	}
}

// modelFromDataURL infers the model from data.url when explicit model fields
// are missing from the webhook payload.
func modelFromDataURL(data json.RawMessage) string {
	dURL := dataURLFromPayload(data)

	switch {
	case strings.HasPrefix(dURL, "/api/ipam/ip-addresses/"):
		return "ipaddress"
	case strings.HasPrefix(dURL, "/api/virtualization/virtual-machines/"):
		return "virtualmachine"
	case strings.HasPrefix(dURL, "/api/virtualization/interfaces/"):
		return "vminterface"
	default:
		return ""
	}
}

func dataURLFromPayload(data json.RawMessage) string {
	var d struct {
		URL string `json:"url"`
	}
	if err := json.Unmarshal(data, &d); err != nil {
		return ""
	}
	return strings.TrimSpace(d.URL)
}

func buildDelta(p payload, model string) Delta {
	type vmRef struct {
		Name string `json:"name"`
	}
	type assignedObject struct {
		VirtualMachine *vmRef `json:"virtual_machine"`
	}
	type dataEnvelope struct {
		VMName         string          `json:"vm_name"`
		Name           string          `json:"name"`
		InternalIP     string          `json:"internal_ip"`
		Address        string          `json:"address"`
		VMProxmoxVMID  *int            `json:"vm_proxmox_vmid"`
		NATOutsideRaw  json.RawMessage `json:"nat_outside"`
		AssignedObject *assignedObject `json:"assigned_object"`
	}

	var d dataEnvelope
	_ = json.Unmarshal(p.Data, &d)

	vmName := strings.TrimSpace(d.VMName)
	if vmName == "" && d.AssignedObject != nil && d.AssignedObject.VirtualMachine != nil {
		vmName = strings.TrimSpace(d.AssignedObject.VirtualMachine.Name)
	}
	if vmName == "" {
		vmName = strings.TrimSpace(d.Name)
	}

	internalIP := strings.TrimSpace(d.InternalIP)
	if internalIP == "" {
		internalIP = strings.TrimSpace(d.Address)
	}

	return Delta{
		Event:         strings.ToLower(strings.TrimSpace(p.Event)),
		Model:         model,
		ObjectType:    strings.TrimSpace(p.ObjectType),
		RequestID:     strings.TrimSpace(p.RequestID),
		VMName:        vmName,
		InternalIP:    internalIP,
		NATOutside:    parseNATOutside(d.NATOutsideRaw),
		VMProxmoxVMID: derefInt(d.VMProxmoxVMID),
		HasVMID:       d.VMProxmoxVMID != nil,
	}
}

func parseNATOutside(raw json.RawMessage) []string {
	if len(raw) == 0 {
		return nil
	}

	var asStrings []string
	if err := json.Unmarshal(raw, &asStrings); err == nil {
		return compactStrings(asStrings)
	}

	var asObjects []struct {
		Address string `json:"address"`
	}
	if err := json.Unmarshal(raw, &asObjects); err == nil {
		result := make([]string, 0, len(asObjects))
		for _, o := range asObjects {
			if a := strings.TrimSpace(o.Address); a != "" {
				result = append(result, a)
			}
		}
		return result
	}

	return nil
}

func compactStrings(values []string) []string {
	result := make([]string, 0, len(values))
	for _, v := range values {
		if s := strings.TrimSpace(v); s != "" {
			result = append(result, s)
		}
	}
	return result
}

func derefInt(v *int) int {
	if v == nil {
		return 0
	}
	return *v
}

// verifySignature checks the HMAC-SHA512 signature in the Netbox X-Hook-Signature header.
// Netbox sends the bare lowercase hex digest (extras.webhooks.generate_signature
// returns hmac.hexdigest()), so that is the canonical form. The "sha512=<hex>"
// prefixed form is also accepted for compatibility with proxies that add it.
// Returns true (passes) when no secret is configured — security is the operator's responsibility.
func (s *Server) verifySignature(body []byte, headerSig string) bool {
	if s.secret == "" {
		return true
	}
	got := strings.ToLower(strings.TrimSpace(headerSig))
	got = strings.TrimPrefix(got, "sha512=")
	// Use hmac.Equal for constant-time comparison to prevent timing attacks.
	return hmac.Equal([]byte(got), []byte(s.digest(body)))
}

// digest returns the lowercase hex HMAC-SHA512 of body, keyed with the shared secret.
func (s *Server) digest(body []byte) string {
	mac := hmac.New(sha512.New, []byte(s.secret))
	mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}
