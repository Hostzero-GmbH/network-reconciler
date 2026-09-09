package webhook_test

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha512"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"go.uber.org/zap"

	"github.com/hostzero/network-reconciler/internal/webhook"
)

const testSecret = "super-secret-test-key"

// sign returns the "sha512=<hex>" HMAC for body using the test secret.
func sign(body []byte) string {
	mac := hmac.New(sha512.New, []byte(testSecret))
	mac.Write(body)
	return "sha512=" + hex.EncodeToString(mac.Sum(nil))
}

func newTestServer(t *testing.T, secret string, triggered *int) *webhook.Server {
	t.Helper()
	srv := webhook.New(":0", secret, func() { *triggered++ }, zap.NewNop())
	return srv
}

func postWebhook(t *testing.T, srv *webhook.Server, body []byte, sig string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if sig != "" {
		req.Header.Set("X-Hook-Signature", sig)
	}
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)
	return rr
}

func makeBody(model, event string) []byte {
	b, _ := json.Marshal(map[string]string{
		"event": event, "model": model, "request_id": "test-uuid",
	})
	return b
}

func makeBodyWithData(event, model, objectType, dataURL string) []byte {
	b, _ := json.Marshal(map[string]any{
		"event":       event,
		"model":       model,
		"object_type": objectType,
		"request_id":  "test-uuid",
		"data": map[string]string{
			"url": dataURL,
		},
	})
	return b
}

func TestValidSignatureTriggers(t *testing.T) {
	triggered := 0
	srv := newTestServer(t, testSecret, &triggered)

	body := makeBody("ipaddress", "updated")
	rr := postWebhook(t, srv, body, sign(body))

	if rr.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", rr.Code)
	}
	if triggered != 1 {
		t.Fatalf("expected trigger called once, got %d", triggered)
	}
}

func TestInvalidSignatureRejected(t *testing.T) {
	triggered := 0
	srv := newTestServer(t, testSecret, &triggered)

	body := makeBody("ipaddress", "updated")
	rr := postWebhook(t, srv, body, "sha512=badhex")

	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rr.Code)
	}
	if triggered != 0 {
		t.Fatalf("trigger must not be called on bad signature, got %d calls", triggered)
	}
}

func TestMissingSignatureRejectedWhenSecretSet(t *testing.T) {
	triggered := 0
	srv := newTestServer(t, testSecret, &triggered)

	body := makeBody("ipaddress", "updated")
	rr := postWebhook(t, srv, body, "") // no header

	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 when secret configured but no sig header, got %d", rr.Code)
	}
	if triggered != 0 {
		t.Fatal("trigger must not be called when signature is missing")
	}
}

func TestNoSecretSkipsVerification(t *testing.T) {
	triggered := 0
	srv := newTestServer(t, "", &triggered) // empty secret

	body := makeBody("virtualmachine", "created")
	rr := postWebhook(t, srv, body, "") // no signature header

	if rr.Code != http.StatusNoContent {
		t.Fatalf("expected 204 with no secret, got %d", rr.Code)
	}
	if triggered != 1 {
		t.Fatalf("expected trigger called, got %d", triggered)
	}
}

func TestIrrelevantModelDoesNotTrigger(t *testing.T) {
	triggered := 0
	srv := newTestServer(t, "", &triggered)

	body := makeBody("prefix", "updated") // IPAM prefix — not watched
	rr := postWebhook(t, srv, body, "")

	if rr.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", rr.Code)
	}
	if triggered != 0 {
		t.Fatalf("trigger must not be called for irrelevant model, got %d calls", triggered)
	}
}

func TestAllRelevantModels(t *testing.T) {
	models := []string{"ipaddress", "virtualmachine", "vminterface"}
	for _, model := range models {
		t.Run(model, func(t *testing.T) {
			triggered := 0
			srv := newTestServer(t, "", &triggered)
			body := makeBody(model, "updated")
			rr := postWebhook(t, srv, body, "")
			if rr.Code != http.StatusNoContent {
				t.Fatalf("expected 204, got %d", rr.Code)
			}
			if triggered != 1 {
				t.Fatalf("expected trigger for model %s, got %d calls", model, triggered)
			}
		})
	}
}

func TestObjectTypeFormatsAreAccepted(t *testing.T) {
	formats := []string{
		"ipam.ipaddress",
		"virtualization.virtualmachine",
		"virtualization.vminterface",
	}

	for _, objectType := range formats {
		t.Run(objectType, func(t *testing.T) {
			triggered := 0
			srv := newTestServer(t, "", &triggered)
			body := makeBodyWithData("updated", "", objectType, "")
			rr := postWebhook(t, srv, body, "")

			if rr.Code != http.StatusNoContent {
				t.Fatalf("expected 204, got %d", rr.Code)
			}
			if triggered != 1 {
				t.Fatalf("expected trigger for object_type %s, got %d calls", objectType, triggered)
			}
		})
	}
}

func TestMissingModelFallsBackToDataURL(t *testing.T) {
	tests := []struct {
		name    string
		dataURL string
	}{
		{name: "ipaddress", dataURL: "/api/ipam/ip-addresses/652/"},
		{name: "virtualmachine", dataURL: "/api/virtualization/virtual-machines/130/"},
		{name: "vminterface", dataURL: "/api/virtualization/interfaces/159/"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			triggered := 0
			srv := newTestServer(t, "", &triggered)
			body := makeBodyWithData("updated", "", "", tc.dataURL)
			rr := postWebhook(t, srv, body, "")

			if rr.Code != http.StatusNoContent {
				t.Fatalf("expected 204, got %d", rr.Code)
			}
			if triggered != 1 {
				t.Fatalf("expected trigger called once, got %d", triggered)
			}
		})
	}
}

func TestGetMethodRejected(t *testing.T) {
	triggered := 0
	srv := newTestServer(t, "", &triggered)

	req := httptest.NewRequest(http.MethodGet, "/webhook", nil)
	rr := httptest.NewRecorder()
	srv.ServeHTTP(rr, req)

	if rr.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", rr.Code)
	}
}

// Ensure the Server type exposes ServeHTTP so httptest.ResponseRecorder works.
var _ http.Handler = (*webhook.Server)(nil)

// netboxSign returns the bare lowercase hex HMAC-SHA512 that Netbox actually sends
// in X-Hook-Signature (extras.webhooks.generate_signature → hmac.hexdigest()).
func netboxSign(body []byte) string {
	mac := hmac.New(sha512.New, []byte(testSecret))
	mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}

func TestNetboxBareHexSignatureAccepted(t *testing.T) {
	triggered := 0
	srv := newTestServer(t, testSecret, &triggered)

	body := makeBody("ipaddress", "updated")
	rr := postWebhook(t, srv, body, netboxSign(body))

	if rr.Code != http.StatusNoContent {
		t.Fatalf("expected 204 for a Netbox-format signature, got %d (%s)", rr.Code, rr.Body.String())
	}
	if triggered != 1 {
		t.Errorf("expected reconcile trigger, got %d calls", triggered)
	}
}

func TestUppercaseHexSignatureAccepted(t *testing.T) {
	triggered := 0
	srv := newTestServer(t, testSecret, &triggered)

	body := makeBody("ipaddress", "updated")
	rr := postWebhook(t, srv, body, strings.ToUpper(netboxSign(body)))

	if rr.Code != http.StatusNoContent {
		t.Fatalf("expected 204 for an uppercase hex signature, got %d", rr.Code)
	}
}

func TestBareHexSignatureWithWrongSecretRejected(t *testing.T) {
	triggered := 0
	srv := newTestServer(t, testSecret, &triggered)

	body := makeBody("ipaddress", "updated")
	mac := hmac.New(sha512.New, []byte("a-different-secret"))
	mac.Write(body)
	rr := postWebhook(t, srv, body, hex.EncodeToString(mac.Sum(nil)))

	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for a signature from the wrong secret, got %d", rr.Code)
	}
	if triggered != 0 {
		t.Errorf("trigger must not run on a bad signature, got %d calls", triggered)
	}
}
