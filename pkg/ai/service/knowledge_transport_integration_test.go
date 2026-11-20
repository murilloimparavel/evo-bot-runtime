package service

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/EvolutionAPI/evo-bot-runtime/pkg/ai/model"
)

// Synthetic HTTPS services: verifies adapter transport and retry body binding,
// not the actual Rails issuer, JWT validation, or A2A ledger.
func TestKnowledgeTransportPreservesSignedBodyAcrossRetry(t *testing.T) {
	var digest string
	issuer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Knowledge-Dispatch-Token") != "01234567890123456789012345678901" {
			t.Error("missing dispatch credential")
		}
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Error(err)
			return
		}
		if r.URL.Path == "/internal/knowledge_runtime/identity" {
			_, _ = io.WriteString(w, `{"conversation_id":"22222222-2222-4222-8222-222222222222","contact_id":"33333333-3333-4333-8333-333333333333"}`)
			return
		}
		digest, _ = payload["request_sha256"].(string)
		_, _ = io.WriteString(w, `{"grant":"synthetic-grant"}`)
	}))
	defer issuer.Close()
	attempts := 0
	processor := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
			return
		}
		var decoded model.JSONRPCRequest
		if json.Unmarshal(body, &decoded) != nil || decoded.Params.ContextID != "22222222-2222-4222-8222-222222222222" || decoded.Params.UserID != "33333333-3333-4333-8333-333333333333" {
			t.Error("request did not use authenticated CRM identity")
		}
		sum := sha256.Sum256(body)
		if hex.EncodeToString(sum[:]) != digest || r.Header.Get("X-Knowledge-Grant") != "synthetic-grant" {
			t.Error("grant or body binding differs")
		}
		if attempts == 1 {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		_ = json.NewEncoder(w).Encode(model.A2AResponse{Result: &model.A2AResult{Artifacts: []model.A2AArtifact{{Parts: []model.A2APart{{Type: "text", Text: "ok"}}}}}})
	}))
	defer processor.Close()
	roots := x509.NewCertPool()
	roots.AddCert(issuer.Certificate())
	roots.AddCert(processor.Certificate())
	old := http.DefaultTransport
	http.DefaultTransport = &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}}
	t.Cleanup(func() { http.DefaultTransport = old })
	t.Setenv("KNOWLEDGE_DISPATCH_ENABLED", "true")
	t.Setenv("KNOWLEDGE_DISPATCH_URL", issuer.URL+"/internal/knowledge_runtime/grants")
	t.Setenv("KNOWLEDGE_DISPATCH_TOKEN", "01234567890123456789012345678901")
	t.Setenv("KNOWLEDGE_PROCESSOR_ORIGIN", processor.URL)
	adapter := NewAIAdapter(5, 1, 1)
	response, err := adapter.Call(context.Background(), &model.A2ARequest{OutgoingURL: processor.URL + "/api/v1/a2a/11111111-1111-4111-8111-111111111111", AgentBotID: "bot", SourceMessageIDs: []string{"message"}, Message: "teste", Metadata: map[string]any{"evoai_crm_data": map[string]any{"conversation": map[string]any{"id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"}, "contact": map[string]any{"id": "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"}}}})
	if err != nil {
		t.Fatal(err)
	}
	if attempts != 2 || response.Content != "ok" {
		t.Fatalf("attempts=%d response=%+v", attempts, response)
	}
}

func TestKnowledgeTransportRejectsMissingSessionUUIDs(t *testing.T) {
	t.Setenv("KNOWLEDGE_DISPATCH_ENABLED", "true")
	_, err := NewAIAdapter(1, 0, 1).Call(context.Background(), &model.A2ARequest{ConversationID: 1, ContactID: 2})
	if err == nil {
		t.Fatal("numeric fallback must fail closed")
	}
}
