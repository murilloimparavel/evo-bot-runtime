package service

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	dispatchService "github.com/EvolutionAPI/evo-bot-runtime/pkg/dispatch/service"
	pipelineModel "github.com/EvolutionAPI/evo-bot-runtime/pkg/pipeline/model"
	"net/http"
	"os"
	"testing"

	"github.com/EvolutionAPI/evo-bot-runtime/pkg/ai/model"
)

// Opt-in network integration: endpoints are real local Rails and A2A services.
// TLS roots are provided through SSL_CERT_FILE, without disabling verification.
func TestKnowledgeRealServices(t *testing.T) {
	fixturePath := os.Getenv("KNOWLEDGE_E2E_FIXTURE")
	if fixturePath == "" {
		t.Skip("requires isolated real-services integration harness")
	}
	raw, err := os.ReadFile(fixturePath)
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		BotID     string `json:"bot_id"`
		MessageID string `json:"message_id"`
		DisplayID int64  `json:"display_id"`
	}
	if err = json.Unmarshal(raw, &fixture); err != nil {
		t.Fatal(err)
	}
	if fixture.BotID == "" || fixture.MessageID == "" || fixture.DisplayID == 0 {
		t.Fatal("incomplete synthetic fixture")
	}
	ca, err := os.ReadFile(os.Getenv("SSL_CERT_FILE"))
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		t.Fatal("invalid integration CA")
	}
	previousTransport := http.DefaultTransport
	http.DefaultTransport = &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}}
	t.Cleanup(func() { http.DefaultTransport = previousTransport })
	adapter := NewAIAdapter(60, 0, 1)
	input := &model.A2ARequest{
		OutgoingURL: os.Getenv("KNOWLEDGE_PROCESSOR_ORIGIN") + "/api/v1/a2a/11111111-1111-4111-8111-111111111111",
		AgentBotID:  fixture.BotID, ConversationID: fixture.DisplayID, ContactID: 1,
		MessageID: fixture.MessageID, SourceMessageIDs: []string{fixture.MessageID},
		ApiKey: "synthetic-e2e-agent-key", Message: "Olá, integração local",
		Metadata: map[string]any{"evoai_crm_data": map[string]any{"conversation": map[string]any{"id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"}}},
	}
	first, err := adapter.Call(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if first.Content != "E2E autenticado" {
		t.Fatalf("unexpected response: %q", first.Content)
	}
	second, err := adapter.Call(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if second.Content != first.Content {
		t.Fatal("replay response differed")
	}
	if first.KnowledgeProvenance == "" {
		t.Fatal("missing response provenance")
	}
	dispatch := dispatchService.NewDispatchEngine(os.Getenv("BOT_RUNTIME_SECRET"))
	deliveryContext := model.WithResponseProvenance(context.Background(), first.KnowledgeProvenance)
	postbackURL := fmt.Sprintf("%s/webhooks/bot_runtime/postback/%d", os.Getenv("KNOWLEDGE_CRM_ORIGIN"), fixture.DisplayID)
	for i := 0; i < 2; i++ {
		if err := dispatch.Dispatch(deliveryContext, 1, fixture.DisplayID, first.Content, pipelineModel.BotConfig{}, false, postbackURL); err != nil {
			t.Fatal(err)
		}
	}
	bad := *input
	bad.ApiKey = "wrong-synthetic-key"
	if _, err := adapter.Call(context.Background(), &bad); err == nil {
		t.Fatal("invalid API key must be rejected before cached response")
	}
	t.Log("real Rails identity/grant/history + Go adapter + A2A execution/replay and invalid-key denial succeeded")
}
