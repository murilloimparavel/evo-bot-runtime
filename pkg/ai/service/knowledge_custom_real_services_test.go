package service

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"

	"github.com/EvolutionAPI/evo-bot-runtime/pkg/ai/model"
	dispatchService "github.com/EvolutionAPI/evo-bot-runtime/pkg/dispatch/service"
	pipelineModel "github.com/EvolutionAPI/evo-bot-runtime/pkg/pipeline/model"
)

// Core Rails/Go/A2A implementations are real. Vendor/model are synthetic and
// read-only; this test never creates a real reservation or a financial effect.
func TestKnowledgeCustomRealServices(t *testing.T) {
	path := os.Getenv("KNOWLEDGE_E2E_CUSTOM_FIXTURE")
	if path == "" {
		t.Skip("requires custom-tool real-network integration harness")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var f struct {
		BotID     string `json:"bot_id"`
		MessageID string `json:"message_id"`
		DisplayID int64  `json:"display_id"`
		Content   string `json:"content"`
		Expected  string `json:"expected_response"`
		Denied    bool   `json:"expect_denial"`
	}
	if err = json.Unmarshal(raw, &f); err != nil {
		t.Fatal(err)
	}
	if f.BotID == "" || f.MessageID == "" || f.DisplayID == 0 || f.Content == "" {
		t.Fatal("incomplete synthetic fixture")
	}
	ca, err := os.ReadFile(os.Getenv("SSL_CERT_FILE"))
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		t.Fatal("invalid fixture CA")
	}
	previous := http.DefaultTransport
	http.DefaultTransport = &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}}
	t.Cleanup(func() { http.DefaultTransport = previous })
	adapter := NewAIAdapter(60, 0, 1)
	input := &model.A2ARequest{OutgoingURL: os.Getenv("KNOWLEDGE_PROCESSOR_ORIGIN") + "/api/v1/a2a/11111111-1111-4111-8111-111111111111",
		AgentBotID: f.BotID, ConversationID: f.DisplayID, ContactID: 1, MessageID: f.MessageID, SourceMessageIDs: []string{f.MessageID}, ApiKey: "synthetic-e2e-agent-key", Message: f.Content,
		Metadata: map[string]any{"evoai_crm_data": map[string]any{"conversation": map[string]any{"id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"}}}}
	first, err := adapter.Call(context.Background(), input)
	if f.Denied {
		if err == nil {
			t.Fatal("spoofed or changed custom configuration must be denied")
		}
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	if first.Content != f.Expected {
		t.Fatalf("unexpected synthetic response: %q", first.Content)
	}
	if strings.Contains(f.Content, "SCENARIO pending") && strings.Contains(first.Content, "Envie os dados") {
		t.Fatal("unconfirmed payment cannot ask voucher data")
	}
	second, err := adapter.Call(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if second.Content != first.Content {
		t.Fatal("replay changed response")
	}
	if first.KnowledgeProvenance == "" {
		t.Fatal("missing signed delivery attestation for custom-tool response")
	}
	dispatch := dispatchService.NewDispatchEngine(os.Getenv("BOT_RUNTIME_SECRET"))
	delivery := model.WithResponseProvenance(context.Background(), first.KnowledgeProvenance)
	for i := 0; i < 2; i++ {
		if err := dispatch.Dispatch(delivery, 1, f.DisplayID, first.Content, pipelineModel.BotConfig{}, false,
			fmt.Sprintf("%s/webhooks/bot_runtime/postback/%d", os.Getenv("KNOWLEDGE_CRM_ORIGIN"), f.DisplayID)); err != nil {
			t.Fatal(err)
		}
	}
	bad := *input
	bad.ApiKey = "wrong-synthetic-key"
	if _, err := adapter.Call(context.Background(), &bad); err == nil {
		t.Fatal("wrong key must not access custom replay cache")
	}
	t.Log("real custom-tool invocation, replay and postback completed")
}
