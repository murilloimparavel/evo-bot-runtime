package service

import (
	"context"
	"testing"

	"github.com/EvolutionAPI/evo-bot-runtime/pkg/ai/model"
)

func TestKnowledgeDispatchFailsClosedOnUnsafeConfiguration(t *testing.T) {
	t.Setenv("KNOWLEDGE_DISPATCH_TOKEN", "01234567890123456789012345678901")
	input := &model.A2ARequest{AgentBotID: "bot", SourceMessageIDs: []string{"message"}}
	for _, endpoint := range []string{"", "http://example.test/internal/knowledge_runtime/grants", "https://example.test/internal/knowledge_runtime/grants?target=other", "https://user@example.test/internal/knowledge_runtime/grants"} {
		t.Setenv("KNOWLEDGE_DISPATCH_URL", endpoint)
		if _, err := requestKnowledgeGrant(context.Background(), input, []byte("{}")); err == nil {
			t.Fatal("expected unsafe endpoint to be rejected")
		}
	}
}

func TestKnowledgeDispatchRequiresTrustedProcessorOrigin(t *testing.T) {
	const trusted = "https://processor.example"
	if !trustedKnowledgeProcessor("https://processor.example/api/v1/a2a/11111111-1111-4111-8111-111111111111", trusted) {
		t.Fatal("expected exact trusted origin")
	}
	for _, target := range []string{"https://attacker.example/api/v1/a2a/11111111-1111-4111-8111-111111111111", "http://processor.example/api/v1/a2a/11111111-1111-4111-8111-111111111111", "https://processor.example.attacker.test/api/v1/a2a/11111111-1111-4111-8111-111111111111", "https://processor.example/other", "https://processor.example/api/v1/a2a/../other", "https://processor.example/api/v1/a2a/%31%31%31%31%31%31%31%31-1111-4111-8111-111111111111"} {
		if trustedKnowledgeProcessor(target, trusted) {
			t.Fatalf("accepted untrusted target %q", target)
		}
	}
}
