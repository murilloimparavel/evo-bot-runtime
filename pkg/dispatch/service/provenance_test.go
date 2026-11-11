package service_test

import (
	"context"
	"encoding/json"
	aiModel "github.com/EvolutionAPI/evo-bot-runtime/pkg/ai/model"
	"github.com/EvolutionAPI/evo-bot-runtime/pkg/dispatch/service"
	"github.com/EvolutionAPI/evo-bot-runtime/pkg/pipeline/model"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestDispatch_OnlyExactUntransformedTextCarriesProvenance(t *testing.T) {
	for _, tc := range []struct {
		name    string
		cfg     model.BotConfig
		content string
		want    bool
	}{
		{"exact", model.BotConfig{}, "Escolha a opção azul", true},
		{"signature", model.BotConfig{MessageSignature: "Sofia: "}, "Escolha a opção azul", false},
		{"segmented", model.BotConfig{TextSegmentationEnabled: true, TextSegmentationLimit: 7}, "Escolha a opção azul", false},
		{"media", model.BotConfig{}, "Foto https://example.test/file.jpg", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var received []string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var body map[string]any
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Error(err)
				}
				proof, _ := body["knowledge_provenance"].(string)
				received = append(received, proof)
				w.WriteHeader(http.StatusOK)
			}))
			defer server.Close()
			ctx := aiModel.WithResponseProvenance(context.Background(), "opaque-signed-proof")
			if err := service.NewDispatchEngine("secret").Dispatch(ctx, 1, 1, tc.content, tc.cfg, false, server.URL); err != nil {
				t.Fatal(err)
			}
			if len(received) == 0 {
				t.Fatal("no delivery")
			}
			for _, proof := range received {
				if tc.want && proof != "opaque-signed-proof" {
					t.Fatal("proof lost")
				}
				if !tc.want && proof != "" {
					t.Fatal("transformed text retained proof")
				}
			}
		})
	}
}
