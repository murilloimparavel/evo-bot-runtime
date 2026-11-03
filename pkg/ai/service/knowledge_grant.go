package service

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/EvolutionAPI/evo-bot-runtime/pkg/ai/model"
)

var knowledgeProcessorPath = regexp.MustCompile(`^/api/v1/a2a/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
var knowledgeSessionUUID = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

// An absent list retains the existing global opt-in. A configured list must be
// valid in its entirety: a typo cannot silently downgrade a protected request.
func knowledgeDispatchSelected(outgoing string) (bool, error) {
	if os.Getenv("KNOWLEDGE_DISPATCH_ENABLED") != "true" {
		return false, nil
	}
	raw, configured := os.LookupEnv("KNOWLEDGE_RUNTIME_PILOT_ROOT_IDS")
	if !configured {
		return true, nil
	}
	values := strings.Split(raw, ",")
	if len(values) > 100 {
		return false, errors.New("knowledge pilot unavailable")
	}
	roots := make(map[string]bool, len(values))
	for _, value := range values {
		root := strings.TrimSpace(value)
		if !knowledgeSessionUUID.MatchString(root) || roots[root] {
			return false, errors.New("knowledge pilot unavailable")
		}
		roots[root] = true
	}
	target, err := url.Parse(outgoing)
	if err != nil || target.RawPath != "" || target.RawQuery != "" || target.Fragment != "" || target.User != nil || !knowledgeProcessorPath.MatchString(target.Path) {
		return false, errors.New("knowledge pilot unavailable")
	}
	return roots[strings.TrimPrefix(target.Path, "/api/v1/a2a/")], nil
}

// Resolve the persisted session identity through the authenticated CRM, never
// through metadata supplied in the A2A request. The subsequent grant binds
// these identifiers and the exact request body, catching concurrent changes.
func resolveKnowledgeIdentity(ctx context.Context, input *model.A2ARequest) (string, string, error) {
	if !trustedKnowledgeProcessor(input.OutgoingURL, os.Getenv("KNOWLEDGE_PROCESSOR_ORIGIN")) {
		return "", "", errors.New("knowledge dispatch unavailable")
	}
	u, err := url.Parse(os.Getenv("KNOWLEDGE_DISPATCH_URL"))
	secret := os.Getenv("KNOWLEDGE_DISPATCH_TOKEN")
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.RawPath != "" || u.Path != "/internal/knowledge_runtime/grants" || len(secret) < 32 || input.AgentBotID == "" || len(input.SourceMessageIDs) == 0 {
		return "", "", errors.New("knowledge dispatch unavailable")
	}
	u.Path = "/internal/knowledge_runtime/identity"
	payload, err := json.Marshal(map[string]any{"display_id": input.ConversationID, "bot_id": input.AgentBotID, "message_ids": input.SourceMessageIDs})
	if err != nil {
		return "", "", errors.New("knowledge dispatch unavailable")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewReader(payload))
	if err != nil {
		return "", "", errors.New("knowledge dispatch unavailable")
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Knowledge-Dispatch-Token", secret)
	client := &http.Client{Timeout: 5 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	response, err := client.Do(request)
	if err != nil {
		return "", "", errors.New("knowledge dispatch unavailable")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return "", "", errors.New("knowledge dispatch unavailable")
	}
	var data struct {
		ConversationID string `json:"conversation_id"`
		ContactID      string `json:"contact_id"`
	}
	if json.NewDecoder(io.LimitReader(response.Body, 1025)).Decode(&data) != nil || !knowledgeSessionUUID.MatchString(data.ConversationID) || !knowledgeSessionUUID.MatchString(data.ContactID) {
		return "", "", errors.New("knowledge dispatch unavailable")
	}
	return data.ConversationID, data.ContactID, nil
}

func trustedKnowledgeProcessor(outgoing, configured string) bool {
	target, err := url.Parse(outgoing)
	if err != nil || target.Scheme != "https" || target.Host == "" || target.User != nil || target.RawQuery != "" || target.Fragment != "" {
		return false
	}
	origin, err := url.Parse(configured)
	if err != nil || origin.Scheme != "https" || origin.Host == "" || origin.User != nil || origin.RawQuery != "" || origin.Fragment != "" || (origin.Path != "" && origin.Path != "/") {
		return false
	}
	return strings.EqualFold(target.Host, origin.Host) && target.RawPath == "" && knowledgeProcessorPath.MatchString(target.Path)
}

// The endpoint comes from deployment configuration, never from agent data.
// Redirects are disabled to prevent leaking the dedicated dispatch secret.
func requestKnowledgeGrant(ctx context.Context, input *model.A2ARequest, body []byte) (string, error) {
	if !trustedKnowledgeProcessor(input.OutgoingURL, os.Getenv("KNOWLEDGE_PROCESSOR_ORIGIN")) {
		return "", errors.New("knowledge dispatch unavailable")
	}
	endpoint := os.Getenv("KNOWLEDGE_DISPATCH_URL")
	secret := os.Getenv("KNOWLEDGE_DISPATCH_TOKEN")
	u, err := url.Parse(endpoint)
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Path != "/internal/knowledge_runtime/grants" || len(secret) < 32 || input.AgentBotID == "" || len(input.SourceMessageIDs) == 0 {
		return "", errors.New("knowledge dispatch unavailable")
	}
	digest := sha256.Sum256(body)
	payload, err := json.Marshal(map[string]any{"display_id": input.ConversationID, "bot_id": input.AgentBotID, "message_ids": input.SourceMessageIDs, "request_sha256": hex.EncodeToString(digest[:])})
	if err != nil {
		return "", errors.New("knowledge dispatch unavailable")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		return "", errors.New("knowledge dispatch unavailable")
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Knowledge-Dispatch-Token", secret)
	client := &http.Client{Timeout: 5 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	response, err := client.Do(request)
	if err != nil {
		return "", errors.New("knowledge dispatch unavailable")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return "", errors.New("knowledge dispatch unavailable")
	}
	var data struct {
		Grant string `json:"grant"`
	}
	if json.NewDecoder(io.LimitReader(response.Body, 8193)).Decode(&data) != nil || len(data.Grant) == 0 || len(data.Grant) > 8192 {
		return "", errors.New("knowledge dispatch unavailable")
	}
	return data.Grant, nil
}
