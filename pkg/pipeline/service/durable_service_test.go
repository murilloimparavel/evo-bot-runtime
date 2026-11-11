package service

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	aiModel "github.com/EvolutionAPI/evo-bot-runtime/pkg/ai/model"
	"github.com/EvolutionAPI/evo-bot-runtime/pkg/pipeline/model"
	"github.com/EvolutionAPI/evo-bot-runtime/pkg/pipeline/repository"
)

func durableEvent(c int64, id, text string) *model.MessageEvent {
	return &model.MessageEvent{ContactID: c, ConversationID: c, AgentBotID: "bot", MessageID: id, MessageContent: text, PostbackURL: "http://crm/postback", BotConfig: model.BotConfig{DebounceTime: 0}}
}
func testDurableSvc(t *testing.T, ai *mockAIAdapter, d *mockDispatchEngine) (*durableService, repository.DurableRepository) {
	t.Helper()
	legacy, _ := setupSvcWithAIAndDispatch(t, ai, d)
	repo := legacy.repo.(repository.DurableRepository)
	s := newDurableService(repo, ai, d)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		s.Shutdown(ctx)
	})
	return s, repo
}
func TestDurableWorkerRestoresAcceptedBurstAfterRestart(t *testing.T) {
	c := time.Now().UnixNano() % 1000000000000
	calls := make(chan *aiModel.A2ARequest, 2)
	sent := make(chan string, 2)
	ai := &mockAIAdapter{callFn: func(_ context.Context, r *aiModel.A2ARequest) (*aiModel.NormalizedResponse, error) {
		calls <- r
		return &aiModel.NormalizedResponse{Content: "search ready"}, nil
	}}
	d := &mockDispatchEngine{dispatchFn: func(_ context.Context, _, _ int64, content string, _ model.BotConfig, _ bool, _ string) error {
		sent <- content
		return nil
	}}
	before, repo := testDurableSvc(t, ai, d)
	ctx := context.Background()
	defer repo.CancelTurn(ctx, c, c)
	for _, e := range []*model.MessageEvent{durableEvent(c, "values", "values?"), durableEvent(c, "guests", "five adults"), durableEvent(c, "dates", "20/11 to 23/11"), durableEvent(c, "dates", "20/11 to 23/11")} {
		if err := before.Process(ctx, e); err != nil {
			t.Fatal(err)
		}
	}
	// No in-memory worker ran on the accepting instance. A different instance
	// discovers and processes all three messages entirely from Redis.
	after := newDurableService(repo, ai, d)
	defer after.Shutdown(ctx)
	if err := after.Start(); err != nil {
		t.Fatal(err)
	}
	competitor := newDurableService(repo, ai, d)
	defer competitor.Shutdown(ctx)
	if err := competitor.Start(); err != nil {
		t.Fatal(err)
	}
	select {
	case r := <-calls:
		if len(r.SourceMessageIDs) != 3 || r.Message != "values?\n\nfive adults\n\n20/11 to 23/11" {
			t.Fatalf("incomplete burst: %v %q", r.SourceMessageIDs, r.Message)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("accepted events were not recovered")
	}
	select {
	case <-sent:
	case <-time.After(3 * time.Second):
		t.Fatal("not delivered")
	}
	time.Sleep(200 * time.Millisecond)
	if len(calls) != 0 || len(sent) != 0 {
		t.Fatal("multiple replicas executed the same turn")
	}

}
func TestDurableWorkerNewRevisionWaitsForOldCallAndSuppressesOldResult(t *testing.T) {
	c := time.Now().UnixNano() % 1000000000000
	entered := make(chan struct{})
	cancelled := make(chan struct{})
	release := make(chan struct{})
	next := make(chan struct{}, 1)
	sent := make(chan string, 2)
	var calls atomic.Int32
	ai := &mockAIAdapter{callFn: func(ctx context.Context, r *aiModel.A2ARequest) (*aiModel.NormalizedResponse, error) {
		if calls.Add(1) == 1 {
			close(entered)
			<-ctx.Done()
			close(cancelled)
			<-release
			return &aiModel.NormalizedResponse{Content: "stale ask dates"}, nil
		}
		if !strings.Contains(r.Message, "20/11") {
			t.Error("new revision missing dates")
		}
		next <- struct{}{}
		return &aiModel.NormalizedResponse{Content: "fresh answer"}, nil
	}}
	d := &mockDispatchEngine{dispatchFn: func(_ context.Context, _, _ int64, c string, _ model.BotConfig, _ bool, _ string) error {
		sent <- c
		return nil
	}}
	s, repo := testDurableSvc(t, ai, d)
	ctx := context.Background()
	defer repo.CancelTurn(ctx, c, c)
	if err := s.Process(ctx, durableEvent(c, "guests", "five adults")); err != nil {
		t.Fatal(err)
	}
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("first call did not start")
	}
	if err := s.Process(ctx, durableEvent(c, "dates", "20/11 to 23/11")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-cancelled:
	case <-time.After(3 * time.Second):
		t.Fatal("stale call not cancelled")
	}
	select {
	case <-next:
		t.Fatal("new revision overlapped old invocation")
	case <-time.After(250 * time.Millisecond):
	}
	close(release)
	select {
	case c := <-sent:
		if c != "fresh answer" {
			t.Fatalf("stale response delivered: %s", c)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("fresh turn not delivered")
	}
}
func TestDurableWorkerRetriesSavedDispatchWithoutRegeneratingAI(t *testing.T) {
	c := time.Now().UnixNano() % 1000000000000
	var aiCalls, dispatchCalls atomic.Int32
	done := make(chan struct{}, 1)
	ai := &mockAIAdapter{callFn: func(context.Context, *aiModel.A2ARequest) (*aiModel.NormalizedResponse, error) {
		aiCalls.Add(1)
		return &aiModel.NormalizedResponse{Content: "persisted answer"}, nil
	}}
	d := &mockDispatchEngine{dispatchFn: func(_ context.Context, _, _ int64, c string, _ model.BotConfig, _ bool, _ string) error {
		if dispatchCalls.Add(1) == 1 {
			return errors.New("lost HTTP acknowledgement")
		}
		if c != "persisted answer" {
			t.Error("response changed on replay")
		}
		done <- struct{}{}
		return nil
	}}
	s, repo := testDurableSvc(t, ai, d)
	ctx := context.Background()
	defer repo.CancelTurn(ctx, c, c)
	if err := s.Process(ctx, durableEvent(c, "hello", "hello")); err != nil {
		t.Fatal(err)
	}
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("dispatch not retried")
	}
	if aiCalls.Load() != 1 {
		t.Fatalf("AI regenerated %d times", aiCalls.Load())
	}
}

func TestDurableProductionConstructorRejectsUndrainedLegacyWork(t *testing.T) {
	ai := &mockAIAdapter{callFn: func(context.Context, *aiModel.A2ARequest) (*aiModel.NormalizedResponse, error) {
		t.Fatal("must not process legacy state implicitly")
		return nil, nil
	}}
	d := &mockDispatchEngine{dispatchFn: func(context.Context, int64, int64, string, model.BotConfig, bool, string) error { return nil }}
	legacy, _ := setupSvcWithAIAndDispatch(t, ai, d)
	c := time.Now().UnixNano() % 1000000000000
	ctx := context.Background()
	if err := legacy.repo.SetState(ctx, c, c, &model.PipelineState{Stage: model.StageIncoming}); err != nil {
		t.Fatal(err)
	}
	defer legacy.repo.ClearState(ctx, c, c)
	production := NewPipelineService(legacy.repo, legacy.debounce, ai, d)
	defer production.Shutdown(ctx)
	if err := production.Start(); err == nil || !strings.Contains(err.Error(), "drain legacy") {
		t.Fatalf("unsafe cutover accepted: %v", err)
	}
}
