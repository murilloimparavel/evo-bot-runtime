package repository_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/EvolutionAPI/evo-bot-runtime/pkg/pipeline/model"
	"github.com/EvolutionAPI/evo-bot-runtime/pkg/pipeline/repository"
)

func durableRepo(t *testing.T) repository.DurableRepository {
	t.Helper()
	r, done := setupTestRepo(t)
	t.Cleanup(done)
	return r.(repository.DurableRepository)
}
func eventFor(c int64, id, text string) *model.MessageEvent {
	return &model.MessageEvent{ContactID: c, ConversationID: c, AgentBotID: "bot", MessageID: id, MessageContent: text, PostbackURL: "http://crm/postback", ApiKey: "private-test-token", BotConfig: model.BotConfig{DebounceTime: 0}}
}
func TestDurableAcceptDeduplicatesAndRetainsCompleteEvents(t *testing.T) {
	c := time.Now().UnixNano() % 1000000000000
	r := durableRepo(t)
	ctx := context.Background()
	e := eventFor(c, "msg-a", "five adults")
	defer r.CancelTurn(ctx, e.ContactID, e.ConversationID)
	if err := r.AcceptEvent(ctx, e); err != nil {
		t.Fatal(err)
	}
	if err := r.AcceptEvent(ctx, e); err != nil {
		t.Fatal(err)
	}
	turn, err := r.ClaimTurn(ctx, fmt.Sprintf("bot_runtime:durable:%d:%d", c, c), "owner", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if turn == nil {
		t.Fatal("accepted event is not recoverable")
	}
	if len(turn.Events) != 1 || turn.Events[0].ApiKey != e.ApiKey || turn.Events[0].MessageContent != e.MessageContent {
		t.Fatalf("event was not durably and uniquely recorded: count %d", len(turn.Events))
	}
	if ok, err := r.CompleteTurn(ctx, turn); err != nil || !ok {
		t.Fatalf("complete %v %v", ok, err)
	}
	// Completed receipt survives cleanup of payload. Lost HTTP acknowledgement
	// followed by CRM retry must not resurrect an already delivered turn.
	if err := r.AcceptEvent(ctx, e); err != nil {
		t.Fatal(err)
	}
	if turn, err := r.ClaimTurn(ctx, turn.Key, "replay", time.Second); err != nil || turn != nil {
		t.Fatalf("completed event was replayed: %v %v", turn, err)
	}
}
func TestDurableNewEventFencesOldOwner(t *testing.T) {
	c := time.Now().UnixNano() % 1000000000000
	r := durableRepo(t)
	ctx := context.Background()
	e := eventFor(c, "first", "five adults")
	defer r.CancelTurn(ctx, e.ContactID, e.ConversationID)
	if err := r.AcceptEvent(ctx, e); err != nil {
		t.Fatal(err)
	}
	// The key contains only numeric tenant-independent identifiers, never content.
	old, err := r.ClaimTurn(ctx, fmt.Sprintf("bot_runtime:durable:%d:%d", c, c), "old", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if old == nil {
		t.Fatal("missing first event")
	}
	if err := r.MarkTurnNotified(ctx, old); err != nil {
		t.Fatal(err)
	}
	if err := r.AcceptEvent(ctx, eventFor(c, "dates", "20/11 to 23/11")); err != nil {
		t.Fatal(err)
	}
	if ok, err := r.SaveTurnResult(ctx, old, "ask dates", "", false); err != nil || ok {
		t.Fatalf("stale save accepted: %v %v", ok, err)
	}
	if ok, err := r.CompleteTurn(ctx, old); err != nil || ok {
		t.Fatalf("stale cleanup accepted: %v %v", ok, err)
	}
	if ok, err := r.RenewTurn(ctx, old, time.Second); err != nil || ok {
		t.Fatalf("stale lease renewed: %v %v", ok, err)
	}
	_ = r.ReleaseTurn(ctx, old)
	next, err := r.ClaimTurn(ctx, old.Key, "next", time.Second)
	if err != nil || next == nil {
		t.Fatalf("new event disappeared: %v", err)
	}
	if next.FailureNotified {
		t.Fatal("new revision inherited old failure notice flag")
	}
	if len(next.Events) != 2 || next.Events[1].MessageContent != "20/11 to 23/11" {
		t.Fatalf("incomplete next generation: %+v", next)
	}
}
func TestDurableExpiredLeaseRecoversSavedResponse(t *testing.T) {
	c := time.Now().UnixNano() % 1000000000000
	r := durableRepo(t)
	ctx := context.Background()
	e := eventFor(c, "msg", "hello")
	defer r.CancelTurn(ctx, e.ContactID, e.ConversationID)
	if err := r.AcceptEvent(ctx, e); err != nil {
		t.Fatal(err)
	}
	first, err := r.ClaimTurn(ctx, fmt.Sprintf("bot_runtime:durable:%d:%d", c, c), "crashed", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if first == nil {
		t.Fatal("missing event")
	}
	ok, err := r.SaveTurnResult(ctx, first, "saved response", "proof", true)
	if err != nil || !ok {
		t.Fatal(err)
	}
	// Simulate the failed worker releasing its retry lease; claim is atomic, so
	// another worker cannot own the same turn until it becomes eligible.
	if v, err := r.ClaimTurn(ctx, first.Key, "too-early", time.Second); err != nil || v != nil {
		t.Fatal("live lease was stolen")
	}
	if err := r.RetryTurn(ctx, first, 0); err != nil {
		t.Fatal(err)
	}
	_ = r.ReleaseTurn(ctx, first)
	recovered, err := r.ClaimTurn(ctx, first.Key, "recovered", time.Second)
	if err != nil || recovered == nil {
		t.Fatal("turn not recovered")
	}
	if recovered.Phase != "dispatch" || recovered.Content != "saved response" || !recovered.TerminalHandoff || recovered.Provenance != "proof" {
		t.Fatalf("response not preserved: %+v", recovered)
	}
	if ok, _ := r.CompleteTurn(ctx, first); ok {
		t.Fatal("old token cleaned recovered turn")
	}
}
