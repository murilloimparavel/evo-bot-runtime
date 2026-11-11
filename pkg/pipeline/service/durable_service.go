package service

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	aiModel "github.com/EvolutionAPI/evo-bot-runtime/pkg/ai/model"
	aiIface "github.com/EvolutionAPI/evo-bot-runtime/pkg/ai/service"
	dispatchIface "github.com/EvolutionAPI/evo-bot-runtime/pkg/dispatch/service"
	"github.com/EvolutionAPI/evo-bot-runtime/pkg/pipeline/model"
	"github.com/EvolutionAPI/evo-bot-runtime/pkg/pipeline/repository"
)

const turnLease = 30 * time.Second

// durableService stores every accepted event before scheduling work. All replicas
// compete through Redis compare-and-set leases; memory is only an optimization.
type durableService struct {
	repo     repository.DurableRepository
	legacy   repository.PipelineRepository
	ai       aiIface.AIAdapter
	dispatch dispatchIface.DispatchEngine
	ctx      context.Context
	cancel   context.CancelFunc
	once     sync.Once
	wg       sync.WaitGroup
	slots    chan struct{}
}

func newDurableService(repo repository.DurableRepository, ai aiIface.AIAdapter, d dispatchIface.DispatchEngine) *durableService {
	ctx, cancel := context.WithCancel(context.Background())
	return &durableService{repo: repo, ai: ai, dispatch: d, ctx: ctx, cancel: cancel, slots: make(chan struct{}, 32)}
}
func (s *durableService) Process(ctx context.Context, e *model.MessageEvent) error {
	if err := e.Validate(); err != nil {
		return err
	}
	// No goroutine or lock acquisition occurs before this durable commit.
	return s.repo.AcceptEvent(ctx, e)
}
func (s *durableService) Start() error {
	// Legacy buffers cannot be reconstructed losslessly once old code has
	// discarded their delivery config. Require an explicit drain before cutover.
	if s.legacy != nil {
		pairs, err := s.legacy.ScanStates(s.ctx, 1)
		if err != nil {
			return fmt.Errorf("legacy state check failed: %w", err)
		}
		if len(pairs) > 0 {
			return fmt.Errorf("legacy pipeline work exists; drain legacy runtime before durable cutover")
		}
	}
	// Fail startup when the shared queue is unavailable, rather than accepting work
	// that this process cannot discover. Subsequent outages retain pending jobs.
	if _, err := s.repo.ReadyTurns(s.ctx, 1); err != nil {
		return err
	}
	s.once.Do(func() { s.wg.Add(1); go s.poll() })
	return nil
}
func (s *durableService) poll() {
	defer s.wg.Done()
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-tick.C:
		}
		keys, err := s.repo.ReadyTurns(s.ctx, 64)
		if err != nil {
			slog.Error("pipeline.queue.scan_failed")
			continue
		}
		for _, key := range keys {
			select {
			case s.slots <- struct{}{}:
			default:
				continue
			}
			tokenBytes := make([]byte, 16)
			if _, err := rand.Read(tokenBytes); err != nil {
				<-s.slots
				continue
			}
			claimCtx, cancel := context.WithTimeout(s.ctx, 5*time.Second)
			t, err := s.repo.ClaimTurn(claimCtx, key, hex.EncodeToString(tokenBytes), turnLease)
			cancel()
			if err != nil {
				slog.Error("pipeline.turn.claim_failed", "key", key, "error", err)
				<-s.slots
				continue
			}
			if t == nil {
				<-s.slots
				continue
			}
			s.wg.Add(1)
			go func(t *repository.DurableTurn) { defer s.wg.Done(); defer func() { <-s.slots }(); s.run(t) }(t)
		}
	}
}
func (s *durableService) run(t *repository.DurableTurn) {
	ctx, cancel := context.WithCancel(s.ctx)
	defer cancel()
	// Keep the execution lease until the old call has actually returned, even
	// after a new revision cancels it. A new worker cannot run the same session
	// concurrently merely because a newer event has arrived.
	heartbeatCtx, stopHeartbeat := context.WithCancel(s.ctx)
	renewed := make(chan struct{})
	defer func() {
		stopHeartbeat()
		<-renewed
		c, done := cleanupCtx()
		defer done()
		_ = s.repo.ReleaseTurn(c, t)
	}()
	go func() {
		defer close(renewed)
		tick := time.NewTicker(time.Second)
		defer tick.Stop()
		for {
			select {
			case <-heartbeatCtx.Done():
				return
			case <-tick.C:
				c, done := context.WithTimeout(heartbeatCtx, 5*time.Second)
				current, err := s.repo.RenewTurn(c, t, turnLease)
				done()
				if err != nil {
					cancel()
					return
				}
				if !current {
					cancel()
				}
			}
		}
	}()
	defer func() {
		if recover() != nil {
			slog.Error("pipeline.turn.panic", "revision", t.Revision)
			s.retry(t)
		}
	}()
	if len(t.Events) == 0 {
		slog.Error("pipeline.turn.empty")
		s.retry(t)
		return
	}
	last := t.Events[len(t.Events)-1]
	ids := make([]string, 0, len(t.Events))
	texts := make([]string, 0, len(t.Events))
	var atts []aiModel.Attachment
	for _, e := range t.Events {
		ids = append(ids, e.MessageID)
		if e.MessageContent != "" {
			texts = append(texts, e.MessageContent)
		}
		for _, a := range e.Attachments {
			atts = append(atts, aiModel.Attachment{URL: a.URL, ContentType: a.ContentType, FileType: a.FileType})
		}
	}
	sortedIDs := append([]string(nil), ids...)
	sort.Strings(sortedIDs)
	batchID := stableMessageID(sortedIDs)
	slog.Info("pipeline.turn.started", "conversation_id", last.ConversationID, "revision", t.Revision, "source_message_ids", ids, "phase", t.Phase)
	if t.Phase != "dispatch" {
		response, err := s.ai.Call(ctx, &aiModel.A2ARequest{OutgoingURL: last.OutgoingURL, ContactID: last.ContactID, ConversationID: last.ConversationID, MessageID: batchID, SourceMessageIDs: ids, AgentBotID: last.AgentBotID, ApiKey: last.ApiKey, Message: strings.Join(texts, "\n\n"), Metadata: last.Metadata, Attachments: atts})
		if err != nil || response == nil || !validAIResponse(response.Content) {
			slog.Warn("pipeline.turn.ai_failed", "conversation_id", last.ConversationID, "revision", t.Revision)
			s.failureNotice(ctx, t, last, batchID)
			s.retry(t)
			return
		}
		content := appendResponseNotice(response.Content, last.ResponseNotice)
		c, done := cleanupCtx()
		ok, err := s.repo.SaveTurnResult(c, t, content, response.KnowledgeProvenance, response.TerminalHandoff)
		done()
		if err != nil {
			s.retry(t)
			return
		}
		if !ok {
			return
		}
		t.Content = content
		t.Provenance = response.KnowledgeProvenance
		t.TerminalHandoff = response.TerminalHandoff
	}
	dispatchCtx := aiModel.WithResponseProvenance(ctx, t.Provenance)
	dispatchCtx = dispatchIface.WithDelivery(dispatchCtx, batchID, func(ctx context.Context) error {
		ok, err := s.repo.RenewTurn(ctx, t, turnLease)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("turn superseded")
		}
		return nil
	})
	if err := s.dispatch.Dispatch(dispatchCtx, last.ContactID, last.ConversationID, t.Content, last.BotConfig, t.TerminalHandoff, last.PostbackURL); err != nil {
		s.retry(t)
		return
	}
	c, done := cleanupCtx()
	ok, err := s.repo.CompleteTurn(c, t)
	done()
	if err != nil {
		s.retry(t)
		return
	}
	if ok {
		slog.Info("pipeline.turn.completed", "conversation_id", last.ConversationID, "revision", t.Revision, "source_message_ids", ids)
	}
}
func (s *durableService) retry(t *repository.DurableTurn) {
	delay := time.Second * time.Duration(1<<min(t.Attempts, 6))
	c, done := cleanupCtx()
	defer done()
	if err := s.repo.RetryTurn(c, t, delay); err != nil {
		slog.Error("pipeline.turn.retry_failed", "revision", t.Revision)
	}
}
func (s *durableService) Cancel(cid, vid int64) error {
	c, done := cleanupCtx()
	defer done()
	return s.repo.CancelTurn(c, cid, vid)
}
func (s *durableService) Shutdown(ctx context.Context) {
	s.cancel()
	done := make(chan struct{})
	go func() { s.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
	}
}

func (s *durableService) failureNotice(ctx context.Context, t *repository.DurableTurn, e model.MessageEvent, batchID string) {
	if t.FailureNotified || ctx.Err() != nil {
		return
	}
	notice := defaultAIFailureNotice
	if v, ok := os.LookupEnv(aiFailureNoticeEnv); ok {
		notice = strings.TrimSpace(v)
	}
	if notice == "" {
		return
	}
	noticeContext := dispatchIface.WithDelivery(ctx, "notice:"+batchID, func(c context.Context) error {
		ok, err := s.repo.RenewTurn(c, t, turnLease)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("turn superseded")
		}
		return nil
	})
	if err := s.dispatch.Dispatch(noticeContext, e.ContactID, e.ConversationID, notice, e.BotConfig, false, e.PostbackURL); err != nil {
		return
	}
	c, done := cleanupCtx()
	defer done()
	_ = s.repo.MarkTurnNotified(c, t)
}
