package repository

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/EvolutionAPI/evo-bot-runtime/pkg/pipeline/model"
	"github.com/redis/go-redis/v9"
)

// DurableTurn is the recoverable unit of work. Events remain here until delivery
// is acknowledged. Credentials are private Redis data, never logged.
type DurableTurn struct {
	Key             string               `json:"key"`
	Revision        int64                `json:"revision"`
	Token           string               `json:"token,omitempty"`
	Phase           string               `json:"phase"`
	Events          []model.MessageEvent `json:"events"`
	DueAt           int64                `json:"due_at"`
	Attempts        int                  `json:"attempts"`
	Content         string               `json:"content,omitempty"`
	Provenance      string               `json:"provenance,omitempty"`
	FailureNotified bool                 `json:"failure_notified,omitempty"`
	TerminalHandoff bool                 `json:"terminal_handoff,omitempty"`
}

type DurableRepository interface {
	AcceptEvent(context.Context, *model.MessageEvent) error
	ReadyTurns(context.Context, int) ([]string, error)
	ClaimTurn(context.Context, string, string, time.Duration) (*DurableTurn, error)
	RenewTurn(context.Context, *DurableTurn, time.Duration) (bool, error)
	SaveTurnResult(context.Context, *DurableTurn, string, string, bool) (bool, error)
	MarkTurnNotified(context.Context, *DurableTurn) error
	RetryTurn(context.Context, *DurableTurn, time.Duration) error
	CompleteTurn(context.Context, *DurableTurn) (bool, error)
	ReleaseTurn(context.Context, *DurableTurn) error
	CancelTurn(context.Context, int64, int64) error
}

const readyTurnsKey = "bot_runtime:durable:ready"

func durableKey(contactID, conversationID int64) string {
	return fmt.Sprintf("bot_runtime:durable:%d:%d", contactID, conversationID)
}
func (r *redisPipelineRepository) AcceptEvent(ctx context.Context, e *model.MessageEvent) error {
	raw, err := json.Marshal(e)
	if err != nil {
		return err
	}
	key := durableKey(e.ContactID, e.ConversationID)
	digest := sha256.Sum256([]byte(e.AgentBotID + "\x00" + e.MessageID))
	receipt := key + ":receipt:" + hex.EncodeToString(digest[:])
	// Receipt and payload are committed together; no acceptance marker can exist
	// without its corresponding recoverable event. No TTL on unfinished work.
	_, err = r.rdb.Eval(ctx, `
 if redis.call('EXISTS',KEYS[2]) == 1 then return 0 end
 local raw=redis.call('GET',KEYS[1]); local t
 if raw then t=cjson.decode(raw) else t={key=KEYS[1],revision=0,events={}} end
 table.insert(t.events,cjson.decode(ARGV[1])); t.revision=t.revision+1
 t.phase='waiting'; t.token=''; t.due_at=tonumber(ARGV[2]); t.attempts=0
 t.content=nil; t.provenance=nil; t.terminal_handoff=nil; t.failure_notified=nil
 redis.call('SET',KEYS[1],cjson.encode(t))
 redis.call('SET',KEYS[2],'1')
 redis.call('ZADD',KEYS[3],ARGV[2],KEYS[1]); return 1
 `, []string{key, receipt, readyTurnsKey}, raw, time.Now().Add(time.Duration(e.BotConfig.DebounceTime)*time.Second).UnixMilli()).Result()
	return err
}
func (r *redisPipelineRepository) ReadyTurns(ctx context.Context, limit int) ([]string, error) {
	return r.rdb.ZRangeByScore(ctx, readyTurnsKey, &redis.ZRangeBy{Min: "-inf", Max: fmt.Sprint(time.Now().UnixMilli()), Offset: 0, Count: int64(limit)}).Result()
}
func (r *redisPipelineRepository) ClaimTurn(ctx context.Context, key, token string, lease time.Duration) (*DurableTurn, error) {
	raw, err := r.rdb.Eval(ctx, `
 local raw=redis.call('GET',KEYS[1]); if not raw then redis.call('ZREM',KEYS[2],KEYS[1]); return '' end
 local t=cjson.decode(raw); if t.due_at>tonumber(ARGV[1]) then return '' end
 if not redis.call('SET',KEYS[3],ARGV[2],'NX','PX',ARGV[4]) then
 redis.call('ZADD',KEYS[2],tonumber(ARGV[1])+math.max(1,redis.call('PTTL',KEYS[3])),KEYS[1]); return '' end
 t.token=ARGV[2]; t.due_at=tonumber(ARGV[3]); t.attempts=t.attempts+1
 redis.call('SET',KEYS[1],cjson.encode(t)); redis.call('ZADD',KEYS[2],ARGV[3],KEYS[1]); return cjson.encode(t)
 `, []string{key, readyTurnsKey, key + ":lease"}, time.Now().UnixMilli(), token, time.Now().Add(lease).UnixMilli(), lease.Milliseconds()).Text()
	if err != nil {
		return nil, err
	}
	if raw == "" {
		return nil, nil
	}
	var t DurableTurn
	err = json.Unmarshal([]byte(raw), &t)
	return &t, err
}

const guardTurn = `
 local raw=redis.call('GET',KEYS[1]); if not raw then return 0 end
 local t=cjson.decode(raw)
 if t.revision~=tonumber(ARGV[1]) or t.token~=ARGV[2] then return 0 end
 `

func (r *redisPipelineRepository) RenewTurn(ctx context.Context, t *DurableTurn, lease time.Duration) (bool, error) {
	n, err := r.rdb.Eval(ctx, `
 if redis.call('GET',KEYS[3])~=ARGV[2] then return 0 end
 redis.call('PEXPIRE',KEYS[3],ARGV[4])
 `+guardTurn+`
 t.due_at=tonumber(ARGV[3]); redis.call('SET',KEYS[1],cjson.encode(t)); redis.call('ZADD',KEYS[2],ARGV[3],KEYS[1]); return 1
 `, []string{t.Key, readyTurnsKey, t.Key + ":lease"}, t.Revision, t.Token, time.Now().Add(lease).UnixMilli(), lease.Milliseconds()).Int()
	return n == 1, err
}
func (r *redisPipelineRepository) SaveTurnResult(ctx context.Context, t *DurableTurn, content, proof string, terminal bool) (bool, error) {
	n, err := r.rdb.Eval(ctx, guardTurn+`
 t.phase='dispatch'; t.content=ARGV[3]; t.provenance=ARGV[4]; t.terminal_handoff=(ARGV[5]=='true')
 redis.call('SET',KEYS[1],cjson.encode(t)); return 1
 `, []string{t.Key}, t.Revision, t.Token, content, proof, fmt.Sprint(terminal)).Int()
	return n == 1, err
}
func (r *redisPipelineRepository) RetryTurn(ctx context.Context, t *DurableTurn, delay time.Duration) error {
	_, err := r.rdb.Eval(ctx, guardTurn+`
 t.due_at=tonumber(ARGV[3]); t.token=''; redis.call('SET',KEYS[1],cjson.encode(t)); redis.call('ZADD',KEYS[2],ARGV[3],KEYS[1]); return 1
 `, []string{t.Key, readyTurnsKey}, t.Revision, t.Token, time.Now().Add(delay).UnixMilli()).Result()
	return err
}
func (r *redisPipelineRepository) CompleteTurn(ctx context.Context, t *DurableTurn) (bool, error) {
	n, err := r.rdb.Eval(ctx, guardTurn+`
 redis.call('DEL',KEYS[1]); redis.call('ZREM',KEYS[2],KEYS[1]); return 1
 `, []string{t.Key, readyTurnsKey}, t.Revision, t.Token).Int()
	return n == 1, err
}
func (r *redisPipelineRepository) CancelTurn(ctx context.Context, cid, vid int64) error {
	_, err := r.rdb.Eval(ctx, `redis.call('DEL',KEYS[1]);redis.call('ZREM',KEYS[2],KEYS[1]);return 1`, []string{durableKey(cid, vid), readyTurnsKey}).Result()
	return err
}

func (r *redisPipelineRepository) ReleaseTurn(ctx context.Context, t *DurableTurn) error {
	_, err := r.rdb.Eval(ctx, `if redis.call('GET',KEYS[1])~=ARGV[1] then return 0 end
 redis.call('DEL',KEYS[1]); local raw=redis.call('GET',KEYS[2])
 if raw then local t=cjson.decode(raw);redis.call('ZADD',KEYS[3],t.due_at,KEYS[2]) end
 return 1`, []string{t.Key + ":lease", t.Key, readyTurnsKey}, t.Token).Result()
	return err
}

func (r *redisPipelineRepository) MarkTurnNotified(ctx context.Context, t *DurableTurn) error {
	_, err := r.rdb.Eval(ctx, guardTurn+`t.failure_notified=true;redis.call('SET',KEYS[1],cjson.encode(t));return 1`, []string{t.Key}, t.Revision, t.Token).Result()
	return err
}
