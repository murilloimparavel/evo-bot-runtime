// Package reconcile archives reviewed legacy turns without dispatching messages.
package reconcile

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/redis/go-redis/v9"
)

var keyPattern = regexp.MustCompile(`^bot_runtime:durable:([1-9][0-9]*):([1-9][0-9]*)$`)
var referencePattern = regexp.MustCompile(`^[A-Za-z0-9_.@-]{1,100}$`)

// Snapshot contains only metadata suitable for operator review, never payloads.
type Snapshot struct {
	Key              string   `json:"key"`
	ContactID        string   `json:"contact_id"`
	ConversationID   string   `json:"conversation_id"`
	Revision         int64    `json:"revision"`
	Phase            string   `json:"phase"`
	Attempts         int      `json:"attempts"`
	MessageIDs       []string `json:"message_ids"`
	HasSavedResponse bool     `json:"has_saved_response"`
	Legacy           bool     `json:"legacy"`
	SHA256           string   `json:"sha256"`
	LeaseActive      bool     `json:"lease_active"`
}

type storedTurn struct {
	Key      string `json:"key"`
	Revision int64  `json:"revision"`
	Phase    string `json:"phase"`
	Attempts int    `json:"attempts"`
	Content  string `json:"content"`
	Events   []struct {
		MessageID      string          `json:"message_id"`
		ContactID      json.RawMessage `json:"contact_id"`
		ConversationID json.RawMessage `json:"conversation_id"`
	} `json:"events"`
}

func read(ctx context.Context, db *redis.Client, key string) (Snapshot, string, error) {
	match := keyPattern.FindStringSubmatch(key)
	if match == nil {
		return Snapshot{}, "", errors.New("invalid turn key")
	}
	for _, n := range match[1:] {
		if _, err := strconv.ParseInt(n, 10, 64); err != nil {
			return Snapshot{}, "", errors.New("invalid key identifier")
		}
	}
	raw, err := db.Get(ctx, key).Result()
	if err != nil {
		return Snapshot{}, "", errors.New("turn unavailable")
	}
	var turn storedTurn
	if err := json.Unmarshal([]byte(raw), &turn); err != nil || turn.Key != key || turn.Revision < 1 || len(turn.Events) == 0 {
		return Snapshot{}, "", errors.New("invalid turn structure")
	}
	s := Snapshot{Key: key, ContactID: match[1], ConversationID: match[2], Revision: turn.Revision, Phase: turn.Phase, Attempts: turn.Attempts, HasSavedResponse: turn.Content != ""}
	for _, e := range turn.Events {
		if !referencePattern.MatchString(e.MessageID) {
			return Snapshot{}, "", errors.New("invalid message identifier")
		}
		s.MessageIDs = append(s.MessageIDs, e.MessageID)
		if len(e.ContactID) == 0 || len(e.ConversationID) == 0 {
			return Snapshot{}, "", errors.New("missing event identifiers")
		}
		if e.ContactID[0] != '"' || e.ConversationID[0] != '"' {
			s.Legacy = true
		}
	}
	digest := sha256.Sum256([]byte(raw))
	s.SHA256 = hex.EncodeToString(digest[:])
	exists, err := db.Exists(ctx, key+":lease").Result()
	if err != nil {
		return Snapshot{}, "", errors.New("lease check failed")
	}
	s.LeaseActive = exists != 0
	return s, raw, nil
}

func Inspect(ctx context.Context, db *redis.Client, key string) (Snapshot, error) {
	s, _, err := read(ctx, db, key)
	return s, err
}

// Archive atomically removes a reviewed turn from the ready queue while keeping
// the exact payload and receipts. It cannot send messages or run an AI call.
func Archive(ctx context.Context, db *redis.Client, key, expectedHash, decision, operator, ticket string, workersStopped bool) (string, error) {
	if !workersStopped {
		return "", errors.New("workers must be stopped and verified by operator")
	}
	if decision != "obsolete" && decision != "human_handoff" {
		return "", errors.New("invalid decision")
	}
	if !referencePattern.MatchString(operator) || !referencePattern.MatchString(ticket) {
		return "", errors.New("invalid operator or ticket reference")
	}
	s, raw, err := read(ctx, db, key)
	if err != nil {
		return "", err
	}
	if !s.Legacy {
		return "", errors.New("turn is not legacy")
	}
	if s.LeaseActive {
		return "", errors.New("turn has an active lease")
	}
	if s.Phase != "waiting" || s.HasSavedResponse {
		return "", errors.New("turn has a saved or non-waiting result; manual review required")
	}
	if !strings.EqualFold(expectedHash, s.SHA256) || len(expectedHash) != 64 {
		return "", errors.New("snapshot changed or hash does not match")
	}
	return archiveSnapshot(ctx, db, s, raw, decision, operator, ticket)
}

func archiveSnapshot(ctx context.Context, db *redis.Client, s Snapshot, raw, decision, operator, ticket string) (string, error) {
	key := s.Key
	archiveKey := "bot_runtime:reconciled:" + strings.TrimPrefix(key, "bot_runtime:durable:") + ":" + s.SHA256
	auditKey := archiveKey + ":audit"
	// All validations, including type checks, precede writes: Redis scripts do not
	// roll back prior writes on errors. Byte equality fences revision and all events.
	result, err := db.Eval(ctx, `
 if redis.call('GET',KEYS[1]) ~= ARGV[1] then return 'changed' end
 if redis.call('EXISTS',KEYS[2]) ~= 0 then return 'lease' end
 if redis.call('EXISTS',KEYS[4]) ~= 0 or redis.call('EXISTS',KEYS[5]) ~= 0 then return 'archive_exists' end
 local typ=redis.call('TYPE',KEYS[3]).ok
 if typ ~= 'zset' and typ ~= 'none' then return 'invalid_queue' end
 redis.call('SET',KEYS[4],ARGV[1])
 redis.call('HSET',KEYS[5],'sha256',ARGV[2],'decision',ARGV[3],'operator',ARGV[4],'ticket',ARGV[5],'revision',ARGV[6],'time',redis.call('TIME')[1],'original_key',KEYS[1])
 redis.call('ZREM',KEYS[3],KEYS[1])
 redis.call('DEL',KEYS[1])
 return 'ok'
 `, []string{key, key + ":lease", "bot_runtime:durable:ready", archiveKey, auditKey}, raw, s.SHA256, decision, operator, ticket, s.Revision).Text()
	if err != nil {
		return "", errors.New("archive operation failed")
	}
	if result != "ok" {
		return "", fmt.Errorf("archive refused: %s", result)
	}
	return archiveKey, nil
}
