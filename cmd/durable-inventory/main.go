// durable-inventory only reads Redis. It never claims, changes or replays turns.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"sort"
	"time"

	"github.com/redis/go-redis/v9"
)

var legacyKey = regexp.MustCompile(`^bot_runtime:durable:([0-9]+):([0-9]+)$`)
var legacyReceipt = regexp.MustCompile(`^bot_runtime:durable:([0-9]+):([0-9]+):receipt:[a-f0-9]{64}$`)

type row struct {
	ContactID      string   `json:"contact_key"`
	ConversationID string   `json:"conversation_id"`
	Pending        bool     `json:"turn_exists"`
	Ready          bool     `json:"in_legacy_queue"`
	Phase          string   `json:"phase,omitempty"`
	EventCount     int      `json:"event_count"`
	MessageIDs     []string `json:"message_ids"`
	ReceiptCount   int      `json:"receipt_count"`
	Review         string   `json:"review,omitempty"`
}

type storedTurn struct {
	Phase  string            `json:"phase"`
	Events []json.RawMessage `json:"events"`
}

func parseTurn(raw []byte, r *row) {
	var turn storedTurn
	if err := json.Unmarshal(raw, &turn); err != nil {
		r.Review = "invalid_turn_json"
		return
	}
	switch turn.Phase {
	case "waiting", "dispatch":
		r.Phase = turn.Phase
	default:
		r.Review = "unknown_phase"
	}
	r.EventCount = len(turn.Events)
	for _, event := range turn.Events {
		var message struct {
			MessageID string `json:"message_id"`
		}
		if json.Unmarshal(event, &message) != nil || message.MessageID == "" {
			r.Review = "invalid_or_missing_message_id"
			continue
		}
		r.MessageIDs = append(r.MessageIDs, message.MessageID)
	}
	if len(turn.Events) == 0 {
		r.Review = "turn_without_events"
	}
}

func inventory(ctx context.Context, client *redis.Client) ([]*row, error) {
	rows := map[string]*row{}
	seen := map[string]bool{}
	get := func(parts []string) *row {
		key := parts[1] + ":" + parts[2]
		if rows[key] == nil {
			rows[key] = &row{ContactID: parts[1], ConversationID: parts[2], MessageIDs: []string{}}
		}
		return rows[key]
	}
	var cursor uint64
	for {
		keys, next, err := client.Scan(ctx, cursor, "bot_runtime:durable:*", 500).Result()
		if err != nil {
			return nil, err
		}
		for _, key := range keys {
			if seen[key] {
				continue
			}
			seen[key] = true
			if parts := legacyReceipt.FindStringSubmatch(key); parts != nil {
				get(parts).ReceiptCount++
				continue
			}
			parts := legacyKey.FindStringSubmatch(key)
			if parts == nil {
				continue
			}
			r := get(parts)
			raw, err := client.Get(ctx, key).Bytes()
			if err == redis.Nil {
				r.Review = "turn_changed_during_scan"
				continue
			}
			if err != nil {
				return nil, err
			}
			r.Pending = true
			parseTurn(raw, r)
			_, err = client.ZScore(ctx, "bot_runtime:durable:ready", key).Result()
			if err == nil {
				r.Ready = true
			} else if err != redis.Nil {
				return nil, err
			}
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	out := make([]*row, 0, len(rows))
	for _, r := range rows {
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].ConversationID == out[j].ConversationID {
			return out[i].ContactID < out[j].ContactID
		}
		return out[i].ConversationID < out[j].ConversationID
	})
	return out, nil
}

func main() {
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		fmt.Fprintln(os.Stderr, "REDIS_URL is required")
		os.Exit(2)
	}
	options, err := redis.ParseURL(redisURL)
	if err != nil {
		fmt.Fprintln(os.Stderr, "invalid REDIS_URL")
		os.Exit(2)
	}
	client := redis.NewClient(options)
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	rows, err := inventory(ctx, client)
	if err != nil {
		fmt.Fprintln(os.Stderr, "inventory failed; Redis may be unavailable or changed during scan")
		os.Exit(1)
	}
	result := struct {
		GeneratedAt   time.Time `json:"generated_at"`
		Warning       string    `json:"warning"`
		Conversations []*row    `json:"conversations"`
	}{
		time.Now().UTC(), "Read-only, non-atomic inventory. IDs and receipts require CRM reconciliation; do not replay or delete automatically.", rows,
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(result); err != nil {
		fmt.Fprintln(os.Stderr, "failed to write inventory")
		os.Exit(1)
	}
}
