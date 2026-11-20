package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestInventoryIsReadOnlyAndDoesNotExposeContents(t *testing.T) {
	endpoint := os.Getenv("REDIS_TEST_URL")
	if endpoint == "" {
		t.Skip("isolated REDIS_TEST_URL required")
	}
	opts, err := redis.ParseURL(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	client := redis.NewClient(opts)
	ctx := context.Background()
	id := time.Now().UnixNano()
	key := fmt.Sprintf("bot_runtime:durable:%d:%d", id, id)
	receipt := key + ":receipt:" + strings.Repeat("a", 64)
	raw := `{"phase":"waiting","events":[{"message_id":"abc-123","api_key":"private-token","message_content":"private-message"}]}`
	if err := client.Set(ctx, key, raw, 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.Set(ctx, receipt, "1", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := client.ZAdd(ctx, "bot_runtime:durable:ready", redis.Z{Score: 1, Member: key}).Err(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		client.Del(ctx, key, receipt)
		client.ZRem(ctx, "bot_runtime:durable:ready", key)
		client.Close()
	})
	rows, err := inventory(ctx, client)
	if err != nil {
		t.Fatal(err)
	}
	var found *row
	for _, item := range rows {
		if item.ContactID == fmt.Sprint(id) {
			found = item
		}
	}
	if found == nil || !found.Pending || !found.Ready || found.EventCount != 1 || found.ReceiptCount != 1 || len(found.MessageIDs) != 1 || found.MessageIDs[0] != "abc-123" {
		t.Fatalf("inventory incomplete: %+v", found)
	}
	result, err := json.Marshal(rows)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(result), "private-token") || strings.Contains(string(result), "private-message") {
		t.Fatal("sensitive payload exposed")
	}
	if got := client.Get(ctx, key).Val(); got != raw {
		t.Fatal("legacy payload changed")
	}
	if client.Exists(ctx, receipt).Val() != 1 {
		t.Fatal("legacy receipt changed")
	}
	if _, err := client.ZScore(ctx, "bot_runtime:durable:ready", key).Result(); err != nil {
		t.Fatal("legacy queue changed")
	}
}
