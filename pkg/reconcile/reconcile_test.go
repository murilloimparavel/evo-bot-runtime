package reconcile

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/EvolutionAPI/evo-bot-runtime/internal/testhelpers"
	"github.com/redis/go-redis/v9"
)

func testDB(t *testing.T) *redis.Client {
	t.Helper()
	c := redis.NewClient(testhelpers.RedisOptions())
	if err := c.Ping(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}
func seed(t *testing.T, c *redis.Client, key string) string {
	t.Helper()
	raw := `{"key":"` + key + `","revision":2,"phase":"waiting","attempts":23,"events":[{"message_id":"msg-1","contact_id":6.0493024878017e+18,"conversation_id":4729,"message_content":"secret text","api_key":"secret key"}]}`
	ctx := context.Background()
	if err := c.Set(ctx, key, raw, 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := c.ZAdd(ctx, "bot_runtime:durable:ready", redis.Z{Score: 0, Member: key}).Err(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		c.Del(ctx, key, key+":lease", key+":receipt:test")
		c.ZRem(ctx, "bot_runtime:durable:ready", key)
	})
	return raw
}
func hash(s string) string { h := sha256.Sum256([]byte(s)); return hex.EncodeToString(h[:]) }
func TestInspectAndArchivePreservesReceiptsAndPayload(t *testing.T) {
	ctx := context.Background()
	c := testDB(t)
	key := "bot_runtime:durable:6049302487801707118:4729"
	raw := seed(t, c, key)
	c.Set(ctx, key+":receipt:test", "1", 0)
	s, err := Inspect(ctx, c, key)
	if err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(s)
	if strings.Contains(string(data), "secret") {
		t.Fatal("snapshot exposes payload")
	}
	if !s.Legacy || s.SHA256 != hash(raw) || s.ConversationID != "4729" {
		t.Fatalf("bad snapshot: %+v", s)
	}
	archive, err := Archive(ctx, c, key, s.SHA256, "human_handoff", "operator", "incident-1", true)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Del(ctx, archive, archive+":audit") })
	if c.Exists(ctx, key).Val() != 0 || c.ZScore(ctx, "bot_runtime:durable:ready", key).Err() != redis.Nil {
		t.Fatal("turn still queued")
	}
	if c.Get(ctx, archive).Val() != raw || c.Get(ctx, key+":receipt:test").Val() != "1" {
		t.Fatal("payload or receipt lost")
	}
	if c.HGet(ctx, archive+":audit", "decision").Val() != "human_handoff" {
		t.Fatal("audit missing")
	}
	if _, err := Archive(ctx, c, key, s.SHA256, "obsolete", "operator", "incident-1", true); err == nil {
		t.Fatal("archive repeated")
	}
}
func TestArchiveRefusesChangedTurnAndActiveLease(t *testing.T) {
	ctx := context.Background()
	c := testDB(t)
	key := "bot_runtime:durable:6049302487801707118:4730"
	raw := seed(t, c, key)
	old := hash(raw)
	newer := strings.Replace(raw, `"revision":2`, `"revision":3`, 1)
	c.Set(ctx, key, newer, 0)
	if _, err := Archive(ctx, c, key, old, "obsolete", "op", "ticket", true); err == nil {
		t.Fatal("accepted stale snapshot")
	}
	c.Set(ctx, key+":lease", "worker", time.Minute)
	if _, err := Archive(ctx, c, key, hash(newer), "obsolete", "op", "ticket", true); err == nil {
		t.Fatal("accepted active lease")
	}
	if _, err := Archive(ctx, c, key, hash(newer), "obsolete", "op", "ticket", false); err == nil {
		t.Fatal("accepted without pause")
	}
	if c.Get(ctx, key).Val() != newer || c.ZScore(ctx, "bot_runtime:durable:ready", key).Err() != nil {
		t.Fatal("refusal changed turn")
	}
}
func TestArchiveRefusesSavedOutputAndCurrentTurn(t *testing.T) {
	ctx := context.Background()
	c := testDB(t)
	key := "bot_runtime:durable:6049302487801707118:4731"
	raw := seed(t, c, key)
	saved := strings.Replace(raw, `"phase":"waiting"`, `"phase":"dispatch","content":"saved"`, 1)
	c.Set(ctx, key, saved, 0)
	if _, err := Archive(ctx, c, key, hash(saved), "obsolete", "op", "ticket", true); err == nil {
		t.Fatal("accepted saved output")
	}
	current := strings.ReplaceAll(strings.Replace(raw, `6.0493024878017e+18`, `"6049302487801707118"`, 1), `"conversation_id":4729`, `"conversation_id":"4731"`)
	c.Set(ctx, key, current, 0)
	if _, err := Archive(ctx, c, key, hash(current), "obsolete", "op", "ticket", true); err == nil {
		t.Fatal("accepted nonlegacy turn")
	}
}

func TestArchiveAtomicFenceOnChangesAfterInspection(t *testing.T) {
	ctx := context.Background()
	c := testDB(t)
	key := "bot_runtime:durable:6049302487801707118:4732"
	raw := seed(t, c, key)
	s, snapshot, err := read(ctx, c, key)
	if err != nil {
		t.Fatal(err)
	}
	newer := strings.Replace(raw, `"revision":2`, `"revision":3`, 1)
	if err := c.Set(ctx, key, newer, 0).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := archiveSnapshot(ctx, c, s, snapshot, "obsolete", "op", "ticket"); err == nil {
		t.Fatal("accepted concurrent change")
	}
	if c.Get(ctx, key).Val() != newer {
		t.Fatal("overwrote concurrent change")
	}
	if err := c.Set(ctx, key, raw, 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := c.Set(ctx, key+":lease", "worker", time.Minute).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := archiveSnapshot(ctx, c, s, snapshot, "obsolete", "op", "ticket"); err == nil {
		t.Fatal("accepted concurrent lease")
	}
}
