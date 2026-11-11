// reconcile inspects or archives one reviewed legacy turn; it never sends messages.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/EvolutionAPI/evo-bot-runtime/pkg/reconcile"
	"github.com/redis/go-redis/v9"
)

func main() {
	action := flag.String("action", "inspect", "inspect or archive")
	key := flag.String("key", "", "exact durable turn key")
	hash := flag.String("sha256", "", "hash from inspection")
	decision := flag.String("decision", "", "obsolete or human_handoff")
	operator := flag.String("operator", "", "operator reference")
	ticket := flag.String("ticket", "", "review or incident reference")
	stopped := flag.Bool("workers-stopped", false, "confirm ingress and all workers are stopped")
	flag.Parse()
	if flag.NArg() != 0 || (*action != "inspect" && *action != "archive") {
		fail("invalid arguments")
	}
	url := os.Getenv("REDIS_URL")
	if url == "" {
		fail("REDIS_URL is required")
	}
	opt, err := redis.ParseURL(url)
	if err != nil {
		fail("invalid REDIS_URL")
	}
	db := redis.NewClient(opt)
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if *action == "inspect" {
		s, err := reconcile.Inspect(ctx, db, *key)
		if err != nil {
			fail(err.Error())
		}
		if err := json.NewEncoder(os.Stdout).Encode(s); err != nil {
			fail("output failed")
		}
		return
	}
	archive, err := reconcile.Archive(ctx, db, *key, *hash, *decision, *operator, *ticket, *stopped)
	if err != nil {
		fail(err.Error())
	}
	fmt.Printf("archived %s\n", archive)
}
func fail(message string) { fmt.Fprintln(os.Stderr, message); os.Exit(1) }
