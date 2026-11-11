# Durable conversation processing

The production constructor now uses a Redis-backed durable turn worker. The old
pipelineService implementation is retained solely for legacy regression coverage;
new reliability tests exercise durableService and the production constructor.

## Delivery contract

`POST /events` returns 202 only after a Lua operation stores the complete event,
a receipt scoped to contact/conversation/bot/message ID, and its ready index.
Concurrent/repeated deliveries cannot append the same event twice. A Redis error
returns 500 so the CRM delivery ledger retries with the original message ID.

A turn retains all accepted events in acceptance order until its response has
been delivered. New events extend the debounce window and invalidate the previous
revision. IDs and text are derived from the same persisted event array. AI sees
all text and attachments in that snapshot. Original source IDs remain available
to runtime traces and internal authorization; they are not serialized in the A2A
body. Persisted runtime events and the batch hash establish the source mapping.

Workers across replicas claim one execution lease per conversation, renew it, and
use revision+token compare-and-set for result persistence, retry and completion.
A newer revision cancels older work but does not begin another runtime execution
until the previous call has returned or its execution lease has expired. Only
short Redis operations run inside scripts; model calls and HTTP do not lock Redis.

AI output is persisted before dispatch. If a postback fails or its acknowledgement
is lost, retry uses the saved output rather than generating another answer.
`runtime_delivery_id` is stable across retries for each batch/text segment/media
segment. CRM must atomically deduplicate that field with message creation. Deploy
the compatible CRM receiver before enabling this runtime.

Immediately before every segment the dispatcher checks the generation against
Redis. A message arriving after that check can still race with an HTTP request
already in flight. Segments already delivered cannot be retracted. Cancellation
also cannot undo external tool effects; tools must implement their own idempotency
and the Processor must serialize/fence executions if external effects require it.
This is at-least-once delivery with idempotent consumers, not exactly-once effects.

## Persistence and retention requirements

Redis is the durable queue, not an expendable cache: enable AOF, protect its volume,
use noeviction and monitor disk/memory, replication/backups and recovery. Lua
atomicity does not itself provide disk durability. With appendfsync everysec an
abrupt Redis loss can lose the most recent second; select stronger persistence or
source reconciliation when that loss window is unacceptable. CRM outbox must
retain source IDs and support controlled replay/reconciliation after Redis loss.

Pending turns have no TTL. Receipts also have no automatic TTL because CRM retries
indefinitely: expiring them independently would permit old deliveries to resurrect
completed turns. Coordinated receipt cleanup must be tied to deletion/terminal
retention of the corresponding CRM delivery source. Receipts store only a digest
and marker; completed payloads and credentials are deleted. Pending payloads
contain private message content and credentials: Redis access must be restricted,
backups encrypted, and logs must never print those payloads.

A failed turn retries with capped exponential delay (2 to 64 seconds). The
configured AI_FAILURE_NOTICE is sent once per failed generation with a stable
delivery ID, while the pending turn remains available for retry. Work remains
pending across restart, including saved dispatch output. Alert on oldest pending
age, repeated failed turns and queue growth; a permanently invalid endpoint or an
expired attachment/grant requires operator remediation, not silent deletion.
There are at most 32 active turns per replica; other work remains indexed in Redis.
The current scripts require a single Redis primary, not Redis Cluster cross-slot.

## Cutover and rollback

Do not run the legacy and durable consumers against the same conversation feed.
Pause CRM runtime delivery while retaining events in its outbox, let the old
runtime drain, then verify legacy state/buffer/timer work is empty. Investigate
orphaned legacy keys without deleting customer messages. The new constructor fails
startup if any legacy `bot_runtime:state:*` exists, rather than abandoning it.
Legacy incoming states were never sufficient to reconstruct full events; reconcile
those against CRM original IDs and replay via its delivery ledger after cutover.

Start the durable runtime, verify health and canary processing, then resume CRM
outbox delivery. Pending durable turns survive runtime restart. A rollback must
pause ingestion and drain/resolve durable turns first: the old binary does not
understand the new `bot_runtime:durable:*` queue. Never clear Redis as a deployment
or rollback step. Publication/deployment requires its own explicit authorization.

## Validation

Use an isolated disposable Redis. Existing legacy TestMain functions flush their
configured DB, so never point tests at a shared or production Redis. Run packages
serially (`go test -p 1`) because they otherwise share that test DB. New tests cover
atomic deduplication, full-event recovery, stale generation fencing, competing
leases, saved dispatch replay, rapid messages, restart of the accepting process,
and delaying new AI execution until the old cancelled call actually returns.
