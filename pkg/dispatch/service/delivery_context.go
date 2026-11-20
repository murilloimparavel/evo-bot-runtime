package service

import "context"

type deliveryContextKey struct{}
type deliveryContext struct {
	id               string
	guard            func(context.Context) error
	sourceMessageIDs []string
	sourceAgentBotID string
}

// WithDelivery associates stable segment IDs and a distributed generation guard
// with a turn. Old callers retain the existing postback contract.
func WithDelivery(ctx context.Context, id string, guard func(context.Context) error) context.Context {
	return context.WithValue(ctx, deliveryContextKey{}, deliveryContext{id: id, guard: guard})
}
func delivery(ctx context.Context) deliveryContext {
	v, _ := ctx.Value(deliveryContextKey{}).(deliveryContext)
	return v
}

// WithSourceMessages identifies the events that produced a handoff response.
func WithSourceMessages(ctx context.Context, ids []string, agentID string) context.Context {
	info := delivery(ctx)
	info.sourceMessageIDs = append([]string(nil), ids...)
	info.sourceAgentBotID = agentID
	return context.WithValue(ctx, deliveryContextKey{}, info)
}
