package model

import "context"

type responseProvenanceKey struct{}

// WithResponseProvenance carries an opaque signed processor attestation only in
// memory. It is never part of model input or a persisted pipeline state.
func WithResponseProvenance(ctx context.Context, proof string) context.Context {
	return context.WithValue(ctx, responseProvenanceKey{}, proof)
}

func ResponseProvenance(ctx context.Context) string {
	proof, _ := ctx.Value(responseProvenanceKey{}).(string)
	return proof
}
