package ledger

import "context"

type InboxMetadata struct{ ConsumerName, EventID, EventType string }
type inboxMetadataKey struct{}

func WithInboxMetadata(ctx context.Context, metadata InboxMetadata) context.Context {
	return context.WithValue(ctx, inboxMetadataKey{}, metadata)
}

func InboxMetadataFromContext(ctx context.Context) (InboxMetadata, bool) {
	metadata, ok := ctx.Value(inboxMetadataKey{}).(InboxMetadata)
	return metadata, ok && metadata.ConsumerName != "" && metadata.EventID != "" && metadata.EventType != ""
}
