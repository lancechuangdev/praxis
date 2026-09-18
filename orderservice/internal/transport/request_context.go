package transport

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"strings"
)

const (
	requestIDHeader     = "X-Request-ID"
	correlationIDHeader = "X-Correlation-ID"
	traceParentHeader   = "traceparent"
	traceStateHeader    = "tracestate"
)

type requestMetadata struct {
	RequestID     string
	CorrelationID string
	TraceParent   string
	TraceState    string
}

type requestMetadataKey struct{}

func withRequestMetadata(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestID := safeID(r.Header.Get(requestIDHeader))
		if requestID == "" {
			requestID = newID("req_")
		}
		correlationID := safeID(r.Header.Get(correlationIDHeader))
		if correlationID == "" {
			correlationID = requestID
		}
		traceParent := validTraceParent(r.Header.Get(traceParentHeader))
		traceState := validTraceState(r.Header.Get(traceStateHeader))
		w.Header().Set(requestIDHeader, requestID)
		w.Header().Set(correlationIDHeader, correlationID)
		metadata := requestMetadata{RequestID: requestID, CorrelationID: correlationID, TraceParent: traceParent, TraceState: traceState}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), requestMetadataKey{}, metadata)))
	})
}

func validTraceParent(value string) string {
	value = strings.TrimSpace(value)
	if len(value) != 55 || value[2] != '-' || value[35] != '-' || value[52] != '-' || value[:2] == "ff" {
		return ""
	}
	for i, char := range value {
		if i == 2 || i == 35 || i == 52 {
			continue
		}
		if !((char >= '0' && char <= '9') || (char >= 'a' && char <= 'f')) {
			return ""
		}
	}
	if value[3:35] == strings.Repeat("0", 32) || value[36:52] == strings.Repeat("0", 16) {
		return ""
	}
	return value
}

func validTraceState(value string) string {
	value = strings.TrimSpace(value)
	if len(value) > 512 {
		return ""
	}
	for _, char := range value {
		if char < 0x20 || char > 0x7e {
			return ""
		}
	}
	return value
}

func metadataFromContext(ctx context.Context) requestMetadata {
	metadata, _ := ctx.Value(requestMetadataKey{}).(requestMetadata)
	return metadata
}

func safeID(value string) string {
	value = strings.TrimSpace(value)
	if value == "" || len(value) > 128 {
		return ""
	}
	for _, char := range value {
		if (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') || (char >= '0' && char <= '9') || strings.ContainsRune("-_.:", char) {
			continue
		}
		return ""
	}
	return value
}

func newID(prefix string) string {
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		panic("crypto/rand failed: " + err.Error())
	}
	return prefix + hex.EncodeToString(random[:])
}
