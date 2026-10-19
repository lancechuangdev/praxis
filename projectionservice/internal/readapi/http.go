// Package readapi exposes owner-scoped, nonauthoritative projection reads.
package readapi

import (
	"context"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

var ErrNotFound = errors.New("projection not found")
var ErrCursor = errors.New("invalid projection cursor")

type Document struct {
	OccurredAt time.Time       `json:"occurred_at"`
	ID         string          `json:"id"`
	Version    string          `json:"version"`
	Data       json.RawMessage `json:"data"`
}
type Page struct {
	Items []Document `json:"items"`
	Next  string     `json:"next_cursor,omitempty"`
}
type Store interface {
	Ready(context.Context) error
	Get(context.Context, string, string, string) (Document, error)
	List(context.Context, string, string, string, int) (Page, error)
}
type Config struct {
	Issuer, Audience, KeyID string
	PublicKey               *rsa.PublicKey
}
type claims struct {
	jwt.RegisteredClaims
	Scope string `json:"scope"`
}
type Handler struct {
	store  Store
	cfg    Config
	parser *jwt.Parser
}

func New(cfg Config, store Store) (http.Handler, error) {
	if cfg.Issuer == "" || cfg.Audience == "" || cfg.KeyID == "" || cfg.PublicKey == nil || (cfg.PublicKey.N.BitLen() < 2048 || cfg.PublicKey.N.BitLen() > 8192) || store == nil {
		return nil, errors.New("projection reads require issuer, audience, key ID, RSA public key >=2048 bits and store")
	}
	h := &Handler{cfg: cfg, store: store, parser: jwt.NewParser(jwt.WithValidMethods([]string{"RS256"}), jwt.WithIssuer(cfg.Issuer), jwt.WithAudience(cfg.Audience), jwt.WithExpirationRequired(), jwt.WithIssuedAt())}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/orders", h.list("orders"))
	mux.HandleFunc("GET /v1/orders/{id}", h.get("orders"))
	mux.HandleFunc("GET /v1/orders/{id}/history", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		if len(id) == 0 || len(id) > 256 {
			http.Error(w, "invalid ID", 400)
			return
		}
		h.list("order-history:"+id)(w, r)
	})
	mux.HandleFunc("GET /v1/activity", h.list("activity"))
	mux.HandleFunc("GET /v1/activity/{id}", h.get("activity"))
	return h.authenticate(mux), nil
}

type ownerKey struct{}

func (h *Handler) authenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "private, no-cache")
		w.Header().Set("Vary", "Authorization")
		token := r.Header.Get("Authorization")
		if !strings.HasPrefix(token, "Bearer ") || len(token) > 8192 {
			w.Header().Set("WWW-Authenticate", "Bearer")
			http.Error(w, "authentication required", 401)
			return
		}
		c := &claims{}
		parsed, err := h.parser.ParseWithClaims(strings.TrimPrefix(token, "Bearer "), c, func(t *jwt.Token) (any, error) {
			if kid, _ := t.Header["kid"].(string); kid != h.cfg.KeyID {
				return nil, errors.New("unknown signing key")
			}
			return h.cfg.PublicKey, nil
		})
		if err != nil || !parsed.Valid || c.Subject == "" || len(c.Subject) > 256 || strings.ContainsAny(c.Subject, "\x00\r\n") || c.IssuedAt == nil || c.ExpiresAt == nil || c.ExpiresAt.Time.Sub(c.IssuedAt.Time) > time.Hour || time.Since(c.IssuedAt.Time) > time.Hour {
			w.Header().Set("WWW-Authenticate", "Bearer")
			http.Error(w, "invalid access token", 401)
			return
		}
		allowed := false
		for _, scope := range strings.Fields(c.Scope) {
			if scope == "projection:read" {
				allowed = true
			}
		}
		if !allowed {
			http.Error(w, "read scope required", 403)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := h.store.Ready(ctx); err != nil {
			http.Error(w, "projection reads unavailable", 503)
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(ctx, ownerKey{}, c.Subject)))
	})
}
func (h *Handler) get(kind string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		if id == "" || len(id) > 256 {
			http.Error(w, "invalid ID", 400)
			return
		}
		owner := r.Context().Value(ownerKey{}).(string)
		doc, err := h.store.Get(r.Context(), kind, owner, id)
		if err != nil {
			readError(w, err)
			return
		}
		respond(w, r, doc)
	}
}
func (h *Handler) list(kind string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if len(r.URL.Query().Get("cursor")) > 2048 {
			http.Error(w, "invalid cursor", 400)
			return
		}
		// Fixed page bounds prevent arbitrary query amplification.
		owner := r.Context().Value(ownerKey{}).(string)
		page, err := h.store.List(r.Context(), kind, owner, r.URL.Query().Get("cursor"), 50)
		if err != nil {
			readError(w, err)
			return
		}
		respond(w, r, page)
	}
}
func readError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrNotFound):
		http.Error(w, "not found", 404)
	case errors.Is(err, ErrCursor):
		http.Error(w, "invalid cursor", 400)
	default:
		http.Error(w, "projection reads unavailable", 503)
	}
}
func respond(w http.ResponseWriter, r *http.Request, v any) {
	raw, err := json.Marshal(v)
	if err != nil {
		http.Error(w, "projection reads unavailable", 503)
		return
	}
	digest := sha256.Sum256(raw)
	etag := fmt.Sprintf("\"%s\"", hex.EncodeToString(digest[:]))
	w.Header().Set("ETag", etag)
	w.Header().Set("Content-Type", "application/json")
	for _, tag := range strings.Split(r.Header.Get("If-None-Match"), ",") {
		tag = strings.TrimSpace(tag)
		if tag == "*" || strings.TrimPrefix(tag, "W/") == etag {
			w.WriteHeader(http.StatusNotModified)
			return
		}
	}
	_, _ = w.Write(append(raw, '\n'))
}
