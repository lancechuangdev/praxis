package readapi

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

type fakeStore struct {
	owner    string
	readyErr error
	calls    int
}

func (s *fakeStore) Ready(context.Context) error { return s.readyErr }
func (s *fakeStore) Get(_ context.Context, kind, owner, id string) (Document, error) {
	s.calls++
	s.owner = owner
	if owner != "alice" {
		return Document{}, ErrNotFound
	}
	return Document{ID: id, Version: "2", Data: json.RawMessage(`{"quantity":"9007199254740993"}`)}, nil
}
func (s *fakeStore) List(_ context.Context, kind, owner, cursor string, limit int) (Page, error) {
	s.owner = owner
	if limit != 50 {
		panic("unbounded page")
	}
	if cursor == "bad" {
		return Page{}, ErrCursor
	}
	return Page{Items: []Document{}}, nil
}
func TestAuthenticatedOwnerReadsAndETags(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	store := &fakeStore{}
	handler, err := New(Config{Issuer: "issuer", Audience: "audience", KeyID: "key-1", PublicKey: &key.PublicKey}, store)
	if err != nil {
		t.Fatal(err)
	}
	token := func(subject string, change func(*claims, *jwt.Token)) string {
		c := &claims{RegisteredClaims: jwt.RegisteredClaims{Issuer: "issuer", Subject: subject, Audience: jwt.ClaimStrings{"audience"}, IssuedAt: jwt.NewNumericDate(time.Now().Add(-time.Minute)), ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Minute))}, Scope: "projection:read"}
		token := jwt.NewWithClaims(jwt.SigningMethodRS256, c)
		token.Header["kid"] = "key-1"
		if change != nil {
			change(c, token)
		}
		raw, err := token.SignedString(key)
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	request := func(path, auth, etag string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodGet, path, nil)
		if auth != "" {
			r.Header.Set("Authorization", "Bearer "+auth)
		}
		if etag != "" {
			r.Header.Set("If-None-Match", etag)
		}
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		return w
	}
	auth := token("alice", nil)
	response := request("/v1/orders/order-1", auth, "")
	if response.Code != 200 || store.owner != "alice" || response.Header().Get("Cache-Control") != "private, no-cache" || !strings.Contains(response.Body.String(), "9007199254740993") {
		t.Fatal(response.Code, response.Body.String(), store.owner)
	}
	etag := response.Header().Get("ETag")
	if etag == "" {
		t.Fatal("missing ETag")
	}
	if got := request("/v1/orders/order-1", auth, "W/"+etag); got.Code != 304 || got.Body.Len() != 0 {
		t.Fatal(got.Code, got.Body.String())
	}
	if got := request("/v1/orders/order-1", token("bob", nil), etag); got.Code != 404 {
		t.Fatal("cross-owner read", got.Code)
	}
	if got := request("/v1/orders", auth, ""); got.Code != 200 {
		t.Fatal(got.Code)
	}
	if got := request("/v1/orders?cursor=bad", auth, ""); got.Code != 400 {
		t.Fatal(got.Code)
	}
	for _, change := range []func(*claims, *jwt.Token){
		func(c *claims, _ *jwt.Token) { c.Issuer = "other" }, func(c *claims, _ *jwt.Token) { c.Audience = jwt.ClaimStrings{"other"} }, func(c *claims, _ *jwt.Token) { c.IssuedAt = nil }, func(c *claims, _ *jwt.Token) { c.ExpiresAt = jwt.NewNumericDate(time.Now().Add(-time.Minute)) }, func(c *claims, _ *jwt.Token) { c.Subject = "" }, func(_ *claims, t *jwt.Token) { t.Header["kid"] = "other" },
	} {
		if got := request("/v1/orders/order-1", token("alice", change), etag); got.Code != 401 {
			t.Fatal("invalid token accepted", got.Code)
		}
	}
	if got := request("/v1/orders/order-1", token("alice", func(c *claims, _ *jwt.Token) { c.Scope = "other" }), etag); got.Code != 403 {
		t.Fatal(got.Code)
	}
	if got := request("/v1/orders/order-1", "", etag); got.Code != 401 {
		t.Fatal(got.Code)
	}
}
