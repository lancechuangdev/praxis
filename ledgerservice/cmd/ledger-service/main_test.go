package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestCheckReady(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/readyz" {
			t.Errorf("path = %q", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	if err := checkReady(strings.TrimPrefix(server.URL, "http://")); err != nil {
		t.Fatal(err)
	}
}

func TestCheckReadyRejectsUnhealthy(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()
	if err := checkReady(strings.TrimPrefix(server.URL, "http://")); err == nil {
		t.Fatal("expected unhealthy status")
	}
}

func TestCommandMode(t *testing.T) {
	for _, tc := range []struct {
		args    []string
		want    string
		wantErr bool
	}{
		{want: "serve"},
		{args: []string{"healthcheck"}, want: "healthcheck"},
		{args: []string{"migrate"}, want: "migrate"},
		{args: []string{"unknown"}, wantErr: true},
		{args: []string{"migrate", "extra"}, wantErr: true},
	} {
		got, err := commandMode(tc.args)
		if got != tc.want || (err != nil) != tc.wantErr {
			t.Fatalf("commandMode(%v) = %q, %v; want %q, error=%t", tc.args, got, err, tc.want, tc.wantErr)
		}
	}
}
