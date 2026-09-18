package transport

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestReady(t *testing.T) {
	tests := []struct {
		name   string
		ready  func(context.Context) error
		status int
	}{
		{name: "dependencies ready", ready: func(context.Context) error { return nil }, status: http.StatusOK},
		{name: "dependency unavailable", ready: func(context.Context) error { return errors.New("unavailable") }, status: http.StatusServiceUnavailable},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodGet, "/readyz", nil)
			response := httptest.NewRecorder()

			HTTP{Ready: test.ready}.Handler().ServeHTTP(response, request)

			if response.Code != test.status {
				t.Fatalf("status=%d, want %d", response.Code, test.status)
			}
		})
	}
}
