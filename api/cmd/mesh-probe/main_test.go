package main

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	health "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

type testHealth struct {
	health.UnimplementedHealthServer
	code    codes.Code
	message string
	serving health.HealthCheckResponse_ServingStatus
}

func (s testHealth) Check(context.Context, *health.HealthCheckRequest) (*health.HealthCheckResponse, error) {
	if s.code != codes.OK {
		return nil, status.Error(s.code, s.message)
	}
	return &health.HealthCheckResponse{Status: s.serving}, nil
}
func TestProbeDistinguishesDenialFromOutage(t *testing.T) {
	for _, tt := range []struct {
		name, expectation string
		server            testHealth
		pass              bool
	}{
		{"serving", "allow", testHealth{serving: health.HealthCheckResponse_SERVING}, true},
		{"not serving", "allow", testHealth{serving: health.HealthCheckResponse_NOT_SERVING}, false},
		{"denied", "deny", testHealth{code: codes.PermissionDenied, message: "RBAC: access denied"}, true},
		{"allowed unexpectedly", "deny", testHealth{serving: health.HealthCheckResponse_SERVING}, false},
		{"unavailable", "deny", testHealth{code: codes.Unavailable, message: "connection reset"}, false},
		{"application denial", "deny", testHealth{code: codes.PermissionDenied, message: "application error"}, false},
		{"denied authorized identity", "allow", testHealth{code: codes.PermissionDenied, message: "RBAC: access denied"}, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			server := grpc.NewServer()
			health.RegisterHealthServer(server, tt.server)
			go server.Serve(listener)
			defer server.Stop()
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()
			err = check(ctx, listener.Addr().String(), tt.expectation)
			if (err == nil) != tt.pass {
				t.Fatalf("check returned %v, want pass=%v", err, tt.pass)
			}
		})
	}
}
