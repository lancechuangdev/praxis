// mesh-probe verifies mesh authorization through the read-only gRPC health API.
package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	health "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

func check(ctx context.Context, target, expectation string) error {
	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()
	response, err := health.NewHealthClient(conn).Check(ctx, &health.HealthCheckRequest{}, grpc.WaitForReady(true))
	if expectation == "deny" {
		if status.Code(err) == codes.PermissionDenied && strings.Contains(status.Convert(err).Message(), "RBAC") {
			return nil
		}
		return fmt.Errorf("expected explicit mesh RBAC PermissionDenied, got %v", err)
	}
	if err != nil {
		return err
	}
	if response.GetStatus() != health.HealthCheckResponse_SERVING {
		return fmt.Errorf("backend is not serving: %v", response.GetStatus())
	}
	return nil
}

func main() {
	target := flag.String("target", "", "Backend host:port")
	expect := flag.String("expect", "allow", "allow or deny")
	quit := flag.Bool("quit-sidecar", false, "Stop this probe pod's classic sidecar after the check")
	flag.Parse()
	if *target == "" || (*expect != "allow" && *expect != "deny") {
		fmt.Fprintln(os.Stderr, "target and allow/deny expectation are required")
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	err := check(ctx, *target, *expect)
	cancel()
	if *quit {
		client := &http.Client{Timeout: 3 * time.Second}
		response, quitErr := client.Post("http://127.0.0.1:15020/quitquitquit", "text/plain", nil)
		if quitErr == nil {
			response.Body.Close()
			if response.StatusCode != http.StatusOK {
				quitErr = fmt.Errorf("sidecar shutdown status: %s", response.Status)
			}
		}
		if quitErr != nil {
			fmt.Fprintln(os.Stderr, quitErr)
			if err == nil {
				err = quitErr
			}
		}
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("PASS: %s expected %s\n", *target, *expect)
}
