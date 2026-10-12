package adapters

import (
	"context"
	"net"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"
)

type metadataHealthServer struct {
	grpc_health_v1.UnimplementedHealthServer
	metadata chan metadata.MD
}

func (s *metadataHealthServer) Check(ctx context.Context, _ *grpc_health_v1.HealthCheckRequest) (*grpc_health_v1.HealthCheckResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	s.metadata <- md
	return &grpc_health_v1.HealthCheckResponse{Status: grpc_health_v1.HealthCheckResponse_SERVING}, nil
}

func TestGRPCClientPropagatesActiveTraceContext(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	health := &metadataHealthServer{metadata: make(chan metadata.MD, 1)}
	grpc_health_v1.RegisterHealthServer(server, health)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	previousProvider, previousPropagator := otel.GetTracerProvider(), otel.GetTextMapPropagator()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()))
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() {
		otel.SetTracerProvider(previousProvider)
		otel.SetTextMapPropagator(previousPropagator)
		_ = provider.Shutdown(context.Background())
	})
	options := grpcClientOptions()
	options = append(options, grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	connection, err := grpc.NewClient("passthrough:///bufnet", options...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	ctx, span := provider.Tracer("test").Start(context.Background(), "parent")
	defer span.End()
	if _, err = grpc_health_v1.NewHealthClient(connection).Check(ctx, &grpc_health_v1.HealthCheckRequest{}); err != nil {
		t.Fatal(err)
	}
	md := <-health.metadata
	if values := md.Get("traceparent"); len(values) != 1 || values[0] == "" {
		t.Fatalf("traceparent metadata=%v", values)
	}
}
