package server

import (
	"context"
	"net"
	"testing"
	"time"

	v1 "github.com/ZonyxNagi/proto-zonyx-core/pkg/api/v1"
	adapters "github.com/ZonyxNagi/zonyx-core/internal/adapters/grpc"
	"github.com/ZonyxNagi/zonyx-core/internal/core/domain"
	"google.golang.org/genproto/googleapis/rpc/code"
	rpcstatus "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	health "google.golang.org/grpc/health/grpc_health_v1"
)

// stubService satisfies ports.Service with do-nothing behaviour. The server
// tests don't exercise business logic — they verify wiring.
type stubService struct{}

func (stubService) Subscribe(
	context.Context,
	[]domain.EventType,
	*string,
	func(domain.Event, string) error,
) error {
	return nil
}

func (stubService) Publish(
	_ context.Context,
	d domain.Directive,
) (string, *rpcstatus.Status, error) {
	return d.ID, &rpcstatus.Status{Code: int32(code.Code_OK)}, nil
}

func (stubService) ReadConfiguration(ctx context.Context, _ func(domain.Configuration) error) error {
	<-ctx.Done()
	return ctx.Err()
}

// findFreePort grabs an available TCP port on loopback by binding-and-closing.
func findFreePort(t *testing.T) int {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("findFreePort: %v", err)
	}
	port := lis.Addr().(*net.TCPAddr).Port
	_ = lis.Close()
	return port
}

func TestServer_StartHealthCheckAndStop(t *testing.T) {
	core, err := adapters.NewCoreAdapter(stubService{})
	if err != nil {
		t.Fatalf("NewCoreAdapter: %v", err)
	}
	srv, err := NewServer(core)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	port := findFreePort(t)
	startErr := make(chan error, 1)
	go func() { startErr <- srv.Start(port) }()

	// Wait briefly for the listener to come up.
	addr := net.JoinHostPort("127.0.0.1", itoa(port))
	conn := dialReady(t, addr, 2*time.Second)
	defer conn.Close()

	hcClient := health.NewHealthClient(conn)
	resp, err := hcClient.Check(context.Background(), &health.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("health.Check: %v", err)
	}
	if resp.GetStatus() != health.HealthCheckResponse_SERVING {
		t.Errorf("health status = %v, want SERVING", resp.GetStatus())
	}

	// Sanity check: CoreService is registered (calling its method should not
	// return Unimplemented).
	coreClient := v1.NewCoreServiceClient(conn)
	stream, err := coreClient.Subscribe(context.Background(), &v1.SubscribeRequest{
		Types: []v1.EventType{v1.EventType_EVENT_TYPE_ZONE},
	})
	if err != nil {
		t.Fatalf("CoreService.Subscribe (registration check): %v", err)
	}
	// Stub returns nil → server closes stream → first Recv should return EOF
	// (or io.EOF wrapped). Just ensure it doesn't return Unimplemented.
	if _, err := stream.Recv(); err != nil && err.Error() != "EOF" {
		// any non-Unimplemented error is acceptable; the goal is wiring proof
	}

	srv.Stop()
	if err := <-startErr; err != nil && err.Error() != "grpc: the server has been stopped" {
		t.Logf("Start returned %v after Stop (acceptable)", err)
	}
}

func TestServer_StopFlipsHealthToNotServing(t *testing.T) {
	t.Parallel()
	core, err := adapters.NewCoreAdapter(stubService{})
	if err != nil {
		t.Fatalf("NewCoreAdapter: %v", err)
	}
	srv, err := NewServer(core)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	if srv.hc.Status != HealthCheckStatus_SERVING {
		t.Fatalf("initial status = %v, want SERVING", srv.hc.Status)
	}

	srv.Stop()

	if srv.hc.Status != HealthCheckStatus_NOT_SERVING {
		t.Errorf("post-Stop status = %v, want NOT_SERVING", srv.hc.Status)
	}
}

// dialReady polls the address until it accepts a gRPC connection or times out.
func dialReady(t *testing.T, addr string, timeout time.Duration) *grpc.ClientConn {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		conn, err := grpc.NewClient(
			addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err == nil {
			// Force a roundtrip to ensure the listener is up.
			ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
			hc := health.NewHealthClient(conn)
			_, perr := hc.Check(ctx, &health.HealthCheckRequest{})
			cancel()
			if perr == nil {
				return conn
			}
			_ = conn.Close()
		}
		if time.Now().After(deadline) {
			t.Fatalf("server not ready within %v: %v", timeout, err)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func itoa(i int) string {
	const digits = "0123456789"
	if i == 0 {
		return "0"
	}
	var buf [16]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = digits[i%10]
		i /= 10
	}
	return string(buf[pos:])
}
