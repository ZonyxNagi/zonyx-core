package adapters

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	v1 "github.com/ZonyxNagi/proto-zonyx-core/pkg/api/v1"
	"github.com/ZonyxNagi/zonyx-core/internal/core/domain"
	"google.golang.org/genproto/googleapis/rpc/code"
	rpcstatus "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

const bufSize = 1024 * 64

// fakeService implements ports.Service for adapter-level tests. The hooks let
// each test rewrite Subscribe/Publish behaviour without spinning up the real
// core service.
type fakeService struct {
	mu sync.Mutex

	subscribeCalls []struct {
		types  []domain.EventType
		offset *string
	}
	subscribeFn func(ctx context.Context, types []domain.EventType, offset *string,
		send func(e domain.Event, offset string) error) error

	publishCalls []domain.Directive
	publishFn    func(ctx context.Context, d domain.Directive) (string, *rpcstatus.Status, error)
}

func (f *fakeService) Subscribe(
	ctx context.Context,
	types []domain.EventType,
	offset *string,
	send func(e domain.Event, offset string) error,
) error {
	f.mu.Lock()
	f.subscribeCalls = append(f.subscribeCalls, struct {
		types  []domain.EventType
		offset *string
	}{types, offset})
	fn := f.subscribeFn
	f.mu.Unlock()
	if fn != nil {
		return fn(ctx, types, offset, send)
	}
	return nil
}

func (f *fakeService) Publish(
	ctx context.Context,
	d domain.Directive,
) (string, *rpcstatus.Status, error) {
	f.mu.Lock()
	f.publishCalls = append(f.publishCalls, d)
	fn := f.publishFn
	f.mu.Unlock()
	if fn != nil {
		return fn(ctx, d)
	}
	return d.ID, &rpcstatus.Status{Code: int32(code.Code_OK)}, nil
}

func (f *fakeService) ReadConfiguration(ctx context.Context, _ func(domain.Configuration) error) error {
	<-ctx.Done()
	return ctx.Err()
}

// startServer spins up a CoreAdapter behind a bufconn listener and returns a
// client + cleanup func.
func startServer(t *testing.T, svc *fakeService) (v1.CoreServiceClient, func()) {
	t.Helper()
	lis := bufconn.Listen(bufSize)
	srv := grpc.NewServer()
	adapter, err := NewCoreAdapter(svc)
	if err != nil {
		t.Fatalf("NewCoreAdapter: %v", err)
	}
	v1.RegisterCoreServiceServer(srv, adapter)
	go func() { _ = srv.Serve(lis) }()

	dialer := func(context.Context, string) (net.Conn, error) { return lis.Dial() }
	conn, err := grpc.NewClient(
		"passthrough://bufnet",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc dial: %v", err)
	}

	cleanup := func() {
		_ = conn.Close()
		srv.Stop()
	}
	return v1.NewCoreServiceClient(conn), cleanup
}

func TestCoreAdapter_PublishEchoesOK(t *testing.T) {
	t.Parallel()
	svc := &fakeService{}
	client, cleanup := startServer(t, svc)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := client.Publish(ctx)
	if err != nil {
		t.Fatalf("client.Publish: %v", err)
	}

	// Send three directives.
	directives := []*v1.Directive{
		{Id: "directive-1", Name: "register_device"},
		{Id: "directive-2", Name: "update_zone"},
		{Id: "directive-3", Name: "reset_actor"},
	}

	for _, d := range directives {
		if err := stream.Send(&v1.PublishRequest{Directive: d}); err != nil {
			t.Fatalf("stream.Send: %v", err)
		}
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("CloseSend: %v", err)
	}

	// Expect three responses with matching directive IDs and OK status.
	for i, d := range directives {
		resp, err := stream.Recv()
		if err != nil {
			t.Fatalf("Recv #%d: %v", i, err)
		}
		if resp.GetDirectiveId() != d.GetId() {
			t.Errorf("DirectiveId = %q, want %q", resp.GetDirectiveId(), d.GetId())
		}
		if resp.GetStatus() == nil {
			t.Errorf("Status is nil")
		}
		if got := codes.Code(resp.GetStatus().GetCode()); got != codes.OK {
			t.Errorf("Status.Code = %v, want OK", got)
		}
	}

	// Server returns nil after EOF — Recv should now return io.EOF.
	if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
		t.Errorf("expected io.EOF after CloseSend, got %v", err)
	}
}

func TestCoreAdapter_PublishHandlesEmptyDirective(t *testing.T) {
	t.Parallel()
	svc := &fakeService{}
	client, cleanup := startServer(t, svc)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := client.Publish(ctx)
	if err != nil {
		t.Fatalf("client.Publish: %v", err)
	}

	// Sending an empty PublishRequest (no directive) — current adapter still
	// echoes a response with an empty directive_id and OK status. This locks
	// the placeholder behaviour until Service.Publish wiring lands.
	if err := stream.Send(&v1.PublishRequest{}); err != nil {
		t.Fatalf("stream.Send: %v", err)
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("CloseSend: %v", err)
	}

	resp, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if resp.GetDirectiveId() != "" {
		t.Errorf("DirectiveId = %q, want empty", resp.GetDirectiveId())
	}
	if got := codes.Code(resp.GetStatus().GetCode()); got != codes.OK {
		t.Errorf("Status.Code = %v, want OK", got)
	}
}

func TestCoreAdapter_Subscribe_PropagatesTypesAndOffset(t *testing.T) {
	t.Parallel()

	var capturedTypes []domain.EventType
	var capturedOffset *string

	svc := &fakeService{}
	svc.subscribeFn = func(
		_ context.Context,
		types []domain.EventType,
		offset *string,
		_ func(domain.Event, string) error,
	) error {
		capturedTypes = types
		capturedOffset = offset
		return nil
	}

	client, cleanup := startServer(t, svc)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	off := "042"
	stream, err := client.Subscribe(ctx, &v1.SubscribeRequest{
		Types:  []v1.EventType{v1.EventType_EVENT_TYPE_ZONE, v1.EventType_EVENT_TYPE_PRESENCE},
		Offset: &off,
	})
	if err != nil {
		t.Fatalf("client.Subscribe: %v", err)
	}

	// subscribeFn returns nil → stream closes → client sees io.EOF.
	if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
		t.Errorf("expected io.EOF, got %v", err)
	}

	if len(capturedTypes) != 2 ||
		capturedTypes[0] != domain.EventTypeZone ||
		capturedTypes[1] != domain.EventTypePresence {
		t.Errorf("types = %v, want [Zone Presence]", capturedTypes)
	}
	if capturedOffset == nil || *capturedOffset != "042" {
		t.Errorf("offset = %v, want \"042\"", capturedOffset)
	}
}

func TestCoreAdapter_Subscribe_MarshalEventsToResponse(t *testing.T) {
	t.Parallel()

	now := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	evt := domain.Event{
		ID:   "event-abc",
		Type: domain.EventTypeZone,
		Actor: domain.Actor{
			ID:       "actor-1",
			Metadata: map[string]string{"role": "player"},
		},
		State: domain.State{
			Location: &domain.Location{
				Point: &domain.Point{X: 1.0, Y: 2.0, Z: 3.0},
				Zones: []string{"zone-a"},
			},
			Presence: domain.Presence{
				Type:  domain.PresenceTypeActive,
				Since: now,
			},
		},
		Timestamp: now,
	}

	svc := &fakeService{}
	svc.subscribeFn = func(
		_ context.Context,
		_ []domain.EventType,
		_ *string,
		send func(domain.Event, string) error,
	) error {
		return send(evt, "042")
	}

	client, cleanup := startServer(t, svc)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := client.Subscribe(ctx, &v1.SubscribeRequest{})
	if err != nil {
		t.Fatalf("client.Subscribe: %v", err)
	}

	resp, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}

	if resp.GetOffset() != "042" {
		t.Errorf("Offset = %q, want \"042\"", resp.GetOffset())
	}
	if len(resp.GetEvents()) != 1 {
		t.Fatalf("Events count = %d, want 1", len(resp.GetEvents()))
	}

	e := resp.GetEvents()[0]
	if e.GetId() != "event-abc" {
		t.Errorf("Event.Id = %q, want \"event-abc\"", e.GetId())
	}
	if e.GetType() != v1.EventType_EVENT_TYPE_ZONE {
		t.Errorf("Event.Type = %v, want EVENT_TYPE_ZONE", e.GetType())
	}
	if e.GetActor().GetId() != "actor-1" {
		t.Errorf("Actor.Id = %q, want \"actor-1\"", e.GetActor().GetId())
	}
	if e.GetState().GetLocation().GetPoint().GetX() != float32(1.0) {
		t.Errorf("Point.X = %v, want 1.0", e.GetState().GetLocation().GetPoint().GetX())
	}
	if e.GetState().GetPresence().GetType() != v1.PresenceType_PRESENCE_TYPE_ACTIVE {
		t.Errorf("Presence.Type = %v, want PRESENCE_TYPE_ACTIVE", e.GetState().GetPresence().GetType())
	}

	// subscribeFn already returned after one send → stream closes → io.EOF.
	if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
		t.Errorf("expected io.EOF after single event, got %v", err)
	}
}

func TestNewCoreAdapter(t *testing.T) {
	t.Parallel()
	svc := &fakeService{}
	a, err := NewCoreAdapter(svc)
	if err != nil {
		t.Fatalf("NewCoreAdapter: %v", err)
	}
	if a == nil {
		t.Fatal("nil adapter")
	}
	if a.service == nil {
		t.Error("service field not wired")
	}
}

