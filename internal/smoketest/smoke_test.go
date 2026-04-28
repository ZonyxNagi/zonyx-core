//go:build smoke

// Package smoketest exercises the full request path end-to-end using an
// in-process stack (embedded NATS, JetStream repo, QUUPPA adapter, gRPC
// server). No external services are required.
//
// Run with: go test -tags smoke -v -timeout 120s ./internal/smoketest/
package smoketest

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"strconv"
	"testing"
	"time"

	pb "github.com/ZonyxNagi/proto-zonyx-core/pkg/api/v1"
	adapters "github.com/ZonyxNagi/zonyx-core/internal/adapters/grpc"
	"github.com/ZonyxNagi/zonyx-core/internal/adapters/grpc/server"
	"github.com/ZonyxNagi/zonyx-core/internal/adapters/quuppa"
	"github.com/ZonyxNagi/zonyx-core/internal/core/ports"
	"github.com/ZonyxNagi/zonyx-core/internal/core/services"
	"github.com/ZonyxNagi/zonyx-core/internal/repositories/jetstream"
	natsserver "github.com/nats-io/nats-server/v2/server"
	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	health "google.golang.org/grpc/health/grpc_health_v1"
)

// ---- Stack setup -------------------------------------------------------

type stack struct {
	udpAddr  string
	grpcAddr string
}

// startStack boots an in-process stack and registers cleanup with t.
// Cleanup order (LIFO): cancel ctx → stop gRPC server → close NATS conn
// → shut down embedded NATS server.
func startStack(t *testing.T) *stack {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	log := quietLog()

	// Embedded NATS with JetStream.
	ns := natstest.RunServer(&natsserver.Options{
		Port:      -1,
		JetStream: true,
		StoreDir:  t.TempDir(),
		NoLog:     true,
		NoSigs:    true,
	})
	t.Cleanup(ns.Shutdown)

	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatalf("nats connect: %v", err)
	}
	t.Cleanup(nc.Close)

	repo, err := jetstream.NewRepository(nc, jetstream.Config{MaxAge: time.Hour}, nil)
	if err != nil {
		t.Fatalf("jetstream repo: %v", err)
	}

	udpAddr := freeUDPAddr(t)
	grpcPort := freeTCPPort(t)
	grpcAddr := net.JoinHostPort("127.0.0.1", strconv.Itoa(grpcPort))

	provider := quuppa.New(udpAddr, log)
	svc, err := services.NewService([]ports.Provider{provider}, repo, nil)
	if err != nil {
		t.Fatalf("service: %v", err)
	}

	core, err := adapters.NewCoreAdapter(svc)
	if err != nil {
		t.Fatalf("core adapter: %v", err)
	}

	srv, err := server.NewServer(core)
	if err != nil {
		t.Fatalf("grpc server: %v", err)
	}

	// Register teardown in LIFO order: cancel runs first, ns.Shutdown last.
	t.Cleanup(srv.Stop)
	t.Cleanup(cancel)

	go func() {
		if err := svc.Start(ctx); err != nil && !errors.Is(err, context.Canceled) {
			// Cannot safely call t.Error here — goroutine may outlive the test.
		}
	}()
	go func() { _ = srv.Start(grpcPort) }()

	// Poll until gRPC is accepting connections.
	waitGRPCReady(t, grpcAddr, 3*time.Second)

	// Grace period for the QUUPPA UDP socket to bind inside svc.Start.
	time.Sleep(200 * time.Millisecond)

	return &stack{udpAddr: udpAddr, grpcAddr: grpcAddr}
}

// waitGRPCReady polls addr until the health check succeeds or the deadline passes.
func waitGRPCReady(t *testing.T, addr string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		cc, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err == nil {
			ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
			_, perr := health.NewHealthClient(cc).Check(ctx, &health.HealthCheckRequest{})
			cancel()
			_ = cc.Close()
			if perr == nil {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("gRPC server not ready within %v", timeout)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

// ---- Port helpers ------------------------------------------------------

func freeUDPAddr(t *testing.T) string {
	t.Helper()
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("freeUDPAddr: %v", err)
	}
	addr := pc.LocalAddr().String()
	_ = pc.Close()
	return addr
}

func freeTCPPort(t *testing.T) int {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("freeTCPPort: %v", err)
	}
	port := lis.Addr().(*net.TCPAddr).Port
	_ = lis.Close()
	return port
}

// ---- UDP packet helpers ------------------------------------------------

// datagram mirrors the QPE 9.5+ DefaultLocationAndInfo JSON shape.
// LocationZoneIds is *[]string so nil marshals as JSON null (LOCATION_LOST).
type datagram struct {
	TagID                  string    `json:"tagId"`
	ResponseTS             int64     `json:"responseTS"`
	LastPacketTS           int64     `json:"lastPacketTS"`
	LocationType           string    `json:"locationType"`
	LocationZoneIds        *[]string `json:"locationZoneIds"`
	Location               []float64 `json:"location,omitempty"`
	LocationMovementStatus string    `json:"locationMovementStatus,omitempty"`
}

func positionPkt(id string, zones []string) datagram {
	z := append([]string(nil), zones...)
	ms := time.Now().UnixMilli()
	return datagram{
		TagID: id, ResponseTS: ms, LastPacketTS: ms,
		LocationType: "position", LocationZoneIds: &z,
		Location: []float64{1.5, 2.3, 0.0}, LocationMovementStatus: "moving",
	}
}

func noLocationPkt(id string) datagram {
	ms := time.Now().UnixMilli()
	return datagram{
		TagID: id, ResponseTS: ms, LastPacketTS: ms,
		LocationType: "noLocation",
		// LocationZoneIds nil → JSON null → triggers LOCATION_LOST in derive.go
	}
}

func sendUDP(t *testing.T, conn net.Conn, d datagram) {
	t.Helper()
	b, _ := json.Marshal(d)
	if _, err := conn.Write(b); err != nil {
		t.Logf("udp send warning: %v", err)
	}
}

// ---- gRPC subscriber ---------------------------------------------------

// openEventStream subscribes to CoreService and returns a buffered channel
// of received events. The stream goroutine runs until ctx is cancelled.
func openEventStream(t *testing.T, ctx context.Context, grpcAddr string) <-chan *pb.Event {
	t.Helper()
	cc, err := grpc.NewClient(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc dial: %v", err)
	}
	t.Cleanup(func() { _ = cc.Close() })

	stream, err := pb.NewCoreServiceClient(cc).Subscribe(ctx, &pb.SubscribeRequest{})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	ch := make(chan *pb.Event, 512)
	go func() {
		defer close(ch)
		for {
			resp, err := stream.Recv()
			if err != nil {
				return
			}
			for _, ev := range resp.GetEvents() {
				select {
				case ch <- ev:
				default:
					// buffer full — event dropped
				}
			}
		}
	}()
	return ch
}

// ---- Assertion engine --------------------------------------------------

// check is a named predicate over a proto Event.
type check struct {
	desc string
	fn   func(*pb.Event) bool
}

// zoneCheck matches an EVENT_TYPE_ZONE for id. If wantZones is nil, any
// zone set is accepted (useful for "just verify a Zone event arrived").
func zoneCheck(id string, wantZones []string) check {
	return check{
		desc: "Zone actor=" + id,
		fn: func(e *pb.Event) bool {
			if e.GetType() != pb.EventType_EVENT_TYPE_ZONE || e.GetActor().GetId() != id {
				return false
			}
			if wantZones == nil {
				return true
			}
			got := e.GetState().GetLocation().GetZones()
			if len(got) != len(wantZones) {
				return false
			}
			m := make(map[string]bool, len(wantZones))
			for _, z := range wantZones {
				m[z] = true
			}
			for _, z := range got {
				if !m[z] {
					return false
				}
			}
			return true
		},
	}
}

// presenceCheck matches an EVENT_TYPE_PRESENCE with the given type for id.
func presenceCheck(id string, pt pb.PresenceType) check {
	name := map[pb.PresenceType]string{
		pb.PresenceType_PRESENCE_TYPE_ACTIVE:   "Active",
		pb.PresenceType_PRESENCE_TYPE_INACTIVE: "Inactive",
	}[pt]
	return check{
		desc: "Presence{" + name + "} actor=" + id,
		fn: func(e *pb.Event) bool {
			return e.GetType() == pb.EventType_EVENT_TYPE_PRESENCE &&
				e.GetActor().GetId() == id &&
				e.GetState().GetPresence().GetType() == pt
		},
	}
}

// runChecks consumes events from ch until each check in order is satisfied
// or the shared timer fires. The timeout budget is shared across all checks.
func runChecks(t *testing.T, ctx context.Context, ch <-chan *pb.Event, checks []check, timeout time.Duration) {
	t.Helper()
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for i, c := range checks {
		t.Logf("[%d/%d] waiting: %s", i+1, len(checks), c.desc)
		if !waitForCheck(ctx, ch, c, timer) {
			t.Errorf("[%d/%d] FAIL (timeout or stream closed): %s", i+1, len(checks), c.desc)
			return
		}
		t.Logf("[%d/%d] PASS: %s", i+1, len(checks), c.desc)
	}
}

func waitForCheck(ctx context.Context, ch <-chan *pb.Event, c check, timer *time.Timer) bool {
	for {
		select {
		case ev, ok := <-ch:
			if !ok {
				return false
			}
			if c.fn(ev) {
				return true
			}
		case <-timer.C:
			return false
		case <-ctx.Done():
			return false
		}
	}
}

// ---- Misc helpers ------------------------------------------------------

func quietLog() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
}

// ---- Smoke tests -------------------------------------------------------

// TestSmoke_Walk verifies the zone-transition path:
//
//	UDP position{zone-a} → gRPC EventTypeZone{zone-a}
//	UDP position{zone-b} → gRPC EventTypeZone{zone-b}
//	UDP position{zone-c} → gRPC EventTypeZone{zone-c}
func TestSmoke_Walk(t *testing.T) {
	st := startStack(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	events := openEventStream(t, ctx, st.grpcAddr)
	time.Sleep(200 * time.Millisecond) // let stream register server-side

	conn, err := net.Dial("udp", st.udpAddr)
	if err != nil {
		t.Fatalf("udp dial: %v", err)
	}
	defer conn.Close()

	const id = "aabbccdd0000"
	zones := []string{"zone-a", "zone-b", "zone-c"}

	for _, z := range zones {
		sendUDP(t, conn, positionPkt(id, []string{z}))
		time.Sleep(200 * time.Millisecond)
	}

	runChecks(t, ctx, events, []check{
		zoneCheck(id, []string{"zone-a"}),
		zoneCheck(id, []string{"zone-b"}),
		zoneCheck(id, []string{"zone-c"}),
	}, 15*time.Second)
}

// TestSmoke_Lost verifies LOCATION_LOST and LOCATION_RESTORED:
//
//	position{zone-a} → EventTypeZone{zone-a}
//	noLocation       → EventTypeZone{} + EventTypePresence{Inactive}
//	position{zone-a} → EventTypePresence{Active} + EventTypeZone{zone-a}
func TestSmoke_Lost(t *testing.T) {
	st := startStack(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	events := openEventStream(t, ctx, st.grpcAddr)
	time.Sleep(200 * time.Millisecond)

	conn, err := net.Dial("udp", st.udpAddr)
	if err != nil {
		t.Fatalf("udp dial: %v", err)
	}
	defer conn.Close()

	const id = "aabbccdd0001"
	const zone = "zone-a"

	// Sequence: position → noLocation → position.
	sendUDP(t, conn, positionPkt(id, []string{zone}))
	time.Sleep(300 * time.Millisecond)
	sendUDP(t, conn, noLocationPkt(id))
	time.Sleep(300 * time.Millisecond)
	sendUDP(t, conn, positionPkt(id, []string{zone}))

	runChecks(t, ctx, events, []check{
		zoneCheck(id, []string{zone}),
		presenceCheck(id, pb.PresenceType_PRESENCE_TYPE_INACTIVE),
		presenceCheck(id, pb.PresenceType_PRESENCE_TYPE_ACTIVE),
		zoneCheck(id, nil), // Zone after LOCATION_RESTORED
	}, 10*time.Second)
}

// TestSmoke_Offline verifies the watchdog path:
//
//	3 position packets → EventTypeZone (tag known)
//	15s silence        → EventTypePresence{Inactive} (watchdog fires at 12s)
//	position packet    → EventTypePresence{Active}   (TAG_ONLINE)
//
// This test takes ~20s. The -timeout flag must be ≥ 60s (make smoke uses 120s).
func TestSmoke_Offline(t *testing.T) {
	st := startStack(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	events := openEventStream(t, ctx, st.grpcAddr)
	time.Sleep(200 * time.Millisecond)

	conn, err := net.Dial("udp", st.udpAddr)
	if err != nil {
		t.Fatalf("udp dial: %v", err)
	}
	defer conn.Close()

	const id = "aabbccdd0002"
	const zone = "zone-a"

	// Establish the tag with 3 packets.
	for i := 0; i < 3; i++ {
		sendUDP(t, conn, positionPkt(id, []string{zone}))
		time.Sleep(time.Second)
	}

	// Go silent — watchdog fires after 12s.
	t.Log("tag silent for 15s (watchdog threshold = 12s)")
	time.Sleep(15 * time.Second)

	// Resume — triggers TAG_ONLINE.
	t.Log("tag resuming")
	sendUDP(t, conn, positionPkt(id, []string{zone}))

	// By now all events should be buffered in the channel.
	runChecks(t, ctx, events, []check{
		zoneCheck(id, nil),
		presenceCheck(id, pb.PresenceType_PRESENCE_TYPE_INACTIVE),
		presenceCheck(id, pb.PresenceType_PRESENCE_TYPE_ACTIVE),
	}, 10*time.Second)
}
