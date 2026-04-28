package quuppa

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/ZonyxNagi/zonyx-core/internal/core/domain"
)

// freePort grabs an available UDP port on loopback by binding-and-closing.
// There is a tiny race window between Close and the adapter's later bind,
// but on a single test host it's overwhelmingly safe.
func freePort(t *testing.T) string {
	t.Helper()
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("freePort: %v", err)
	}
	addr := pc.LocalAddr().String()
	_ = pc.Close()
	return addr
}

// quietLogger discards everything; warns from malformed-packet tests should
// not pollute test output.
func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
}

// validDatagram builds a §3-shaped JSON UDP payload with the fields the
// adapter actually consumes.
func validDatagram(tagID string, locType string, zones []string, responseMs, lastPacketMs int64) []byte {
	zonesPart := "null"
	if zones != nil {
		zonesPart = "["
		for i, z := range zones {
			if i > 0 {
				zonesPart += ","
			}
			zonesPart += `"` + z + `"`
		}
		zonesPart += "]"
	}
	return []byte(fmt.Sprintf(
		`{"tagId":%q,"responseTS":%d,"lastPacketTS":%d,"locationType":%q,"locationZoneIds":%s}`,
		tagID, responseMs, lastPacketMs, locType, zonesPart,
	))
}

// TestAdapter_LoopbackEmitsForZoneChange verifies that the adapter emits a
// Zone event only when the zone set changes (§6 diff algorithm).
// Duplicate packets with the same zones are silently dropped.
func TestAdapter_LoopbackEmitsForEveryDatagram(t *testing.T) {
	addr := freePort(t)
	adapter := New(addr, quietLogger())

	events := make(chan domain.Event, 16)
	emit := func(e domain.Event) error {
		events <- e
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	streamErr := make(chan error, 1)
	go func() { streamErr <- adapter.Run(ctx, emit) }()

	conn, err := net.Dial("udp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	// First datagram — ride out the bind race with retries.
	first := validDatagram("a4da22e4e75d", "position", []string{"pool-main"}, 1714502400000, 1714502399900)
	got, ok := waitForEvent(t, events, conn, first, 3*time.Second)
	if !ok {
		t.Fatal("timed out waiting for first event")
	}
	if got.Actor.ID != "a4da22e4e75d" {
		t.Errorf("Actor.ID = %q", got.Actor.ID)
	}
	if got.Type != domain.EventTypeZone {
		t.Errorf("Type = %v, want EventTypeZone", got.Type)
	}
	if len(got.State.Location.Zones) != 1 || got.State.Location.Zones[0] != "pool-main" {
		t.Errorf("State.Location.Zones = %v", got.State.Location.Zones)
	}

	drainEvents(events, 100*time.Millisecond)

	// Re-send identical datagram — the adapter must NOT emit (zones unchanged).
	if _, err := conn.Write(first); err != nil {
		t.Fatalf("write duplicate: %v", err)
	}
	select {
	case ev := <-events:
		t.Fatalf("duplicate datagram (same zones) should not emit, got %v", ev.Type)
	case <-time.After(300 * time.Millisecond):
		// expected: no event
	}

	drainEvents(events, 100*time.Millisecond)

	// Different zone — must also emit.
	second := validDatagram("a4da22e4e75d", "position", []string{"lane-2"}, 1714502400500, 1714502400400)
	if _, err := conn.Write(second); err != nil {
		t.Fatalf("write second: %v", err)
	}
	select {
	case ev := <-events:
		if len(ev.State.Location.Zones) != 1 || ev.State.Location.Zones[0] != "lane-2" {
			t.Errorf("State.Location.Zones = %v, want [lane-2]", ev.State.Location.Zones)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for zone-change event")
	}

	cancel()
	select {
	case err := <-streamErr:
		if !errors.Is(err, context.Canceled) && err != nil {
			t.Logf("Stream returned %v after cancel (acceptable)", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Stream did not return after cancel")
	}
}

func TestAdapter_BindFailureReturnsError(t *testing.T) {
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("seed listen: %v", err)
	}
	defer pc.Close()
	addr := pc.LocalAddr().String()

	adapter := New(addr, quietLogger())
	err = adapter.Run(context.Background(), func(domain.Event) error { return nil })
	if err == nil {
		t.Fatal("expected bind error, got nil")
	}
}

func TestAdapter_MalformedDatagramIsSkipped(t *testing.T) {
	addr := freePort(t)
	adapter := New(addr, quietLogger())

	events := make(chan domain.Event, 4)
	emit := func(e domain.Event) error {
		events <- e
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = adapter.Run(ctx, emit) }()

	conn, err := net.Dial("udp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	good := validDatagram("a4da22e4e75d", "position", []string{"pool-main"}, 1714502400000, 1714502399900)
	garbage := []byte(`not json {{{`)

	got, ok := waitForEventWithMixedSends(t, events, conn, garbage, good, 3*time.Second)
	if !ok {
		t.Fatal("valid datagram never produced an event")
	}
	if got.Actor.ID != "a4da22e4e75d" {
		t.Errorf("Actor.ID = %q", got.Actor.ID)
	}
}

// fakeClock is a manually-advanced clock for watchdog tests. Goroutine-safe
// so the Stream goroutine and test goroutine can both touch it.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock(t time.Time) *fakeClock { return &fakeClock{now: t} }

func (f *fakeClock) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

func (f *fakeClock) Advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = f.now.Add(d)
}

func TestAdapter_WatchdogEmitsInactiveAfterSilence(t *testing.T) {
	addr := freePort(t)
	clk := newFakeClock(time.UnixMilli(1714502400000))
	// 50ms tick keeps the loop responsive in tests; OfflineThreshold (12s)
	// is logical-time, advanced via clk.Advance.
	adapter := newWithClock(addr, quietLogger(), clk, 50*time.Millisecond)

	events := make(chan domain.Event, 8)
	emit := func(e domain.Event) error {
		events <- e
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = adapter.Run(ctx, emit) }()

	conn, err := net.Dial("udp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	// Deliver a packet so the adapter learns about this tag.
	pkt := validDatagram("a4da22e4e75d", "position", []string{"pool-main"}, 1714502400000, 1714502400000)
	got, ok := waitForEvent(t, events, conn, pkt, 3*time.Second)
	if !ok || got.Type != domain.EventTypeZone {
		t.Fatalf("expected baseline Zone event, got ok=%v ev=%+v", ok, got)
	}
	drainEvents(events, 100*time.Millisecond)

	// Advance logical time past the offline threshold. The next watchdog
	// tick should emit Presence{Inactive} carrying the last-known zones.
	clk.Advance(OfflineThreshold + time.Second)

	select {
	case ev := <-events:
		if ev.Type != domain.EventTypePresence {
			t.Errorf("Type = %v, want Presence", ev.Type)
		}
		if ev.State.Presence.Type != domain.PresenceTypeInactive {
			t.Errorf("Presence = %v, want Inactive", ev.State.Presence.Type)
		}
		if len(ev.State.Location.Zones) != 1 || ev.State.Location.Zones[0] != "pool-main" {
			t.Errorf("Zones = %v, want last-known [pool-main]", ev.State.Location.Zones)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("watchdog never fired")
	}
}

func TestAdapter_OnlineAfterOffline(t *testing.T) {
	addr := freePort(t)
	clk := newFakeClock(time.UnixMilli(1714502400000))
	adapter := newWithClock(addr, quietLogger(), clk, 50*time.Millisecond)

	events := make(chan domain.Event, 16)
	emit := func(e domain.Event) error {
		events <- e
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = adapter.Run(ctx, emit) }()

	conn, err := net.Dial("udp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	pkt1 := validDatagram("a4da22e4e75d", "position", []string{"pool-main"}, 1714502400000, 1714502400000)
	if _, ok := waitForEvent(t, events, conn, pkt1, 3*time.Second); !ok {
		t.Fatal("first event timeout")
	}
	drainEvents(events, 100*time.Millisecond)

	// Force watchdog → Presence{Inactive}.
	clk.Advance(OfflineThreshold + time.Second)
	select {
	case ev := <-events:
		if ev.State.Presence.Type != domain.PresenceTypeInactive {
			t.Fatalf("expected Inactive, got %v", ev.State.Presence.Type)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("watchdog never fired")
	}

	// Send another packet — should produce Presence{Active} (TAG_ONLINE).
	pkt2 := validDatagram("a4da22e4e75d", "position", []string{"pool-main"}, 1714502420000, 1714502420000)
	got, ok := waitForEvent(t, events, conn, pkt2, 3*time.Second)
	if !ok {
		t.Fatal("no event after re-online packet")
	}
	if got.Type != domain.EventTypePresence || got.State.Presence.Type != domain.PresenceTypeActive {
		t.Errorf("expected Presence{Active}, got Type=%v Presence=%v", got.Type, got.State.Presence.Type)
	}
}

func TestAdapter_WatchdogSkipsLocationLostState(t *testing.T) {
	addr := freePort(t)
	clk := newFakeClock(time.UnixMilli(1714502400000))
	adapter := newWithClock(addr, quietLogger(), clk, 50*time.Millisecond)

	events := make(chan domain.Event, 16)
	emit := func(e domain.Event) error {
		events <- e
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = adapter.Run(ctx, emit) }()

	conn, err := net.Dial("udp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	// 1. Tag in position with zones (baseline Zone event).
	pkt1 := validDatagram("a4da22e4e75d", "position", []string{"pool-main"}, 1714502400000, 1714502400000)
	if _, ok := waitForEvent(t, events, conn, pkt1, 3*time.Second); !ok {
		t.Fatal("baseline event timeout")
	}
	drainEvents(events, 100*time.Millisecond)

	// 2. Tag transitions to noLocation → emit [Zone(empty), Presence(Inactive)].
	pkt2 := validDatagram("a4da22e4e75d", "noLocation", nil, 1714502401000, 1714502401000)
	if _, err := conn.Write(pkt2); err != nil {
		t.Fatalf("write pkt2: %v", err)
	}

	// Collect both events.
	for i := 0; i < 2; i++ {
		select {
		case <-events:
		case <-time.After(2 * time.Second):
			t.Fatalf("missing event %d after location-lost transition", i)
		}
	}

	// 3. Advance past offline threshold — watchdog must NOT fire because
	// locationLostAt is set.
	clk.Advance(OfflineThreshold + 5*time.Second)
	select {
	case ev := <-events:
		t.Fatalf("watchdog re-emitted while in location-lost state: %+v", ev)
	case <-time.After(300 * time.Millisecond):
		// good
	}
}

func TestAdapter_SendNotImplemented(t *testing.T) {
	t.Parallel()
	adapter := New("127.0.0.1:0", quietLogger())
	err := adapter.Send(context.Background(), domain.Directive{Name: "register_device"})
	if err == nil {
		t.Fatal("expected error from Send")
	}
}

func waitForEvent(t *testing.T, events <-chan domain.Event, conn net.Conn, payload []byte, timeout time.Duration) (domain.Event, bool) {
	t.Helper()
	deadline := time.After(timeout)
	tick := time.NewTicker(25 * time.Millisecond)
	defer tick.Stop()

	if _, err := conn.Write(payload); err != nil {
		t.Fatalf("initial write: %v", err)
	}
	for {
		select {
		case ev := <-events:
			return ev, true
		case <-tick.C:
			_, _ = conn.Write(payload)
		case <-deadline:
			return domain.Event{}, false
		}
	}
}

func waitForEventWithMixedSends(t *testing.T, events <-chan domain.Event, conn net.Conn, garbage, good []byte, timeout time.Duration) (domain.Event, bool) {
	t.Helper()
	deadline := time.After(timeout)
	tick := time.NewTicker(25 * time.Millisecond)
	defer tick.Stop()

	for {
		select {
		case ev := <-events:
			return ev, true
		case <-tick.C:
			_, _ = conn.Write(garbage)
			_, _ = conn.Write(good)
		case <-deadline:
			return domain.Event{}, false
		}
	}
}

func drainEvents(events <-chan domain.Event, window time.Duration) {
	deadline := time.After(window)
	for {
		select {
		case <-events:
		case <-deadline:
			return
		}
	}
}
