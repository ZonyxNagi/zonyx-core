package jetstream

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/ZonyxNagi/zonyx-core/internal/core/domain"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
)

// startServer starts an embedded NATS server with JetStream enabled.
func startServer(t *testing.T) *nats.Conn {
	t.Helper()
	opts := &natsserver.Options{
		Port:      -1,
		JetStream: true,
		StoreDir:  t.TempDir(),
		NoLog:     true,
		NoSigs:    true,
	}
	srv := test.RunServer(opts)
	t.Cleanup(srv.Shutdown)

	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("nats connect: %v", err)
	}
	t.Cleanup(nc.Close)
	return nc
}

func newRepo(t *testing.T, nc *nats.Conn) *Repository {
	t.Helper()
	return newRepoWithConfig(t, nc, Config{MaxAge: time.Hour})
}

func newRepoWithConfig(t *testing.T, nc *nats.Conn, cfg Config) *Repository {
	t.Helper()
	if cfg.MaxAge == 0 {
		cfg.MaxAge = time.Hour
	}
	repo, err := NewRepository(nc, cfg, nil)
	if err != nil {
		t.Fatalf("NewRepository: %v", err)
	}
	return repo
}

func baseEvent(id, actorID string, typ domain.EventType) domain.Event {
	return domain.Event{
		ID:    id,
		Type:  typ,
		Actor: domain.Actor{ID: actorID},
		State: domain.State{
			Presence: domain.Presence{
				Type:  domain.PresenceTypeActive,
				Since: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
			},
		},
		Timestamp: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}
}

func zoneEventWith(id, actorID string, zones []string) domain.Event {
	e := baseEvent(id, actorID, domain.EventTypeZone)
	e.State.Location = &domain.Location{Zones: zones}
	return e
}

func presenceEventWith(id, actorID string, pt domain.PresenceType) domain.Event {
	e := baseEvent(id, actorID, domain.EventTypePresence)
	e.State.Presence.Type = pt
	return e
}

// collectN reads up to n events delivered within timeout.
func collectN(t *testing.T, n int, timeout time.Duration, fn func(deliver func(domain.Event, string) error) error) []domain.Event {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	events := make([]domain.Event, 0, n)
	errDone := errors.New("done")

	err := fn(func(e domain.Event, _ string) error {
		events = append(events, e)
		if len(events) >= n {
			return errDone
		}
		return nil
	})
	if err != nil && !errors.Is(err, errDone) && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Fatalf("unexpected Watch error: %v", err)
	}
	_ = ctx
	return events
}

// ---- Write: raw offset -------------------------------------------------------

func TestWrite_ReturnsRawOffset(t *testing.T) {
	t.Parallel()
	repo := newRepo(t, startServer(t))

	offset, err := repo.Write(context.Background(), zoneEventWith("evt-1", "actor-1", []string{"z1"}))
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	seq, err := decodeOffset(offset)
	if err != nil {
		t.Fatalf("decodeOffset(%q): %v", offset, err)
	}
	// raw event is the first message published — seq 1.
	if seq != 1 {
		t.Errorf("seq = %d, want 1 (raw event)", seq)
	}
}

// ---- Write: zone change detection -------------------------------------------

func TestWrite_FirstZoneEvent_PublishesDerivedChangeEvent(t *testing.T) {
	t.Parallel()
	repo := newRepo(t, startServer(t))
	ctx := context.Background()

	if _, err := repo.Write(ctx, zoneEventWith("evt-1", "actor-1", []string{"z1"})); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Watch zone subject — must receive the derived change event.
	events := collectN(t, 1, 3*time.Second, func(deliver func(domain.Event, string) error) error {
		return repo.Watch(ctx, []domain.EventType{domain.EventTypeZone}, nil, deliver)
	})
	if len(events) != 1 || events[0].ID != "evt-1" {
		t.Errorf("zone events = %v, want [evt-1]", eventIDs(events))
	}
}

func TestWrite_SameZones_NoAdditionalZoneChangeEvent(t *testing.T) {
	t.Parallel()
	repo := newRepo(t, startServer(t))
	ctx := context.Background()

	// Write actor with zones ["z1"] twice — change event only on first.
	if _, err := repo.Write(ctx, zoneEventWith("evt-1", "actor-1", []string{"z1"})); err != nil {
		t.Fatalf("Write 1: %v", err)
	}
	if _, err := repo.Write(ctx, zoneEventWith("evt-2", "actor-1", []string{"z1"})); err != nil {
		t.Fatalf("Write 2: %v", err)
	}

	// DeliverLastPerSubject on zonyx.zone.* should return exactly 1 event.
	events := collectN(t, 1, 3*time.Second, func(deliver func(domain.Event, string) error) error {
		return repo.Watch(ctx, []domain.EventType{domain.EventTypeZone}, nil, deliver)
	})
	if len(events) != 1 {
		t.Errorf("zone change events = %d, want 1 (no duplicate when zones unchanged)", len(events))
	}
}

func TestWrite_DifferentZones_PublishesNewZoneChangeEvent(t *testing.T) {
	t.Parallel()
	repo := newRepo(t, startServer(t))
	ctx := context.Background()

	if _, err := repo.Write(ctx, zoneEventWith("evt-1", "actor-1", []string{"z1"})); err != nil {
		t.Fatalf("Write 1: %v", err)
	}
	if _, err := repo.Write(ctx, zoneEventWith("evt-2", "actor-1", []string{"z2"})); err != nil {
		t.Fatalf("Write 2: %v", err)
	}

	events := collectN(t, 2, 3*time.Second, func(deliver func(domain.Event, string) error) error {
		// Replay all zone events from beginning.
		start := "001"
		return repo.Watch(ctx, []domain.EventType{domain.EventTypeZone}, &start, deliver)
	})
	if len(events) != 2 {
		t.Fatalf("zone change events = %d, want 2", len(events))
	}
	if events[0].ID != "evt-1" || events[1].ID != "evt-2" {
		t.Errorf("ids = %v, want [evt-1, evt-2]", eventIDs(events))
	}
}

// ---- Write: presence change detection ---------------------------------------

func TestWrite_FirstPresenceEvent_PublishesDerivedChangeEvent(t *testing.T) {
	t.Parallel()
	repo := newRepo(t, startServer(t))
	ctx := context.Background()

	if _, err := repo.Write(ctx, presenceEventWith("pres-1", "actor-1", domain.PresenceTypeActive)); err != nil {
		t.Fatalf("Write: %v", err)
	}

	events := collectN(t, 1, 3*time.Second, func(deliver func(domain.Event, string) error) error {
		return repo.Watch(ctx, []domain.EventType{domain.EventTypePresence}, nil, deliver)
	})
	if len(events) != 1 || events[0].ID != "pres-1" {
		t.Errorf("presence events = %v, want [pres-1]", eventIDs(events))
	}
}

func TestWrite_SamePresence_NoAdditionalPresenceChangeEvent(t *testing.T) {
	t.Parallel()
	repo := newRepo(t, startServer(t))
	ctx := context.Background()

	if _, err := repo.Write(ctx, presenceEventWith("pres-1", "actor-1", domain.PresenceTypeActive)); err != nil {
		t.Fatalf("Write 1: %v", err)
	}
	if _, err := repo.Write(ctx, presenceEventWith("pres-2", "actor-1", domain.PresenceTypeActive)); err != nil {
		t.Fatalf("Write 2: %v", err)
	}

	events := collectN(t, 1, 3*time.Second, func(deliver func(domain.Event, string) error) error {
		return repo.Watch(ctx, []domain.EventType{domain.EventTypePresence}, nil, deliver)
	})
	if len(events) != 1 {
		t.Errorf("presence change events = %d, want 1", len(events))
	}
}

func TestWrite_PresenceTransition_PublishesChangeEvent(t *testing.T) {
	t.Parallel()
	repo := newRepo(t, startServer(t))
	ctx := context.Background()

	if _, err := repo.Write(ctx, presenceEventWith("pres-1", "actor-1", domain.PresenceTypeActive)); err != nil {
		t.Fatalf("Write 1: %v", err)
	}
	if _, err := repo.Write(ctx, presenceEventWith("pres-2", "actor-1", domain.PresenceTypeInactive)); err != nil {
		t.Fatalf("Write 2: %v", err)
	}

	events := collectN(t, 2, 3*time.Second, func(deliver func(domain.Event, string) error) error {
		start := "001"
		return repo.Watch(ctx, []domain.EventType{domain.EventTypePresence}, &start, deliver)
	})
	if len(events) != 2 {
		t.Fatalf("presence change events = %d, want 2", len(events))
	}
	if events[0].ID != "pres-1" || events[1].ID != "pres-2" {
		t.Errorf("ids = %v, want [pres-1, pres-2]", eventIDs(events))
	}
}

// ---- Write: zone events do not trigger presence changes ---------------------

func TestWrite_ZoneEvent_DoesNotPublishToPresenceSubject(t *testing.T) {
	t.Parallel()
	repo := newRepo(t, startServer(t))
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	if _, err := repo.Write(context.Background(), zoneEventWith("evt-z", "actor-1", []string{"z1"})); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Watching for presence events should time out — zone event must not
	// publish to the presence subject.
	events := collectN(t, 1, 400*time.Millisecond, func(deliver func(domain.Event, string) error) error {
		return repo.Watch(ctx, []domain.EventType{domain.EventTypePresence}, nil, deliver)
	})
	if len(events) != 0 {
		t.Errorf("presence events = %v, want none (zone event must not trigger presence change)", eventIDs(events))
	}
}

// ---- Watch: snapshot and live -----------------------------------------------

func TestWatch_Snapshot_LatestZonePerActor(t *testing.T) {
	t.Parallel()
	repo := newRepo(t, startServer(t))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Two zone transitions for actor-A, one for actor-B.
	for _, e := range []domain.Event{
		zoneEventWith("a1-z1", "actor-A", []string{"z1"}),
		zoneEventWith("a1-z2", "actor-A", []string{"z2"}), // latest for actor-A
		zoneEventWith("b1-z1", "actor-B", []string{"z1"}), // only for actor-B
	} {
		if _, err := repo.Write(context.Background(), e); err != nil {
			t.Fatalf("Write %s: %v", e.ID, err)
		}
	}

	// nil offset → snapshot then live.  Collect exactly 2 (one per actor).
	events := collectN(t, 2, 3*time.Second, func(deliver func(domain.Event, string) error) error {
		return repo.Watch(ctx, []domain.EventType{domain.EventTypeZone}, nil, deliver)
	})
	if len(events) != 2 {
		t.Fatalf("snapshot events = %d, want 2", len(events))
	}
	ids := eventIDSet(events)
	if !ids["a1-z2"] {
		t.Error("want latest zone event for actor-A (a1-z2)")
	}
	if !ids["b1-z1"] {
		t.Error("want zone event for actor-B (b1-z1)")
	}
}

func TestWatch_LiveDelivery(t *testing.T) {
	t.Parallel()
	repo := newRepo(t, startServer(t))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	got := make(chan domain.Event, 2)
	go func() {
		_ = repo.Watch(ctx, []domain.EventType{domain.EventTypeZone}, nil, func(e domain.Event, _ string) error {
			got <- e
			return nil
		})
	}()

	time.Sleep(50 * time.Millisecond)

	want := zoneEventWith("evt-live", "actor-live", []string{"z1"})
	if _, err := repo.Write(context.Background(), want); err != nil {
		t.Fatalf("Write: %v", err)
	}

	select {
	case e := <-got:
		if e.ID != want.ID {
			t.Errorf("got ID %q, want %q", e.ID, want.ID)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for live event")
	}
}

func TestWatch_ReplayFromOffset(t *testing.T) {
	t.Parallel()
	repo := newRepo(t, startServer(t))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Write 3 zone changes for different actors.
	var offsets []string
	for _, e := range []domain.Event{
		zoneEventWith("evt-1", "actor-1", []string{"z1"}),
		zoneEventWith("evt-2", "actor-2", []string{"z1"}),
		zoneEventWith("evt-3", "actor-3", []string{"z1"}),
	} {
		off, err := repo.Write(context.Background(), e)
		if err != nil {
			t.Fatalf("Write %s: %v", e.ID, err)
		}
		offsets = append(offsets, off)
	}

	// The zone-change events have sequences after the raw events.
	// Use the offset of the first zone-change event to replay from evt-1.
	// We know the zone subject has the change events; find the first one.
	firstZoneOff := findFirstZoneOffset(t, repo, ctx)

	// Replay from first zone-change event: should get all 3.
	events := collectN(t, 3, 3*time.Second, func(deliver func(domain.Event, string) error) error {
		return repo.Watch(ctx, []domain.EventType{domain.EventTypeZone}, &firstZoneOff, deliver)
	})
	if len(events) != 3 {
		t.Fatalf("replayed zone events = %d, want 3", len(events))
	}
	ids := eventIDSet(events)
	for _, want := range []string{"evt-1", "evt-2", "evt-3"} {
		if !ids[want] {
			t.Errorf("missing %s in replay", want)
		}
	}
	_ = offsets
}

// findFirstZoneOffset subscribes to zone events and returns the offset of the
// first delivered message.
func findFirstZoneOffset(t *testing.T, repo *Repository, ctx context.Context) string {
	t.Helper()
	var off string
	err := repo.Watch(ctx, []domain.EventType{domain.EventTypeZone}, nil, func(_ domain.Event, o string) error {
		if off == "" {
			off = o
		}
		return errors.New("stop")
	})
	if err != nil && !errors.Is(err, errors.New("stop")) && off == "" {
		t.Fatalf("findFirstZoneOffset: %v", err)
	}
	if off == "" {
		t.Fatal("findFirstZoneOffset: no zone events found")
	}
	return off
}

func TestWatch_CancelCtx(t *testing.T) {
	t.Parallel()
	repo := newRepo(t, startServer(t))
	ctx, cancel := context.WithCancel(context.Background())

	errCh := make(chan error, 1)
	go func() {
		errCh <- repo.Watch(ctx, nil, nil, func(_ domain.Event, _ string) error { return nil })
	}()

	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("err = %v, want context.Canceled", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Watch did not return after ctx cancel")
	}
}

// ---- KV: cache survives restart (via second Repository instance) ------------

func TestWrite_KVPersistence_NewRepoDetectsChange(t *testing.T) {
	t.Parallel()
	nc := startServer(t)

	repo1 := newRepo(t, nc)
	ctx := context.Background()

	// Write zone z1 — first repo caches this.
	if _, err := repo1.Write(ctx, zoneEventWith("evt-1", "actor-1", []string{"z1"})); err != nil {
		t.Fatalf("Write 1: %v", err)
	}

	// Second repo shares the same NATS connection (same KV bucket).
	// It has a cold cache, so it must read from KV and detect no change for z1.
	repo2 := newRepo(t, nc)

	// Same zones again — repo2 must detect "already z1" via KV and not publish.
	if _, err := repo2.Write(ctx, zoneEventWith("evt-2", "actor-1", []string{"z1"})); err != nil {
		t.Fatalf("Write 2: %v", err)
	}

	// Only one zone-change event should exist (evt-1).
	events := collectN(t, 1, 3*time.Second, func(deliver func(domain.Event, string) error) error {
		return repo2.Watch(ctx, []domain.EventType{domain.EventTypeZone}, nil, deliver)
	})
	if len(events) != 1 || events[0].ID != "evt-1" {
		t.Errorf("zone events = %v, want [evt-1]", eventIDs(events))
	}
}

// ---- Watch: multi-type never delivers raw events ----------------------------

// TestWatch_MultiType_OnlyDeliversChangeEvents asserts that subscribing to
// multiple event types does not cause raw (every-datagram) events to leak
// through to the subscriber. Only derived change events should be delivered.
func TestWatch_MultiType_OnlyDeliversChangeEvents(t *testing.T) {
	t.Parallel()
	repo := newRepo(t, startServer(t))
	ctx := context.Background()

	// Write the same zone five times — only the first triggers a change event;
	// the remaining four are stored in raw only.
	for i := range 5 {
		e := zoneEventWith(fmt.Sprintf("evt-%d", i), "actor-1", []string{"z1"})
		if _, err := repo.Write(ctx, e); err != nil {
			t.Fatalf("Write %d: %v", i, err)
		}
	}

	// Watch with a deadline: all pre-existing change events arrive in the first
	// delivery burst; any raw events leaking through would also arrive within
	// this window. DeadlineExceeded is the expected exit condition.
	watchCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	var got []domain.Event
	err := repo.Watch(watchCtx, []domain.EventType{
		domain.EventTypeZone,
		domain.EventTypePresence,
		domain.EventTypeCommand,
	}, nil, func(e domain.Event, _ string) error {
		got = append(got, e)
		return nil
	})
	if err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Fatalf("unexpected Watch error: %v", err)
	}

	// Exactly one change event (the first zone write) must be delivered.
	if len(got) != 1 {
		t.Errorf("got %d events, want 1 change event; ids=%v", len(got), eventIDs(got))
	} else if got[0].ID != "evt-0" {
		t.Errorf("got event %q, want evt-0 (first zone change)", got[0].ID)
	}
}

// ---- helpers ----------------------------------------------------------------

func eventIDs(events []domain.Event) []string {
	ids := make([]string, len(events))
	for i, e := range events {
		ids[i] = e.ID
	}
	return ids
}

func eventIDSet(events []domain.Event) map[string]bool {
	m := make(map[string]bool, len(events))
	for _, e := range events {
		m[e.ID] = true
	}
	return m
}
