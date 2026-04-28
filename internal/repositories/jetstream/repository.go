// Package jetstream implements ports.Repository backed by NATS JetStream.
//
// Subject scheme — all subjects live under the single ZONYX_EVENTS stream
// (wildcard "zonyx.>"):
//
//	zonyx.raw.<actor_id>       every parsed datagram (full position history)
//	zonyx.zone.<actor_id>      derived: zone-set changed from previous zone event
//	zonyx.presence.<actor_id>  derived: presence type changed from previous presence event
//	zonyx.command.<actor_id>   command events (always written, no dedup)
//
// Zone and presence change detection use independent write-through in-memory
// caches, each backed by the ZONYX_ACTOR_STATE JetStream KV bucket.
// Steady-state comparisons are in-memory (nanosecond); the KV is consulted
// only on the first event for each actor after a process restart, so NATS
// write pressure scales with actual state changes, not datagram rate.
//
// Offset encoding: decimal-stringified JetStream stream sequence number,
// zero-padded to a minimum of 3 characters (satisfies proto [3..64] constraint).
package jetstream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/ZonyxNagi/zonyx-core/internal/core/domain"
	"github.com/nats-io/nats.go"
)

const (
	streamName    = "ZONYX_EVENTS"
	streamSubject = "zonyx.>"
	kvBucket      = "ZONYX_ACTOR_STATE"
	defaultMaxAge = 7 * 24 * time.Hour
)

// actorState is the minimal per-actor state kept for change detection.
// hasZones and hasPresence record whether we have seen any prior event of
// that kind — nil zones or zero presence would otherwise be ambiguous.
type actorState struct {
	zones          []string
	presence       domain.PresenceType
	locationType   string
	movementStatus string
	hasZones       bool
	hasPresence    bool
}

// kvActorState is the JSON shape persisted in the KV bucket.
type kvActorState struct {
	Zones          []string `json:"z,omitempty"`
	Presence       int      `json:"p"`
	LocationType   string   `json:"lt,omitempty"`
	MovementStatus string   `json:"ms,omitempty"`
	HasZones       bool     `json:"hz"`
	HasPresence    bool     `json:"hp"`
}

// Repository is a ports.Repository implementation backed by NATS JetStream.
type Repository struct {
	js     nats.JetStreamContext
	kv     nats.KeyValue
	stream string
	logger *slog.Logger
	cfg    Config

	mu    sync.RWMutex
	cache map[string]actorState // keyed by actor ID
}

// Config holds optional stream configuration. Zero values use the defaults.
type Config struct {
	// MaxAge is the maximum age of messages retained in the stream.
	// Defaults to 7 days when zero.
	MaxAge time.Duration
}

// NewRepository connects to JetStream via nc, provisions the ZONYX_EVENTS
// stream and the ZONYX_ACTOR_STATE KV bucket idempotently. Callers must call
// nc.Drain() on shutdown.
func NewRepository(nc *nats.Conn, cfg Config, logger *slog.Logger) (*Repository, error) {
	if logger == nil {
		logger = slog.Default()
	}

	js, err := nc.JetStream()
	if err != nil {
		return nil, fmt.Errorf("jetstream: get context: %w", err)
	}

	maxAge := cfg.MaxAge
	if maxAge == 0 {
		maxAge = defaultMaxAge
	}

	if err := provisionStream(js, maxAge); err != nil {
		return nil, err
	}

	kv, err := provisionKV(js, maxAge)
	if err != nil {
		return nil, err
	}

	return &Repository{
		js:     js,
		kv:     kv,
		stream: streamName,
		logger: logger,
		cfg:    cfg,
		cache:  make(map[string]actorState),
	}, nil
}

func provisionStream(js nats.JetStreamContext, maxAge time.Duration) error {
	cfg := &nats.StreamConfig{
		Name:      streamName,
		Subjects:  []string{streamSubject},
		Storage:   nats.FileStorage,
		Retention: nats.LimitsPolicy,
		MaxAge:    maxAge,
	}

	_, err := js.AddStream(cfg)
	if err == nil {
		return nil
	}
	if errors.Is(err, nats.ErrStreamNameAlreadyInUse) {
		return nil
	}
	var apiErr *nats.APIError
	if errors.As(err, &apiErr) && apiErr.ErrorCode == nats.JSErrCodeStreamNameInUse {
		return nil
	}
	return fmt.Errorf("jetstream: provision stream: %w", err)
}

func provisionKV(js nats.JetStreamContext, maxAge time.Duration) (nats.KeyValue, error) {
	kv, err := js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket:  kvBucket,
		TTL:     maxAge,
		Storage: nats.FileStorage,
	})
	if err == nil {
		return kv, nil
	}
	// Bucket already exists — bind to it directly.
	if errors.Is(err, nats.ErrStreamNameAlreadyInUse) {
		kv, err = js.KeyValue(kvBucket)
		if err != nil {
			return nil, fmt.Errorf("jetstream: bind kv bucket: %w", err)
		}
		return kv, nil
	}
	return nil, fmt.Errorf("jetstream: provision kv bucket: %w", err)
}

// Write stores e in the raw history stream.
//
// For EventTypeZone: if the actor's zone membership changed from the last
// zone event, a derived change event is also published to zonyx.zone.<actor>.
//
// For EventTypePresence: if the actor's presence type changed from the last
// presence event, a derived change event is published to zonyx.presence.<actor>.
//
// For EventTypeCommand: the event is published directly to zonyx.command.<actor>
// with no deduplication.
//
// Change detection uses a write-through in-memory cache backed by the
// ZONYX_ACTOR_STATE KV bucket so state survives process restarts.
func (r *Repository) Write(ctx context.Context, e domain.Event) (string, error) {
	// 1. Always write the raw event (full position history).
	rawOffset, err := r.publish(ctx, rawSubject(e.Actor.ID), e)
	if err != nil {
		return "", fmt.Errorf("jetstream: write raw event %s: %w", e.ID, err)
	}

	switch e.Type {
	case domain.EventTypeZone:
		if err := r.handleZoneChange(ctx, e); err != nil {
			return "", err
		}
	case domain.EventTypePresence:
		if err := r.handlePresenceChange(ctx, e); err != nil {
			return "", err
		}
	case domain.EventTypeCommand:
		if _, err := r.publish(ctx, commandSubject(e.Actor.ID), e); err != nil {
			return "", fmt.Errorf("jetstream: write command event %s: %w", e.ID, err)
		}
	}

	return rawOffset, nil
}

func (r *Repository) handleZoneChange(ctx context.Context, e domain.Event) error {
	var currZones []string
	if e.State.Location != nil {
		currZones = e.State.Location.Zones
	}
	currLocationType := e.Actor.Metadata["locationType"]
	currMovementStatus := e.Actor.Metadata["movementStatus"]

	prev, err := r.getActorState(e.Actor.ID)
	if err != nil {
		r.logger.Warn("jetstream: kv lookup failed, treating as first zone event",
			slog.String("actor", e.Actor.ID), slog.Any("err", err))
	}

	// No change from the confirmed zone set.
	if prev.hasZones && zonesEqual(prev.zones, currZones) &&
		prev.locationType == currLocationType && prev.movementStatus == currMovementStatus {
		return nil
	}

	// First-ever zone event for this actor: publish immediately.
	if !prev.hasZones {
		goto publish
	}

publish:
	if _, err := r.publish(ctx, zoneSubject(e.Actor.ID), e); err != nil {
		return fmt.Errorf("jetstream: write zone change event %s: %w", e.ID, err)
	}

	prev.zones = append([]string(nil), currZones...)
	prev.locationType = currLocationType
	prev.movementStatus = currMovementStatus
	prev.hasZones = true
	r.setActorState(e.Actor.ID, prev)
	if err := r.persistActorState(e.Actor.ID, prev); err != nil {
		r.logger.Warn("jetstream: kv persist failed",
			slog.String("actor", e.Actor.ID), slog.Any("err", err))
	}
	return nil
}

func (r *Repository) handlePresenceChange(ctx context.Context, e domain.Event) error {
	currPresence := e.State.Presence.Type

	prev, err := r.getActorState(e.Actor.ID)
	if err != nil {
		r.logger.Warn("jetstream: kv lookup failed, treating as first presence event",
			slog.String("actor", e.Actor.ID), slog.Any("err", err))
	}

	if prev.hasPresence && prev.presence == currPresence {
		return nil // no change
	}

	if _, err := r.publish(ctx, presenceSubject(e.Actor.ID), e); err != nil {
		return fmt.Errorf("jetstream: write presence change event %s: %w", e.ID, err)
	}

	prev.presence = currPresence
	prev.hasPresence = true
	r.setActorState(e.Actor.ID, prev)
	if err := r.persistActorState(e.Actor.ID, prev); err != nil {
		r.logger.Warn("jetstream: kv persist failed",
			slog.String("actor", e.Actor.ID), slog.Any("err", err))
	}
	return nil
}

// Watch delivers events to deliver. Only derived change events are delivered —
// raw events (every datagram) are stored but never forwarded to subscribers.
//
// When offset is nil it first delivers the current snapshot (one latest event
// per actor per subject matching types via DeliverLastPerSubject), then
// continues live. When offset is non-nil it replays from that sequence then
// continues live. Watch blocks until ctx is cancelled or deliver returns an
// error.
//
// For each requested type a separate JetStream subscription is opened on the
// corresponding typed subject (e.g. zonyx.zone.> for EventTypeZone). All
// subscriptions share a single message channel so delivery is ordered by
// arrival and no Go-side type filter is needed.
func (r *Repository) Watch(
	ctx context.Context,
	types []domain.EventType,
	offset *string,
	deliver func(domain.Event, string) error,
) error {
	subjects := watchSubjects(types)
	if len(subjects) == 0 {
		<-ctx.Done()
		return ctx.Err()
	}

	var opts []nats.SubOpt
	if offset == nil {
		opts = append(opts, nats.DeliverLastPerSubject())
	} else {
		seq, err := decodeOffset(*offset)
		if err != nil {
			return err
		}
		opts = append(opts, nats.StartSequence(seq))
	}
	opts = append(opts, nats.AckNone())

	msgCh := make(chan *nats.Msg, 64*len(subjects))
	subs := make([]*nats.Subscription, 0, len(subjects))
	for _, subj := range subjects {
		sub, err := r.js.ChanSubscribe(subj, msgCh, opts...)
		if err != nil {
			for _, s := range subs {
				_ = s.Unsubscribe()
			}
			return fmt.Errorf("jetstream: subscribe %s: %w", subj, err)
		}
		subs = append(subs, sub)
	}
	defer func() {
		for _, s := range subs {
			_ = s.Unsubscribe()
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-msgCh:
			if !ok {
				return nil
			}
			event, off, err := r.decode(msg)
			if err != nil {
				r.logger.Warn("jetstream: skip undecodable message", slog.Any("err", err))
				continue
			}
			if err := deliver(event, off); err != nil {
				return err
			}
		}
	}
}

// getActorState returns the cached state for an actor, falling back to KV on
// a cache miss (first event after a process restart).
func (r *Repository) getActorState(actorID string) (actorState, error) {
	r.mu.RLock()
	s, ok := r.cache[actorID]
	r.mu.RUnlock()
	if ok {
		return s, nil
	}

	entry, err := r.kv.Get(actorID)
	if errors.Is(err, nats.ErrKeyNotFound) {
		return actorState{}, nil
	}
	if err != nil {
		return actorState{}, err
	}

	var kvas kvActorState
	if jsonErr := json.Unmarshal(entry.Value(), &kvas); jsonErr != nil {
		return actorState{}, jsonErr
	}
	s = actorState{
		zones:          kvas.Zones,
		presence:       domain.PresenceType(kvas.Presence),
		locationType:   kvas.LocationType,
		movementStatus: kvas.MovementStatus,
		hasZones:       kvas.HasZones,
		hasPresence:    kvas.HasPresence,
	}
	r.mu.Lock()
	r.cache[actorID] = s
	r.mu.Unlock()
	return s, nil
}

func (r *Repository) setActorState(actorID string, s actorState) {
	r.mu.Lock()
	r.cache[actorID] = s
	r.mu.Unlock()
}

func (r *Repository) persistActorState(actorID string, s actorState) error {
	data, err := json.Marshal(kvActorState{
		Zones:          s.zones,
		Presence:       int(s.presence),
		LocationType:   s.locationType,
		MovementStatus: s.movementStatus,
		HasZones:       s.hasZones,
		HasPresence:    s.hasPresence,
	})
	if err != nil {
		return err
	}
	_, err = r.kv.Put(actorID, data)
	return err
}

func (r *Repository) publish(ctx context.Context, subject string, e domain.Event) (string, error) {
	data, err := marshal(e)
	if err != nil {
		return "", fmt.Errorf("marshal: %w", err)
	}
	ack, err := r.js.PublishMsg(&nats.Msg{Subject: subject, Data: data}, nats.Context(ctx))
	if err != nil {
		return "", err
	}
	return encodeOffset(ack.Sequence), nil
}

func (r *Repository) decode(msg *nats.Msg) (domain.Event, string, error) {
	event, err := unmarshal(msg.Data)
	if err != nil {
		return domain.Event{}, "", err
	}
	meta, err := msg.Metadata()
	if err != nil {
		return domain.Event{}, "", fmt.Errorf("jetstream: read message metadata: %w", err)
	}
	return event, encodeOffset(meta.Sequence.Stream), nil
}

// zonesEqual reports whether two zone slices represent the same set.
func zonesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
outer:
	for _, az := range a {
		for _, bz := range b {
			if az == bz {
				continue outer
			}
		}
		return false
	}
	return true
}
