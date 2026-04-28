// Package ports defines the interfaces that connect the Zonyx core service to
// the outside world. Adapters live in internal/adapters and implement these
// interfaces. Inbound (driving) ports are what the gRPC adapter calls into;
// outbound (driven) ports are what the Service calls out to.
package ports

import (
	"context"

	"github.com/ZonyxNagi/zonyx-core/internal/core/domain"
	rpcstatus "google.golang.org/genproto/googleapis/rpc/status"
)

// Generate service-based mocks
//go:generate mockgen -source=./ports.go -destination=../../mocks/service.go -package=mocks github.com/ZonyxNagi/zonyx-core

// Service is the inbound port the gRPC adapter calls into. It is the public
// surface of the core for client-facing RPCs.
type Service interface {
	// Subscribe registers a session, replays history (or current state) when
	// appropriate, and forwards live events to send until ctx is cancelled.
	// `types` is an optional EventType filter; an empty/nil slice means all
	// types. When `offset` is nil the caller receives the current snapshot
	// (one event per actor matching `types`) followed by live events; when
	// non-nil the caller receives the replay from that cursor followed by
	// live events.
	Subscribe(
		ctx context.Context,
		types []domain.EventType,
		offset *string,
		send func(e domain.Event, offset string) error,
	) error

	// Publish handles a control-plane Directive submitted via
	// CoreService.Publish. Returns the directive id (echoed back to the
	// producer for correlation) and the per-message rpc Status. A non-OK
	// status does NOT end the publish stream — the gRPC adapter is
	// responsible for keeping it open.
	Publish(ctx context.Context, d domain.Directive) (directiveID string, status *rpcstatus.Status, err error)

	// ReadConfiguration delivers configuration snapshots to send. The first
	// call should deliver the current configuration; subsequent calls deliver
	// updated snapshots whenever the configuration changes. Blocks until ctx
	// is cancelled or send returns an error.
	ReadConfiguration(ctx context.Context, send func(cfg domain.Configuration) error) error
}

// Provider is a driven inbound port: the Service starts each registered
// Provider on Service.Start() and consumes events through the emit callback.
// QUUPPA is one provider; computer-vision will be a sibling implementation.
//
// Run owns its own transport (UDP socket, RTSP connection, etc.). It
// blocks until either:
//   - ctx is cancelled — Run MUST clean up its transport and return
//     ctx.Err(); the errgroup in Service.Start treats this as the trigger
//     to unwind, not as a fault.
//   - an unrecoverable error occurs — Run returns it; the errgroup
//     cancels every other provider (fail-fast) and Service.Start returns.
//
// Per-message decode failures must be logged and skipped, never returned —
// one bad packet must not bring down the whole pipeline.
//
// Send dispatches a control-plane Directive to the hardware or system
// behind the provider (e.g. a QUUPPA command). Returns an error if the
// provider does not handle the directive or if the underlying transport
// fails. A non-nil error from Send must NOT affect the running Run.
type Provider interface {
	Run(ctx context.Context, emit func(domain.Event) error) error
	Send(ctx context.Context, d domain.Directive) error
}

// Repository is the driven outbound port the Service uses to persist events
// and serve subscribers. It is implemented by adapters such as NATS
// JetStream, Kafka, or Valkey Streams; the Service is the only caller.
type Repository interface {
	// Write durably stores an event and returns its opaque cursor. The
	// returned offset is what subscribers pass back later to resume from
	// this position; it must satisfy the proto's [3..64] char constraint.
	Write(ctx context.Context, e domain.Event) (offset string, err error)

	// Watch delivers events to deliver(). Behaviour depends on offset:
	//
	//   nil      → deliver the current snapshot (latest event per actor
	//              matching `types`) so the caller has a complete picture
	//              of the world right now, then continue with live events.
	//   non-nil  → replay every event since that cursor (matching `types`),
	//              then continue with live events.
	//
	// The replay→live transition is the adapter's responsibility; deliver
	// receives every event exactly once in offset order without gaps.
	// Watch blocks until ctx is cancelled or deliver returns an error.
	Watch(
		ctx context.Context,
		types []domain.EventType,
		offset *string,
		deliver func(e domain.Event, offset string) error,
	) error

}
