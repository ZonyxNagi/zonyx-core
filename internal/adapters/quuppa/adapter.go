// Package quuppa is the QUUPPA positioning-engine inbound provider. It binds
// a UDP socket, parses the QPE 9.5+ DefaultLocationAndInfo JSON datagrams,
// runs the per-tag state machine described in docs/QUUPPA.md §7/§8, and
// emits domain.Event values through the Provider port handler the core
// Service registered with it on startup.
//
// Wire-format spec, transition rules, and tuning constants live in
// docs/QUUPPA.md — that file is the source of truth for everything in this
// package; treat package code as an executable mirror of those sections.
//
// Lifecycle and ownership rules — see internal/core/ports/ports.go for the
// Provider contract.
package quuppa

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"sync"
	"time"

	"github.com/ZonyxNagi/zonyx-core/internal/core/domain"
)

// readBufSize is the per-packet read buffer. QUUPPA datagrams in
// DefaultLocationAndInfo format are well under 1 KB; 4 KB leaves headroom
// for future fields without spilling into multiple reads.
const readBufSize = 4096

// emitChanSize is the depth of the buffered channel between the UDP read loop
// and the emit goroutine. Sized for ~5 s of events at 100 tags × 4 Hz × 3
// events/packet = 6 000 events; 1 024 is a conservative lower bound that still
// absorbs normal bursts without blocking the read loop.
const emitChanSize = 1024

// udpRecvBufSize is the requested kernel-side UDP receive-buffer size (4 MB).
// The actual size granted is capped by net.core.rmem_max; set that sysctl to
// at least 8 388 608 (8 MB) in the container so this request is honoured.
const udpRecvBufSize = 4 * 1024 * 1024

// clock abstracts time.Now so tests can drive the watchdog deterministically
// without sleeping. Production uses realClock; tests inject a fake.
type clock interface {
	Now() time.Time
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

// Adapter is the QUUPPA UDP inbound provider. Construct with New and pass to
// the Service as a ports.Provider.
type Adapter struct {
	addr   string
	logger *slog.Logger

	// test seams — left at zero values in production (real clock, default tick).
	clk          clock
	tickInterval time.Duration
}

// New builds a QUUPPA adapter that will bind to addr (e.g. ":9090") on the
// first call to Stream. If logger is nil the default slog logger is used.
func New(addr string, logger *slog.Logger) *Adapter {
	if logger == nil {
		logger = slog.Default()
	}
	return &Adapter{addr: addr, logger: logger}
}

// newWithClock is the test-only constructor used to inject a fake clock and a
// shorter watchdog tick interval. Not part of the public API.
func newWithClock(addr string, logger *slog.Logger, clk clock, tick time.Duration) *Adapter {
	a := New(addr, logger)
	a.clk = clk
	a.tickInterval = tick
	return a
}

// Run implements ports.Provider. It binds the UDP socket, then loops over
// (1) timed reads and (2) periodic watchdog ticks. Per-actor state lives in a
// map local to this invocation — single-goroutine, no mutex.
//
// Derived events are handed off to a dedicated emit goroutine via a buffered
// channel so that downstream latency (NATS publish, gRPC fan-out) never blocks
// the read loop. If the channel fills, the event is dropped and a warning is
// logged — the read loop always has priority over downstream back-pressure.
//
// Worst-case detection latency for TAG_OFFLINE is OfflineThreshold + tick.
//
// It blocks until ctx is cancelled (returns ctx.Err()) or an unrecoverable
// error occurs. Per-packet errors (malformed input, transient network) are
// logged and skipped — they never abort the stream.
func (a *Adapter) Run(ctx context.Context, emit func(domain.Event) error) error {
	pc, err := net.ListenPacket("udp", a.addr)
	if err != nil {
		return fmt.Errorf("quuppa: bind %s: %w", a.addr, err)
	}
	defer pc.Close()

	// Widen the kernel-side receive buffer so bursts don't drop packets before
	// the read loop can drain them. The actual size granted is capped by the
	// container sysctl net.core.rmem_max; we warn but don't fail if the OS
	// rejects the request.
	if udpConn, ok := pc.(*net.UDPConn); ok {
		if err := udpConn.SetReadBuffer(udpRecvBufSize); err != nil {
			a.logger.Warn("quuppa: could not set UDP receive buffer", slog.Any("err", err))
		}
	}

	// Close the socket when ctx is cancelled so the blocking ReadFrom returns.
	go func() {
		<-ctx.Done()
		_ = pc.Close()
	}()

	a.logger.Info("quuppa: listening", slog.String("addr", a.addr))

	clk := a.clk
	if clk == nil {
		clk = realClock{}
	}
	tick := a.tickInterval
	if tick <= 0 {
		tick = WatchdogTick
	}

	// evCh decouples the read loop from the emit goroutine. The read loop sends
	// events non-blocking; on full it drops and logs. The emit goroutine is the
	// sole caller of the upstream emit func — no concurrency on emit.
	evCh := make(chan domain.Event, emitChanSize)
	errCh := make(chan error, 1)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for ev := range evCh {
			if err := emit(ev); err != nil {
				select {
				case errCh <- err:
				default:
				}
				return
			}
		}
	}()
	// On return: close evCh so the emit goroutine sees EOF and exits, then wait
	// for it to finish draining before we return (avoids use-after-return on emit).
	defer func() {
		close(evCh)
		wg.Wait()
	}()

	// send dispatches a single event to the emit goroutine, dropping if the
	// channel is full to keep the read loop unblocked.
	send := func(ev domain.Event) {
		select {
		case evCh <- ev:
		default:
			a.logger.Warn("quuppa: emit channel full, dropping event",
				slog.String("actor", ev.Actor.ID),
				slog.String("type", ev.Type.String()))
		}
	}

	states := make(map[string]*tagState)
	buf := make([]byte, readBufSize)

	for {
		// Check if the emit goroutine has signalled a fatal error.
		select {
		case err := <-errCh:
			return fmt.Errorf("quuppa: emit: %w", err)
		default:
		}

		// Bound each read by the watchdog tick. When the deadline trips
		// without a packet, we run the offline-detection sweep and loop.
		// The deadline must use real wall-clock time (the kernel evaluates
		// it against time.Now), but the watchdog's "is this tag silent"
		// comparison uses the injectable clk so tests can drive it without
		// real-time waits.
		_ = pc.SetReadDeadline(time.Now().Add(tick))

		n, _, err := pc.ReadFrom(buf)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			// Deadline expired with no packet — drive the watchdog and
			// continue. os.ErrDeadlineExceeded is the canonical sentinel
			// across platforms (also matches *net.OpError.Timeout()).
			if errors.Is(err, os.ErrDeadlineExceeded) {
				// runWatchdog keeps its error-returning emit signature for
				// testability; in production the wrapper is fire-and-forget.
				_ = runWatchdog(states, clk.Now(), func(ev domain.Event) error {
					send(ev)
					return nil
				})
				continue
			}
			a.logger.Warn("quuppa: read error", slog.Any("err", err))
			continue
		}

		msg, ok, reason := parseDatagram(buf[:n])
		if !ok {
			a.logger.Warn("quuppa: malformed datagram",
				slog.Int("bytes", n),
				slog.String("reason", reason))
			continue
		}

		st, seen := states[msg.TagID]
		if !seen {
			st = &tagState{}
			states[msg.TagID] = st
		}

		// Use response timestamp as the event time (server-clock). Falls
		// back to the wall clock if the field is somehow zero — should
		// never happen since parseDatagram rejects ResponseTS <= 0.
		now := time.UnixMilli(msg.ResponseTS)
		if now.IsZero() {
			now = clk.Now()
		}

		for _, ev := range deriveEvents(st, msg, now) {
			send(ev)
		}
	}
}

// Send implements ports.Provider. QUUPPA directive dispatch is not yet
// implemented; the method logs the attempt and returns an error so that
// Service.Publish can fall through to any other registered provider.
func (a *Adapter) Send(_ context.Context, d domain.Directive) error {
	return errors.New("quuppa: Send not implemented")
}
