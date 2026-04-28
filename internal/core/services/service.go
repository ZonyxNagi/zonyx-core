// Package services contains the core business logic of zonyx-core. It is
// transport- and storage-agnostic — gRPC and persistence concerns live in
// internal/adapters and internal/repositories respectively. The Service
// struct here implements ports.Service and is the only writer to the
// Repository port.
package services

import (
	"context"
	"errors"
	"log/slog"

	"github.com/ZonyxNagi/zonyx-core/internal/core/domain"
	"github.com/ZonyxNagi/zonyx-core/internal/core/ports"
	"golang.org/x/sync/errgroup"
	"google.golang.org/genproto/googleapis/rpc/code"
	rpcstatus "google.golang.org/genproto/googleapis/rpc/status"
)

// Service is the concrete core of zonyx-core. Construct with NewService and
// call Start to drive the registered providers' lifecycle.
type Service struct {
	providers []ports.Provider
	repo      ports.Repository
	logger    *slog.Logger
}

// NewService wires the core. providers are driven on Start; repo is the
// outbound persistence + replay backend; logger is used for everything the
// service emits (a nil logger falls back to slog.Default).
func NewService(providers []ports.Provider, repo ports.Repository, logger *slog.Logger) (*Service, error) {
	if repo == nil {
		return nil, errors.New("services: repository is required")
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Service{
		providers: providers,
		repo:      repo,
		logger:    logger,
	}, nil
}

// Start launches every registered provider in its own goroutine and blocks
// until ctx is cancelled or any provider returns an error. Fail-fast: the
// first error cancels every other provider via errgroup.
func (s *Service) Start(ctx context.Context) error {
	if len(s.providers) == 0 {
		s.logger.Warn("services: no providers registered, Start will idle until ctx cancels")
		<-ctx.Done()
		return ctx.Err()
	}

	g, gctx := errgroup.WithContext(ctx)
	for _, p := range s.providers {
		p := p
		g.Go(func() error {
			err := p.Run(gctx, func(e domain.Event) error {
				return s.handleEvent(gctx, e)
			})
			// ctx cancellation is the expected unwind path, not a fault.
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		})
	}
	return g.Wait()
}

func (s *Service) handleEvent(ctx context.Context, e domain.Event) error {
	if _, err := s.repo.Write(ctx, e); err != nil {
		// context.Canceled is the expected unwind path on shutdown — same
		// treatment as provider errors in Start. Don't log it as an error.
		if errors.Is(err, context.Canceled) {
			return err
		}
		s.logger.Error("services: failed to write event",
			slog.String("id", e.ID),
			slog.String("type", e.Type.String()),
			slog.String("actor", e.Actor.ID),
			slog.Any("err", err),
		)
		return err
	}
	return nil
}

// Subscribe delegates entirely to Repository.Watch. The backing store's
// consumer handles snapshot (nil offset), replay (non-nil offset), and
// seamless live continuation — no separate in-memory registry is needed
// here while a single consumer covers all three phases.
func (s *Service) Subscribe(
	ctx context.Context,
	types []domain.EventType,
	offset *string,
	send func(e domain.Event, offset string) error,
) error {
	return s.repo.Watch(ctx, types, offset, send)
}

// Publish routes the directive to every registered provider in order,
// returning on the first one that accepts it (returns nil). If no provider
// handles the directive, an UNIMPLEMENTED status is returned; the gRPC
// adapter keeps the stream open regardless.
func (s *Service) Publish(ctx context.Context, d domain.Directive) (string, *rpcstatus.Status, error) {
	for _, p := range s.providers {
		if err := p.Send(ctx, d); err == nil {
			return d.ID, &rpcstatus.Status{Code: int32(code.Code_OK)}, nil
		}
	}
	s.logger.Warn("services: no provider handled directive", slog.String("name", d.Name))
	return d.ID, &rpcstatus.Status{
		Code:    int32(code.Code_UNIMPLEMENTED),
		Message: "no provider handled directive: " + d.Name,
	}, nil
}

// ReadConfiguration blocks until ctx is cancelled. Configuration management
// (storing zones, broadcasting updates) is future work — until zones are
// registered there is nothing to deliver.
func (s *Service) ReadConfiguration(ctx context.Context, _ func(domain.Configuration) error) error {
	<-ctx.Done()
	return ctx.Err()
}

// ensure Service satisfies the inbound port at compile time.
var _ ports.Service = (*Service)(nil)
