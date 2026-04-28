package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	adapters "github.com/ZonyxNagi/zonyx-core/internal/adapters/grpc"
	"github.com/ZonyxNagi/zonyx-core/internal/adapters/grpc/server"
	"github.com/ZonyxNagi/zonyx-core/internal/adapters/quuppa"
	"github.com/ZonyxNagi/zonyx-core/internal/core/ports"
	"github.com/ZonyxNagi/zonyx-core/internal/core/services"
	"github.com/ZonyxNagi/zonyx-core/internal/helpers"
	"github.com/ZonyxNagi/zonyx-core/internal/repositories/jetstream"
	"github.com/nats-io/nats.go"
	"golang.org/x/sync/errgroup"
)

func main() {
	// ---- config ------------------------------------------------------------

	name, _ := helpers.GetEnvOr("SERVICE_NAME", "unknown")
	port, _ := helpers.GetEnvOr("PORT", 8080)
	quuppaAddr, _ := helpers.GetEnvOr("QUUPPA_LISTEN_ADDR", ":9090")
	natsURL, _ := helpers.GetEnvOr("NATS_URL", "nats://localhost:4222")

	// ---- logger ------------------------------------------------------------

	var level slog.Level
	_ = level.UnmarshalText([]byte(os.Getenv("LOG_LEVEL")))

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: level,
	}))
	slog.SetDefault(logger)

	// ---- root context tied to OS signals -----------------------------------

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// ---- composition -------------------------------------------------------

	nc, err := nats.Connect(natsURL, nats.MaxReconnects(-1))
	if err != nil {
		logger.Error("connect to NATS", slog.String("url", natsURL), slog.Any("err", err))
		os.Exit(1)
	}

	repo, err := jetstream.NewRepository(nc, jetstream.Config{}, logger.With(slog.String("component", "repository")))
	if err != nil {
		logger.Error("init repository", slog.Any("err", err))
		nc.Close()
		os.Exit(1)
	}

	providers := []ports.Provider{
		quuppa.New(quuppaAddr, logger.With(slog.String("provider", "quuppa"))),
	}

	service, err := services.NewService(providers, repo, logger)
	if err != nil {
		logger.Error("init service", slog.Any("err", err))
		nc.Close()
		os.Exit(1)
	}

	core, err := adapters.NewCoreAdapter(service)
	if err != nil {
		logger.Error("init core adapter", slog.Any("err", err))
		nc.Close()
		os.Exit(1)
	}

	srv, err := server.NewServer(core)
	if err != nil {
		logger.Error("init grpc server", slog.Any("err", err))
		nc.Close()
		os.Exit(1)
	}

	// ---- run all components in parallel; first error cancels everyone ------

	logger.Info("starting",
		slog.String("service", name),
		slog.Int("port", port),
		slog.String("quuppa_addr", quuppaAddr),
		slog.String("nats_url", natsURL),
	)

	g, gctx := errgroup.WithContext(ctx)

	// Drive QUUPPA (and any future provider) lifecycles.
	g.Go(func() error {
		err := service.Start(gctx)
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return err
	})

	// gRPC server. srv.Start blocks until srv.Stop is called.
	g.Go(func() error {
		return srv.Start(port)
	})

	// Tear-down goroutine: wait for either ctx cancellation (signal) or any
	// errgroup goroutine returning an error (gctx will cancel), then stop
	// the gRPC server and drain NATS gracefully.
	g.Go(func() error {
		<-gctx.Done()
		logger.Info("shutdown signal received, stopping gracefully", slog.String("service", name))
		srv.Stop()
		nc.Drain() //nolint:errcheck
		return nil
	})

	if err := g.Wait(); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("shutdown with error", slog.Any("err", err))
		os.Exit(1)
	}

	logger.Info("stopped cleanly", slog.String("service", name))
}
