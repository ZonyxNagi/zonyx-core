package server

import (
	"context"
	"fmt"
	"net"
	"time"

	v1 "github.com/ZonyxNagi/proto-zonyx-core/pkg/api/v1"
	adapters "github.com/ZonyxNagi/zonyx-core/internal/adapters/grpc"
	"github.com/ZonyxNagi/zonyx-core/internal/adapters/grpc/server/interceptors"
	"google.golang.org/grpc"
	health "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
)

type Server struct {
	gs *grpc.Server
	hc *HealhCheck
}

// NewServer constructs the gRPC server and registers the CoreService (which
// exposes both the Subscribe and Publish RPCs) along with the gRPC health
// check and reflection.
func NewServer(core *adapters.CoreAdapter) (*Server, error) {
	// Create a new grpc server
	srv := grpc.NewServer([]grpc.ServerOption{
		grpc.ConnectionTimeout(10 * time.Second),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             10 * time.Second,
			PermitWithoutStream: false,
		}),
		grpc.ChainStreamInterceptor(
			interceptors.ValidateStream(),
			interceptors.MonitorStream(),
		),
	}...)

	// Register rpc's
	v1.RegisterCoreServiceServer(srv, core)

	// Health check
	hc := NewHealhCheck()

	health.RegisterHealthServer(srv, hc)

	// Reflection
	reflection.Register(srv)

	return &Server{gs: srv, hc: hc}, nil
}

// Start the server and listen for incoming requests.
func (srv *Server) Start(port int) error {
	_, cancel := context.WithCancel(context.Background())

	defer cancel()

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return err
	}

	return srv.gs.Serve(lis)
}

// Stop shutdown the server gracefully.
func (srv *Server) Stop() {
	// Set the server to not available for serving
	srv.hc.Status = HealthCheckStatus_NOT_SERVING

	srv.gs.GracefulStop()
}
