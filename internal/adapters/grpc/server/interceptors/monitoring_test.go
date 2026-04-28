package interceptors

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestMonitorStream_PassesThroughOK(t *testing.T) {
	t.Parallel()
	ic := MonitorStream()

	called := false
	handler := func(srv any, stream grpc.ServerStream) error {
		called = true
		return nil
	}

	err := ic(nil, noopStream{ctx: context.Background()},
		&grpc.StreamServerInfo{FullMethod: "/test/Method"}, handler)
	if err != nil {
		t.Errorf("err = %v, want nil", err)
	}
	if !called {
		t.Error("handler never invoked")
	}
}

func TestMonitorStream_SwallowsContextCanceled(t *testing.T) {
	t.Parallel()
	ic := MonitorStream()

	handler := func(any, grpc.ServerStream) error { return context.Canceled }

	err := ic(nil, noopStream{ctx: context.Background()},
		&grpc.StreamServerInfo{FullMethod: "/test/Cancelled"}, handler)

	// Per monitoring.go: ctx.Cancelled is swallowed and returned as nil.
	if err != nil {
		t.Errorf("err = %v, want nil (context.Canceled is silenced)", err)
	}
}

func TestMonitorStream_PropagatesGRPCStatusError(t *testing.T) {
	t.Parallel()
	ic := MonitorStream()

	handlerErr := status.Error(codes.InvalidArgument, "bad input")
	handler := func(any, grpc.ServerStream) error { return handlerErr }

	err := ic(nil, noopStream{ctx: context.Background()},
		&grpc.StreamServerInfo{FullMethod: "/test/Bad"}, handler)
	if !errors.Is(err, handlerErr) {
		t.Errorf("err = %v, want propagated InvalidArgument", err)
	}
}

func TestMonitorStream_PropagatesNonStatusError(t *testing.T) {
	t.Parallel()
	ic := MonitorStream()

	boom := errors.New("plain error not a grpc status")
	handler := func(any, grpc.ServerStream) error { return boom }

	err := ic(nil, noopStream{ctx: context.Background()},
		&grpc.StreamServerInfo{FullMethod: "/test/Plain"}, handler)
	if !errors.Is(err, boom) {
		t.Errorf("err = %v, want plain error propagated", err)
	}
}

// noopStream is a minimal grpc.ServerStream the interceptor wraps. The real
// gRPC server provides a real one; for unit testing the interceptor in
// isolation a no-op is enough.
type noopStream struct {
	ctx context.Context
}

func (n noopStream) SetHeader(_ metadata.MD) error  { return nil }
func (n noopStream) SendHeader(_ metadata.MD) error { return nil }
func (n noopStream) SetTrailer(_ metadata.MD)       {}
func (n noopStream) Context() context.Context       { return n.ctx }
func (n noopStream) SendMsg(_ any) error            { return nil }
func (n noopStream) RecvMsg(_ any) error            { return nil }
