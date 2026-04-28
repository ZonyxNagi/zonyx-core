package interceptors

import (
	"context"
	"errors"
	"testing"

	v1 "github.com/ZonyxNagi/proto-zonyx-core/pkg/api/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// recvOnceStream is a grpc.ServerStream stub whose RecvMsg returns the
// preloaded proto message exactly once, then errors on subsequent calls.
type recvOnceStream struct {
	ctx context.Context
	msg proto.Message
	hit bool
}

func (s *recvOnceStream) SetHeader(_ metadata.MD) error  { return nil }
func (s *recvOnceStream) SendHeader(_ metadata.MD) error { return nil }
func (s *recvOnceStream) SetTrailer(_ metadata.MD)       {}
func (s *recvOnceStream) Context() context.Context       { return s.ctx }
func (s *recvOnceStream) SendMsg(_ any) error            { return nil }
func (s *recvOnceStream) RecvMsg(m any) error {
	if s.hit {
		return errors.New("eof")
	}
	s.hit = true
	dst, ok := m.(proto.Message)
	if !ok {
		return errors.New("recvOnceStream: not a proto.Message")
	}
	// proto.Merge copies the populated fields without touching the embedded
	// MessageState mutex.
	proto.Merge(dst, s.msg)
	return nil
}

func TestValidateStream_AcceptsValidSubscribeRequest(t *testing.T) {
	t.Parallel()
	ic := ValidateStream()

	stream := &recvOnceStream{
		ctx: context.Background(),
		msg: &v1.SubscribeRequest{Types: []v1.EventType{v1.EventType_EVENT_TYPE_ZONE}},
	}

	handlerCalled := false
	handler := func(_ any, ss grpc.ServerStream) error {
		handlerCalled = true
		// Drive a Recv through the validating wrapper.
		var got v1.SubscribeRequest
		if err := ss.RecvMsg(&got); err != nil {
			return err
		}
		if len(got.GetTypes()) != 1 || got.GetTypes()[0] != v1.EventType_EVENT_TYPE_ZONE {
			t.Errorf("Types = %v", got.GetTypes())
		}
		return nil
	}

	err := ic(nil, stream, &grpc.StreamServerInfo{FullMethod: "/v1.CoreService/Subscribe"}, handler)
	if err != nil {
		t.Errorf("interceptor err = %v, want nil", err)
	}
	if !handlerCalled {
		t.Error("handler not invoked")
	}
}

func TestValidateStream_RejectsUnspecifiedEventType(t *testing.T) {
	t.Parallel()
	ic := ValidateStream()

	// EVENT_TYPE_UNSPECIFIED (0) violates the proto's `not 0` validator.
	stream := &recvOnceStream{
		ctx: context.Background(),
		msg: &v1.SubscribeRequest{Types: []v1.EventType{v1.EventType_EVENT_TYPE_UNSPECIFIED}},
	}

	handler := func(_ any, ss grpc.ServerStream) error {
		var got v1.SubscribeRequest
		// This Recv goes through the validator and should return InvalidArgument.
		return ss.RecvMsg(&got)
	}

	err := ic(nil, stream, &grpc.StreamServerInfo{FullMethod: "/v1.CoreService/Subscribe"}, handler)
	if err == nil {
		t.Fatal("expected validation error, got nil")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("err is not a status: %v", err)
	}
	if st.Code() != codes.InvalidArgument {
		t.Errorf("code = %v, want InvalidArgument", st.Code())
	}
}

func TestValidateStream_RejectsShortDirectiveID(t *testing.T) {
	t.Parallel()
	ic := ValidateStream()

	// Directive.id requires len ∈ [3..64].
	stream := &recvOnceStream{
		ctx: context.Background(),
		msg: &v1.PublishRequest{Directive: &v1.Directive{Id: "x", Name: "register_device"}},
	}

	handler := func(_ any, ss grpc.ServerStream) error {
		var got v1.PublishRequest
		return ss.RecvMsg(&got)
	}

	err := ic(nil, stream, &grpc.StreamServerInfo{FullMethod: "/v1.CoreService/Publish"}, handler)
	if err == nil {
		t.Fatal("expected validation error for short directive id")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("err is not a status: %v", err)
	}
	if st.Code() != codes.InvalidArgument {
		t.Errorf("code = %v, want InvalidArgument", st.Code())
	}
}
