package interceptors

import (
	"buf.build/go/protovalidate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// validatingServerStream wraps grpc.ServerStream to validate incoming messages.
type validatingServerStream struct {
	grpc.ServerStream
	validator protovalidate.Validator
}

func (s *validatingServerStream) RecvMsg(m any) error {
	if err := s.ServerStream.RecvMsg(m); err != nil {
		return err
	}

	if msg, ok := m.(proto.Message); ok {
		if err := s.validator.Validate(msg); err != nil {
			return status.Error(codes.InvalidArgument, err.Error())
		}
	}

	return nil
}

// ValidateStream returns a gRPC stream interceptor that validates incoming messages using protovalidate.
func ValidateStream() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		validator, err := protovalidate.New()
		if err != nil {
			return err
		}

		return handler(srv, &validatingServerStream{ServerStream: ss, validator: validator})
	}
}
