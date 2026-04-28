package interceptors

import (
	"context"
	"fmt"
	"log/slog"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// MonitorStream returns a gRPC stream interceptor that logs the request and response.
func MonitorStream() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx := ss.Context()

		// Call the handler and get the response
		err := handler(srv, ss)

		// Log the error if there is one
		if err != nil {
			// Check if the context was canceled
			if err == context.Canceled {
				return nil
			}

			// Add the attributes to the log
			attrs := map[string]string{
				"attr.name": info.FullMethod,
			}

			// Get the metadata from the context
			if md, ok := metadata.FromIncomingContext(ctx); ok {
				attrs["attr.md"] = fmt.Sprintf("%v", md)
			}

			// Transform the error to get the status code
			status, ok := status.FromError(err)
			if !ok {
				slog.Error(err.Error(), slog.Any("attrs", attrs))

				return err
			}

			switch status.Code() {
			case codes.InvalidArgument, codes.NotFound, codes.PermissionDenied:
				slog.Warn(err.Error(), slog.Any("attrs", attrs))
			default:
				slog.Error(err.Error(), slog.Any("attrs", attrs))
			}
		}

		return err
	}
}
