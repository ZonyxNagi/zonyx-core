package server

import (
	"context"
	"testing"

	health "google.golang.org/grpc/health/grpc_health_v1"
)

func TestHealhCheck_DefaultIsServing(t *testing.T) {
	t.Parallel()
	hc := NewHealhCheck()
	if hc.Status != HealthCheckStatus_SERVING {
		t.Fatalf("default status = %v, want SERVING", hc.Status)
	}
}

func TestHealhCheck_CheckReportsCurrentStatus(t *testing.T) {
	t.Parallel()
	hc := NewHealhCheck()

	resp, err := hc.Check(context.Background(), &health.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if resp.GetStatus() != health.HealthCheckResponse_SERVING {
		t.Errorf("status = %v, want SERVING", resp.GetStatus())
	}

	// Mutate the field — Server.Stop() flips this to NOT_SERVING during graceful shutdown.
	hc.Status = HealthCheckStatus_NOT_SERVING
	resp, err = hc.Check(context.Background(), &health.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("Check after flip: %v", err)
	}
	if resp.GetStatus() != health.HealthCheckResponse_NOT_SERVING {
		t.Errorf("status = %v, want NOT_SERVING", resp.GetStatus())
	}
}
