package services

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/golang/mock/gomock"

	"github.com/ZonyxNagi/zonyx-core/internal/core/domain"
	"github.com/ZonyxNagi/zonyx-core/internal/mocks"
)

// helpers

func newService(t *testing.T, repo *mocks.MockRepository) *Service {
	t.Helper()
	svc, err := NewService(nil, repo, nil)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	return svc
}

func makeZoneEvent(actorID string, zones []string) domain.Event {
	return domain.Event{
		ID:   "evt-" + actorID,
		Type: domain.EventTypeZone,
		Actor: domain.Actor{ID: actorID},
		State: domain.State{
			Location: &domain.Location{Zones: zones},
			Presence: domain.Presence{Type: domain.PresenceTypeActive, Since: time.Now()},
		},
		Timestamp: time.Now(),
	}
}

func makePresenceEvent(actorID string, pt domain.PresenceType) domain.Event {
	return domain.Event{
		ID:   "evt-" + actorID,
		Type: domain.EventTypePresence,
		Actor: domain.Actor{ID: actorID},
		State: domain.State{
			Presence: domain.Presence{Type: pt, Since: time.Now()},
		},
		Timestamp: time.Now(),
	}
}

func makeCommandEvent(actorID string) domain.Event {
	return domain.Event{
		ID:    "evt-cmd-" + actorID,
		Type:  domain.EventTypeCommand,
		Actor: domain.Actor{ID: actorID},
		State: domain.State{
			Command: &domain.Command{ID: "cmd-1", Name: "button_press"},
		},
		Timestamp: time.Now(),
	}
}

// handleEvent — all events reach the repository unconditionally

func TestHandleEvent_Zone_AlwaysWrites(t *testing.T) {
	ctrl := gomock.NewController(t)
	repo := mocks.NewMockRepository(ctrl)
	svc := newService(t, repo)

	e := makeZoneEvent("actor-1", []string{"zone-A"})
	// Write must be called regardless of prior state.
	repo.EXPECT().Write(gomock.Any(), e).Return("off-1", nil).Times(1)

	if err := svc.handleEvent(context.Background(), e); err != nil {
		t.Errorf("err = %v, want nil", err)
	}
}

func TestHandleEvent_Zone_RepeatState_StillWrites(t *testing.T) {
	ctrl := gomock.NewController(t)
	repo := mocks.NewMockRepository(ctrl)
	svc := newService(t, repo)

	e := makeZoneEvent("actor-1", []string{"zone-A"})
	// Two identical events — both must be written; service has no dedup.
	repo.EXPECT().Write(gomock.Any(), e).Return("off-1", nil).Times(2)

	for range 2 {
		if err := svc.handleEvent(context.Background(), e); err != nil {
			t.Errorf("err = %v, want nil", err)
		}
	}
}

func TestHandleEvent_Presence_AlwaysWrites(t *testing.T) {
	ctrl := gomock.NewController(t)
	repo := mocks.NewMockRepository(ctrl)
	svc := newService(t, repo)

	e := makePresenceEvent("actor-1", domain.PresenceTypeActive)
	repo.EXPECT().Write(gomock.Any(), e).Return("off-1", nil)

	if err := svc.handleEvent(context.Background(), e); err != nil {
		t.Errorf("err = %v, want nil", err)
	}
}

func TestHandleEvent_Command_AlwaysWrites(t *testing.T) {
	ctrl := gomock.NewController(t)
	repo := mocks.NewMockRepository(ctrl)
	svc := newService(t, repo)

	e := makeCommandEvent("actor-1")
	repo.EXPECT().Write(gomock.Any(), e).Return("off-1", nil)

	if err := svc.handleEvent(context.Background(), e); err != nil {
		t.Errorf("err = %v, want nil", err)
	}
}

func TestHandleEvent_WriteError_PropagatesError(t *testing.T) {
	ctrl := gomock.NewController(t)
	repo := mocks.NewMockRepository(ctrl)
	svc := newService(t, repo)

	e := makeZoneEvent("actor-1", []string{"zone-A"})
	repo.EXPECT().Write(gomock.Any(), e).Return("", errors.New("nats down"))

	if err := svc.handleEvent(context.Background(), e); err == nil {
		t.Error("expected error, got nil")
	}
}

