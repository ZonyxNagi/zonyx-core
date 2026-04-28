package jetstream

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ZonyxNagi/zonyx-core/internal/core/domain"
)

// Subject helpers.  All subjects share the "zonyx." namespace so they are
// covered by a single stream with wildcard "zonyx.>".
//
//   zonyx.raw.<actor_id>      — every parsed datagram (full position history)
//   zonyx.zone.<actor_id>     — derived zone-change events only
//   zonyx.presence.<actor_id> — derived presence-change events only
//   zonyx.command.<actor_id>  — command events (always pass-through)

func rawSubject(actorID string) string      { return "zonyx.raw." + actorID }
func zoneSubject(actorID string) string     { return "zonyx.zone." + actorID }
func presenceSubject(actorID string) string { return "zonyx.presence." + actorID }
func commandSubject(actorID string) string  { return "zonyx.command." + actorID }

// allChangeSubjects is the complete set of subject patterns that carry derived
// change events. Raw subjects are intentionally excluded — Watch must never
// deliver raw events to subscribers.
var allChangeSubjects = []string{
	"zonyx.zone.>",
	"zonyx.presence.>",
	"zonyx.command.>",
}

// watchSubjects returns the JetStream subject patterns for a Watch call.
// nil/empty types → all three change subjects.
// Known types → the subjects that match; unknown types are silently ignored.
// Raw subjects are never returned.
func watchSubjects(types []domain.EventType) []string {
	if len(types) == 0 {
		return allChangeSubjects
	}
	seen := make(map[string]struct{}, len(types))
	out := make([]string, 0, len(types))
	for _, t := range types {
		var s string
		switch t {
		case domain.EventTypeZone:
			s = "zonyx.zone.>"
		case domain.EventTypePresence:
			s = "zonyx.presence.>"
		case domain.EventTypeCommand:
			s = "zonyx.command.>"
		default:
			continue
		}
		if _, dup := seen[s]; !dup {
			seen[s] = struct{}{}
			out = append(out, s)
		}
	}
	return out
}

// encodeOffset converts a JetStream stream sequence number to an opaque,
// URL-safe offset string that satisfies the proto [3..64] char constraint.
func encodeOffset(seq uint64) string {
	s := strconv.FormatUint(seq, 10)
	if len(s) < 3 {
		s = strings.Repeat("0", 3-len(s)) + s
	}
	return s
}

func decodeOffset(offset string) (uint64, error) {
	seq, err := strconv.ParseUint(strings.TrimLeft(offset, "0"), 10, 64)
	if err != nil {
		// offset was all zeros — that's sequence 0, which is invalid for JetStream
		if strings.TrimLeft(offset, "0") == "" {
			return 0, fmt.Errorf("jetstream: offset %q decodes to zero sequence", offset)
		}
		return 0, fmt.Errorf("jetstream: invalid offset %q: %w", offset, err)
	}
	return seq, nil
}

// wireEvent is the on-wire JSON representation of a domain.Event.
// time.Time fields are encoded as RFC3339Nano.
type wireEvent struct {
	ID        string          `json:"id"`
	Type      int             `json:"type"`
	Actor     wireActor       `json:"actor"`
	State     wireState       `json:"state"`
	Timestamp time.Time       `json:"timestamp"`
}

type wireActor struct {
	ID       string            `json:"id"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

type wireState struct {
	Location *wireLocation `json:"location,omitempty"`
	Presence wirePresence  `json:"presence"`
	Command  *wireCommand  `json:"command,omitempty"`
}

type wireLocation struct {
	Point *wirePoint `json:"point,omitempty"`
	Zones []string   `json:"zones,omitempty"`
}

type wirePoint struct {
	X float64 `json:"x"`
	Y float64 `json:"y"`
	Z float64 `json:"z"`
}

type wirePresence struct {
	Type  int       `json:"type"`
	Since time.Time `json:"since"`
}

type wireCommand struct {
	ID        string            `json:"id"`
	Name      string            `json:"name"`
	Arguments map[string]string `json:"arguments,omitempty"`
}

func marshal(e domain.Event) ([]byte, error) {
	w := wireEvent{
		ID:   e.ID,
		Type: int(e.Type),
		Actor: wireActor{
			ID:       e.Actor.ID,
			Metadata: e.Actor.Metadata,
		},
		State:     toWireState(e.State),
		Timestamp: e.Timestamp,
	}
	return json.Marshal(w)
}

func unmarshal(data []byte) (domain.Event, error) {
	var w wireEvent
	if err := json.Unmarshal(data, &w); err != nil {
		return domain.Event{}, fmt.Errorf("jetstream: unmarshal event: %w", err)
	}
	return domain.Event{
		ID:        w.ID,
		Type:      domain.EventType(w.Type),
		Actor:     domain.Actor{ID: w.Actor.ID, Metadata: w.Actor.Metadata},
		State:     fromWireState(w.State),
		Timestamp: w.Timestamp,
	}, nil
}

func toWireState(s domain.State) wireState {
	ws := wireState{
		Presence: wirePresence{
			Type:  int(s.Presence.Type),
			Since: s.Presence.Since,
		},
	}
	if s.Location != nil {
		wl := &wireLocation{Zones: s.Location.Zones}
		if s.Location.Point != nil {
			wl.Point = &wirePoint{X: s.Location.Point.X, Y: s.Location.Point.Y, Z: s.Location.Point.Z}
		}
		ws.Location = wl
	}
	if s.Command != nil {
		ws.Command = &wireCommand{
			ID:        s.Command.ID,
			Name:      s.Command.Name,
			Arguments: s.Command.Arguments,
		}
	}
	return ws
}

func fromWireState(ws wireState) domain.State {
	s := domain.State{
		Presence: domain.Presence{
			Type:  domain.PresenceType(ws.Presence.Type),
			Since: ws.Presence.Since,
		},
	}
	if ws.Location != nil {
		dl := &domain.Location{Zones: ws.Location.Zones}
		if ws.Location.Point != nil {
			dl.Point = &domain.Point{X: ws.Location.Point.X, Y: ws.Location.Point.Y, Z: ws.Location.Point.Z}
		}
		s.Location = dl
	}
	if ws.Command != nil {
		s.Command = &domain.Command{
			ID:        ws.Command.ID,
			Name:      ws.Command.Name,
			Arguments: ws.Command.Arguments,
		}
	}
	return s
}
