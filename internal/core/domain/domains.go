// Package domain holds the internal types the Zonyx core service operates on.
// They mirror the proto contract in github.com/ZonyxNagi/proto-zonyx-core but
// are decoupled from the generated stubs — the gRPC adapter translates between
// these types and the wire format, so the service layer can be tested without
// importing pb.
package domain

import "time"

// EventType classifies what kind of state change an Event records. Mirrors
// zonyx.core.v1.EventType. The zero value is invalid by contract — events
// must be constructed with one of EventTypeZone, EventTypePresence, or
// EventTypeCommand.
type EventType int

const (
	EventTypeUnspecified EventType = iota
	EventTypeZone
	EventTypePresence
	EventTypeCommand
)

func (t EventType) String() string {
	switch t {
	case EventTypeZone:
		return "ZONE"
	case EventTypePresence:
		return "PRESENCE"
	case EventTypeCommand:
		return "COMMAND"
	default:
		return "UNSPECIFIED"
	}
}

// PresenceType describes whether an actor is active or inactive at a point in
// time. Mirrors zonyx.core.v1.PresenceType. The zero value is invalid.
type PresenceType int

const (
	PresenceTypeUnspecified PresenceType = iota
	PresenceTypeActive
	PresenceTypeInactive
)

func (t PresenceType) String() string {
	switch t {
	case PresenceTypeActive:
		return "ACTIVE"
	case PresenceTypeInactive:
		return "INACTIVE"
	default:
		return "UNSPECIFIED"
	}
}

// Actor is the entity being tracked inside the environment. ID is assigned
// externally (e.g. by a roster service) at device-pairing time; Metadata is
// operator-defined and may be nil or empty.
type Actor struct {
	ID       string
	Metadata map[string]string
}

// Zone is a named region within the tracked environment. State.Location.Zones
// carries Zone IDs, not full Zone values. Polygon carries the geometric boundary
// used by providers for zone-membership determination.
type Zone struct {
	ID      string
	Name    string
	Polygon Polygon
}

// Polygon is a closed geometric boundary defined by an ordered list of 3D
// vertices. The last vertex implicitly connects back to the first.
type Polygon struct {
	Vertices []Point
}

// Configuration describes the complete zone layout for a provider deployment.
// It is delivered to clients via CoreService.ReadConfiguration.
type Configuration struct {
	Zones []Zone
}

// Point is a 3-D coordinate in deployment-defined units. The exact origin,
// scale, and axis orientation are configured externally per deployment.
type Point struct {
	X, Y, Z float64
}

// Location bundles a coordinate snapshot with the zone membership inferred
// from that position. Point is nil when no position fix is available (e.g.
// a PRESENCE event where the actor is offline).
type Location struct {
	Point *Point
	Zones []string
}

// Presence captures the lifecycle status of an actor at a point in time.
// Since is the wall-clock instant the actor transitioned into Type.
type Presence struct {
	Type  PresenceType
	Since time.Time
}

// Command captures a discrete action originated by an actor's tracking
// device (e.g. a button push on a BLE tag). Embedded in State.Command for
// EventTypeCommand events; nil for all other event types.
//
// Command is distinct from Directive: a Command is an actor-originated
// event payload reported on the Subscribe stream, whereas a Directive is
// a control-plane instruction submitted via Publish.
type Command struct {
	ID        string
	Name      string
	Arguments map[string]string
}

// Directive is a control-plane instruction sent INTO the Zonyx platform via
// CoreService.Publish. Name identifies the operation
// (e.g. "register_device", "update_zone"); Arguments carries the
// operation-specific parameters.
type Directive struct {
	ID        string
	Name      string
	Arguments map[string]string
}

// State is a snapshot of an actor's situation at the moment an Event is
// produced. Location carries the coordinate point and the complete set of
// Zone.ID values the actor occupies at that moment; nil is valid and means
// no position data is available. Command is set only when the embedding
// Event has Type EventTypeCommand; nil otherwise (the proto enforces this
// invariant via a message-level CEL rule).
type State struct {
	Location *Location
	Presence Presence
	Command  *Command
}

// Event is an immutable record of a state change for a specific actor.
// Timestamp is the event time (when the change happened), not the processing
// time. Consumers should be idempotent on ID.
type Event struct {
	ID        string
	Type      EventType
	Actor     Actor
	State     State
	Timestamp time.Time
}
