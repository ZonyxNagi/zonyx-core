package adapters

import (
	"io"

	v1 "github.com/ZonyxNagi/proto-zonyx-core/pkg/api/v1"
	"github.com/ZonyxNagi/zonyx-core/internal/core/domain"
	"github.com/ZonyxNagi/zonyx-core/internal/core/ports"
	"google.golang.org/genproto/googleapis/rpc/code"
	rpcstatus "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
)

// CoreAdapter implements pb.CoreServiceServer — the single gRPC surface
// exposed by the proto contract. It forwards client calls (Subscribe,
// Publish) into the core Service through ports.Service.
type CoreAdapter struct {
	v1.UnimplementedCoreServiceServer
	service ports.Service
}

// NewCoreAdapter wires the gRPC adapter to the core Service.
func NewCoreAdapter(service ports.Service) (*CoreAdapter, error) {
	return &CoreAdapter{service: service}, nil
}

// Subscribe is the server-streaming RPC for event consumers. It translates
// the pb.SubscribeRequest into domain types, calls Service.Subscribe with a
// send closure that marshals each delivered domain.Event into a
// pb.SubscribeResponse, and closes on ctx.Done().
func (a *CoreAdapter) Subscribe(req *v1.SubscribeRequest, str grpc.ServerStreamingServer[v1.SubscribeResponse]) error {
	ctx := str.Context()

	types := make([]domain.EventType, 0, len(req.GetTypes()))
	for _, t := range req.GetTypes() {
		types = append(types, pbEventTypeToDomain(t))
	}

	return a.service.Subscribe(ctx, types, req.Offset, func(e domain.Event, off string) error {
		return str.Send(&v1.SubscribeResponse{
			Events: []*v1.Event{domainEventToPb(e)},
			Offset: off,
		})
	})
}

// Publish is the bidi-streaming control-plane RPC. For each PublishRequest it
// calls Service.Publish and echoes the returned status back to the producer.
// Per-message failures (non-OK status or service error) are echoed as a
// PublishResponse WITHOUT closing the stream; only transport-level errors return.
func (a *CoreAdapter) Publish(str grpc.BidiStreamingServer[v1.PublishRequest, v1.PublishResponse]) error {
	ctx := str.Context()
	for {
		msg, err := str.Recv()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return status.Error(codes.Internal, err.Error())
		}

		directive := pbDirectiveToDomain(msg.GetDirective())
		id, st, err := a.service.Publish(ctx, directive)
		if err != nil {
			st = &rpcstatus.Status{
				Code:    int32(code.Code_INTERNAL),
				Message: err.Error(),
			}
		}

		if err := str.Send(&v1.PublishResponse{DirectiveId: id, Status: st}); err != nil {
			return status.Error(codes.Internal, err.Error())
		}
	}
}

// ReadConfiguration is the server-streaming RPC that delivers the current zone
// Configuration and any subsequent updates. It forwards each snapshot from
// Service.ReadConfiguration as a ReadConfigurationResponse.
func (a *CoreAdapter) ReadConfiguration(_ *v1.ReadConfigurationRequest, str grpc.ServerStreamingServer[v1.ReadConfigurationResponse]) error {
	return a.service.ReadConfiguration(str.Context(), func(cfg domain.Configuration) error {
		return str.Send(&v1.ReadConfigurationResponse{
			Configuration: domainConfigurationToPb(cfg),
		})
	})
}

// --- pb → domain ---

func pbEventTypeToDomain(t v1.EventType) domain.EventType {
	switch t {
	case v1.EventType_EVENT_TYPE_ZONE:
		return domain.EventTypeZone
	case v1.EventType_EVENT_TYPE_PRESENCE:
		return domain.EventTypePresence
	case v1.EventType_EVENT_TYPE_COMMAND:
		return domain.EventTypeCommand
	default:
		return domain.EventTypeUnspecified
	}
}

func pbDirectiveToDomain(d *v1.Directive) domain.Directive {
	if d == nil {
		return domain.Directive{}
	}
	return domain.Directive{
		ID:        d.GetId(),
		Name:      d.GetName(),
		Arguments: d.GetArguments(),
	}
}

// --- domain → pb ---

func domainEventToPb(e domain.Event) *v1.Event {
	return &v1.Event{
		Id:        e.ID,
		Type:      domainEventTypeToPb(e.Type),
		Actor:     domainActorToPb(e.Actor),
		State:     domainStateToPb(e.State),
		Timestamp: timestamppb.New(e.Timestamp),
	}
}

func domainEventTypeToPb(t domain.EventType) v1.EventType {
	switch t {
	case domain.EventTypeZone:
		return v1.EventType_EVENT_TYPE_ZONE
	case domain.EventTypePresence:
		return v1.EventType_EVENT_TYPE_PRESENCE
	case domain.EventTypeCommand:
		return v1.EventType_EVENT_TYPE_COMMAND
	default:
		return v1.EventType_EVENT_TYPE_UNSPECIFIED
	}
}

func domainActorToPb(a domain.Actor) *v1.Actor {
	return &v1.Actor{
		Id:       a.ID,
		Metadata: a.Metadata,
	}
}

func domainStateToPb(s domain.State) *v1.State {
	return &v1.State{
		Location: domainLocationToPb(s.Location),
		Presence: domainPresenceToPb(s.Presence),
		Command:  domainCommandToPb(s.Command),
	}
}

func domainLocationToPb(l *domain.Location) *v1.Location {
	if l == nil {
		return nil
	}
	return &v1.Location{
		Point: domainPointToPb(l.Point),
		Zones: l.Zones,
	}
}

func domainPointToPb(p *domain.Point) *v1.Location_Point {
	if p == nil {
		return nil
	}
	return &v1.Location_Point{
		X: float32(p.X),
		Y: float32(p.Y),
		Z: float32(p.Z),
	}
}

func domainPresenceToPb(p domain.Presence) *v1.Presence {
	return &v1.Presence{
		Type:  domainPresenceTypeToPb(p.Type),
		Since: timestamppb.New(p.Since),
	}
}

func domainPresenceTypeToPb(t domain.PresenceType) v1.PresenceType {
	switch t {
	case domain.PresenceTypeActive:
		return v1.PresenceType_PRESENCE_TYPE_ACTIVE
	case domain.PresenceTypeInactive:
		return v1.PresenceType_PRESENCE_TYPE_INACTIVE
	default:
		return v1.PresenceType_PRESENCE_TYPE_UNSPECIFIED
	}
}

func domainCommandToPb(c *domain.Command) *v1.Command {
	if c == nil {
		return nil
	}
	return &v1.Command{
		Id:        c.ID,
		Name:      c.Name,
		Arguments: c.Arguments,
	}
}

func domainConfigurationToPb(cfg domain.Configuration) *v1.Configuration {
	zones := make([]*v1.Zone, 0, len(cfg.Zones))
	for _, z := range cfg.Zones {
		zones = append(zones, domainZoneToPb(z))
	}
	return &v1.Configuration{Zones: zones}
}

func domainZoneToPb(z domain.Zone) *v1.Zone {
	return &v1.Zone{
		Id:      z.ID,
		Name:    z.Name,
		Polygon: domainPolygonToPb(z.Polygon),
	}
}

func domainPolygonToPb(p domain.Polygon) *v1.Zone_Polygon {
	verts := make([]*v1.Location_Point, 0, len(p.Vertices))
	for _, v := range p.Vertices {
		verts = append(verts, &v1.Location_Point{
			X: float32(v.X),
			Y: float32(v.Y),
			Z: float32(v.Z),
		})
	}
	return &v1.Zone_Polygon{Vertices: verts}
}
