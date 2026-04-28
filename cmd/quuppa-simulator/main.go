package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"time"

	pb "github.com/ZonyxNagi/proto-zonyx-core/pkg/api/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// datagram mirrors the QPE 9.5+ DefaultLocationAndInfo JSON shape.
// LocationZoneIds is *[]string so nil marshals to JSON null (LOCATION_LOST signal).
type datagram struct {
	TagID                  string    `json:"tagId"`
	ResponseTS             int64     `json:"responseTS"`
	LastPacketTS           int64     `json:"lastPacketTS"`
	LocationType           string    `json:"locationType"`
	LocationZoneIds        *[]string `json:"locationZoneIds"`
	Location               []float64 `json:"location,omitempty"`
	LocationMovementStatus string    `json:"locationMovementStatus,omitempty"`
}

func tagID(i int) string { return fmt.Sprintf("aabbccdd%04d", i) }
func nowMS() int64       { return time.Now().UnixMilli() }

func positionPkt(id string, zones []string) datagram {
	z := append([]string(nil), zones...)
	return datagram{
		TagID:                  id,
		ResponseTS:             nowMS(),
		LastPacketTS:           nowMS(),
		LocationType:           "position",
		LocationZoneIds:        &z,
		Location:               []float64{1.5, 2.3, 0.0},
		LocationMovementStatus: "moving",
	}
}

func noLocationPkt(id string) datagram {
	return datagram{
		TagID:        id,
		ResponseTS:   nowMS(),
		LastPacketTS: nowMS(),
		LocationType: "noLocation",
		// LocationZoneIds nil → JSON null → LOCATION_LOST trigger in derive.go
	}
}

func emit(conn net.Conn, d datagram, log *slog.Logger, verbose bool) {
	b, _ := json.Marshal(d)
	if _, err := conn.Write(b); err != nil {
		log.Warn("udp write", "err", err)
		return
	}
	if verbose {
		log.Debug("→ sent", "tagId", d.TagID, "locationType", d.LocationType, "zones", d.LocationZoneIds)
	}
}

// --- Assertions ---------------------------------------------------------

// check is a named assertion over a received proto Event.
type check struct {
	desc string
	fn   func(*pb.Event) bool
}

func zoneCheck(id string, wantZones []string) check {
	return check{
		desc: fmt.Sprintf("Zone actor=%s zones=%v", id, wantZones),
		fn: func(e *pb.Event) bool {
			if e.GetType() != pb.EventType_EVENT_TYPE_ZONE || e.GetActor().GetId() != id {
				return false
			}
			if wantZones == nil {
				return true // accept any zone set
			}
			got := e.GetState().GetLocation().GetZones()
			if len(got) != len(wantZones) {
				return false
			}
			m := make(map[string]bool, len(wantZones))
			for _, z := range wantZones {
				m[z] = true
			}
			for _, z := range got {
				if !m[z] {
					return false
				}
			}
			return true
		},
	}
}

func presenceCheck(id string, pt pb.PresenceType) check {
	name := map[pb.PresenceType]string{
		pb.PresenceType_PRESENCE_TYPE_ACTIVE:   "Active",
		pb.PresenceType_PRESENCE_TYPE_INACTIVE: "Inactive",
	}[pt]
	return check{
		desc: fmt.Sprintf("Presence{%s} actor=%s", name, id),
		fn: func(e *pb.Event) bool {
			return e.GetType() == pb.EventType_EVENT_TYPE_PRESENCE &&
				e.GetActor().GetId() == id &&
				e.GetState().GetPresence().GetType() == pt
		},
	}
}

// runChecks consumes events from the channel until every check in order is
// satisfied or the shared timer fires. The timer budget is shared across all
// checks so that the total --timeout covers the entire scenario.
func runChecks(ctx context.Context, events <-chan *pb.Event, checks []check, timeout time.Duration, log *slog.Logger) bool {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for i, c := range checks {
		log.Info("waiting", "check", fmt.Sprintf("[%d/%d] %s", i+1, len(checks), c.desc))
		if !waitForCheck(ctx, events, c, timer, log) {
			return false
		}
		log.Info("PASS", "check", c.desc)
	}
	return true
}

func waitForCheck(ctx context.Context, events <-chan *pb.Event, c check, timer *time.Timer, log *slog.Logger) bool {
	for {
		select {
		case ev, ok := <-events:
			if !ok {
				log.Error("stream closed", "pending", c.desc)
				return false
			}
			if c.fn(ev) {
				return true
			}
		case <-timer.C:
			log.Error("FAIL timeout", "check", c.desc)
			return false
		case <-ctx.Done():
			return false
		}
	}
}

// --- gRPC subscriber ----------------------------------------------------

func subscribe(ctx context.Context, grpcAddr string) (<-chan *pb.Event, error) {
	cc, err := grpc.NewClient(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial: %w", err)
	}
	stream, err := pb.NewCoreServiceClient(cc).Subscribe(ctx, &pb.SubscribeRequest{})
	if err != nil {
		cc.Close()
		return nil, fmt.Errorf("subscribe: %w", err)
	}
	ch := make(chan *pb.Event, 512)
	go func() {
		defer cc.Close()
		defer close(ch)
		for {
			resp, err := stream.Recv()
			if err != nil {
				return
			}
			for _, ev := range resp.GetEvents() {
				select {
				case ch <- ev:
				default:
					// buffer full — event dropped
				}
			}
		}
	}()
	return ch, nil
}

// --- Scenarios ----------------------------------------------------------

// walk: each tag cycles through zones in order, one per tick.
func runWalk(ctx context.Context, conn net.Conn, tags int, zones []string, interval time.Duration, log *slog.Logger, verbose bool) {
	var wg sync.WaitGroup
	for i := 0; i < tags; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := tagID(i)
			tick := time.NewTicker(interval)
			defer tick.Stop()
			idx := 0
			for {
				select {
				case <-ctx.Done():
					return
				case <-tick.C:
					emit(conn, positionPkt(id, []string{zones[idx%len(zones)]}), log, verbose)
					idx++
				}
			}
		}(i)
	}
	wg.Wait()
}

// validateWalk sends one packet per tag per zone (sequentially per tag) and
// asserts a Zone event arrives for each. Tags are validated one at a time so
// events from different tags don't confuse assertions.
func validateWalk(ctx context.Context, conn net.Conn, events <-chan *pb.Event, tags int, zones []string, interval time.Duration, timeout time.Duration, log *slog.Logger, verbose bool) bool {
	for i := 0; i < tags; i++ {
		id := tagID(i)
		var tagChecks []check
		for _, z := range zones {
			tagChecks = append(tagChecks, zoneCheck(id, []string{z}))
		}
		for _, z := range zones {
			emit(conn, positionPkt(id, []string{z}), log, verbose)
			time.Sleep(interval)
		}
		if !runChecks(ctx, events, tagChecks, timeout, log) {
			return false
		}
	}
	return true
}

// offline: sends a burst of 3 packets, goes silent for 15s (> 12s watchdog),
// then resumes. Demonstrates TAG_OFFLINE → TAG_ONLINE.
func runOffline(ctx context.Context, conn net.Conn, tags int, zones []string, interval time.Duration, log *slog.Logger, verbose bool) {
	const silenceDuration = 15 * time.Second
	var wg sync.WaitGroup
	for i := 0; i < tags; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := tagID(i)
			z := zones[0]
			for {
				for j := 0; j < 3; j++ {
					select {
					case <-ctx.Done():
						return
					default:
						emit(conn, positionPkt(id, []string{z}), log, verbose)
						time.Sleep(interval)
					}
				}
				log.Info("tag going silent", "tagId", id, "for", silenceDuration)
				select {
				case <-ctx.Done():
					return
				case <-time.After(silenceDuration):
				}
				log.Info("tag resuming", "tagId", id)
			}
		}(i)
	}
	wg.Wait()
}

// validateOffline exercises the TAG_OFFLINE → TAG_ONLINE flow for a single tag.
// Use --timeout ≥40s because 15s of silence is needed to trigger the watchdog.
func validateOffline(ctx context.Context, conn net.Conn, events <-chan *pb.Event, timeout time.Duration, log *slog.Logger, verbose bool) bool {
	id := tagID(0)
	zone := "zone-a"
	checks := []check{
		zoneCheck(id, nil),
		presenceCheck(id, pb.PresenceType_PRESENCE_TYPE_INACTIVE),
		presenceCheck(id, pb.PresenceType_PRESENCE_TYPE_ACTIVE),
	}
	go func() {
		for j := 0; j < 3; j++ {
			select {
			case <-ctx.Done():
				return
			default:
				emit(conn, positionPkt(id, []string{zone}), log, verbose)
				time.Sleep(time.Second)
			}
		}
		log.Info("tag silent for 15s (watchdog threshold = 12s)", "tagId", id)
		select {
		case <-ctx.Done():
			return
		case <-time.After(15 * time.Second):
		}
		log.Info("tag resuming", "tagId", id)
		emit(conn, positionPkt(id, []string{zone}), log, verbose)
	}()
	return runChecks(ctx, events, checks, timeout, log)
}

// lost: alternates position ↔ noLocation on every tick.
// Demonstrates LOCATION_LOST and LOCATION_RESTORED transitions.
func runLost(ctx context.Context, conn net.Conn, tags int, zones []string, interval time.Duration, log *slog.Logger, verbose bool) {
	var wg sync.WaitGroup
	for i := 0; i < tags; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := tagID(i)
			z := zones[0]
			hasLoc := true
			tick := time.NewTicker(interval)
			defer tick.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-tick.C:
					if hasLoc {
						emit(conn, positionPkt(id, []string{z}), log, verbose)
					} else {
						emit(conn, noLocationPkt(id), log, verbose)
					}
					hasLoc = !hasLoc
				}
			}
		}(i)
	}
	wg.Wait()
}

// validateLost sends: position → noLocation → position for a single tag, then
// asserts the expected LOCATION_LOST / LOCATION_RESTORED event sequence.
func validateLost(ctx context.Context, conn net.Conn, events <-chan *pb.Event, interval time.Duration, timeout time.Duration, log *slog.Logger, verbose bool) bool {
	id := tagID(0)
	zone := "zone-a"
	checks := []check{
		zoneCheck(id, []string{zone}),
		presenceCheck(id, pb.PresenceType_PRESENCE_TYPE_INACTIVE),
		presenceCheck(id, pb.PresenceType_PRESENCE_TYPE_ACTIVE),
		zoneCheck(id, nil),
	}
	go func() {
		steps := []func() datagram{
			func() datagram { return positionPkt(id, []string{zone}) },
			func() datagram { return noLocationPkt(id) },
			func() datagram { return positionPkt(id, []string{zone}) },
		}
		for _, mkPkt := range steps {
			select {
			case <-ctx.Done():
				return
			default:
				emit(conn, mkPkt(), log, verbose)
				time.Sleep(interval)
			}
		}
	}()
	return runChecks(ctx, events, checks, timeout, log)
}

// stress: many tags firing at high frequency. No assertions.
func runStress(ctx context.Context, conn net.Conn, tags int, zones []string, interval time.Duration, log *slog.Logger, verbose bool) {
	log.Info("stress mode", "tags", tags, "interval", interval)
	var wg sync.WaitGroup
	for i := 0; i < tags; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := tagID(i)
			z := zones[0]
			tick := time.NewTicker(interval)
			defer tick.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-tick.C:
					emit(conn, positionPkt(id, []string{z}), log, verbose)
				}
			}
		}(i)
	}
	wg.Wait()
}

// --- Main ---------------------------------------------------------------

func main() {
	udpAddr  := flag.String("udp-addr", ":9090", "QUUPPA adapter UDP address")
	grpcAddr := flag.String("grpc-addr", ":8080", "CoreService gRPC address")
	scenario := flag.String("scenario", "walk", "Scenario: walk | offline | lost | stress")
	tags     := flag.Int("tags", 1, "Number of concurrent tags (≥1)")
	interval := flag.Duration("interval", 500*time.Millisecond, "Delay between packets per tag")
	timeout  := flag.Duration("timeout", 30*time.Second, "Assertion timeout (validate mode); use ≥40s for offline")
	zonesCSV := flag.String("zones", "zone-a,zone-b,zone-c", "Comma-separated zone IDs")
	validate := flag.Bool("validate", false, "Assert expected events via gRPC Subscribe; exit 1 on failure")
	verbose  := flag.Bool("verbose", false, "Log every packet sent and event received")
	flag.Parse()

	if *tags < 1 {
		fmt.Fprintln(os.Stderr, "error: --tags must be ≥1")
		os.Exit(2)
	}

	level := slog.LevelInfo
	if *verbose {
		level = slog.LevelDebug
	}
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level}))

	zones := strings.Split(*zonesCSV, ",")
	for i, z := range zones {
		zones[i] = strings.TrimSpace(z)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	conn, err := net.Dial("udp", *udpAddr)
	if err != nil {
		log.Error("udp dial", "addr", *udpAddr, "err", err)
		os.Exit(2)
	}
	defer conn.Close()

	log.Info("quuppa-simulator", "scenario", *scenario, "udp", *udpAddr, "tags", *tags, "validate", *validate)

	var events <-chan *pb.Event
	if *validate && *scenario != "stress" {
		events, err = subscribe(ctx, *grpcAddr)
		if err != nil {
			log.Error("grpc subscribe", "addr", *grpcAddr, "err", err)
			os.Exit(2)
		}
		log.Info("subscribed to CoreService", "addr", *grpcAddr)
		// Grace period for the stream to be registered server-side.
		time.Sleep(200 * time.Millisecond)
	}

	var passed bool
	switch *scenario {
	case "walk":
		if *validate {
			passed = validateWalk(ctx, conn, events, *tags, zones, *interval, *timeout, log, *verbose)
		} else {
			runWalk(ctx, conn, *tags, zones, *interval, log, *verbose)
		}
	case "offline":
		if *validate {
			passed = validateOffline(ctx, conn, events, *timeout, log, *verbose)
		} else {
			runOffline(ctx, conn, *tags, zones, *interval, log, *verbose)
		}
	case "lost":
		if *validate {
			passed = validateLost(ctx, conn, events, *interval, *timeout, log, *verbose)
		} else {
			runLost(ctx, conn, *tags, zones, *interval, log, *verbose)
		}
	case "stress":
		runStress(ctx, conn, *tags, zones, *interval, log, *verbose)
	default:
		log.Error("unknown scenario", "scenario", *scenario, "valid", "walk, offline, lost, stress")
		os.Exit(2)
	}

	if *validate && *scenario != "stress" {
		if passed {
			log.Info("all checks PASSED")
		} else {
			log.Error("checks FAILED")
			os.Exit(1)
		}
	}
}
