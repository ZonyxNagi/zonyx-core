package quuppa

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/ZonyxNagi/zonyx-core/internal/core/domain"
)

// zoneIDs is a small helper to take the address of a literal []string so we
// can populate udpMsg.LocationZoneIds (which is *[]string to differentiate
// nil from []).
func zoneIDs(z ...string) *[]string {
	zz := append([]string(nil), z...)
	return &zz
}

// makeMsg builds a minimal valid udpMsg for tests. Pass nil to zoneIDs for
// the "null" case (no fix); pass &[]string{} for "empty array" (fix, no zones).
func makeMsg(tagID, locType string, zones *[]string, responseMs, lastPacketMs int64) *udpMsg {
	return &udpMsg{
		TagID:           tagID,
		ResponseTS:      responseMs,
		LastPacketTS:    lastPacketMs,
		LocationType:    locType,
		LocationZoneIds: zones,
	}
}

func eventTypes(events []domain.Event) []string {
	out := make([]string, len(events))
	for i, e := range events {
		switch e.Type {
		case domain.EventTypeZone:
			out[i] = "Zone"
		case domain.EventTypePresence:
			out[i] = "Presence:" + e.State.Presence.Type.String()
		case domain.EventTypeCommand:
			out[i] = "Command"
		default:
			out[i] = "Unspecified"
		}
	}
	return out
}

func TestDeriveEvents_FirstPacket(t *testing.T) {
	t.Parallel()

	t.Run("first packet with zones emits a baseline Zone event but no Presence{Active}", func(t *testing.T) {
		t.Parallel()
		st := &tagState{}
		msg := makeMsg("a4da22e4e75d", "position", zoneIDs("pool-main"), 1000, 900)
		now := time.UnixMilli(1000)

		evs := deriveEvents(st, msg, now)
		if got := eventTypes(evs); !reflect.DeepEqual(got, []string{"Zone"}) {
			t.Fatalf("events = %v, want [Zone]", got)
		}
		if !reflect.DeepEqual(evs[0].State.Location.Zones, []string{"pool-main"}) {
			t.Errorf("Zones = %v", evs[0].State.Location.Zones)
		}
		if !st.online {
			t.Error("online flag not set")
		}
	})

	t.Run("first packet with no fix and no zones emits nothing", func(t *testing.T) {
		t.Parallel()
		st := &tagState{}
		msg := makeMsg("a4da22e4e75d", "noData", nil, 1000, 900)
		evs := deriveEvents(st, msg, time.UnixMilli(1000))
		if len(evs) != 0 {
			t.Fatalf("events = %v, want []", eventTypes(evs))
		}
	})

	t.Run("first packet with fix but zero zones emits Zone", func(t *testing.T) {
		t.Parallel()
		st := &tagState{}
		empty := []string{}
		msg := makeMsg("a4da22e4e75d", "position", &empty, 1000, 900)
		evs := deriveEvents(st, msg, time.UnixMilli(1000))
		// No Zone event on the first packet when the tag is in no zones:
		// prevZones (nil) equals currZones ([]) under the §6 diff — no zone
		// was entered or exited so there is nothing to emit.
		if len(evs) != 0 {
			t.Fatalf("events = %v, want []", eventTypes(evs))
		}
	})
}

func TestDeriveEvents_ZoneChange(t *testing.T) {
	t.Parallel()
	st := &tagState{
		online:           true,
		lastLocationType: "position",
		lastZoneIds:      []string{"pool-main"},
		lastPacketTS:     time.UnixMilli(900),
	}
	msg := makeMsg("a4da22e4e75d", "position", zoneIDs("pool-main", "wall-east"), 2000, 1900)

	evs := deriveEvents(st, msg, time.UnixMilli(2000))
	if got := eventTypes(evs); !reflect.DeepEqual(got, []string{"Zone"}) {
		t.Fatalf("events = %v", got)
	}
	if !zonesEqual(evs[0].State.Location.Zones, []string{"pool-main", "wall-east"}) {
		t.Errorf("Zones = %v", evs[0].State.Location.Zones)
	}
}

func TestDeriveEvents_SameZones_NoZoneEvent(t *testing.T) {
	t.Parallel()
	st := &tagState{
		online:           true,
		lastLocationType: "position",
		lastZoneIds:      []string{"pool-main"},
		lastPacketTS:     time.UnixMilli(900),
	}
	msg := makeMsg("a4da22e4e75d", "position", zoneIDs("pool-main"), 2000, 1900)

	evs := deriveEvents(st, msg, time.UnixMilli(2000))
	// No Zone event when zone membership is unchanged — §6 diff algorithm.
	if len(evs) != 0 {
		t.Fatalf("events = %v, want []", eventTypes(evs))
	}
}

func TestDeriveEvents_LocationLost(t *testing.T) {
	t.Parallel()

	t.Run("position → noLocation with prior zones: [Zone(empty), Presence(inactive)]", func(t *testing.T) {
		t.Parallel()
		st := &tagState{
			online:           true,
			lastLocationType: "position",
			lastZoneIds:      []string{"pool-main"},
			lastPacketTS:     time.UnixMilli(900),
		}
		msg := makeMsg("a4da22e4e75d", "noLocation", nil, 2000, 1900)

		evs := deriveEvents(st, msg, time.UnixMilli(2000))
		if got := eventTypes(evs); !reflect.DeepEqual(got, []string{"Zone", "Presence:INACTIVE"}) {
			t.Fatalf("events = %v", got)
		}
		if len(evs[0].State.Location.Zones) != 0 {
			t.Errorf("Zone event should carry empty zones, got %v", evs[0].State.Location.Zones)
		}
		// Presence{Inactive} carries last-known zones for context.
		if !reflect.DeepEqual(evs[1].State.Location.Zones, []string{"pool-main"}) {
			t.Errorf("Presence zones = %v, want [pool-main]", evs[1].State.Location.Zones)
		}
		if st.locationLostAt.IsZero() {
			t.Error("locationLostAt not set")
		}
	})

	t.Run("position → noLocation with no prior zones: [Presence(inactive)] only", func(t *testing.T) {
		t.Parallel()
		st := &tagState{
			online:           true,
			lastLocationType: "position",
			lastZoneIds:      []string{},
			lastPacketTS:     time.UnixMilli(900),
		}
		msg := makeMsg("a4da22e4e75d", "noData", nil, 2000, 1900)

		evs := deriveEvents(st, msg, time.UnixMilli(2000))
		if got := eventTypes(evs); !reflect.DeepEqual(got, []string{"Presence:INACTIVE"}) {
			t.Fatalf("events = %v", got)
		}
	})

	t.Run("position → approximate with prior zones: [Zone(empty), Presence(inactive)]", func(t *testing.T) {
		t.Parallel()
		st := &tagState{
			online:           true,
			lastLocationType: "position",
			lastZoneIds:      []string{"pool-main"},
			lastPacketTS:     time.UnixMilli(900),
		}
		msg := makeMsg("a4da22e4e75d", "approximate", zoneIDs("pool-main"), 2000, 1900)

		evs := deriveEvents(st, msg, time.UnixMilli(2000))
		if got := eventTypes(evs); !reflect.DeepEqual(got, []string{"Zone", "Presence:INACTIVE"}) {
			t.Fatalf("events = %v, want [Zone, Presence:INACTIVE] when approximate is treated as location loss", got)
		}
		if len(evs[0].State.Location.Zones) != 0 {
			t.Errorf("Zone event should carry empty zones, got %v", evs[0].State.Location.Zones)
		}
		if !reflect.DeepEqual(evs[1].State.Location.Zones, []string{"pool-main"}) {
			t.Errorf("Presence zones = %v, want [pool-main]", evs[1].State.Location.Zones)
		}
	})

	t.Run("position → degraded location types emit inactive presence", func(t *testing.T) {
		t.Parallel()

		cases := []struct {
			name         string
			locationType string
		}{
			{name: "approximate", locationType: "approximate"},
			{name: "proximity", locationType: "proximity"},
			{name: "presence", locationType: "presence"},
			{name: "noLocation", locationType: "noLocation"},
			{name: "noData", locationType: "noData"},
		}

		for _, tc := range cases {
			tc := tc
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				st := &tagState{
					online:           true,
					lastLocationType: "position",
					lastZoneIds:      []string{"pool-main"},
					lastPacketTS:     time.UnixMilli(900),
				}
				msg := makeMsg("a4da22e4e75d", tc.locationType, zoneIDs("pool-main"), 2000, 1900)

				evs := deriveEvents(st, msg, time.UnixMilli(2000))
				if got := eventTypes(evs); !reflect.DeepEqual(got, []string{"Zone", "Presence:INACTIVE"}) {
					t.Fatalf("events = %v, want [Zone, Presence:INACTIVE] when locationType=%q", got, tc.locationType)
				}
				if len(evs[0].State.Location.Zones) != 0 {
					t.Errorf("Zone event should carry empty zones, got %v", evs[0].State.Location.Zones)
				}
				if !reflect.DeepEqual(evs[1].State.Location.Zones, []string{"pool-main"}) {
					t.Errorf("Presence zones = %v, want [pool-main]", evs[1].State.Location.Zones)
				}
			})
		}
	})

	t.Run("dedupe: second non-location packet emits nothing", func(t *testing.T) {
		t.Parallel()
		st := &tagState{
			online:           true,
			lastLocationType: "noLocation",
			lastZoneIds:      []string{},
			lastPacketTS:     time.UnixMilli(900),
			locationLostAt:   time.UnixMilli(900),
		}
		msg := makeMsg("a4da22e4e75d", "noData", nil, 2000, 1900)

		evs := deriveEvents(st, msg, time.UnixMilli(2000))
		if len(evs) != 0 {
			t.Fatalf("expected no events on stable non-location, got %v", eventTypes(evs))
		}
	})
}

func TestDeriveEvents_LocationRestored(t *testing.T) {
	t.Parallel()

	t.Run("noLocation → position with new zones: [Presence(active), Zone]", func(t *testing.T) {
		t.Parallel()
		st := &tagState{
			online:           true,
			lastLocationType: "noLocation",
			lastZoneIds:      []string{},
			lastPacketTS:     time.UnixMilli(900),
			locationLostAt:   time.UnixMilli(900),
		}
		msg := makeMsg("a4da22e4e75d", "position", zoneIDs("pool-main"), 2000, 1900)

		evs := deriveEvents(st, msg, time.UnixMilli(2000))
		if got := eventTypes(evs); !reflect.DeepEqual(got, []string{"Presence:ACTIVE", "Zone"}) {
			t.Fatalf("events = %v", got)
		}
		if !st.locationLostAt.IsZero() {
			t.Error("locationLostAt should be cleared on restore")
		}
	})

	t.Run("noLocation → position with no zones: [Presence(active)] only", func(t *testing.T) {
		t.Parallel()
		st := &tagState{
			online:           true,
			lastLocationType: "noLocation",
			lastZoneIds:      []string{},
			lastPacketTS:     time.UnixMilli(900),
			locationLostAt:   time.UnixMilli(900),
		}
		empty := []string{}
		msg := makeMsg("a4da22e4e75d", "position", &empty, 2000, 1900)

		evs := deriveEvents(st, msg, time.UnixMilli(2000))
		// No zone change ([]→[]) so no Zone event; only Presence(active).
		if got := eventTypes(evs); !reflect.DeepEqual(got, []string{"Presence:ACTIVE"}) {
			t.Fatalf("events = %v, want [Presence:ACTIVE]", got)
		}
	})
}

func TestDeriveEvents_TagOnlineAfterWatchdogOffline(t *testing.T) {
	t.Parallel()

	t.Run("same zone after watchdog offline: only Presence(active)", func(t *testing.T) {
		t.Parallel()
		st := &tagState{
			online:           false, // watchdog flipped this
			lastLocationType: "position",
			lastZoneIds:      []string{"pool-main"},
			lastPacketTS:     time.UnixMilli(900),
		}
		msg := makeMsg("a4da22e4e75d", "position", zoneIDs("pool-main"), 5000, 4900)

		evs := deriveEvents(st, msg, time.UnixMilli(5000))
		// TAG_ONLINE emits Presence:ACTIVE. No zone change (same zones) → no Zone.
		if got := eventTypes(evs); !reflect.DeepEqual(got, []string{"Presence:ACTIVE"}) {
			t.Fatalf("events = %v, want [Presence:ACTIVE]", got)
		}
		if !st.online {
			t.Error("online flag not restored")
		}
	})

	t.Run("zone changed after watchdog offline: Presence(active) then Zone", func(t *testing.T) {
		t.Parallel()
		st := &tagState{
			online:           false,
			lastLocationType: "position",
			lastZoneIds:      []string{"pool-main"},
			lastPacketTS:     time.UnixMilli(900),
		}
		msg := makeMsg("a4da22e4e75d", "position", zoneIDs("lane-3"), 5000, 4900)

		evs := deriveEvents(st, msg, time.UnixMilli(5000))
		if got := eventTypes(evs); !reflect.DeepEqual(got, []string{"Presence:ACTIVE", "Zone"}) {
			t.Fatalf("events = %v, want [Presence:ACTIVE, Zone]", got)
		}
	})
}

func TestDeriveEvents_OrderOnComboTransitions(t *testing.T) {
	t.Parallel()
	// Tag was offline (online=false), comes back AND the locationType is degraded
	// in the same datagram. Spec: emit TAG_ONLINE first, then no LOCATION_RESTORED
	// (since prev was already LOCATION_TYPES; we never recorded a transition into
	// non-location while offline).
	st := &tagState{
		online:           false,
		lastLocationType: "position",
		lastZoneIds:      []string{"pool-main"},
		lastPacketTS:     time.UnixMilli(900),
	}
	msg := makeMsg("a4da22e4e75d", "noLocation", nil, 5000, 4900)

	evs := deriveEvents(st, msg, time.UnixMilli(5000))
	got := eventTypes(evs)
	want := []string{"Presence:ACTIVE", "Zone", "Presence:INACTIVE"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("events = %v, want %v (TAG_ONLINE → ZONE_EXITED → LOCATION_LOST)", got, want)
	}
}

func TestDeriveEvents_MovementStateUpdates(t *testing.T) {
	t.Parallel()

	baseState := func() *tagState {
		return &tagState{
			online:           true,
			lastLocationType: "position",
			lastZoneIds:      []string{"pool-main"},
			lastPacketTS:     time.UnixMilli(900),
		}
	}

	t.Run("stationary emits zone event when snapshot changes", func(t *testing.T) {
		t.Parallel()
		st := baseState()
		msg := makeMsg("a4da22e4e75d", "position", zoneIDs("pool-main"), 2000, 1900)
		msg.LocationMovementStatus = "stationary"

		evs := deriveEvents(st, msg, time.UnixMilli(2000))
		if got := eventTypes(evs); !reflect.DeepEqual(got, []string{"Zone"}) {
			t.Fatalf("events = %v, want [Zone]", got)
		}
		if st.lastMovementStatus != "stationary" {
			t.Errorf("lastMovementStatus = %q, want stationary", st.lastMovementStatus)
		}
		if evs[0].Actor.Metadata["movementStatus"] != "stationary" {
			t.Errorf("movementStatus = %q", evs[0].Actor.Metadata["movementStatus"])
		}
	})

	t.Run("stationary emits zone event when zone ids change", func(t *testing.T) {
		t.Parallel()
		st := baseState()
		msg := makeMsg("a4da22e4e75d", "position", zoneIDs("lane-3"), 2000, 1900)
		msg.LocationMovementStatus = "stationary"

		evs := deriveEvents(st, msg, time.UnixMilli(2000))
		if got := eventTypes(evs); !reflect.DeepEqual(got, []string{"Zone"}) {
			t.Fatalf("events = %v, want [Zone]", got)
		}
		if !reflect.DeepEqual(evs[0].State.Location.Zones, []string{"lane-3"}) {
			t.Fatalf("zones = %v, want [lane-3]", evs[0].State.Location.Zones)
		}
	})

	t.Run("moving emits zone event when zones change", func(t *testing.T) {
		t.Parallel()
		st := baseState()
		msg := makeMsg("a4da22e4e75d", "position", zoneIDs("pool-main", "lane-3"), 2000, 1900)
		msg.LocationMovementStatus = "moving"

		evs := deriveEvents(st, msg, time.UnixMilli(2000))
		if got := eventTypes(evs); !reflect.DeepEqual(got, []string{"Zone"}) {
			t.Fatalf("events = %v, want [Zone]", got)
		}
	})

	t.Run("absent movement status emits zone event when zones change (undocumented state; not suppressed)", func(t *testing.T) {
		t.Parallel()
		st := baseState()
		msg := makeMsg("a4da22e4e75d", "position", zoneIDs("pool-main", "lane-3"), 2000, 1900)
		// LocationMovementStatus left as "" (zero value)

		evs := deriveEvents(st, msg, time.UnixMilli(2000))
		if got := eventTypes(evs); !reflect.DeepEqual(got, []string{"Zone"}) {
			t.Fatalf("events = %v, want [Zone]", got)
		}
	})

	t.Run("noData with valid location emits zone event when zones change (location-availability signal, not motion)", func(t *testing.T) {
		t.Parallel()
		st := baseState()
		msg := makeMsg("a4da22e4e75d", "position", zoneIDs("pool-main", "lane-3"), 2000, 1900)
		msg.LocationMovementStatus = "noData"

		evs := deriveEvents(st, msg, time.UnixMilli(2000))
		if got := eventTypes(evs); !reflect.DeepEqual(got, []string{"Zone"}) {
			t.Fatalf("events = %v, want [Zone]", got)
		}
	})

	t.Run("hidden emits zone event when zones change (QSP zone-masking policy, not motion signal)", func(t *testing.T) {
		t.Parallel()
		st := baseState()
		msg := makeMsg("a4da22e4e75d", "position", zoneIDs("pool-main", "lane-3"), 2000, 1900)
		msg.LocationMovementStatus = "hidden"

		evs := deriveEvents(st, msg, time.UnixMilli(2000))
		if got := eventTypes(evs); !reflect.DeepEqual(got, []string{"Zone"}) {
			t.Fatalf("events = %v, want [Zone]", got)
		}
	})

	t.Run("zone changes emit regardless of movement state", func(t *testing.T) {
		t.Parallel()
		st := baseState()

		// Stationary + zone change still emits a Zone snapshot.
		msg1 := makeMsg("a4da22e4e75d", "position", zoneIDs("pool-main", "lane-3"), 2000, 1900)
		msg1.LocationMovementStatus = "stationary"
		evs1 := deriveEvents(st, msg1, time.UnixMilli(2000))
		if got := eventTypes(evs1); !reflect.DeepEqual(got, []string{"Zone"}) {
			t.Fatalf("first packet (stationary): events = %v, want [Zone]", got)
		}

		// Moving + same zones as the prior packet still emits a Zone snapshot
		// because movement status is part of the state-change trigger.
		msg2 := makeMsg("a4da22e4e75d", "position", zoneIDs("pool-main", "lane-3"), 3000, 2900)
		msg2.LocationMovementStatus = "moving"
		evs2 := deriveEvents(st, msg2, time.UnixMilli(3000))
		if got := eventTypes(evs2); !reflect.DeepEqual(got, []string{"Zone"}) {
			t.Fatalf("second packet (moving, same zone): events = %v, want [Zone]", got)
		}
	})

	t.Run("stationary tag LOCATION_LOST is still emitted unconditionally", func(t *testing.T) {
		t.Parallel()
		st := baseState()
		msg := makeMsg("a4da22e4e75d", "noLocation", nil, 2000, 1900)
		msg.LocationMovementStatus = "stationary"

		evs := deriveEvents(st, msg, time.UnixMilli(2000))
		if got := eventTypes(evs); !reflect.DeepEqual(got, []string{"Zone", "Presence:INACTIVE"}) {
			t.Fatalf("events = %v, want [Zone, Presence:INACTIVE] (LOCATION_LOST must fire regardless of movement)", got)
		}
	})
}

func TestRunWatchdog(t *testing.T) {
	t.Parallel()

	t.Run("emits Presence{Inactive} when silence exceeds threshold", func(t *testing.T) {
		t.Parallel()
		states := map[string]*tagState{
			"a4da22e4e75d": {
				online:           true,
				lastLocationType: "position",
				lastZoneIds:      []string{"pool-main"},
				lastPacketTS:     time.UnixMilli(0),
			},
		}
		var captured []domain.Event
		emit := func(e domain.Event) error {
			captured = append(captured, e)
			return nil
		}
		now := time.UnixMilli(0).Add(OfflineThreshold).Add(time.Second)

		if err := runWatchdog(states, now, emit); err != nil {
			t.Fatalf("runWatchdog: %v", err)
		}
		if len(captured) != 1 {
			t.Fatalf("got %d events, want 1", len(captured))
		}
		if captured[0].Type != domain.EventTypePresence {
			t.Errorf("Type = %v", captured[0].Type)
		}
		if captured[0].State.Presence.Type != domain.PresenceTypeInactive {
			t.Errorf("Presence = %v", captured[0].State.Presence.Type)
		}
		if !reflect.DeepEqual(captured[0].State.Location.Zones, []string{"pool-main"}) {
			t.Errorf("Zones = %v, want last-known [pool-main]", captured[0].State.Location.Zones)
		}
		if states["a4da22e4e75d"].online {
			t.Error("watchdog should flip online → false")
		}
	})

	t.Run("skip when already location-lost (locationLostAt set)", func(t *testing.T) {
		t.Parallel()
		states := map[string]*tagState{
			"a4da22e4e75d": {
				online:           true,
				lastLocationType: "noLocation",
				lastZoneIds:      []string{},
				lastPacketTS:     time.UnixMilli(0),
				locationLostAt:   time.UnixMilli(0),
			},
		}
		emit := func(domain.Event) error {
			t.Fatal("watchdog should not have emitted while in location-lost state")
			return nil
		}
		runWatchdog(states, time.UnixMilli(0).Add(OfflineThreshold).Add(time.Second), emit)
	})

	t.Run("skip when within threshold", func(t *testing.T) {
		t.Parallel()
		states := map[string]*tagState{
			"a4da22e4e75d": {
				online:       true,
				lastPacketTS: time.UnixMilli(0),
			},
		}
		emit := func(domain.Event) error {
			t.Fatal("watchdog fired before threshold")
			return nil
		}
		runWatchdog(states, time.UnixMilli(0).Add(time.Second), emit)
	})

	t.Run("propagates emit error", func(t *testing.T) {
		t.Parallel()
		states := map[string]*tagState{
			"a4da22e4e75d": {
				online:       true,
				lastPacketTS: time.UnixMilli(0),
			},
		}
		boom := errors.New("boom")
		emit := func(domain.Event) error { return boom }

		err := runWatchdog(states, time.UnixMilli(0).Add(OfflineThreshold).Add(time.Second), emit)
		if !errors.Is(err, boom) {
			t.Fatalf("err = %v, want boom", err)
		}
	})
}

func TestEventID(t *testing.T) {
	t.Parallel()
	id := eventID("a4da22e4e75d", time.UnixMilli(1714123456789), 2)
	want := "evt-a4da22e4e75d-1714123456789000000-2"
	if id != want {
		t.Fatalf("eventID = %q, want %q", id, want)
	}
	if l := len(id); l < 3 || l > 64 {
		t.Errorf("eventID length %d out of [3..64]", l)
	}
}
