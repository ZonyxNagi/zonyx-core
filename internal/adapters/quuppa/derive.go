package quuppa

import (
	"fmt"
	"time"

	"github.com/ZonyxNagi/zonyx-core/internal/core/domain"
)

// QPE detection constants (docs/QUUPPA.md §10).
const (
	OfflineThreshold = 12 * time.Second
	WatchdogTick     = 2 * time.Second
)

// locationTypes is the set of locationType values that count as a real
// position fix. Only "position" (full AoA) is treated as a valid location.
// "approximate" and anything less accurate (proximity, presence, noLocation,
// noData) is treated as "no location" — when the location degrades to
// "approximate" or worse, an INACTIVE PRESENCE event is emitted.
// See docs/QUUPPA.md §4.
var locationTypes = map[string]struct{}{
	"position": {},
}

func isLocationType(t string) bool {
	_, ok := locationTypes[t]
	return ok
}

// deriveEvents applies the §7/§8 transition rules to (prev, msg) and
// returns the ordered list of domain.Events the adapter should emit. It
// also mutates `prev` to the new state — caller must persist it back into
// the per-tag map.
//
// First-ever packet for an actor (prev.lastPacketTS zero, prev.lastLocationType
// empty) is treated as "no prior history": no TAG_ONLINE event, no transition
// events. Subsequent zone changes / location-type changes will fire normally.
//
// Order matters when both LOCATION_LOST and a zone transition apply: the
// implicit zones-exit (per §7 "emit ZONE_EXITED for all previously occupied
// zones, then emit LOCATION_LOST") is collapsed into a single zone-snapshot
// event with State.Zones = [] preceding the Presence{Inactive} event — our
// domain is snapshot-based, so a one-event diff is the right shape.
func deriveEvents(prev *tagState, msg *udpMsg, now time.Time) []domain.Event {
	var out []domain.Event
	seq := 0

	hadHistory := !prev.lastPacketTS.IsZero() || prev.lastLocationType != ""

	prevType := prev.lastLocationType
	prevZones := prev.lastZoneIds

	currType := msg.LocationType
	currZones := normaliseZones(msg.LocationZoneIds)

	// 1. TAG_ONLINE — only when we previously flagged the tag offline.
	//    First-ever sight does not emit (no prior offline state to leave).
	if hadHistory && !prev.online {
		out = append(out, presenceEvent(msg, now, prevZones, domain.PresenceTypeActive, seq))
		seq++
	}

	// 2. LOCATION_LOST: was tracked → no longer tracked.
	//    Emit a zone-empty snapshot first if we had zones, then Presence{Inactive}.
	prevHasLocation := isLocationType(prevType)
	currHasLocation := isLocationType(currType)

	switch {
	case hadHistory && prevHasLocation && !currHasLocation:
		if len(prevZones) > 0 {
			out = append(out, zoneEvent(msg, now, []string{}, seq))
			seq++
		}
		out = append(out, presenceEvent(msg, now, prevZones, domain.PresenceTypeInactive, seq))
		seq++
		prev.locationLostAt = now

	case hadHistory && !prevHasLocation && currHasLocation:
		out = append(out, presenceEvent(msg, now, currZones, domain.PresenceTypeActive, seq))
		seq++
		prev.locationLostAt = time.Time{}
	}

	// 3. Zone snapshot — emitted when the snapshot meaningfully changes and we
	//    have a valid location fix. That includes zone membership changes as well
	//    as state changes that affect the snapshot itself (for example a moving →
	//    stationary transition while the tag remains in the same zone). The
	//    repository deduplicates exact repeats, so this still avoids flooding on
	//    unchanged packets while keeping the latest live state visible to
	//    subscribers.
	zoneChanged := !zonesEqual(currZones, prevZones)
	stateChanged := currType != prevType || msg.LocationMovementStatus != prev.lastMovementStatus
	hasZoneContext := len(currZones) > 0 || len(prevZones) > 0

	if currHasLocation && ((hadHistory && hasZoneContext && (zoneChanged || stateChanged)) ||
		(!hadHistory && len(currZones) > 0)) {
		out = append(out, zoneEvent(msg, now, currZones, seq))
		seq++
	}

	// 4. Update prev → curr for the next packet.
	prev.online = true
	prev.lastResponseTS = now
	prev.lastPacketTS = time.UnixMilli(msg.LastPacketTS)
	prev.lastLocationType = currType
	prev.lastZoneIds = append(prev.lastZoneIds[:0:0], currZones...)
	if currHasLocation && msg.Location != nil {
		prev.lastKnownLocation = append(prev.lastKnownLocation[:0:0], msg.Location...)
		prev.lastKnownLocationTS = time.UnixMilli(msg.LocationTS)
	}
	prev.lastMovementStatus = msg.LocationMovementStatus
	if msg.TagName != "" {
		prev.name = msg.TagName
	}
	if msg.TagGroupName != "" {
		prev.groupName = msg.TagGroupName
	}

	return out
}

// runWatchdog scans every known tagState and emits Presence{Inactive} for
// any tag that has been silent longer than OfflineThreshold and is still
// flagged online. Tags already in a non-location state (locationLostAt set)
// are skipped — the LOCATION_LOST path already produced an inactive event,
// double-emitting would be a duplicate from the consumer's perspective.
func runWatchdog(states map[string]*tagState, now time.Time, emit func(domain.Event) error) error {
	for tagID, st := range states {
		if !st.online {
			continue
		}
		if !st.locationLostAt.IsZero() {
			continue
		}
		if now.Sub(st.lastPacketTS) < OfflineThreshold {
			continue
		}

		ev := domain.Event{
			ID:    eventID(tagID, now, 0),
			Type:  domain.EventTypePresence,
			Actor: domain.Actor{ID: tagID, Metadata: actorMetadataFromState(st)},
			State: domain.State{
				Location: &domain.Location{
					Point: pointFromSlice(st.lastKnownLocation),
					Zones: append([]string(nil), st.lastZoneIds...),
				},
				Presence: domain.Presence{
					Type:  domain.PresenceTypeInactive,
					Since: now,
				},
			},
			Timestamp: now,
		}
		if err := emit(ev); err != nil {
			return err
		}
		st.online = false
	}
	return nil
}

// normaliseZones collapses the *[]string from the JSON shape to a plain
// []string. nil and empty-pointer both become an empty slice. This mirrors
// the §6 "diff algorithm" rule: `null` is treated as `[]` for diffing —
// the LOCATION_LOST signal is carried separately by locationType.
func normaliseZones(p *[]string) []string {
	if p == nil {
		return []string{}
	}
	return *p
}

func zoneEvent(msg *udpMsg, now time.Time, zones []string, seq int) domain.Event {
	return domain.Event{
		ID:    eventID(msg.TagID, now, seq),
		Type:  domain.EventTypeZone,
		Actor: domain.Actor{ID: msg.TagID, Metadata: actorMetadata(msg)},
		State: domain.State{
			Location: locationFromMsg(msg, zones),
			Presence: domain.Presence{
				Type:  domain.PresenceTypeActive,
				Since: now,
			},
		},
		Timestamp: now,
	}
}

func presenceEvent(msg *udpMsg, now time.Time, zones []string, pt domain.PresenceType, seq int) domain.Event {
	return domain.Event{
		ID:    eventID(msg.TagID, now, seq),
		Type:  domain.EventTypePresence,
		Actor: domain.Actor{ID: msg.TagID, Metadata: actorMetadata(msg)},
		State: domain.State{
			Location: &domain.Location{Zones: append([]string(nil), zones...)},
			Presence: domain.Presence{
				Type:  pt,
				Since: now,
			},
		},
		Timestamp: now,
	}
}

// locationFromMsg builds a Location carrying the coordinate point (when
// msg.Location has at least 3 elements) and the provided zone list.
func locationFromMsg(msg *udpMsg, zones []string) *domain.Location {
	return &domain.Location{
		Point: pointFromMsg(msg),
		Zones: append([]string(nil), zones...),
	}
}

// pointFromMsg extracts a 3-D point from the QPE location array [x, y, z].
// Returns nil when no fix data is present.
func pointFromMsg(msg *udpMsg) *domain.Point {
	return pointFromSlice(msg.Location)
}

// pointFromSlice converts a raw [x, y, z] float64 slice to a Point.
// Returns nil when the slice has fewer than 3 elements (no fix available).
func pointFromSlice(loc []float64) *domain.Point {
	if len(loc) < 3 {
		return nil
	}
	return &domain.Point{X: loc[0], Y: loc[1], Z: loc[2]}
}

// actorMetadata builds the Actor.Metadata map from the freshest fields of
// an incoming datagram. Only non-empty values are written so consumers can
// distinguish "missing" from "explicit empty string".
func actorMetadata(msg *udpMsg) map[string]string {
	if msg.TagName == "" && msg.TagGroupName == "" &&
		msg.LocationType == "" && msg.LocationMovementStatus == "" {
		return nil
	}
	md := make(map[string]string, 4)
	if msg.TagName != "" {
		md["tagName"] = msg.TagName
	}
	if msg.TagGroupName != "" {
		md["tagGroupName"] = msg.TagGroupName
	}
	if msg.LocationType != "" {
		md["locationType"] = msg.LocationType
	}
	if msg.LocationMovementStatus != "" {
		md["movementStatus"] = msg.LocationMovementStatus
	}
	return md
}

// actorMetadataFromState reconstructs Actor.Metadata for watchdog-emitted
// events, where there is no incoming datagram to read from.
func actorMetadataFromState(st *tagState) map[string]string {
	md := map[string]string{}
	if st.name != "" {
		md["tagName"] = st.name
	}
	if st.groupName != "" {
		md["tagGroupName"] = st.groupName
	}
	if st.lastLocationType != "" {
		md["locationType"] = st.lastLocationType
	}
	if len(md) == 0 {
		return nil
	}
	return md
}

// eventID builds a deterministic event identifier. Format:
// "evt-<actor>-<unixnano>-<seq>". seq disambiguates events emitted from the
// same datagram (LOCATION_LOST emits Zone+Presence, two events at the same
// time). Length stays under the proto's [3..64] cap for any 12-char MAC.
func eventID(actorID string, ts time.Time, seq int) string {
	return fmt.Sprintf("evt-%s-%d-%d", actorID, ts.UnixNano(), seq)
}
