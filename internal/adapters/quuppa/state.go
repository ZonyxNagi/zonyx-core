package quuppa

import "time"

// tagState is the per-tag state record the adapter maintains across
// datagrams. It mirrors the §8 pseudocode in docs/QUUPPA.md: the fields needed
// to derive LOCATION_LOST / LOCATION_RESTORED / ZONE_ENTERED / ZONE_EXITED
// transitions plus the watchdog's offline detection.
type tagState struct {
	online              bool
	lastPacketTS        time.Time
	lastResponseTS      time.Time
	lastLocationType    string
	lastZoneIds         []string
	lastKnownLocation   []float64
	lastKnownLocationTS time.Time
	locationLostAt      time.Time
	lastMovementStatus  string // moving | stationary | noData | hidden | ""
	name                string
	groupName           string
}

// zonesEqual reports whether two zone slices represent the same set
// (membership-wise — order is not significant). The proto caps zone membership
// at 8 entries so an O(n²) scan is both correct and allocation-free.
func zonesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
outer:
	for _, az := range a {
		for _, bz := range b {
			if az == bz {
				continue outer
			}
		}
		return false
	}
	return true
}
