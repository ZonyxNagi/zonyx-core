package quuppa

import "github.com/bytedance/sonic"

// udpMsg is the decoded shape of a single QUUPPA Positioning Engine UDP
// datagram in the DefaultLocationAndInfo format (QPE 9.5+, JSON output).
//
// LocationZoneIds is *[]string rather than []string because QPE distinguishes
// `null` (no fix — locationType is in NON_LOCATION_TYPES) from `[]` (valid
// fix, tag is in zero defined zones). The diff in derive.go relies on that
// distinction; collapsing nil into [] would lose the LOCATION_LOST signal.
//
// Fields not used by the adapter are deliberately omitted.
type udpMsg struct {
	TagID                  string    `json:"tagId"`
	TagName                string    `json:"tagName"`
	TagGroupName           string    `json:"tagGroupName"`
	ResponseTS             int64     `json:"responseTS"`
	LastPacketTS           int64     `json:"lastPacketTS"`
	LastSeenTS             int64     `json:"lastSeenTS"`
	LocationTS             int64     `json:"locationTS"`
	Location               []float64 `json:"location"`
	LocationType           string    `json:"locationType"`
	LocationMovementStatus string    `json:"locationMovementStatus"`
	LocationZoneIds        *[]string `json:"locationZoneIds"`
	RSSILocatorCount       int       `json:"rssiLocatorCount"`
}

// parseDatagram decodes a QUUPPA UDP datagram into a *udpMsg, returning
// (nil, false) on a malformed body or when a required field is missing.
//
// Required fields: TagID (12-char BLE MAC, used as Actor.ID), ResponseTS
// (server-clock timestamp, used as event time), LocationType (drives the
// state machine — empty makes every transition undefined).
//
// Everything else is optional and may be the JSON zero value. Per QPE docs
// (§3 in docs/QUUPPA.md), all fields can legitimately be absent or null in some
// situations, so we accept the message and let derive.go interpret it.
func parseDatagram(b []byte) (*udpMsg, bool, string) {
	var m udpMsg
	if err := sonic.Unmarshal(b, &m); err != nil {
		return nil, false, "json_error:" + err.Error()
	}
	if m.TagID == "" {
		return nil, false, "missing_tagId"
	}
	// responseTS may be absent in QPE output targets that omit $(response.ts).
	// Fall back to lastSeenTS (always present) then lastPacketTS.
	if m.ResponseTS <= 0 {
		if m.LastSeenTS > 0 {
			m.ResponseTS = m.LastSeenTS
		} else if m.LastPacketTS > 0 {
			m.ResponseTS = m.LastPacketTS
		} else {
			return nil, false, "missing_responseTS"
		}
	}
	if m.LocationType == "" {
		return nil, false, "missing_locationType"
	}
	return &m, true, ""
}
