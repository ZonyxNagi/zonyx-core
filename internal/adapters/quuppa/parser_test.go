package quuppa

import (
	"reflect"
	"testing"
)

func TestParseDatagram_Valid(t *testing.T) {
	t.Parallel()

	// Sample drawn from docs/QUUPPA.md §3 / §7 — the DefaultLocationAndInfo shape
	// QPE 9.5+ pushes onDataChange.
	input := `{
	  "tagId":                  "a4da22e4e75d",
	  "tagName":                "swimmer-03",
	  "tagGroupName":           "Swimmers",
	  "responseTS":             1714123456789,
	  "lastPacketTS":           1714123448210,
	  "locationTS":             1714123447900,
	  "location":               [12.4, 3.1, 1.2],
	  "locationType":           "position",
	  "locationRadius":         0.42,
	  "locationCoordSysId":     "pool-floor-plan",
	  "locationMovementStatus": "moving",
	  "locationZoneIds":        ["pool-main", "lane-3"],
	  "locationZoneNames":      ["Pool Main", "Lane 3"],
	  "rssi":                   38,
	  "rssiLocatorCount":       3,
	  "tagState":               "triggered",
	  "batteryAlarm":           "ok"
	}`

	m, ok, reason := parseDatagram([]byte(input))
	if !ok {
		t.Fatalf("parseDatagram returned !ok for a valid §3 payload: %s", reason)
	}
	if m.TagID != "a4da22e4e75d" {
		t.Errorf("TagID = %q", m.TagID)
	}
	if m.LocationType != "position" {
		t.Errorf("LocationType = %q", m.LocationType)
	}
	if m.LocationZoneIds == nil {
		t.Fatal("LocationZoneIds is nil; expected pointer to populated slice")
	}
	if !reflect.DeepEqual(*m.LocationZoneIds, []string{"pool-main", "lane-3"}) {
		t.Errorf("LocationZoneIds = %v", *m.LocationZoneIds)
	}
	if !reflect.DeepEqual(m.Location, []float64{12.4, 3.1, 1.2}) {
		t.Errorf("Location = %v", m.Location)
	}
	if m.RSSILocatorCount != 3 {
		t.Errorf("RSSILocatorCount = %d", m.RSSILocatorCount)
	}
}

func TestParseDatagram_NullVsEmptyZones(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name        string
		input       string
		wantNilZone bool
		wantLen     int
	}{
		{
			name:        "null zones (location lost)",
			input:       `{"tagId":"a4da22e4e75d","responseTS":1,"locationType":"noLocation","locationZoneIds":null}`,
			wantNilZone: true,
		},
		{
			name:        "empty array zones (in-bounds, no zone match)",
			input:       `{"tagId":"a4da22e4e75d","responseTS":1,"locationType":"position","locationZoneIds":[]}`,
			wantNilZone: false,
			wantLen:     0,
		},
		{
			name:        "populated zones",
			input:       `{"tagId":"a4da22e4e75d","responseTS":1,"locationType":"position","locationZoneIds":["pool-main"]}`,
			wantNilZone: false,
			wantLen:     1,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			m, ok, reason := parseDatagram([]byte(tc.input))
			if !ok {
				t.Fatalf("parseDatagram returned !ok: %s", reason)
			}
			if tc.wantNilZone {
				if m.LocationZoneIds != nil {
					t.Errorf("expected nil pointer (json null), got %v", *m.LocationZoneIds)
				}
				return
			}
			if m.LocationZoneIds == nil {
				t.Fatal("expected non-nil pointer")
			}
			if got := len(*m.LocationZoneIds); got != tc.wantLen {
				t.Errorf("len(zones) = %d, want %d", got, tc.wantLen)
			}
		})
	}
}

func TestParseDatagram_NullLocation(t *testing.T) {
	t.Parallel()
	// Per §4: when locationType is in NON_LOCATION_TYPES, location[] is null.
	input := `{"tagId":"a4da22e4e75d","responseTS":1,"locationType":"noData","location":null,"locationZoneIds":null}`
	m, ok, reason := parseDatagram([]byte(input))
	if !ok {
		t.Fatalf("expected ok: %s", reason)
	}
	if m.Location != nil {
		t.Errorf("Location = %v, want nil", m.Location)
	}
}

func TestParseDatagram_Rejected(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		input string
	}{
		{"missing tagId", `{"responseTS":1,"locationType":"position"}`},
		{"empty tagId", `{"tagId":"","responseTS":1,"locationType":"position"}`},
		{"missing responseTS", `{"tagId":"a4da22e4e75d","locationType":"position"}`},
		{"zero responseTS", `{"tagId":"a4da22e4e75d","responseTS":0,"locationType":"position"}`},
		{"negative responseTS", `{"tagId":"a4da22e4e75d","responseTS":-1,"locationType":"position"}`},
		{"missing locationType", `{"tagId":"a4da22e4e75d","responseTS":1}`},
		{"empty locationType", `{"tagId":"a4da22e4e75d","responseTS":1,"locationType":""}`},
		{"malformed json", `not json`},
		{"empty input", ``},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if _, ok, _ := parseDatagram([]byte(tc.input)); ok {
				t.Fatal("expected !ok")
			}
		})
	}
}

func TestZonesEqual(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		a, b []string
		want bool
	}{
		{"both empty", nil, nil, true},
		{"empty vs nil-empty", []string{}, nil, true},
		{"identical order", []string{"a", "b"}, []string{"a", "b"}, true},
		{"different order", []string{"a", "b"}, []string{"b", "a"}, true},
		{"different length", []string{"a"}, []string{"a", "b"}, false},
		{"disjoint", []string{"a"}, []string{"b"}, false},
		{"overlap but not equal", []string{"a", "b"}, []string{"a", "c"}, false},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := zonesEqual(tc.a, tc.b); got != tc.want {
				t.Errorf("zonesEqual(%v, %v) = %v, want %v", tc.a, tc.b, got, tc.want)
			}
		})
	}
}
