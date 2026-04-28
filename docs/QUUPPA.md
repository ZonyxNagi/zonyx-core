# QUUPPA — Pool Safety System Technical Reference

> Drowning Detection & Location Loss Detection using Quuppa RTLS
> QPE 9.5+ · UDP Push · Event-driven · No REST polling

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [UDP Output Target Configuration](#2-udp-output-target-configuration)
3. [UDP Message Format — Field Reference](#3-udp-message-format--field-reference)
4. [locationType — Confidence Ladder](#4-locationtype--confidence-ladder)
5. [locationMovementStatus — Motion States](#5-locationmovementstatus--motion-states)
6. [Zone Fields](#6-zone-fields)
7. [State Transitions & Emitted Events](#7-state-transitions--emitted-events)
8. [Detection Service — Pseudocode Logic](#8-detection-service--pseudocode-logic)
9. [Recommended QSP Zone Definitions](#9-recommended-qsp-zone-definitions)
10. [Constants & Tuning Reference](#10-constants--tuning-reference)
11. [Key Design Decisions](#11-key-design-decisions)

---

## 1. Architecture Overview

```text
Quuppa Tags (worn on swim caps, waterproof enclosure)
        │ BLE AoA (Angle of Arrival)
        ▼
Q17 Locators — ceiling-mounted, ≥3 per lane group
        │ IPv4
        ▼
Quuppa Positioning Engine (QPE 9.5+)
        │
        └── UDP Push (onDataChange) ──→ Detection Service :5000
                                              │
                              ┌───────────────┼───────────────┐
                              ▼               ▼               ▼
                        LOCATION_LOST   ZONE_ENTERED    TAG_OFFLINE
                        LOCATION_RESTORED ZONE_EXITED   TAG_ONLINE
                              │
                        Consumer Service
                        (evaluates duration + zone context → DROWNING_ALERT)
```

**Separation of concerns:**

- The **Detection Service** reports raw QPE state transitions. It has no domain logic.
- The **Consumer Service** decides what a transition means over time (drowning vs. flip turn vs. rest).

---

## 2. UDP Output Target Configuration

Configure once via `/qpe/createOutputTarget` API or QSP Output Targets editor.

```text
GET /qpe/createOutputTarget
  ?name                       = drowning-monitor
  &format                     = DefaultLocationAndInfo
  &target                     = udp
  &type                       = json
  &triggerMode                = LastSeenUpdate
  &stopOutputIfTagIsNotSeenIn = 12
  &ipAddress                  = YOUR_APP_IP
  &port                       = 5000
  &start
```

### Parameter Reference

| Parameter                    | Value              | Why                                                                                                                           |
| ---------------------------- | ------------------ | ----------------------------------------------------------------------------------------------------------------------------- |
| `triggerMode`                | `LastSeenUpdate`   | Fire on every new BLE packet received — not on a fixed interval. Required for continuous coordinate streaming.                |
| `onDataChange`               | _(omit)_           | Omitting this parameter lets QPE push on every `LastSeenUpdate`, which is necessary to stream coordinate position updates. Adding a filter (e.g. `$(location.type),$(location.zone.ids)`) reduces UDP volume but suppresses coordinate-only updates — only do this if you do not need continuous positioning. |
| `stopOutputIfTagIsNotSeenIn` | `12`               | QPE stops sending after 12s of silence. App detects gap as offline.                                                          |
| `format`                     | `DefaultLocationAndInfo` | Contains all location, zone, movement, and signal fields.                                                               |
| `type`                       | `json`             | Structured for easy parsing.                                                                                                  |
| `target`                     | `udp`              | Lowest latency delivery.                                                                                                      |
| `startAtSystemStart`         | `true`             | Survive QPE restarts without manual intervention.                                                                             |

> **`responseTS` is optional.** Some QPE output target configurations omit `$(response.ts)`. The adapter falls back automatically to `lastSeenTS` (then `lastPacketTS`) as the event timestamp. Include `$(response.ts)` in the format string if you need the exact QPE server-clock build time rather than the last-seen instant.

> **Why 12s and not 8s for `stopOutputIfTagIsNotSeenIn`?**
> When a tag enters `stationary` state it drops to **0.1 Hz** advertising (one packet every 10 seconds). Setting the threshold to 8s would produce spurious offline events for a swimmer resting at the wall. 12s provides the necessary grace window.

---

## 3. UDP Message Format — Field Reference

Create a custom output target in QSP. The tables below list the fields to include. Fields marked **not yet parsed** are kept in the UDP output for future use but not currently consumed by the adapter; fields with no use case have been omitted to reduce per-datagram byte cost.

### Identity

| QSP Format Key      | JSON Field     | Type   | Example          | Notes                        |
| ------------------- | -------------- | ------ | ---------------- | ---------------------------- |
| `$(tag.id)`         | `tagId`        | string | `"a4da22e4e75d"` | Unique BLE MAC address       |
| `$(tag.name)`       | `tagName`      | string | `"swimmer-03"`   | Human label from QSP project |
| `$(tag.group.name)` | `tagGroupName` | string | `"Swimmers"`     | Tag group defined in QSP     |

### Timing

| QSP Format Key         | JSON Field     | Type    | Example         | Notes                                                                                      |
| ---------------------- | -------------- | ------- | --------------- | ------------------------------------------------------------------------------------------ |
| `$(response.ts)`       | `responseTS`   | long ms | `1714123456789` | Server clock when QPE built this response. **Optional** — omitted by some configurations; adapter falls back to `lastSeenTS`. |
| `$(tag.lastpacket.ts)` | `lastPacketTS` | long ms | `1714123448210` | Last BLE packet received from tag by any Locator.                                          |
| `$(tag.lastseen.ts)`   | `lastSeenTS`   | long ms | `1714123456789` | Last time QPE processed a packet for this tag. **Always present.** Used as the event timestamp when `responseTS` is absent. Typically equals `lastPacketTS`. |

### Location

| QSP Format Key            | JSON Field           | Type      | Example             | Notes                                     |
| ------------------------- | -------------------- | --------- | ------------------- | ----------------------------------------- |
| `$(location.value)`       | `location`           | `[x,y,z]` | `[12.4, 3.1, 1.2]`  | Coords in meters. `null` when no fix.     |
| `$(location.ts)`          | `locationTS`         | long ms   | `1714123447900`     | Timestamp of this location estimate       |
| `$(location.type)`        | `locationType`       | enum      | `"position"`        | Confidence level — see §4                 |
| `$(location.radius)`      | `locationRadius`     | float m   | `0.42`              | Accuracy radius. Larger = less confident. |
| `$(location.coordsys.id)` | `locationCoordSysId` | string    | `"pool-floor-plan"` | Coordinate system UUID from QSP           |

### Movement

| QSP Format Key                | JSON Field               | Type | Example    | Notes                     |
| ----------------------------- | ------------------------ | ---- | ---------- | ------------------------- |
| `$(location.movement.status)` | `locationMovementStatus` | enum | `"moving"` | QPE motion state — see §5 |

### Zones

| QSP Format Key           | JSON Field          | Type     | Example                  | Notes                                                           |
| ------------------------ | ------------------- | -------- | ------------------------ | --------------------------------------------------------------- |
| `$(location.zone.ids)`   | `locationZoneIds`   | string[] | `["pool-main","lane-3"]` | All zones tag currently occupies. `[]` = none. `null` = no fix. |
| `$(location.zone.names)` | `locationZoneNames` | string[] | `["Pool Main","Lane 3"]` | Human-readable zone names from QSP. **Not yet parsed** — kept for future display/logging use. |

### Signal

| QSP Format Key              | JSON Field         | Type    | Example | Notes                                                        |
| --------------------------- | ------------------ | ------- | ------- | ------------------------------------------------------------ |
| `$(tag.rssi)`               | `rssi`             | 0–63 dB | `38`    | Signal strength. >40 ≈ within ~5m of a Locator.              |
| `$(tag.rssi.locator.count)` | `rssiLocatorCount` | int     | `3`     | Number of Locators currently receiving packets from this tag |

### Tag State

| QSP Format Key         | JSON Field     | Type | Example       | Notes                               |
| ---------------------- | -------------- | ---- | ------------- | ----------------------------------- |
| `$(tag.state)`         | `tagState`     | enum | `"triggered"` | Internal tag firmware state machine (e.g. `triggered` = button press — relevant to the `EVENT_TYPE_COMMAND` path). **Not yet parsed.** |
| `$(tag.battery.alarm)` | `batteryAlarm` | enum | `"ok"`        | `"ok"` \| `"low"` \| `null`. **Not yet parsed** — kept for future battery-alert feature.         |

> **Format string tips:**
>
> - Use `$(location.ts.isoutc)` for ISO 8601 timestamps instead of epoch ms.
> - Use `$(location.zone.ids.,)` with the `.,` delimiter for CSV-safe zone lists.
> - All fields return `null` if not yet received or not applicable.

---

## 4. `locationType` — Confidence Ladder

**Source:** `$(location.type)` · `getTagData → DefaultLocation`

This field degrades as BLE signal weakens. It is the primary indicator of location-loss events.

| Value         | `location[]` | Coords are tag?        | Description                                                               | Pool context                                                      |
| ------------- | ------------ | ---------------------- | ------------------------------------------------------------------------- | ----------------------------------------------------------------- |
| `position`    | present      | ✅ yes                 | Full AoA fix. All data points: RSSI, spectrum, movement history.          | Normal swimming. Tag clearly visible to 3+ Locators.              |
| `approximate` | present      | ✅ yes                 | Partial fix. Fewer data points — RSSI + spectrum but no movement history. | Tag near surface, slightly degraded. `locationRadius` is larger.  |
| `proximity`   | present      | ❌ **nearest Locator** | No reliable fix. Coords are the nearest Locator's position, NOT the tag.  | Tag submerging or at edge of coverage. Treat as non-location.     |
| `presence`    | `null`       | ❌ null                | Tag detected but no location computable.                                  | Tag barely heard by one Locator. Signal too weak for positioning. |
| `noLocation`  | `null`       | ❌ null                | Tag heard but signals conflicting or too weak.                            | Severe multipath or underwater. Flip turn or submersion.          |
| `noData`      | `null`       | ❌ null                | No packets received at all. Tag not visible to any Locator.               | Tag fully submerged or battery dead. `lastPacketTS` goes stale.   |

### Non-Location Threshold

```text
LOCATION_LOST  triggered when:
  prev ∈ { position, approximate }
  curr ∈ { proximity, presence, noLocation, noData }

LOCATION_RESTORED triggered when:
  prev ∈ { proximity, presence, noLocation, noData }
  curr ∈ { position, approximate }
```

> `proximity` returns coordinates but they are the **Locator's position**, not the tag's.
> For safety-critical logic, treat `proximity` as no-location.

---

## 5. `locationMovementStatus` — Motion States

**Source:** `$(location.movement.status)` · `getTagData → DefaultLocationAndInfo`

Derived from the tag's internal accelerometer. Also controls BLE advertising TX rate.

| Value        | TX Rate | Description                                                       | Pool context                                        |
| ------------ | ------- | ----------------------------------------------------------------- | --------------------------------------------------- |
| `moving`     | 3 Hz    | Tag accelerometer triggered. QPE confirmed motion in progress.    | Active swimming stroke.                             |
| `stationary` | 0.1 Hz  | No motion for ≥20 seconds. Tag dropped to low-power advertising.  | Resting at wall, floating, or motionless in water.  |
| `noData`     | —       | Movement status cannot be determined. No location data available. | Tag completely silent — underwater or battery dead. |
| `hidden`     | —       | Tag is in a zone configured to hide location in QSP.              | Changing room / locker area — intentionally masked. |

### Adapter gate rule

`EVENT_TYPE_ZONE` events serve a dual purpose: they carry **zone membership** and **coordinate position** in the same snapshot. The adapter emits one on every inbound packet that has a valid location fix (`locationType ∈ {position, approximate}`), subject to one gate:

The adapter suppresses zone snapshot events (`EVENT_TYPE_ZONE`) when `locationMovementStatus == "stationary"`. This is the only documented value that definitively means "accelerometer confirmed no motion for ≥20s". Emitting zone events for a stationary tag risks false zone-membership drift caused by multipath reflection on a motionless device, which can produce spurious ZONE_ENTERED/ZONE_EXITED transitions. All other values (`moving`, `noData`, `hidden`, or absent) pass through without suppression.

**Consequence for coordinate streaming:** stationary tags (0.1 Hz advertising rate after the 5-burst period) do not produce coordinate events while motionless. When the swimmer starts moving again, the `stationary → moving` transition fires a datagram and coordinate streaming resumes immediately. Lifecycle events (LOCATION_LOST, LOCATION_RESTORED, TAG_ONLINE, TAG_OFFLINE) are **never** gated — they fire regardless of movement status.

### TX Rate Impact on Detection

When a tag transitions to `stationary`, it sends **5 burst packets at 1 Hz**, then drops to **0.1 Hz** (one packet every 10 seconds).

This means `lastPacketTS` can be up to **10 seconds stale** even when the tag is alive and above water. The `stopOutputIfTagIsNotSeenIn` and app-side `OFFLINE_THRESHOLD` must both account for this.

**QPE state machine timing:**

- Motion detected → immediately announces `moving`
- No motion for **≥20 seconds** → announces `stationary`
- On `stationary` entry → 5 × 1 Hz burst, then 0.1 Hz

---

## 6. Zone Fields

### `locationZoneIds` — Source: `$(location.zone.ids)`

| Value                     | Meaning                                                                       |
| ------------------------- | ----------------------------------------------------------------------------- |
| `["pool-main"]`           | Tag inside pool area                                                          |
| `["pool-main", "lane-3"]` | Tag in pool AND lane 3 (overlapping zones)                                    |
| `["wall-east"]`           | Tag at east turn wall                                                         |
| `[]` (empty array)        | Tag has a valid position but is not inside any defined zone                   |
| `null`                    | Tag has no position — `locationType` is `noData`, `noLocation`, or `presence` |

> A tag can occupy **multiple overlapping zones simultaneously**. Always handle `locationZoneIds` as an array.

### Zone Change Detection — Diff Algorithm

```text
// On each UDP message, compare current vs previous zones

prev = tag.lastZoneIds        // may be [] or null
curr = msg.locationZoneIds    // may be [] or null

// null means no position — treat as empty for diff purposes
prev_safe = prev ?? []
curr_safe = curr ?? []

// But emit LOCATION_LOST separately when curr goes null (handled in §7)

entered = curr_safe.filter(z => z NOT IN prev_safe)
exited  = prev_safe.filter(z => z NOT IN curr_safe)

FOR EACH z in entered → emit(ZONE_ENTERED, tagId, z)
FOR EACH z in exited  → emit(ZONE_EXITED,  tagId, z)
```

> When `locationZoneIds` transitions to `null` (location lost), emit `ZONE_EXITED` for all previously occupied zones, then emit `LOCATION_LOST`.

---

## 7. State Transitions & Emitted Events

The Detection Service emits these six generic events. No domain logic (drowning vs. flip turn) is encoded at this layer.

---

### `LOCATION_LOST`

**Trigger:** `locationType` crosses from `{position, approximate}` → `{proximity, presence, noLocation, noData}`

**Zone context:** Capture `locationZoneIds` at the moment of crossing — this is the last known zone.

**Payload:**

```json
{
  "event": "LOCATION_LOST",
  "tagId": "a4da22e4e75d",
  "eventTS": 1714123456789,
  "locationType": "noLocation",
  "lastKnownLocation": [12.4, 3.1, 1.2],
  "lastKnownLocationTS": 1714123447900,
  "locationZoneIds": ["pool-main"],
  "rssiLocatorCount": 1,
  "lastPacketTS": 1714123448210
}
```

> Generic event. Does not imply drowning. Consumer decides based on duration + zone context.

---

### `LOCATION_RESTORED`

**Trigger:** `locationType` crosses from `{proximity, presence, noLocation, noData}` → `{position, approximate}`

**Zone context:** Capture `locationZoneIds` at restore time — this is where the tag reappeared.

**Payload:**

```json
{
  "event": "LOCATION_RESTORED",
  "tagId": "a4da22e4e75d",
  "eventTS": 1714123470000,
  "locationType": "position",
  "location": [12.4, 3.1, 1.2],
  "locationZoneIds": ["pool-main"],
  "rssiLocatorCount": 4,
  "gapDurationMs": 21790
}
```

> `gapDurationMs` = `eventTS` − `LOCATION_LOST.eventTS`. Computed by detection service. Consumer uses this to decide alert level.

---

### `ZONE_ENTERED`

**Trigger:** A zone ID appears in `locationZoneIds` that was not present in the previous message.

**Payload:**

```json
{
  "event": "ZONE_ENTERED",
  "tagId": "a4da22e4e75d",
  "eventTS": 1714123456789,
  "zoneId": "wall-east",
  "zoneName": "East Turn Wall",
  "locationZoneIds": ["pool-main", "wall-east"],
  "location": [24.0, 3.1, 1.2],
  "locationType": "position"
}
```

> Emit one event per zone entered. A tag entering two overlapping zones simultaneously produces two `ZONE_ENTERED` events.

---

### `ZONE_EXITED`

**Trigger:** A zone ID present in the previous `locationZoneIds` is now absent.

**Payload:**

```json
{
  "event": "ZONE_EXITED",
  "tagId": "a4da22e4e75d",
  "eventTS": 1714123458500,
  "zoneId": "wall-east",
  "zoneName": "East Turn Wall",
  "locationZoneIds": ["pool-main"],
  "location": [23.5, 3.1, 1.2],
  "locationType": "position"
}
```

> If `locationZoneIds` goes to `null` (location lost), emit `ZONE_EXITED` for all previously occupied zones.

---

### `TAG_OFFLINE`

**Trigger:** App-side watchdog detects `now − lastPacketTS > OFFLINE_THRESHOLD_MS`

> QPE does not emit this event. It is detected by the app watchdog monitoring `lastPacketTS` staleness.

**Payload:**

```json
{
  "event": "TAG_OFFLINE",
  "tagId": "a4da22e4e75d",
  "eventTS": 1714123460000,
  "lastPacketTS": 1714123448000,
  "silenceDurationMs": 12000,
  "lastKnownLocation": [12.4, 3.1, 1.2],
  "lastKnownZoneIds": ["pool-main"],
  "locationType": "noData"
}
```

---

### `TAG_ONLINE`

**Trigger:** UDP message received from a tag that was previously in `TAG_OFFLINE` state.

**Payload:**

```json
{
  "event": "TAG_ONLINE",
  "tagId": "a4da22e4e75d",
  "eventTS": 1714123472000,
  "offlineDurationMs": 24000,
  "locationType": "approximate",
  "locationZoneIds": ["pool-main"],
  "lastPacketTS": 1714123472000
}
```

---

### Full State Machine — Transition Table

```text
[ANY STATE]        + packet received after silence  →  TAG_ONLINE          (if was offline)
[HAS POSITION]     + zone array changes             →  ZONE_ENTERED / ZONE_EXITED
[HAS LOCATION]     + locationType degrades          →  LOCATION_LOST       + last known zones
[NO LOCATION]      + locationType recovers          →  LOCATION_RESTORED   + gap duration
[ALIVE]            + silence > OFFLINE_THRESHOLD    →  TAG_OFFLINE         + last known zones
[OFFLINE]          + packet received                →  TAG_ONLINE          + offline duration
```

---

## 8. Detection Service — Pseudocode Logic

### Constants

```python
OFFLINE_THRESHOLD_MS     = 12_000   # 12s silence → TAG_OFFLINE
WATCHDOG_TICK_MS         = 2_000    # watchdog loop interval
NON_LOCATION_TYPES       = {"proximity", "presence", "noLocation", "noData"}
LOCATION_TYPES           = {"position", "approximate"}
```

### Per-Tag State Record

```python
TagState {
  tagId                 : string
  isOnline              : bool    = false
  lastPacketTS          : long    = 0
  silenceStartedAt      : long    = 0
  lastLocationType      : string  = null    # locationType
  lastMovementStatus    : string  = null    # locationMovementStatus
  lastZoneIds           : list    = []      # locationZoneIds
  lastKnownLocation     : list    = null    # last [x,y,z] with valid fix
  lastKnownLocationTS   : long    = 0
  locationLostAt        : long    = 0       # ts when LOCATION_LOST emitted
}
```

### UDP Message Handler

```python
onUDPMessage(msg):

  tag = getOrCreate(tagState[msg.tagId])
  now = msg.responseTS

  # ── 1. Update raw fields ────────────────────────────────────────────────
  tag.lastPacketTS      = msg.lastPacketTS
  prevLocationType      = tag.lastLocationType
  prevZoneIds           = tag.lastZoneIds

  tag.lastLocationType   = msg.locationType
  tag.lastMovementStatus = msg.locationMovementStatus
  tag.lastZoneIds        = msg.locationZoneIds ?? []

  # ── 2. TAG_ONLINE ───────────────────────────────────────────────────────
  if NOT tag.isOnline:
    tag.isOnline        = true
    offlineDuration     = now - tag.silenceStartedAt
    emit(TAG_ONLINE, tag, offlineDurationMs=offlineDuration)

  # ── 3. Update last known valid location ─────────────────────────────────
  if msg.locationType in LOCATION_TYPES and msg.location is not null:
    tag.lastKnownLocation   = msg.location
    tag.lastKnownLocationTS = msg.locationTS

  # ── 4. LOCATION_LOST / LOCATION_RESTORED ────────────────────────────────
  if prevLocationType in LOCATION_TYPES and msg.locationType in NON_LOCATION_TYPES:
    tag.locationLostAt = now
    # emit ZONE_EXITED for all currently occupied zones first
    FOR EACH z in prevZoneIds:
      emit(ZONE_EXITED, tag.tagId, zoneId=z, eventTS=now)
    emit(LOCATION_LOST, tag, lastKnownLocation=tag.lastKnownLocation,
         locationZoneIds=prevZoneIds)

  elif prevLocationType in NON_LOCATION_TYPES and msg.locationType in LOCATION_TYPES:
    gapMs = now - tag.locationLostAt
    emit(LOCATION_RESTORED, tag, gapDurationMs=gapMs)

  # ── 5. Zone changes (only when location is valid) ────────────────────────
  if msg.locationType in LOCATION_TYPES:
    entered = [z for z in tag.lastZoneIds if z NOT IN prevZoneIds]
    exited  = [z for z in prevZoneIds if z NOT IN tag.lastZoneIds]

    FOR EACH z in entered: emit(ZONE_ENTERED, tag, zoneId=z)
    FOR EACH z in exited:  emit(ZONE_EXITED,  tag, zoneId=z)
```

### Silence Watchdog (runs every 2s)

```python
watchdogTick():

  now = currentTimeMs()

  FOR EACH tag IN activeTags:

    silence = now - tag.lastPacketTS

    if silence >= OFFLINE_THRESHOLD_MS AND tag.isOnline:
      tag.isOnline         = false
      tag.silenceStartedAt = tag.lastPacketTS
      emit(TAG_OFFLINE, tag,
           silenceDurationMs = silence,
           lastKnownZoneIds  = tag.lastZoneIds)
```

---

## 9. Recommended QSP Zone Definitions

Define these zones in **Quuppa Site Planner (QSP)** before deployment.

| Zone ID             | Shape     | Width            | Purpose                                                        |
| ------------------- | --------- | ---------------- | -------------------------------------------------------------- |
| `pool-main`         | Rectangle | Full pool area   | Outer boundary. Required guard for all safety logic.           |
| `wall-east`         | Rectangle | ~1 m strip       | East turn wall. Suppresses false alarms during flip turns.     |
| `wall-west`         | Rectangle | ~1 m strip       | West turn wall. Mirror of `wall-east`.                         |
| `lane-1` … `lane-N` | Rectangle | Per-lane width   | Lane identification for lap counting and per-lane reporting.   |
| `pool-shallow`      | Rectangle | Shallow end area | Lower sensitivity zone — children's area.                      |
| `pool-deck`         | Polygon   | Poolside / deck  | Recovery zone. `TAG_ONLINE` here = swimmer exited pool safely. |

> Overlapping zones are fully supported by QPE. `pool-main` + `wall-east` can overlap — the tag will appear in both simultaneously and `locationZoneIds` will contain both IDs.

---

## 10. Constants & Tuning Reference

| Constant                     | Recommended Value                           | Rationale                                                                                  |
| ---------------------------- | ------------------------------------------- | ------------------------------------------------------------------------------------------ |
| `OFFLINE_THRESHOLD_MS`       | `12 000` ms                                 | Stationary tags TX at 0.1 Hz → up to 10s between packets. 12s avoids false offline events. |
| `WATCHDOG_TICK_MS`           | `2 000` ms                                  | Balance between detection latency and CPU cost.                                            |
| `NON_LOCATION_TYPES`         | `{proximity, presence, noLocation, noData}` | `proximity` coords are the Locator's, not the tag's — treat as non-location.               |
| `stopOutputIfTagIsNotSeenIn` | `12` s                                      | Must match `OFFLINE_THRESHOLD_MS`. Set in QPE output target config.                        |
| `onDataChange`               | _(omit)_                                    | Omit to stream every position fix (coordinate streaming mode). Set to `$(location.type),$(location.zone.ids),$(location.movement.status)` only if continuous coordinates are not needed — this reduces UDP volume but suppresses position updates when a tag moves within the same zone. |

---

## 11. Key Design Decisions

### Why the detection service is generic

The detection service only knows about QPE field transitions. It has no concept of "drowning", "flip turn", or "lap". This makes it reusable for any location-loss use case (equipment tracking, staff safety, child monitoring) and makes the drowning logic independently testable and replaceable.

### Why no per-tag timers

A single watchdog loop checking `now - lastPacketTS` for all active tags every 2 seconds is O(n) and uses a single shared timer. Per-tag `setTimeout` calls would create N timers, complicate cleanup on reconnect, and risk timer drift on high-tag-count deployments.

### Why `proximity` is treated as non-location

When `locationType = proximity`, the `location[]` array is populated — but it contains the coordinates of the **nearest Locator**, not the tag. Using these coordinates for safety logic (e.g. zone membership checks) would silently produce wrong zone associations. The conservative choice is to treat `proximity` as no-location and emit `LOCATION_LOST`.

### Why `gapDurationMs` is computed by the detection service, not the consumer

The detection service has the exact `LOCATION_LOST.eventTS` and `LOCATION_RESTORED.eventTS`. Computing the gap at this layer means the consumer receives a single enriched event rather than having to maintain its own state to correlate two separate events. Stateless consumers are simpler and easier to scale.

### Why zone diffs are emitted as individual events

A tag entering `["pool-main", "wall-east"]` from `["pool-main"]` could be handled as a single "zones changed" event. However, emitting one `ZONE_ENTERED` per zone means consumers can subscribe to a specific zone transition without filtering — a consumer only interested in wall-zone entries receives exactly those events without parsing arrays.

---

_QPE 9.5+ · Quuppa Intelligent Locating System · DefaultLocationAndInfo format · UDP Push_
