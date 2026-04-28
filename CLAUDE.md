# CLAUDE.md — zonyx-core

Onboarding context for Claude Code sessions working on this service. Read this first; it spares you from re-discovering the scaffold and re-fetching the proto contract.

---

## 1. Service summary

`zonyx-core` is the dispatcher that sits between **real-world event sources** (positioning hardware, vision, …) and **API clients**. It abstracts the source technology behind a single normalised event stream.

Roles, in the service's own terms:

- **gRPC adapter** — handles inbound client calls (e.g. another internal API service consuming the event feed).
- **Provider adapters** (QUUPPA today; computer vision planned) — produce real-world events when an `Actor`'s state changes (zone transition, presence active/inactive on signal loss, device-originated commands). All providers implement a single `Provider` port; they are pluggable.
- **Service (core)** — manages provider lifecycles, persists every event through the persistence port, and forwards every event back to the original gRPC caller(s) that have an active `Subscribe` stream matching the event.
- **Persistence adapter** (NATS JetStream today) — durable storage and offset-based replay; pluggable behind an `Repository` port.

Three RPCs:

All RPCs live on a single gRPC service, **`CoreService`**:

- **`CoreService.Subscribe`** — server-side stream. Consumers subscribe once and receive `Event`s as they happen, with optional filtering and resumable offsets.
- **`CoreService.Publish`** — bidirectional stream. Producers push `Directive`s (control-plane instructions: `register_device`, `update_zone`, `reset_actor`, …) and receive per-message acknowledgements.
- **`CoreService.ReadConfiguration`** — server-side stream. Clients receive the current `Configuration` (zones with polygon geometry) immediately on open, then receive updated snapshots whenever the configuration changes.

Domain shape: `Actor` ──generates──▶ `Event` ──embeds──▶ `State` ──embeds──▶ `Location{Point, Zones}`.

---

## 2. Architecture

Hexagonal / ports-and-adapters. The core has no transport, storage, or hardware knowledge.

Three port kinds. The Service is the only component that talks to all three:

| Port                                              | Direction                   | Purpose                                                                | Impl(s) today                             |
| ------------------------------------------------- | --------------------------- | ---------------------------------------------------------------------- | ----------------------------------------- |
| Transport (`CoreService`)                         | inbound                     | Client-facing API (Subscribe + Publish + ReadConfiguration RPCs)       | gRPC adapter                              |
| `Provider`                                        | inbound (driven by Service) | Real-world event source — emits `Event`s when an actor's state changes | QUUPPA adapter, Vision (future)           |
| `Repository`                                      | outbound                    | Durable persistence + replay                                           | NATS JetStream adapter ✓                  |

```
   ┌──── inbound transport (gRPC) ────┐                     ┌──── Repository port ────┐
                                      │                     │                         │
   gRPC Subscribe ──► SubAdapter ────►│                     │   JetStream adapter ────┼──► NATS
                                      ├──► Service ◄───────►│   (impls Write/Watch)   │
   gRPC Publish   ──► PubAdapter ────►│        ▲            │                         │
                                      │        │            └─────────────────────────┘
   ┌──── Provider port (inbound) ─────┘        │
   │                                           │
   │   QUUPPA UDP   ──► QuuppaAdapter  ────────┤  emits domain.Event
   │                                           │
   │   Vision (TBD) ──► VisionAdapter  ────────┘
   └───────────────────────────────────────────
```

### How an event flows

1. Provider adapter (e.g. QUUPPA) reads its hardware feed (UDP datagrams), detects an actor state change inside its own logic, and emits a fully-formed `domain.Event` through the `Provider` port handler the Service registered with it on startup.
2. Service receives the event and calls `Repository.Write(event)` — the JetStream adapter writes it to the durable stream and returns a string `offset`.
3. Service forwards the event to every active `Subscribe` session whose `EventType` filter matches. The original gRPC caller receives the event on the wire.

### How a Subscribe call is served

1. Client opens `CoreService.Subscribe(SubscribeRequest)`.
2. The gRPC `SubAdapter` registers the session with the Service, including the requested `types` filter and any `offset`.
3. If `offset` was provided, Service first calls `Repository.Watch(types, offset, deliver)` to replay missed history; the adapter forwards each replayed event as a `SubscribeResponse`.
4. Once replay catches up to live, the same session continues to receive live events from step 1.3 above.
5. The `Repository` adapter is responsible for hiding the replay→live transition without gaps (JetStream durable consumers do this naturally with `DeliverByStartSequence`).

### Why providers don't write to NATS directly

The Service is the only writer to `Repository` so that:

- The persistence adapter can be swapped (NATS today, something else tomorrow) without touching providers.
- When a second provider lands, the Service can deduplicate / merge events for the same actor (the in-service "correlator" step) before anything is persisted.
- The Service can enrich, audit, or rate-limit on a single chokepoint.

Provider adapters never import the persistence package; persistence adapters never import providers.

---

## 3. Repository layout

| Path                                                                                                                 | Role                                                                                                                                         |
| -------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------- |
| [cmd/main.go](cmd/main.go)                                                                                           | Composition root: env config, logger, DI wiring, graceful shutdown                                                                           |
| [internal/core/ports/ports.go](internal/core/ports/ports.go)                                                         | `Service`, `Provider`, `Repository` interfaces                                                                                              |
| [internal/core/services/service.go](internal/core/services/service.go)                                               | Business logic — `Start` drives providers via `errgroup`; `handleEvent` calls `Repository.Write` unconditionally for every event (no dedup — change detection is in the repository layer); `Subscribe` delegates to `Repository.Watch`; `Publish` dispatches to providers |
| [internal/core/domain/domains.go](internal/core/domain/domains.go)                                                   | Domain types: `Actor`, `Event`, `State`, `Location`, `Point`, `Presence`, `Command`, `Directive`, `Zone`, `EventType`, `PresenceType`        |
| [internal/adapters/grpc/adapter.go](internal/adapters/grpc/adapter.go)                                               | `CoreAdapter` — implements `pb.CoreServiceServer`. `Subscribe` translates `SubscribeRequest` → domain, calls `Service.Subscribe` with a send closure that marshals each event into a one-event `SubscribeResponse`. `Publish` receive-loops, translates each directive via `pbDirectiveToDomain`, calls `Service.Publish`, echoes the returned status (non-OK keeps stream open). All proto↔domain helpers live here. |
| [internal/adapters/grpc/server/server.go](internal/adapters/grpc/server/server.go)                                   | gRPC server, health check, reflection, interceptor wiring                                                                                    |
| [internal/adapters/grpc/server/interceptors/monitoring.go](internal/adapters/grpc/server/interceptors/monitoring.go) | `MonitorStream()` — stream interceptor; logs handler errors by severity (WARN for InvalidArgument/NotFound/PermissionDenied, ERROR otherwise); swallows `context.Canceled`. |
| [internal/adapters/grpc/server/interceptors/validation.go](internal/adapters/grpc/server/interceptors/validation.go) | `ValidateStream()` — stream interceptor; runs `protovalidate.Validate` on each inbound message, rejects with `INVALID_ARGUMENT` on failure. Chained ahead of `MonitorStream` in server.go. |
| [internal/adapters/quuppa/](internal/adapters/quuppa/)                                                               | QUUPPA UDP listener — implements `Provider`. Parses real QPE 9.5+ `DefaultLocationAndInfo` JSON; runs the §7/§8 state machine + offline watchdog. Wire-format spec: [docs/QUUPPA.md](docs/QUUPPA.md) |
| `internal/adapters/vision/` _(planned, future)_                                                                      | Computer-vision provider — sibling to QUUPPA                                                                                                 |
| [internal/repositories/jetstream/](internal/repositories/jetstream/)                                                 | **Active** `ports.Repository` backed by NATS JetStream. Stream `ZONYX_EVENTS` (`zonyx.>`), four subject families: `zonyx.raw.<id>` (every datagram), `zonyx.zone.<id>` (zone changes), `zonyx.presence.<id>` (presence changes), `zonyx.command.<id>` (commands). `Write` always publishes to raw, then publishes a derived change event only when zone membership or presence type differs from cached state. Change state is kept in a write-through in-memory cache backed by the `ZONYX_ACTOR_STATE` KV bucket (survives restarts; no warm-up needed). `Watch` subscribes to the typed subject pattern for single-type requests; falls back to `zonyx.>` with Go-side filtering for multi-type. Offset = zero-padded decimal sequence. |
| `internal/repositories/valkey/` _(planned)_                                                                          | Future `ports.Repository` backed by Valkey Streams — sibling to JetStream. Directory scaffolded; no implementation yet.                     |
| `internal/core/services/correlator/` _(planned)_                                                                     | Fuses multiple provider streams into a single per-actor state. Pass-through while only one provider is active                                |
| [internal/mocks/](internal/mocks/)                                                                                   | Generated gomock stubs for `ports.Service` and `ports.Repository` (`MockService`, `MockRepository`). Produced by `go generate` on [internal/core/ports/ports.go](internal/core/ports/ports.go). Used by service-layer and adapter tests. |
| [internal/helpers/](internal/helpers/)                                                                               | Env helpers (`GetEnvOr` with type inference)                                                                                                 |
| [configs/](configs/)                                                                                                 | `develop.env.yaml` / `staging.env.yaml` / `production.env.yaml` (placeholders)                                                               |
| [build/package/Dockerfile](build/package/Dockerfile)                                                                 | Multi-stage build: `golang:1.26-alpine` → `distroless/static-debian13:nonroot`; `upx`-compressed binary. Uses `--mount=type=ssh` to forward the host ssh-agent for private module auth — key never lands in an image layer. Runtime sets `GOGC=200` / `GOMEMLIMIT=256MiB` for GC tuning. |
| [.air.toml](.air.toml)                                                                                               | Hot reload: `go build -o ./tmp/main ./cmd`, sources `.env`                                                                                   |
| [Makefile](Makefile)                                                                                                 | Common entry points: `make help`, `dev`, `build`, `run`, `test`, `vet`, `tidy`, `docker`, `clean`                                            |
| [.env.example](.env.example)                                                                                         | Template env file; copy to `.env` for local dev                                                                                              |
| [docker-compose.yml](docker-compose.yml)                                                                             | Runs `zonyx-core` + `nats:2-alpine` (JetStream, monitoring on 8222, `nats-data` volume). Forwards host ssh-agent via `ssh: [default]` for private module auth during build. |
| [.github/workflows/](.github/workflows/)                                                                             | `pull-request.yml` (build + `go test -cover -v ./...`, Go version pinned via `go-version-file: go.mod`), `deploy.dev.yml`, `deploy.prod.yml` |

Test framework: **gomock** for mocking `ports.Repository` and `ports.Service`; plain `testing` everywhere else. Generated mocks live in [internal/mocks/](internal/mocks/) (`MockRepository`, `MockService`) — regenerate with `go generate ./internal/core/ports/...`. Adapter-level and repository integration tests use plain `testing` — see [internal/adapters/quuppa/](internal/adapters/quuppa/) and [internal/repositories/jetstream/](internal/repositories/jetstream/).

---

## 4. The proto contract (authoritative)

Module: `github.com/ZonyxNagi/proto-zonyx-core` (pinned in [go.mod](go.mod) as `v0.0.0-20260519062958-f55e9dc0f4b0`).
Package: `zonyx.core.v1`.
Imported as: `pb "github.com/ZonyxNagi/proto-zonyx-core/pkg/api/v1"`.
Single source file: `api/proto/v1/index.proto` (also available locally under `~/go/pkg/mod/github.com/!zonyx!nagi/proto-zonyx-core@<ver>/`).

### Enums

- `EventType`: `EVENT_TYPE_UNSPECIFIED` (0, rejected by validation), `EVENT_TYPE_ZONE` (1), `EVENT_TYPE_PRESENCE` (2), `EVENT_TYPE_COMMAND` (3).
- `PresenceType`: `PRESENCE_TYPE_UNSPECIFIED` (0, rejected), `PRESENCE_TYPE_ACTIVE` (1), `PRESENCE_TYPE_INACTIVE` (2).

### Messages (with key validation rules)

- `Actor { string id [3..32]; map<string,string> metadata [≤32 pairs, key 1..64, val ≤256] }`
- `Zone { string id [3..32]; string name [3..64]; Polygon polygon (required) }` — carries a required geometric boundary. `Zone.Polygon { repeated Location.Point vertices [≥3] }` is a nested message defining the closed polygon.
- `Presence { PresenceType type (not 0); google.protobuf.Timestamp since (required, ≥ 2001-09-09T01:46:40Z) }`
- `Command { string id [3..64]; string name [1..64]; map<string,string> arguments [≤32, key 1..64, val ≤256] }` — embedded in `State.command` for `EVENT_TYPE_COMMAND` events; describes an actor-originated action (e.g. button push). Distinct from `Directive`.
- `Directive { string id [3..64]; string name [1..64]; map<string,string> arguments [≤32, key 1..64, val ≤256] }` — payload of `PublishRequest`; a control-plane instruction sent INTO the platform.
- `Location { Location_Point point (optional, X/Y/Z float32); repeated string zones [≤8 items, each 3..32] }` — coordinate snapshot + zone membership. `point` is present for `EVENT_TYPE_ZONE`; may be absent for presence/command events. `zones` carries Zone IDs.
- `State { Location location (optional); Presence presence (required); optional Command command }`
- `Event { string id [3..64]; EventType type (not 0); Actor actor (required); State state (required); google.protobuf.Timestamp timestamp (required, ≥ 2001-09-09T01:46:40Z) }`
  - **Message-level CEL constraint** (`event.command_state_consistency`): `EVENT_TYPE_COMMAND` requires `state.command` to be set; for any other event type `state.command` MUST be absent. The constraint is evaluated by protovalidate, so producers (including our QUUPPA adapter) need to enforce it on construction.
- `Configuration { repeated Zone zones [≥1] }` — complete zone layout for a provider deployment; delivered by `CoreService.ReadConfiguration`.
- `ReadConfigurationRequest {}` — empty; no parameters needed to open the configuration stream.
- `ReadConfigurationResponse { Configuration configuration (required) }` — one snapshot per stream message.

### Services

Single gRPC service: **`CoreService`**, with three RPCs.

- **`CoreService.Subscribe(SubscribeRequest) returns (stream SubscribeResponse)`**
  - `SubscribeRequest { repeated EventType types [unique, defined_only, not 0; empty list = all types]; optional string offset [3..64, pattern ^[A-Za-z0-9_-]+$] }`
  - `SubscribeResponse { repeated Event events [≥1]; string offset [3..64, pattern ^[A-Za-z0-9_-]+$] }`
- **`CoreService.Publish(stream PublishRequest) returns (stream PublishResponse)`**
  - `PublishRequest { Directive directive (required) }`
  - `PublishResponse { string directive_id [3..64]; google.rpc.Status status (required) }`
- **`CoreService.ReadConfiguration(ReadConfigurationRequest) returns (stream ReadConfigurationResponse)`**
  - Delivers the current `Configuration` (zone layout with polygon geometry) immediately on open, then streams updated snapshots whenever the configuration changes. Stream stays open until the client cancels.

### Semantics worth pinning down

- `Subscribe` is server-stream; client opens once, server pushes batched `events` until disconnect. `SubscribeResponse.events` MUST contain ≥ 1 event — there are no heartbeat frames; connection liveness is detected via gRPC keepalive (already configured in [server.go](internal/adapters/grpc/server/server.go)).
- `Publish` is the **control-plane channel**. `Directive`s are platform instructions (e.g. `register_device`, `update_zone`), not positioning telemetry. Positioning data flows in via internal `Provider` adapters and never goes through this RPC. Responses may arrive out of order — correlate by `directive_id`. A non-OK `status` does NOT close the stream; the sender may continue submitting further directives.
- `ReadConfiguration` is a server-push stream for zone layout. The server delivers the current `Configuration` (zones + polygon boundaries) immediately on open, then pushes an updated snapshot whenever the configuration changes. Clients open once and stay connected; reconnect semantics are the same as `Subscribe` (cancel + re-open).
- `offset` is opaque to the client but **must be URL-safe** (`[A-Za-z0-9_-]+`). Clients persist the last `offset` from each `SubscribeResponse` and pass it back on reconnect to get at-least-once replay. No offset → start from live head. The JetStream adapter encodes the offset as the stream sequence number decimal-stringified and zero-padded to ≥3 chars (e.g. `"001"`, `"042"`, `"1234"`), which satisfies the character set constraint.
- Validation is performed via [protovalidate](https://protovalidate.com); we MUST run a `protovalidate` interceptor before any handler logic.
- Idempotency is the consumer's responsibility, keyed on `Event.id`.

---

## 5. Implementation roadmap

Strict ordering — each step unblocks the next. Pick the lowest unfinished step.

1. **Domain types** in [internal/core/domain/domains.go](internal/core/domain/domains.go) — ✓ **done**. Internal Go mirrors of `Actor`, `Event`, `State`, `Location`, `Point`, `Presence`, `Command`, `Directive`, `Zone`. `State.Location` holds a `*Point` (X/Y/Z float64) plus `Zones []string`, mirroring the proto `Location` message. `time.Time` for `Presence.Since` and `Event.Timestamp` (converted to/from `*timestamppb.Timestamp` at the gRPC adapter boundary). Decoupled from generated proto types so `services` doesn't import `pb`.
2. **Port methods** in [internal/core/ports/ports.go](internal/core/ports/ports.go) — ✓ **done**:
   - `Service.Subscribe(ctx, types []domain.EventType, offset *string, send func(domain.Event, string) error) error`
   - `Service.Publish(ctx, d domain.Directive) (directiveID string, status *rpcstatus.Status, err error)`
   - `Repository.Write(ctx, domain.Event) (offset string, err error)` — persists an event and returns the cursor for it.
   - `Repository.Watch(ctx, types []domain.EventType, offset *string, deliver func(domain.Event, string) error) error` — **snapshot built into Watch**: `offset == nil` ⇒ deliver the current snapshot (latest event per actor matching `types`) then live; `offset != nil` ⇒ replay from that cursor then live. The replay→live seam is the adapter's responsibility.
   - `Provider.Run(ctx, emit func(domain.Event) error) error` — driven port. Each provider emits **fully-formed `domain.Event`s** for every meaningful update (e.g. the QUUPPA adapter emits a Zone event for every valid datagram). Change detection and deduplication are the repository's responsibility, not the provider's. The Service starts each registered `Provider` and supplies the `emit` callback. Multiple `Provider`s can be registered; the Service is responsible for fan-in.
3. **NATS JetStream Repository** in [internal/repositories/jetstream/](internal/repositories/jetstream/) — ✓ **done**:
   - Stream `ZONYX_EVENTS` with wildcard subject `zonyx.>`. Four subject families: `zonyx.raw.<id>` (every event), `zonyx.zone.<id>` (zone-set changes only), `zonyx.presence.<id>` (presence-type changes only), `zonyx.command.<id>` (commands, no dedup).
   - `Write` always publishes to `zonyx.raw.<id>`. Then, depending on event type: for Zone events it compares the incoming zone set against the cached state and publishes to `zonyx.zone.<id>` only on change; for Presence events it does the same for `zonyx.presence.<id>`; for Command it always publishes to `zonyx.command.<id>`. Zone and presence change-detection are independent.
   - Change state is held in a write-through in-memory cache (`map[string]actorState` guarded by `sync.RWMutex`) backed by the `ZONYX_ACTOR_STATE` JetStream KV bucket (`TTL` = `MaxAge`). Steady-state = nanosecond map lookup; KV consulted only on cache miss (first event per actor after process restart). No blocking warm-up on startup.
   - `Watch` with nil offset → `nats.DeliverLastPerSubject()` (snapshot + live); with non-nil offset → `nats.StartSequence(seq)` (replay + live). Single-type requests subscribe directly to the typed subject (e.g. `zonyx.zone.>`) so JetStream does the routing natively; multi-type requests subscribe to `zonyx.>` and filter in Go.
   - Offset encoding: `fmt.Sprintf("%03d", seq)` — zero-padded decimal, always ≥3 chars, satisfies `[A-Za-z0-9_-]{3,64}`.
   - `MaxAge` defaults to 7 days; configurable via `Config.MaxAge`. Stream and KV bucket provisioned idempotently in `NewRepository`.
   - Tests at [internal/repositories/jetstream/repository_test.go](internal/repositories/jetstream/repository_test.go) use an embedded NATS server (`github.com/nats-io/nats-server/v2/test`).
4. **QUUPPA provider** in [internal/adapters/quuppa/](internal/adapters/quuppa/) — ✓ **done**:
   - `Adapter.Run(ctx, emit)` binds UDP, reads QPE 9.5+ `DefaultLocationAndInfo` JSON datagrams ([parser.go](internal/adapters/quuppa/parser.go)), and runs the per-tag state machine in [derive.go](internal/adapters/quuppa/derive.go) translating QUUPPA's six logical events (`LOCATION_LOST`, `LOCATION_RESTORED`, `ZONE_ENTERED/EXITED`, `TAG_OFFLINE/ONLINE`) down to our snapshot-based `EVENT_TYPE_ZONE` and `EVENT_TYPE_PRESENCE` domain events.
   - A read-deadline-driven watchdog runs the offline sweep every `WatchdogTick` (2s) and emits `Presence{Inactive}` for any tag silent past `OfflineThreshold` (12s). Watchdog dedupes against `LOCATION_LOST` so a degrading tag doesn't double-emit.
   - Per-tag state lives in a map local to each `Run` invocation — single-goroutine, no mutex. Test seam `newWithClock` injects a fake clock for deterministic watchdog tests.
   - Wire-format spec and transition rules: [docs/QUUPPA.md](docs/QUUPPA.md) (§3 fields, §4 location-type ladder, §7 transitions, §8 pseudocode, §10 constants).
   - Malformed packets are logged and skipped; ctx cancel closes the socket and Run returns `ctx.Err()`.
   - **Follow-up**: device-originated button presses → `EVENT_TYPE_COMMAND` events (not yet specified in the QPE output target — needs vendor confirmation on which payload field carries the press).
5. **Service layer** in [internal/core/services/service.go](internal/core/services/service.go) — ✓ **done** (single-provider path):
   - `Service.Start(ctx)` launches every registered `Provider.Run` via `errgroup` (fail-fast). No warm-up or cache pre-population — the repository layer is self-healing on the first event per actor.
   - `handleEvent` calls `Repository.Write(event)` unconditionally for every event emitted by a provider. There is no in-service deduplication; change detection lives entirely in the repository. **Remaining**: forward to active Subscribe sessions; insert correlator when ≥2 providers are active.
   - `Service.Subscribe` delegates entirely to `Repository.Watch` — snapshot, replay, and live delivery are all handled by the JetStream consumer.
   - `Service.Publish` tries each registered provider's `Send`; returns `OK` on first acceptance, `UNIMPLEMENTED` if none handle the directive. Stream stays open in both cases.
   - `Service.ReadConfiguration` is a stub that blocks on `ctx.Done()` and returns `ctx.Err()` without sending anything. Configuration management (zone storage, update broadcast) is future work — until zones are registered there is nothing valid to deliver (the proto requires `Configuration.zones ≥ 1`).
6. **gRPC adapter** in [internal/adapters/grpc/adapter.go](internal/adapters/grpc/adapter.go) — ✓ **done**: single `CoreAdapter` implementing `pb.CoreServiceServer`:
   - `CoreAdapter.Subscribe` — translates `SubscribeRequest` → domain (`types`, `offset`), calls `Service.Subscribe` with a send closure that marshals each delivered event into a one-event `SubscribeResponse`. Closes on `ctx.Done()` (propagated via `stream.Context()`).
   - `CoreAdapter.Publish` — receive loop with `stream.Recv()`, translates each directive via `pbDirectiveToDomain`, calls `Service.Publish`, sends `PublishResponse` with the returned status. **Per-message failures (non-OK status or service error) are echoed back without closing the stream.**
   - `CoreAdapter.ReadConfiguration` — calls `Service.ReadConfiguration` with a send closure that marshals each `domain.Configuration` snapshot into a `ReadConfigurationResponse`. Blocks until ctx is cancelled (stub; see note on Service below).
   - Proto↔domain helpers: `pbEventTypeToDomain`, `pbDirectiveToDomain` (pb→domain); `domainEventToPb`, `domainActorToPb`, `domainStateToPb`, `domainLocationToPb`, `domainPointToPb` (float64→float32), `domainPresenceToPb`, `domainCommandToPb`, `domainEventTypeToPb`, `domainPresenceTypeToPb`, `domainConfigurationToPb`, `domainZoneToPb`, `domainPolygonToPb` (domain→pb).
7. **Protovalidate interceptor** in [internal/adapters/grpc/server/interceptors/validation.go](internal/adapters/grpc/server/interceptors/validation.go) — ✓ **done**: `ValidateStream()` runs `protovalidate.Validate` on each inbound message, rejects with `INVALID_ARGUMENT` on failure. Chained in [server.go](internal/adapters/grpc/server/server.go) ahead of `MonitorStream()`. (Unary interceptor omitted — both RPCs are streaming.)
8. **Bootstrap** in [cmd/main.go](cmd/main.go) — ✓ **done**: connects to NATS via `NATS_URL` (`MaxReconnects(-1)`), builds `jetstream.NewRepository` (stream provisioned idempotently), wires QUUPPA provider and JetStream repo into the Service, orchestrates everything under a single `errgroup` rooted at `signal.NotifyContext`. Teardown goroutine calls `srv.Stop()` then `nc.Drain()` before the group unwinds.
9. **Tests** — colocated. Today: (a) QUUPPA parser fixtures from real §3/§7 payloads, table-driven `deriveEvents` covering every §7 transition row, watchdog + online/offline round-trips with a fake clock under [internal/adapters/quuppa/](internal/adapters/quuppa/); (b) gRPC `CoreAdapter` `bufconn` round-trips: `Publish` wiring (directive IDs and status echoed, empty-directive handled), `Subscribe` wiring (types/offset propagation to service, domain events marshalled into `SubscribeResponse`) at [internal/adapters/grpc/](internal/adapters/grpc/); (c) gRPC server wiring + health-check tests at [internal/adapters/grpc/server/](internal/adapters/grpc/server/); (d) `MonitorStream` and `ValidateStream` interceptor tests including protovalidate rejection of `EVENT_TYPE_UNSPECIFIED` and short directive ids at [internal/adapters/grpc/server/interceptors/](internal/adapters/grpc/server/interceptors/); (e) JetStream repository integration tests (Write — raw offset, first/same/changed zone, first/same/changed presence, zone↔presence independence, KV persistence across `Repository` instances; Watch — live, replay, `DeliverLastPerSubject` snapshot, cancel) using embedded NATS at [internal/repositories/jetstream/](internal/repositories/jetstream/); (f) service-layer tests (`handleEvent` calls `Write` unconditionally for Zone, Presence, Command, and repeated identical events; write-error propagation) using gomock at [internal/core/services/](internal/core/services/). Still missing: correlator diff-detection tests once it lands; happy-path provider→Service→NATS→Subscribe end-to-end round-trip.
10. **Observability** — wire OpenTelemetry tracing on the gRPC server, span the provider→correlator→event-store path, and emit counters per `EventType` and per provider. (OTel is not yet in `go.mod`; add `go.opentelemetry.io/otel` and the gRPC instrumentation contrib when starting this step.)

### When the second provider lands

Adding computer vision later should require only:

- A new `internal/adapters/vision/` adapter implementing `Provider` (does its own change detection internally, just like QUUPPA).
- Switching on the correlator step inside the Service (deduplicate / merge by `Actor.id`).
- Registering the new provider in [cmd/main.go](cmd/main.go).

No changes to the gRPC layer, the event store, or the proto contract.

---

## 6. Conventions and gotchas

- **Providers are driven, not driving.** The Service starts each `Provider.Run` and owns the lifecycle. The UDP socket is opened _inside_ the QUUPPA adapter's `Run` method, not in `main`. This keeps providers swappable and keeps `cmd/main.go` provider-agnostic.
- **The Service is the only writer to the `Repository` port.** Provider adapters hand `domain.Event`s to the Service; the Service writes through `Repository.Write`. The JetStream adapter is the only thing that knows about NATS subjects, streams, and sequence numbers.
- **Private module access.** `go mod download` inside Docker needs SSH access for `github.com/ZonyxNagi/*`. The Dockerfile uses `--mount=type=ssh` (BuildKit) to forward the host `ssh-agent` socket — the key is never copied into the image. Before building, make sure your key is loaded: `ssh-add ~/.ssh/id_ed25519`. On macOS the system agent usually has it already. `docker compose up --build` and `make docker` both pass `--ssh default` automatically. For local `go build` outside Docker your existing SSH config is used directly.
- **Go version is the source of truth in `go.mod` (`1.26`).** The Dockerfile is pinned to the same major; CI reads the version via `go-version-file: go.mod`. When bumping, change `go.mod` and the Dockerfile `FROM` line together.
- **No unary interceptor chain today.** All three RPCs are streaming, so only `grpc.ChainStreamInterceptor` is wired (`ValidateStream()` → `MonitorStream()`). If a unary RPC is ever introduced, also add `grpc.ChainUnaryInterceptor(...)` at [server.go](internal/adapters/grpc/server/server.go).
- **Hot reload** sources `.env` on start (`export $(egrep -v '^#' .env | xargs)`). Use [.env.example](.env.example) as the template; add new keys there in the same PR that introduces them so others know to update their local `.env`.
- **Makefile is the canonical entry point** for build/test/run/docker. CI calls `go` directly today, but the Makefile mirrors the same commands so local and CI stay aligned. Run `make help` to see targets.
- **Proto contract is in sync with this CLAUDE.md** as of module version `v0.0.0-20260519062958-f55e9dc0f4b0`. When the proto changes, refresh section 4 and the type names referenced in section 5.

---

## 7. Environment variables

| Var                  | Default                               | Purpose                                                                     |
| -------------------- | ------------------------------------- | --------------------------------------------------------------------------- |
| `SERVICE_NAME`       | `unknown`                             | Logged on startup; informational                                            |
| `LOG_LEVEL`          | (empty → slog default `INFO`)         | slog level (`DEBUG` / `INFO` / `WARN` / `ERROR`)                            |
| `PORT`               | `8080`                                | gRPC listen port                                                            |
| `NATS_URL`           | `nats://localhost:4222`               | JetStream connection                                                        |
| `QUUPPA_LISTEN_ADDR` | `:9090`                               | UDP address the QUUPPA provider binds to                                    |

---

## 8. Reference

- **Proto source**: pinned in [go.mod](go.mod). Locally available at `~/go/pkg/mod/github.com/!zonyx!nagi/proto-zonyx-core@<ver>/api/proto/v1/index.proto`. README and generated HTML docs (`docs/index.html`) live alongside it.
- **NATS JetStream Go client**: `github.com/nats-io/nats.go` — JetStream API under `nats.JetStream(...)`.
- **Embedded NATS for tests**: `github.com/nats-io/nats-server/v2/test`.
- **protovalidate-go**: `github.com/bufbuild/protovalidate-go`.
- **QUUPPA**: [docs/QUUPPA.md](docs/QUUPPA.md) holds the UDP wire-format spec (QPE 9.5+ `DefaultLocationAndInfo`, JSON, `triggerMode=LastSeenUpdate`) and the §7 transition table the adapter implements. Test with the QUUPPA Site Planner's UDP simulator when available; deployments must be configured per §2 for `onDataChange=$(location.type),$(location.zone.ids)` and `stopOutputIfTagIsNotSeenIn=12`.
