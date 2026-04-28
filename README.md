# zonyx-core

Dispatcher between real-world positioning event sources (QUUPPA today;
computer-vision planned) and gRPC API clients. Hexagonal architecture; see
[CLAUDE.md](CLAUDE.md) for the full architecture, port contracts, and
implementation roadmap.

## Quick start

```sh
cp .env.example .env
make dev      # hot reload via air
make test     # go test ./...
make build    # produces ./tmp/main
make help     # list all targets
```

### Docker

```sh
docker compose up --build
```

Starts `zonyx-core` and a NATS JetStream node (`nats:2-alpine`). JetStream data persists in the `nats-data` Docker volume.

## Layout

- `cmd/` — composition root.
- `internal/core/` — domain types, ports, services.
- `internal/adapters/` — gRPC inbound, QUUPPA inbound provider.
- `internal/repositories/` — persistence (`jetstream/` — NATS JetStream backed; `valkey/` — planned).

For everything else (proto contract, env vars, conventions), read [CLAUDE.md](CLAUDE.md).

## Vendor references

- [docs/QUUPPA.md](docs/QUUPPA.md) — QUUPPA Positioning Engine UDP wire format
  and event-derivation rules. Source of truth for the QUUPPA provider adapter
  in `internal/adapters/quuppa/`.
