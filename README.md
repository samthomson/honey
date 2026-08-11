# honey

Nostr relay logging proxy. Sits transparently between clients and your relay, passing all traffic through unchanged while logging IP addresses, published events, and subscription requests.

## How It Works

```
Clients (WSS) → Honey (:8080) → Your Relay Backend
                    │
                    └── SQLite (IPs, events, subscriptions)
```

Honey is a transparent WebSocket proxy. Clients connect to Honey as if it were the relay itself. All Nostr messages pass through unchanged — Honey just logs metadata along the way.

### What gets logged

- **Connections** — IP, user-agent, connect/disconnect timestamps
- **Published events (EVENT)** — IP, pubkey, kind, event ID, tags, content length
- **Subscriptions (REQ)** — IP, subscription ID, full filter JSON
- **Subscription closes (CLOSE)** — IP, subscription ID
- **Auth (NIP-42)** — logged as events

Message content is **never** stored — only its length.

## Quick Start

```bash
cp .env.example .env
# Edit BACKEND_HOST to point to your relay

# Dev (hot reload):
docker compose --profile dev up

# Prod:
docker compose up -d
```

## Production (Dokploy)

1. Create a new project in Dokploy pointing to this repo
2. Set environment variables:
   - `BACKEND_HOST` — `your-relay:port`
   - `ADMIN_TOKEN` — optional, protects the dashboard
3. Deploy — Dokploy handles TLS via Let's Encrypt
4. Repoint your relay's DNS to the Dokploy service

SQLite database persists in a Docker named volume automatically.

## Configuration

| Env Var | Default | Description |
|---------|---------|-------------|
| `BACKEND_HOST` | `localhost:8008` | Host (and optional port) of the real relay |
| `BACKEND_SCHEME` | `wss` | `wss` for TLS (default), `ws` for plain |
| `ADMIN_TOKEN` | _(none)_ | Optional Bearer token to protect the admin API |

## Tech Stack

- **Node.js** + `ws` — WebSocket proxy
- **SQLite** (`node:sqlite`) — zero-dependency data storage
- **Express** — admin API + dashboard server
- **Vanilla JS** — dashboard frontend (no build step)

## License

MIT
