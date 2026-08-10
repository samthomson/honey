# honey

Nostr relay logging proxy. Sits transparently between clients and your relay, passing all traffic through unchanged while logging IP addresses, published events, and subscription requests.

Designed for research — built to gather data for a talk on nostr relay usage at a privacy-focused Bitcoin conference.

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
# Edit BACKEND_WS_URL and BACKEND_HTTP_URL to point to your real relay
docker compose up -d
```

Then visit `http://localhost:8080/` for the dashboard.

## Configuration

| Env Var | Default | Description |
|---------|---------|-------------|
| `BACKEND_WS_URL` | `ws://localhost:8008` | WebSocket URL of the real relay |
| `BACKEND_HTTP_URL` | `http://localhost:8008` | HTTP URL for NIP-11 relay info passthrough |
| `PORT` | `8080` | Port to listen on |
| `ADMIN_TOKEN` | _(none)_ | Optional Bearer token to protect the admin API |
| `DATA_DIR` | `./data` | SQLite database location |

## Deployment (Dokploy)

1. Create a new project in Dokploy
2. Point it at this repo
3. Set environment variables (at minimum `BACKEND_WS_URL` and `BACKEND_HTTP_URL`)
4. Deploy — Dokploy handles TLS via Let's Encrypt
5. Repoint your relay's DNS to the Dokploy service

## Tech Stack

- **Node.js** + `ws` — WebSocket proxy
- **SQLite** (`better-sqlite3`) — zero-ops data storage
- **Express** — admin API + dashboard server
- **Vanilla JS** — dashboard frontend (no build step)

## License

MIT
