# AGENTS.md

## Principles

- **No fallbacks.** Something works or it doesn't. If it doesn't, fail and throw errors. Surface errors, don't plan for failure.
- **No default env vars for required services.** If a value is needed, it must be explicitly set. No silent defaults that might be wrong.
- **Hardcode service-to-service URLs within a compose project.** The hostname is always the container name. It won't change. No env var needed.

## Boundaries

- Do not include personal context, conversations, or private details in public files (README, comments, commit messages)
- Commit as: `HAL 9000 <hal9000.zehy2@4wrd.cc>`
- Default branch: `master`
- Never force-push
- Ask before adding new dependencies
- Honey's codebase: src/ for app logic, public/ for dashboard, tests TBD
- Architecture decisions: keep it simple, minimal deps, no frameworks unless asked
- Geocoding/enrichment: separate project later, not in this repo

## Deployment

- Prod target: Dokploy (Docker)
- Dev target: local Node.js with hot reload
- SQLite DB stored in named Docker volume (prod) or ./data (dev)
