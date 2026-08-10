# AGENTS.md

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
