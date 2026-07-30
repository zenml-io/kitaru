# Kitaru Dev Dashboard

A read-only web UI for browsing everything a Kitaru spec-v2 server holds:
sessions (with full traces and evaluations), agents and versions, cohorts,
experiments and their runs, replays, evaluators, and ops state (workers,
jobs, tasks). Built for backend developers who want to see server state
without curl.

## Running it

```bash
# 1. Start a kitaru server (from the repo root). Either docker compose:
docker compose up
# ...or a local postgres + uvicorn with auth disabled:
docker compose up -d db
KITARU_SERVER_DB_HOST=localhost KITARU_SERVER_DB_PORT=5433 \
KITARU_SERVER_DB_USER=postgres KITARU_SERVER_DB_PWD=password \
KITARU_SERVER_SECRET_ENCRYPTION_KEY=dev \
uv run python -m kitaru.server.api.main

# 2. Optionally seed demo data:
uv run python dashboard/scripts/seed.py            # [--url ...] [--api-key ...]

# 3. Start the dashboard:
just dashboard                                     # or: cd dashboard && pnpm install && pnpm run dev
```

The dev server proxies `/v1` and `/health` to the kitaru server, so the
browser stays same-origin (the server has no CORS middleware). Point it at a
different server with:

```bash
KITARU_SERVER_URL=http://localhost:9000 just dashboard
```

## Authentication

The server's default `KITARU_SERVER_AUTH_SCHEME=none` needs no credentials.
Against an `auth_scheme=local` server (the committed docker-compose default),
set an API key via the gear icon in the top-right corner — it is sent as a
bearer token and stored in localStorage. The status bar shows the server's
auth scheme, version, and reachability.

This dashboard is read-only and deliberately never calls
`GET /v1/tasks/{id}/spec` or fetches secret values — both expose decrypted
secret material.

## Development

```bash
just dashboard-check    # biome lint + tsc, run before committing
just dashboard-build    # production build (dist/)
pnpm run fix            # auto-fix lint/format issues
pnpm run gen:api        # regenerate src/api/schema.d.ts from ../openapi/openapi.json
```

`src/api/schema.d.ts` is generated from the committed OpenAPI contract and
checked in, so installs are hermetic and API drift shows up in diffs. Re-run
`pnpm run gen:api` after changing the server API.

Conventions match the `docs/` app: pnpm (lockfile committed), Node 22
(`.node-version`), Tailwind CSS v4, Biome for lint/format. No node tooling
at the repo root.

TODO: add a path-filtered CI job (mirroring `.github/workflows/docs.yml`
setup steps) running `pnpm run check && pnpm run build` once the spec-v2
branch stabilizes.
