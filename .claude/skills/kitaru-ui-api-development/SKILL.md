---
name: kitaru-ui-api-development
description: Add, reuse, or change a frontend-specific Kitaru REST read model under /api/v1/ui and its typed consumer in zenml-frontend-monorepo. Use when the dashboard needs server-side joins or aggregates, not for ordinary reusable resources or UI bundle releases.
---

# Kitaru UI API Development

Use this for changes that cross the Kitaru server API and the Kitaru UI in `zenml-frontend-monorepo`. Load the same-name `kitaru-dev` repo skill for the current host for general commands and PR guidance. Use `FRONTEND-TESTING.md` and the current host's `kitaru-release` repo skill only when the change also affects UI bundling, serving, selection, or publication.

These are conservative initial rules inferred from the current UI endpoints and their frontend consumers. Ask the Kitaru frontend maintainer to review any new endpoint's placement and contract; do not turn an inference here into policy by silently expanding it.

## Decide whether the route belongs under `/api/v1/ui`

Search the live routers, OpenAPI contract, and frontend modules for an existing route and consumer before designing another one. If they already satisfy the requested screen, verify that flow and do not add a duplicate endpoint.

Use a UI-specific endpoint for a read model that the frontend cannot derive correctly or efficiently from the normal paginated resource APIs. Current examples join evaluations to each session page and aggregate all evaluations across an experiment run. Both avoid frontend-side page caps and differing aggregation semantics.

Prefer the normal `/api/v1/<resource>` API when the behavior is a reusable domain operation or an external SDK consumer would reasonably need it. Do not add a UI endpoint merely to move ordinary presentation logic to the server.

Before implementation, state:

- the concrete frontend consumer and the incorrect, capped, or wasteful request sequence it replaces
- why the contract is UI-specific rather than a reusable resource API
- ordering, pagination, caps, missing-data, and empty-state semantics
- whether the response can be assembled through current application services

If the request adds a mutation, new domain behavior, or a generally useful external API, stop and ask whether it should go through the normal resource checklist instead. Do not create a frontend-only write path by default.

## Backend shape

Keep frontend-specific DTOs in `src/kitaru/api_models/v1/ui.py` and routes in `src/kitaru/server/adapters/rest/routers/ui.py`, registered under `/api/v1/ui` in `src/kitaru/server/api/app.py`.

Follow `src/kitaru/server/adapters/rest/AGENTS.md`:

- use `APIRouter(route_class=KitaruAPIRoute)`
- require `authorize` and pass the resulting `actor` to every application service
- compose current service and repository interfaces instead of querying the database directly or bypassing application layers
- use explicit response DTO mappings and let app-level handlers map domain errors
- state client-visible statuses in route docstrings

UI read models may traverse all pages needed for correct joins or aggregates, but review the cost of that full traversal and define and test the response bounds. Treat result ordering, truncation, missing evaluations, duplicate names, type grouping, and `null` values as API contract, not implementation detail. The current experiment-run aggregate covers all replays; do not assume that is correct for a comparison screen that may need a shared cohort or other subset. Keep frontend keys aligned with the backend grouping, including data type when the same evaluation name can carry more than one type.

Do not add SDK, CLI, or MCP parity automatically for a UI-only read model. If another consumer needs the same contract, stop and decide whether to promote it to a normal resource API.

## Frontend consumer

The generated frontend types live in `zenml-frontend-monorepo/shared/kitaru/src/api/openapi.d.ts`. Regenerate them from a server exposing the current `openapi.json`; never hand-edit that file.

Keep the transport call and API-to-domain mapping in the owning module under `shared/kitaru/src/modules/`. Add focused tests for the request, mapping, empty states, errors, and any cap or ordering that affects what the user sees. Update the actual consumer in the same change or explain why the backend endpoint must land first.

The existing generation command is run from `shared/kitaru/`:

```bash
pnpm generate:types -- http://localhost:8000
```

## Validation

In Kitaru:

- update `tests/server/test_route_manifest.py` when a route is added, removed, or renamed
- add focused ASGI route tests beside `tests/server/test_ui_api.py` or `tests/server/test_ui_experiment_runs_api.py`
- run the focused tests and `just openapi-check`
- regenerate `openapi/openapi.json` with `uv run python scripts/generate_openapi.py` when a route or schema changes
- run `just check` and the relevant broader server tests

In `zenml-frontend-monorepo`, regenerate the OpenAPI declarations, then run the shared Kitaru package's typecheck and tests plus the affected Kitaru UI checks. Exercise the real UI flow against the changed backend when the environment is available.

## Maintainer review

Ask the frontend maintainer to confirm these points in the PR:

1. Does this read model belong under `/api/v1/ui`, or should it be a reusable resource endpoint?
2. Are its caps, ordering, missing-data, and error semantics what the UI should promise?
3. Must the backend endpoint and typed frontend consumer merge together?
4. Which OSS Kitaru UI and managed Pro flows need live verification before the draft is ready?
