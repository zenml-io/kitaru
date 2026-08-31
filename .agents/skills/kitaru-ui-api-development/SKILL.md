---
name: kitaru-ui-api-development
description: Add, reuse, or change a frontend-specific Kitaru REST response under /api/v1/ui and its OpenAPI contract in zenml-frontend-monorepo. Use when a dashboard screen needs a purpose-built data shape, not for ordinary reusable resources or UI bundle releases.
---

# Kitaru UI API Development

Use this for changes that cross the Kitaru server API and the Kitaru UI in `zenml-frontend-monorepo`. Load the same-name `kitaru-dev` repo skill for the current host for general commands and PR guidance. Use `FRONTEND-TESTING.md` and the current host's `kitaru-release` repo skill only when the change also affects UI bundling, serving, selection, or publication.

These rules reflect the current UI endpoints, their frontend consumers, and frontend maintainer guidance.

## Decide whether the route belongs under `/api/v1/ui`

Search the live routers, OpenAPI contract, and frontend modules for an existing route and consumer before designing another one. If they already satisfy the requested screen, verify that flow and do not add a duplicate endpoint.

Use a UI-specific endpoint when a frontend screen needs a response shape that the existing resource APIs do not provide. For example, `/api/v1/sessions` returns sessions without their evaluations, while `/api/v1/ui/sessions` returns sessions together with their evaluations for the sessions table. Another current endpoint aggregates evaluations across an experiment run for a frontend view.

Prefer the normal `/api/v1/<resource>` API when the behavior is a reusable domain operation or an external SDK consumer would reasonably need it. Do not add a UI endpoint merely to move ordinary presentation logic to the server.

Before implementation, state:

- the concrete frontend consumer and the data shape it needs
- why the contract is UI-specific rather than a reusable resource API
- whether the response is a pageable collection, a singular resource, or a bounded aggregate, with its ordering, pagination, caps, missing-data, and empty-state semantics
- whether the response can be assembled through current application services

There is no established precedent for frontend-only mutations. If the request adds one, stop and ask the maintainers to agree on its placement. New domain behavior and generally useful external APIs should go through the normal resource checklist instead.

## Backend shape

Keep frontend-specific DTOs in `src/kitaru/api_models/v1/ui.py` and routes in `src/kitaru/server/adapters/rest/routers/ui.py`, registered under `/api/v1/ui` in `src/kitaru/server/api/app.py`.

Follow `src/kitaru/server/adapters/rest/AGENTS.md`:

- use `APIRouter(route_class=KitaruAPIRoute)`
- require `authorize` and pass the resulting `actor` to every application service
- compose current service and repository interfaces instead of querying the database directly or bypassing application layers
- use explicit response DTO mappings and let app-level handlers map domain errors
- state client-visible statuses in route docstrings

Paginate a UI endpoint when it returns a collection that clients need to page through, using the same pagination contract as other collection endpoints. Do not add artificial pagination to singular resources or bounded aggregate responses.

UI read models may internally traverse all pages needed for correct joins or aggregates, but review the cost of that full traversal and define and test the response bounds. Treat result ordering, truncation, missing evaluations, duplicate names, type grouping, and `null` values as API contract, not implementation detail. The current experiment-run aggregate covers all replays; do not assume that is correct for a comparison screen that may need a shared cohort or other subset. Keep frontend keys aligned with the backend grouping, including data type when the same evaluation name can carry more than one type.

Do not add SDK, CLI, or MCP parity automatically for a UI-only read model. If another consumer needs the same contract, stop and decide whether to promote it to a normal resource API.

## Frontend consumer

The generated frontend types live in `zenml-frontend-monorepo/shared/kitaru/src/api/openapi.d.ts`. Apply the same OpenAPI rules as for any other endpoint: regenerate the declarations when the endpoint adds or changes a response schema; no new frontend type is needed when it returns an existing schema. Regenerate from a server exposing the current `openapi.json`; never hand-edit that file.

Keep the transport call and API-to-domain mapping in the owning module under `shared/kitaru/src/modules/`. Add focused tests for the request, mapping, empty states, errors, and any cap or ordering that affects what the user sees. Treat backend and frontend delivery like any other cross-repository API change: verify compatibility and deployment order, but do not impose a blanket same-merge requirement.

The existing generation command is run from `shared/kitaru/`:

```bash
pnpm generate:types -- http://localhost:8000
```

## Validation

In Kitaru:

- update `tests/server/test_route_manifest.py` when a route is added, removed, or renamed
- add focused ASGI route tests beside `tests/server/test_ui_api.py` or `tests/server/test_ui_experiment_runs_api.py`
- run the focused tests and `just openapi-check`
- regenerate `openapi/openapi.json` with `uv run python scripts/generate_openapi.py` when a route or schema changes, then the generated TypeScript types with `pnpm run generate`
- run `just check` and the relevant broader server tests

In `zenml-frontend-monorepo`, regenerate the OpenAPI declarations, then run the shared Kitaru package's typecheck and tests plus the affected Kitaru UI checks. Exercise the real UI flow against the changed backend when the environment is available.

## Reviewer focus

Ask reviewers to verify that the response shape is genuinely specific to a frontend screen, that pageable collections use the normal pagination contract, and that caps, ordering, missing-data, and error semantics match what the UI should promise. Exercise the affected OSS or managed Pro flow when the endpoint changes one; there is no blanket requirement to test unrelated frontend deployments.
