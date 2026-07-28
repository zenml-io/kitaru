# Kitaru package rules

The package has four independent top-level parts. `client` and `server` never
import each other and both sit on `api_models` and `analytics`. import-linter
enforces this, along with the server layering (adapters above application
above domain).

- `analytics/`: Async analytics client and event source tracking.
- `api_models/`: Versioned request and response DTOs shared by server and SDK.
- `client/`: Async SDK making REST calls.
- `server/`: FastAPI server in layers (API, application, domain,
  infrastructure adapters).

Module naming: modules that define per-entity types are singular
(`domain/order.py`, `orm/order.py`, `application/models/order.py`,
`api_models/v1/order.py`). Routers and client resources are named after the
URL segment they serve (`/v1/orders` → `orders.py`), and mapping modules
follow their router.

## New resource checklist

Adding a resource named `Order` touches these places. Skipping one produces a
half-wired resource that still imports cleanly, so walk the whole list.

1. `server/domain/order.py`: entity plus domain errors.
2. `server/application/interfaces/order_repository.py`: repository Protocol.
3. `server/application/models/order.py`: filter model.
4. `server/application/services/order_service.py`: use cases.
5. `server/adapters/db/orm/order.py`: `OrderORM` class, exported from
   `server/adapters/db/orm/__init__.py`.
6. New Alembic revision under `server/database/migrations/versions/`.
7. `server/adapters/db/repositories/order_repository.py`: SQL repository.
8. `server/adapters/rest/mapping/orders.py`: DTO and filter conversions.
9. `server/adapters/rest/routers/orders.py`: routes, registered in
   `server/api/app.py`, service dependency added to
   `server/adapters/rest/dependencies.py`.
10. `api_models/v1/order.py`: request, list params, and response DTOs.
11. `client/resources/orders.py`: SDK resource, registered in
    `client/api_client.py`.
12. Regenerate `openapi/openapi.json` via `scripts/generate_openapi.py`.
13. Tests for all surfaces listed in `tests/AGENTS.md`, plus a fake repository
    in `tests/conftest.py`.
