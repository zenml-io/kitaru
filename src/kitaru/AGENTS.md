# Kitaru package rules

The package has three independent top-level parts. `client` and `server` never
import each other and both sit on `api_models`. import-linter enforces this,
along with the server layering (adapters above application above domain).

- `api_models/`: Versioned request and response DTOs shared by server and SDK.
- `client/`: Async SDK making REST calls.
- `server/`: FastAPI server in layers (API, application, domain,
  infrastructure adapters).

## New resource checklist

Adding a resource named `Order` touches these places. Skipping one produces a
half-wired resource that still imports cleanly, so walk the whole list.

1. `server/domain/order.py`: entity plus domain errors.
2. `server/application/interfaces/order_repository.py`: repository Protocol.
3. `server/application/models/orders.py`: filter model.
4. `server/application/services/order_service.py`: use cases.
5. `server/adapters/db/schemas/order.py`: ORM table, exported from
   `server/adapters/db/schemas/__init__.py`.
6. New Alembic revision under `server/database/migrations/versions/`.
7. `server/adapters/db/order_repository.py`: SQL repository.
8. `server/adapters/rest/mapping/orders.py`: DTO conversions.
9. `server/adapters/rest/routers/orders.py`: routes, registered in
   `server/api/app.py`, service dependency added to
   `server/adapters/rest/dependencies.py`.
10. `api_models/v1/orders.py`: request and response DTOs.
11. `client/resources/orders.py`: SDK resource, registered in
    `client/api_client.py`.
12. Regenerate `openapi/openapi.json` via `scripts/generate_openapi.py`.
13. Tests for all surfaces listed in `tests/AGENTS.md`, plus a fake repository
    in `tests/conftest.py`.
