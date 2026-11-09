# REST adapter rules

- One router module per resource under `routers/`, registered in
  `server/api/app.py` with a `/api/v1/<resource>` prefix and matching tag.
- Status codes: 201 for create, 200 for read and update.
- The update body carries only the mutable fields.
- PATCH always flows through `<x>_update_to_command` on `set_fields`, even for
  one-field bodies, so an absent field and an explicit null stay
  distinguishable.
- Routes pass the `actor` from their `authorize` dependency to the service as `actor=actor`.
- Domain errors map to responses in the app-level exception handlers (404 for
  `NotFoundError`, 409 for `ConflictError`, 422 for `ValidationError`).
  Routers never catch domain errors and never raise `HTTPException` for them.
- One mapping module per resource under `mapping/`. Response functions construct DTOs explicitly field by field and assert timestamps are not `None`; `just python-standards-check` (KIT007) rejects `from_attributes`.
- List routes bind query parameters with
  `params: Annotated[XListParams, Query()]`, convert them to the application
  filter through `<x>_list_params_to_filter` in the mapping module, and build
  the `Page` envelope from the items and next cursor the service returns.
- Service dependencies live in the `dependencies` package as
  `get_<resource>_service(session)` returning the service bound to the SQL
  repository: services that build task transitions in `execution.py`,
  credential and authorization providers in `auth.py`, and the rest in
  `services.py`. The package `__init__.py` re-exports every provider, so
  routes import them from `kitaru.server.adapters.rest.dependencies`.
- Route docstrings state the status codes clients observe, including error
  cases.

`tests/server/test_route_conventions.py` fails for a route built without `KitaruAPIRoute`, a `PUT` route, a delete that does not answer 204, and a route without an `authorize` dependency unless it is listed there as public with a reason.
