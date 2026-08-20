# REST adapter rules

- One router module per resource under `routers/`, registered in
  `server/api/app.py` with a `/api/v1/<resource>` prefix and matching tag.
- Every router is built with `APIRouter(route_class=KitaruAPIRoute)` from
  `route.py`. `KitaruAPIRoute` commits the request's database session,
  attached to `request.state` by `get_session`, before the response is
  returned. Any exception skips the commit and pending writes roll back when
  the session closes.
- Status codes: 201 for create, 200 for read and update, 204 for delete.
- Updates are partial and use `PATCH /{id}`, never `PUT`. The update body
  carries only the mutable fields.
- PATCH always flows through `<x>_update_to_command` on `set_fields`, even for
  one-field bodies, so an absent field and an explicit null stay
  distinguishable.
- Every route takes the `authorize` dependency and passes `actor=actor` to the
  service. Explicit exceptions, such as login, are allowed.
- Domain errors map to responses in the app-level exception handlers (404 for
  `NotFoundError`, 409 for `ConflictError`, 422 for `ValidationError`).
  Routers never catch domain errors and never raise `HTTPException` for them.
- One mapping module per resource under `mapping/`. Response functions
  construct DTOs explicitly field by field and assert timestamps are not
  `None`. Never use `model_validate(obj, from_attributes=True)`.
- List routes bind query parameters with
  `params: Annotated[XListParams, Query()]`, convert them to the application
  filter through `<x>_list_params_to_filter` in the mapping module, and build
  the `Page` envelope from the items and next cursor the service returns.
- Service dependencies live in `dependencies.py` as
  `get_<resource>_service(session)` returning the service bound to the SQL
  repository.
- Route docstrings state the status codes clients observe, including error
  cases.
