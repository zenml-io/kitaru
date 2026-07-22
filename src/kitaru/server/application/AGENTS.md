# Application layer rules

- Services take the repository Protocol in the constructor and call only the
  interface. Never import an adapter.
- Auth context is an explicit `actor: AuthContext` parameter on every use-case
  method, always last. Never a field on a request DTO or command.
- Simple operations take raw values. Writes with roughly five or more fields
  take an application-owned command model defined next to the service, never
  the API request DTO.
- List operations take an application-owned filter model extending
  `FrozenModel` with `page` and `page_size` fields, defined in `models/`.
  Pagination is bounded on the model, not only on the router query parameters,
  so validation holds however the filter is constructed:
  `page: PositiveInt = 1` and
  `page_size: int = Field(default=20, ge=1, le=1000)`.
- Use cases return domain objects and raise domain errors. No application
  output models, no HTTP concepts.
- Uniqueness is enforced by database constraints, not by lookups. Do not
  pre-check before a write, the repository translates the constraint violation
  into the domain error.
- Repository interfaces are Protocols in `interfaces/`, one module per
  resource, written in domain terms. Only `query` takes a filter model. The
  write and read methods are named `create`, `get`, `query`, `update`, and
  `delete`. The domain mutator for a single field is `update_<field>`.
- The application layer never imports API schemas.
