# Application layer rules

- Services take the repository Protocol in the constructor and call only the
  interface. Never import an adapter.
- Auth context is an explicit `actor: AuthContext` parameter on every use-case
  method, always last. Never a field on a request DTO or command.
- Simple operations take raw values. Writes with roughly five or more fields
  take an application-owned command model defined in `models/`, never the API
  request DTO.
- List operations take an application-owned filter model extending
  `ListFilter`, which carries `cursor`, `size`, and `sort`, defined in
  `models/`. Pagination is bounded on `ListFilter` itself, not only on the
  wire params model, so validation holds however the filter is constructed.
  Sortable fields are declared via the `sortable_fields` ClassVar, which
  defaults to `created`, riding the UUIDv7 primary key. A future sortable
  field needs a matching `(field, id)` composite index plus `paginate()`
  support. Filter models use `pydantic.AwareDatetime` for datetime bounds,
  never plain `datetime`.
- Use cases return domain objects and raise domain errors. No application
  output models, no HTTP concepts.
- One transaction per use case. The request session commits after the use case
  returns and any exception rolls the writes back. A use case that must commit
  before the request ends, for example to release row locks ahead of slow
  work, opens an explicitly scoped transaction on its own session. Never
  commit the request session mid-request, since commit is transaction-wide
  and would also commit unrelated pending writes.
- A use case that reads a row's status and writes a transition based on it
  must load the row with `get(..., exclusive=True)`. An unlocked transition is
  last-writer-wins, so a racing cancel or claim is silently reverted.
- Uniqueness is enforced by database constraints, not by lookups. Do not
  pre-check before a write, the repository translates the constraint violation
  into the domain error.
- Repository interfaces are Protocols in `interfaces/`, one module per
  resource, written in domain terms. Only `query` takes a filter model. The
  write and read methods are named `create`, `get`, `query`, `update`, and
  `delete`. The domain mutator for a single field is `update_<field>`.
- Lookups by a unique field are named `get_by_<field>`. A lookup whose miss
  reaches the caller raises the domain `NotFound` error. An internal existence
  probe returns `None` instead.
- The application layer never imports API DTOs.
