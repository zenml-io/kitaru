# Client SDK rules

- One resource class per API resource under `resources/`, named
  `<X>sResource`, registered as an attribute in `KitaruAPIClient.__init__`.
- Methods mirror endpoints one to one. When a request DTO exists for the
  endpoint, the method takes it as a parameter instead of unpacked field
  arguments and sends `model_dump(mode="json", exclude_unset=True)`, so only
  fields the caller set reach the wire and partial updates stay partial.
  Endpoints without a request DTO, such as the form-encoded login, take plain
  parameters. Validate the response with `<X>Response.model_validate` (or
  `Page[<X>Response]` for lists).
- List methods take the `XListParams` model, defaulting to a fresh instance,
  and send `model_dump(mode="json", exclude_unset=True)` as query params.
- Each resource also exposes `iter()` alongside `list()`, following
  `next_cursor` across pages until it is exhausted.
- The SDK reuses `api_models` DTOs and carries no business rules. The server
  re-validates everything.
- `KitaruAPIClient.request` attaches a bearer token from the credential store
  and renews it once after an HTTP 401. Endpoints that carry their own
  credential, meaning everything on `AuthResource`, pass `authenticate=False`
  so the token provider does not recurse into the login call it is making.
- Docstrings document `APIError` under `Raises` and name status codes worth
  knowing, such as 409 for duplicate names.
- The client package never imports server code.
