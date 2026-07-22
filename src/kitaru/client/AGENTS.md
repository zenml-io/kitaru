# Client SDK rules

- One resource class per API resource under `resources/`, named
  `<X>sResource`, registered as an attribute in `KitaruAPIClient.__init__`.
- Methods mirror endpoints one to one. Build the request DTO, send
  `model_dump(mode="json")`, validate the response with
  `<X>Response.model_validate` (or `Page[<X>Response]` for lists).
- The SDK reuses `api_models` DTOs and carries no business rules. The server
  re-validates everything.
- Docstrings document `APIError` under `Raises` and name status codes worth
  knowing, such as 409 for duplicate names.
- The client package never imports server code.
