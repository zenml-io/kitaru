# API model rules

- DTOs live in versioned modules named after the entity in the singular
  (`v1/order.py`) and are named `XCreateRequest`, `XUpdateRequest`,
  `XListParams`, `XResponse`.
- Requests extend `RequestModel`, responses extend `ResponseModel`, list
  responses use `Page[XResponse]`, which carries `items` and `next_cursor`,
  never a total count.
- Paginated list endpoints take an `XListParams` model extending `ListParams`
  (base.py), which carries `cursor`, `size`, and `sort`. List params models
  declare only wire-exposed fields, never internal filter dimensions such as
  `owner_id`.
- Field descriptions feed the OpenAPI schema, so they are part of the API contract. Use the shortest noun phrase that disambiguates and never restate what the type, enum, validator, default, or field name already conveys.
- Discriminated request unions mark the `type` discriminator as set in `model_post_init` so `exclude_unset` dumps keep it.
- A deprecated wire field carries `Field(deprecated=...)`.

Import-linter keeps this package free of server and client imports. `tests/api_models/test_model_conventions.py` fails for a field without a description (single-value discriminator tags excepted), a naive `datetime` on a request model, and a deprecated field missing from `DEPRECATIONS.md`.
