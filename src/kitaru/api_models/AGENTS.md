# API model rules

- DTOs live in versioned modules named after the entity in the singular
  (`v1/order.py`) and are named `XCreateRequest`, `XUpdateRequest`,
  `XListParams`, `XResponse`.
- Requests extend `RequestModel`, responses extend `ResponseModel`, list
  responses use `Page[XResponse]`.
- Paginated list endpoints take an `XListParams` model extending `ListParams`
  (base.py), which carries `page` and `page_size`. List params models declare
  only wire-exposed fields, never internal filter dimensions such as
  `owner_id`.
- Every field declares `Field(description=...)`. The descriptions feed the
  OpenAPI schema, so they are part of the API contract. Use the shortest noun
  phrase that disambiguates and never restate what the type, enum, validator,
  default, or field name already conveys.
- Discriminated request unions mark the `type` discriminator as set in
  `model_post_init` so `exclude_unset` dumps keep it. The `type: Literal`
  discriminator is the one field exempt from the description rule.
- This package imports neither server nor client code.

## Datetimes

Every request field or query parameter that accepts a datetime declares
`pydantic.AwareDatetime`, never plain `datetime`. Naive client input must fail
validation with HTTP 422 instead of being interpreted in an implicit timezone.
Plain `datetime` stays fine on response models, where the server controls the
value.
