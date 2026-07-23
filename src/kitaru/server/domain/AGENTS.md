# Domain layer rules

- The domain layer imports nothing from other layers and contains no
  persistence concepts, not even abstract ones.
- Entities extend `DomainModel` and enforce their own invariants in methods.
  An entity method decides whether an operation is legal. Anemic models with
  rules in services above them are a defect.
- Entities declare `id: uuid.UUID = Field(default_factory=uuid7)` using
  `domain.ids.uuid7`. `uuid7` is for entity ids only. Anything
  security-bearing (API keys, tokens, capability references) stays fully
  random, since a UUIDv7 leaks its creation timestamp.
- Entities that expose timestamps declare `created: datetime | None = None`
  and `updated: datetime | None = None`. `None` means not stored yet. Never
  fill them with placeholder defaults.
- Name fields use the `domain.names.Name` annotated type, which validates
  through `domain.names.validate_name`.
- Domain errors subclass the `domain.base` taxonomy and follow its naming:
  `XNotFound(NotFoundError)`, `DuplicateXName(ConflictError)`.
- Error messages follow the canonical formats, which tests assert on:
  `"Agent {id} was not found"` and
  `"Agent name '{name}' is already registered"`.
