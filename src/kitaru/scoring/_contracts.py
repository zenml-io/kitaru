"""Strict scoring contracts and scorer declarations."""

from __future__ import annotations

import hashlib
import inspect
import json
import math
from collections.abc import Callable, Mapping, Sequence
from enum import StrEnum
from typing import Any, Literal, Protocol, get_type_hints

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    JsonValue,
    field_validator,
    model_validator,
)

from kitaru.errors import KitaruUsageError

_SECRET_KEY_PARTS = frozenset(
    {"api_key", "apikey", "auth", "credential", "password", "secret", "token"}
)


def canonical_json(value: Any) -> str:
    """Serialize JSON-compatible data in a stable, hashable form."""
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise KitaruUsageError(
            "Scoring contracts require canonically JSON-serializable values."
        ) from exc


def sha256_json(value: Any) -> str:
    """Return a sha256:<hex> digest for canonical JSON data."""
    return f"sha256:{hashlib.sha256(canonical_json(value).encode('utf-8')).hexdigest()}"


def validate_sha256(value: str) -> str:
    """Validate Kitaru's canonical SHA-256 string representation."""
    normalized = require_string(value, field_name="SHA-256")
    prefix, separator, digest = normalized.partition(":")
    if separator != ":" or prefix != "sha256" or len(digest) != 64:
        raise ValueError("SHA-256 values must use the sha256:<64 hex characters> form.")
    try:
        int(digest, 16)
    except ValueError as exc:
        raise ValueError("SHA-256 values must contain hexadecimal characters.") from exc
    return normalized


def require_string(value: str, *, field_name: str) -> str:
    """Normalize and validate a required non-empty string."""
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{field_name} cannot be empty.")
    return normalized


def _safe_config(value: Mapping[str, JsonValue] | None) -> dict[str, JsonValue]:
    """Copy JSON config after rejecting secret-looking keys."""
    config = dict(value or {})
    for key in config:
        normalized = key.lower().replace("-", "_")
        if any(part in normalized for part in _SECRET_KEY_PARTS):
            raise KitaruUsageError(
                "Scorer configuration cannot include secret-looking keys. "
                "Resolve credentials at evaluation time instead."
            )
    canonical_json(config)
    return config


class Score(BaseModel):
    """One strict finite score in the inclusive [0.0, 1.0] range."""

    schema_version: Literal[1] = 1
    value: float
    explanation: str | None = None
    metadata: dict[str, JsonValue] = Field(default_factory=dict)

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    @field_validator("value", mode="before")
    @classmethod
    def _coerce_boolean_and_reject_non_numbers(cls, value: Any) -> float:
        if isinstance(value, bool):
            return 1.0 if value else 0.0
        if isinstance(value, int | float):
            numeric = float(value)
        else:
            raise ValueError(
                "Score value must be a finite number or boolean shorthand."
            )
        if not math.isfinite(numeric):
            raise ValueError("Score value must be finite.")
        if numeric < 0.0 or numeric > 1.0:
            raise ValueError("Score value must be in the inclusive [0.0, 1.0] range.")
        return numeric

    @field_validator("explanation")
    @classmethod
    def _validate_explanation(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return require_string(value, field_name="Score explanation")

    @field_validator("metadata")
    @classmethod
    def _validate_metadata(cls, value: dict[str, JsonValue]) -> dict[str, JsonValue]:
        return _safe_config(value)


class ScorerCapability(StrEnum):
    """Declared execution mode for a scorer."""

    PURE = "pure"
    GROUNDED = "grounded"


class ScoreObservationStatus(StrEnum):
    """Persisted status for one scorer/target observation."""

    SCORED = "SCORED"
    ABSTAINED = "ABSTAINED"
    BLOCKED = "BLOCKED"
    ERROR = "ERROR"


class ScorerOutputContract(BaseModel):
    """Persisted output contract for a scorer declaration."""

    schema_version: Literal[1] = 1
    returns: Literal["score"] = "score"
    allow_abstain: bool = True
    allow_blocked: bool = True
    allow_error: bool = True

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class SourceSnapshot(BaseModel):
    """Inspectable source captured for a scorer declaration."""

    status: Literal["captured", "unavailable"]
    text: str | None = None
    sha256: str

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    @field_validator("sha256")
    @classmethod
    def _validate_hash(cls, value: str) -> str:
        return validate_sha256(value)

    @model_validator(mode="after")
    def _validate_source_shape(self) -> SourceSnapshot:
        if self.status == "captured" and not self.text:
            raise ValueError("Captured scorer source requires text.")
        if self.status == "unavailable" and self.text is not None:
            raise ValueError("Unavailable scorer source cannot include text.")
        return self


class GroundedCapabilityDeclaration(BaseModel):
    """Frozen declaration for one allowed grounded capability."""

    name: str
    revision: str
    read_only: bool

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    @field_validator("name", "revision")
    @classmethod
    def _validate_strings(cls, value: str) -> str:
        return require_string(value, field_name="Capability field")

    @model_validator(mode="after")
    def _reject_write_capable(self) -> GroundedCapabilityDeclaration:
        if not self.read_only:
            raise ValueError("Grounded scoring supports read-only capabilities only.")
        return self


class GroundedPolicySnapshot(BaseModel):
    """Immutable default-deny policy supplied to grounded scorers."""

    schema_version: Literal[1] = 1
    policy_id: str
    capabilities: list[GroundedCapabilityDeclaration] = Field(default_factory=list)
    allowed_resources: dict[str, list[str]] = Field(default_factory=dict)
    timeout_seconds: float = Field(gt=0.0, default=10.0)
    retry_limit: int = Field(ge=0, default=0)
    evidence_retention: Literal["summary", "none"] = "summary"

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    @field_validator("policy_id")
    @classmethod
    def _validate_policy_id(cls, value: str) -> str:
        return require_string(value, field_name="Policy ID")


class GroundedCallEvidence(BaseModel):
    """Bounded provenance for one grounded capability call."""

    capability_name: str
    resource_identifier: str
    started_at: str
    finished_at: str
    request_summary: dict[str, JsonValue] = Field(default_factory=dict)
    result_summary: dict[str, JsonValue] = Field(default_factory=dict)

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class GroundedProvenance(BaseModel):
    """Grounded policy and call evidence attached to one observation."""

    schema_version: Literal[1] = 1
    policy: GroundedPolicySnapshot
    calls: list[GroundedCallEvidence] = Field(default_factory=list)

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class ScorerSnapshot(BaseModel):
    """Immutable scorer declaration persisted with attempts and observations."""

    schema_version: Literal[1] = 1
    name: str
    qualified_name: str
    source: SourceSnapshot
    configuration: dict[str, JsonValue] = Field(default_factory=dict)
    configuration_hash: str
    capability: ScorerCapability
    comparative: bool = False
    grounded_capabilities: list[GroundedCapabilityDeclaration] = Field(
        default_factory=list
    )
    output_contract: ScorerOutputContract = Field(default_factory=ScorerOutputContract)
    revision: str

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    @field_validator("name", "qualified_name")
    @classmethod
    def _validate_required_strings(cls, value: str) -> str:
        return require_string(value, field_name="Scorer field")

    @field_validator("configuration")
    @classmethod
    def _validate_configuration(
        cls, value: dict[str, JsonValue]
    ) -> dict[str, JsonValue]:
        return _safe_config(value)

    @field_validator("capability", mode="before")
    @classmethod
    def _validate_capability(cls, value: Any) -> ScorerCapability:
        return value if isinstance(value, ScorerCapability) else ScorerCapability(value)

    @field_validator("configuration_hash", "revision")
    @classmethod
    def _validate_hashes(cls, value: str) -> str:
        return validate_sha256(value)

    @model_validator(mode="after")
    def _validate_grounding(self) -> ScorerSnapshot:
        if self.capability == ScorerCapability.PURE and self.grounded_capabilities:
            raise ValueError("Pure scorers cannot declare grounded capabilities.")
        return self

    @classmethod
    def from_callable(
        cls,
        func: Callable[..., Any],
        *,
        name: str | None = None,
        capability: ScorerCapability | Literal["pure", "grounded"],
        comparative: bool = False,
        configuration: Mapping[str, JsonValue] | None = None,
        grounded_capabilities: Sequence[GroundedCapabilityDeclaration] = (),
    ) -> ScorerSnapshot:
        """Capture a scorer snapshot without serializing the callable itself."""
        normalized_capability = ScorerCapability(capability)
        _validate_scorer_signature(
            func,
            capability=normalized_capability,
            comparative=comparative,
        )
        config = _safe_config(configuration)
        fallback_name = getattr(func, "__name__", type(func).__qualname__)
        module_name = getattr(func, "__module__", type(func).__module__)
        qualified_name = f"{module_name}.{getattr(func, '__qualname__', fallback_name)}"
        source = _capture_source(func, qualified_name=qualified_name)
        payload = {
            "name": name or getattr(func, "__name__", qualified_name),
            "qualified_name": qualified_name,
            "source_sha256": source.sha256,
            "configuration_hash": sha256_json(config),
            "capability": normalized_capability.value,
            "comparative": comparative,
            "grounded_capabilities": [
                item.model_dump(mode="json") for item in grounded_capabilities
            ],
            "output_contract": ScorerOutputContract().model_dump(mode="json"),
        }
        return cls(
            name=payload["name"],
            qualified_name=qualified_name,
            source=source,
            configuration=config,
            configuration_hash=payload["configuration_hash"],
            capability=normalized_capability,
            comparative=comparative,
            grounded_capabilities=list(grounded_capabilities),
            revision=sha256_json(payload),
        )


class ScorerProtocol(Protocol):
    """Runtime protocol for decorated scorer declarations."""

    snapshot: ScorerSnapshot

    def __call__(self, *args: Any, **kwargs: Any) -> Any: ...


class ScorerDeclaration:
    """Callable wrapper returned by ``@scorer``."""

    def __init__(self, func: Callable[..., Any], snapshot: ScorerSnapshot) -> None:
        self._func = func
        self.snapshot = snapshot
        self.__name__ = getattr(func, "__name__", snapshot.name)
        self.__qualname__ = getattr(func, "__qualname__", self.__name__)
        self.__doc__ = getattr(func, "__doc__", None)
        self.__module__ = getattr(func, "__module__", "")

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        return self._func(*args, **kwargs)


class ScoreObservationOutcome(BaseModel):
    """Typed score or non-score outcome for one target/scorer pair."""

    status: ScoreObservationStatus
    score: Score | None = None
    reason: str | None = None
    valid: bool = True

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    @field_validator("status", mode="before")
    @classmethod
    def _validate_status(cls, value: Any) -> ScoreObservationStatus:
        return (
            value
            if isinstance(value, ScoreObservationStatus)
            else ScoreObservationStatus(str(value))
        )

    @model_validator(mode="after")
    def _validate_outcome_shape(self) -> ScoreObservationOutcome:
        if self.status is ScoreObservationStatus.SCORED and self.score is None:
            raise ValueError("SCORED observations require a Score.")
        if self.status is not ScoreObservationStatus.SCORED and self.score is not None:
            raise ValueError("Only SCORED observations can contain a Score.")
        if self.status is not ScoreObservationStatus.SCORED and not self.reason:
            raise ValueError("Non-score outcomes require a reason.")
        return self


class ScoreObservation(BaseModel):
    """Append-only persisted observation payload."""

    schema_version: Literal[1] = 1
    observation_id: str | None = None
    project_id: str
    execution_id: str
    experiment_id: str
    scorer: ScorerSnapshot
    outcome: ScoreObservationOutcome
    completed_at: str
    evidence_manifest_sha256: str
    comparative_original_execution_id: str | None = None
    source_observation_ids: list[str] = Field(default_factory=list)
    grounded_provenance: GroundedProvenance | None = None
    supersedes_observation_id: str | None = None
    explanation: str | None = None

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    @field_validator("project_id", "execution_id", "experiment_id", "completed_at")
    @classmethod
    def _validate_required_strings(cls, value: str) -> str:
        return require_string(value, field_name="Observation field")

    @field_validator("evidence_manifest_sha256")
    @classmethod
    def _validate_manifest_hash(cls, value: str) -> str:
        return validate_sha256(value)

    @property
    def status(self) -> ScoreObservationStatus:
        """Return the normalized outcome status for indexing."""
        return self.outcome.status

    @property
    def valid(self) -> bool:
        """Return whether this observation is valid for default projections."""
        return self.outcome.valid


class ScorerAggregate(BaseModel):
    """Immutable descriptive aggregate for one scorer revision/configuration."""

    schema_version: Literal[1] = 1
    scorer_name: str
    scorer_revision: str
    scorer_configuration_hash: str
    planned: int = Field(ge=0)
    denominator: int = Field(ge=0)
    scored: int = Field(ge=0)
    abstained: int = Field(ge=0)
    blocked: int = Field(ge=0)
    error: int = Field(ge=0)
    mean: float | None = None
    minimum: float | None = None
    maximum: float | None = None
    spread: float | None = None
    paired_delta_count: int = Field(ge=0, default=0)
    paired_delta_mean: float | None = None
    paired_delta_minimum: float | None = None
    paired_delta_maximum: float | None = None

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


def scorer(
    func: Callable[..., Any] | None = None,
    *,
    capability: Literal["pure", "grounded"] | ScorerCapability | None = None,
    name: str | None = None,
    comparative: bool = False,
    configuration: Mapping[str, JsonValue] | None = None,
    grounded_capabilities: Sequence[GroundedCapabilityDeclaration] = (),
) -> ScorerDeclaration | Callable[[Callable[..., Any]], ScorerDeclaration]:
    """Declare a scorer without registering or serializing its callable.

    The capability must be explicit: pass ``capability="pure"`` for stored
    evidence only or ``capability="grounded"`` for a default-deny grounded
    scorer that uses declared read-only capabilities.
    """
    if capability is None:
        raise KitaruUsageError(
            "@scorer requires explicit capability='pure' or 'grounded'."
        )

    def decorate(inner: Callable[..., Any]) -> ScorerDeclaration:
        snapshot = ScorerSnapshot.from_callable(
            inner,
            name=name,
            capability=capability,
            comparative=comparative,
            configuration=configuration,
            grounded_capabilities=grounded_capabilities,
        )
        return ScorerDeclaration(inner, snapshot)

    if func is not None:
        return decorate(func)
    return decorate


def _capture_source(func: Callable[..., Any], *, qualified_name: str) -> SourceSnapshot:
    try:
        text = inspect.getsource(func)
    except (OSError, TypeError):
        text = None
    if text is None:
        return SourceSnapshot(
            status="unavailable",
            text=None,
            sha256=sha256_json(
                {"qualified_name": qualified_name, "source": "unavailable"}
            ),
        )
    return SourceSnapshot(status="captured", text=text, sha256=sha256_json(text))


def _validate_scorer_signature(
    func: Callable[..., Any], *, capability: ScorerCapability, comparative: bool
) -> None:
    signature = inspect.signature(func)
    parameters = [
        parameter
        for parameter in signature.parameters.values()
        if parameter.kind
        in (
            parameter.POSITIONAL_ONLY,
            parameter.POSITIONAL_OR_KEYWORD,
            parameter.KEYWORD_ONLY,
        )
        and parameter.default is parameter.empty
    ]
    expected = 2 if comparative else 1
    if capability == ScorerCapability.GROUNDED:
        expected += 1
    has_varargs = any(
        parameter.kind in (parameter.VAR_POSITIONAL, parameter.VAR_KEYWORD)
        for parameter in signature.parameters.values()
    )
    if has_varargs or len(parameters) != expected:
        raise KitaruUsageError(
            "Scorer signatures must be unambiguous: pure scorers take evidence, "
            "comparative scorers take candidate and original evidence, and "
            "grounded scorers add one world handle."
        )
    try:
        hints = get_type_hints(func)
    except Exception:
        hints = {}
    return_hint = hints.get("return")
    if return_hint not in (None, Score, float, int, bool):
        raise KitaruUsageError(
            "Scorer return annotations must be Score, float, int, bool, or omitted."
        )


def scorer_snapshot(
    value: Callable[..., Any] | ScorerDeclaration | ScorerProtocol | ScorerSnapshot,
) -> ScorerSnapshot:
    """Return a scorer snapshot from a declaration or raw snapshot."""
    if isinstance(value, ScorerSnapshot):
        return value
    snapshot = getattr(value, "snapshot", None)
    if isinstance(snapshot, ScorerSnapshot):
        return snapshot
    raise KitaruUsageError("Expected a @scorer declaration or ScorerSnapshot.")
