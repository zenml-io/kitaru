"""Versioned fixtures and inspectable demonstration results."""

import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Literal, Self
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, model_validator
from pydantic_ai.messages import ModelMessage, ModelMessagesTypeAdapter

DEFAULT_OPENING = "When will ORDER-1042 arrive? I need it for a birthday on Friday."
DEFAULT_ACCEPTANCE = (
    "The agent provides a delivery estimate described as an estimate, OR explicitly says the delivery date is unavailable and provides a tracking link or instructions to contact support.",
    "The follow-up has been addressed. An explicit explanation that the record cannot answer it is acceptable; do not require an unavailable date, guarantee, update schedule, or calculation method.",
)


class ShippingState(BaseModel):
    """Mock fulfillment and carrier evidence for one order."""

    order_id: str = "ORDER-1042"
    fulfillment_status: Literal["fulfilled"] = "fulfilled"
    status: str = "In transit"
    estimated_delivery: str | None = None
    tracking_url: str = "https://tracking.example.test/ORDER-1042"
    escalation_available: bool = True


class Scenario(BaseModel):
    """Versioned business state, customer policy, and native seed histories."""

    schema_version: Literal[1] = 1
    name: Literal["missing-date", "known-date"]
    shipping: ShippingState
    customer_known_facts: dict[str, str] = Field(
        default_factory=lambda: {
            "order_id": "ORDER-1042",
            "occasion": "A birthday on Friday",
        }
    )
    customer_policy: str = (
        "Ask for evidence of an estimate; otherwise ask for tracking and escalation."
    )
    goal: str = "Find a supported delivery estimate or an actionable next step."
    opening_message: str = DEFAULT_OPENING
    acceptance_criteria: list[str] = Field(
        default_factory=lambda: list(DEFAULT_ACCEPTANCE)
    )
    seed_histories: dict[str, list[dict[str, Any]]] = Field(default_factory=dict)
    max_agent_turns: int = Field(default=3, ge=1, le=10)

    def calculate_hash(self) -> str:
        """Hash canonical JSON independent of dictionary key order."""
        snapshot = self.model_dump(mode="json")
        # Providers can serialize 0.0 as 0; restore the native typed history before hashing.
        snapshot["seed_histories"] = {
            mode: ModelMessagesTypeAdapter.dump_python(
                ModelMessagesTypeAdapter.validate_python(history), mode="json"
            )
            for mode, history in self.seed_histories.items()
        }
        # Default extensions preserve hashes of previously recorded demo scenarios.
        if self.opening_message == DEFAULT_OPENING:
            snapshot.pop("opening_message")
        if self.acceptance_criteria == list(DEFAULT_ACCEPTANCE):
            snapshot.pop("acceptance_criteria")
        return hashlib.sha256(
            json.dumps(
                snapshot,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            ).encode()
        ).hexdigest()


class VisibleMessage(BaseModel):
    """One customer-visible utterance without tool payloads."""

    role: Literal["user", "assistant"]
    content: str


class SimulationStep(BaseModel):
    """A structured customer continuation or stop decision."""

    next_message: str | None = None
    stop: bool = False


class CustomerReply(BaseModel):
    """Continue the conversation with one customer utterance."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)
    next_message: str = Field(min_length=1)


class CustomerFinished(BaseModel):
    """End the conversation after the customer goal is met."""

    model_config = ConfigDict(extra="forbid")


class Verdict(BaseModel):
    """Narrow fixture checks for ISO and English month-name date mentions."""

    no_unsupported_date: bool
    uses_supported_date: bool

    @property
    def passed(self) -> bool:
        """Report whether both narrow fixture criteria pass."""
        return self.no_unsupported_date and self.uses_supported_date


class SimulatorAttempt(BaseModel):
    """Native customer-generation messages, including rejected output and feedback."""

    messages: list[ModelMessage]
    error: str | None = None


class RunResult(BaseModel):
    """Complete native conversation, provenance, and execution outcome."""

    run_id: UUID = Field(default_factory=uuid4)
    started_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    ended_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    seed_message_count: int = 0
    scenario: Scenario
    scenario_hash: str
    backend: str
    model_name: str
    variant: str
    policy_name: str | None = None
    policy_prompt: str | None = None
    policy_hash: str | None = None
    mode: str
    input_seed: list[dict[str, Any]] = Field(default_factory=list)
    messages: list[ModelMessage]
    visible_transcript: list[VisibleMessage]
    tool_calls: int = 0
    agent_turns: int = 0
    model_requests: int = 0
    status: Literal[
        "completed",
        "boundary-completed",
        "turn-limit",
        "invalid-simulation",
        "agent-error",
    ]
    error: str | None = None
    simulator_attempts: list[SimulatorAttempt] = Field(default_factory=list)
    verdict: Verdict

    @model_validator(mode="after")
    def validate_policy_provenance(self) -> Self:
        """Reject incomplete or conflicting recorded policy provenance."""
        fields = (self.policy_name, self.policy_prompt, self.policy_hash)
        if all(value is None for value in fields):
            return self
        if (
            self.policy_prompt is None
            or self.policy_name is None
            or self.policy_hash is None
        ):
            raise ValueError("Recorded policy provenance is incomplete")
        if hashlib.sha256(self.policy_prompt.encode()).hexdigest() != self.policy_hash:
            raise ValueError("Recorded policy prompt does not match its SHA256")
        return self

    def serialize_native_messages(self) -> bytes:
        """Serialize the complete native PydanticAI message history."""
        return ModelMessagesTypeAdapter.dump_json(self.messages)


def get_scenario(name: str) -> Scenario:
    """Create an isolated missing-date or supported-date fixture."""
    if name not in {"missing-date", "known-date"}:
        raise ValueError(f"Unknown scenario: {name}")
    return Scenario(
        name=name,
        shipping=ShippingState(
            estimated_delivery="2026-10-09" if name == "known-date" else None
        ),
    )
