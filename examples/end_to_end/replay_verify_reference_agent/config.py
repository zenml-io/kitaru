"""Configuration and typed models for the reference-agent example."""

import os
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, Field

EXAMPLE_DIR = Path(__file__).resolve().parent
FIXTURES_DIR = EXAMPLE_DIR / "fixtures"
VARIANTS_DIR = EXAMPLE_DIR / "variants"
SCENARIOS_PATH = EXAMPLE_DIR / "scenarios.yaml"
DEFAULT_AGENT_VERSION = "replay-verify-reference-agent-stage-1"
REQUIRED_TRACE_ENV_VARS = (
    "LANGFUSE_PUBLIC_KEY",
    "LANGFUSE_SECRET_KEY",
    "LANGFUSE_BASE_URL",
    "OPENAI_API_KEY",
)


class Scenario(BaseModel):
    """One seeded support request."""

    scenario_id: str
    case_id: str
    scenario_set: Literal["smoke", "full"]
    title: str
    user_request: str
    customer_key: str | None = None
    expected_policy_label: str
    expected_required_action: str
    notes: str = ""


class AgentVariant(BaseModel):
    """Runtime configuration for one baseline or candidate variant."""

    name: str
    model: str
    prompt_profile: Literal["full_permissions", "trimmed_permissions"]
    tool_policy_name: str
    max_tool_calls: int = 6
    allowed_tools: list[str] = Field(default_factory=list)
    denied_tools: list[str] = Field(default_factory=list)
    dry_run_writes: bool = False

    def allows_tool(self, name: str) -> bool:
        """Return whether this variant may call a named tool."""
        if name in self.denied_tools:
            return False
        return not self.allowed_tools or name in self.allowed_tools


class SupportDecision(BaseModel):
    """Structured final answer returned by the agent."""

    policy_label: Literal[
        "billing_policy",
        "permissions_policy",
        "incident_policy",
        "usage_policy",
        "unknown",
    ]
    risk_status: Literal["safe", "needs_review", "blocked"]
    required_action: Literal[
        "answer_directly",
        "create_ticket",
        "escalate_to_human",
        "refuse_write",
    ]
    summary: str
    evidence_ids: list[str]
    tool_names: list[str]


def load_yaml_mapping(path: Path) -> dict[str, Any]:
    """Load a YAML file and require a mapping at the top level."""
    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        raise ValueError(f"Expected a YAML mapping in {path}")
    return data


def load_scenarios(path: Path = SCENARIOS_PATH) -> list[Scenario]:
    """Load all seeded scenarios."""
    data = load_yaml_mapping(path)
    raw_scenarios = data.get("scenarios", [])
    if not isinstance(raw_scenarios, list):
        raise ValueError(f"Expected 'scenarios' to be a list in {path}")
    return [Scenario.model_validate(item) for item in raw_scenarios]


def select_scenarios(scenario_set: str, scenarios: list[Scenario]) -> list[Scenario]:
    """Return scenarios for the requested set."""
    if scenario_set == "full":
        return scenarios
    if scenario_set == "smoke":
        return [scenario for scenario in scenarios if scenario.scenario_set == "smoke"]
    raise ValueError("scenario_set must be 'smoke' or 'full'")


def load_variant(name: str, variants_dir: Path = VARIANTS_DIR) -> AgentVariant:
    """Load one variant by file stem."""
    path = variants_dir / f"{name}.yaml"
    data = load_yaml_mapping(path)
    return AgentVariant.model_validate(data)


def load_variants(names: list[str]) -> list[AgentVariant]:
    """Load variants in the caller's requested order."""
    return [load_variant(name) for name in names]


def missing_trace_environment() -> list[str]:
    """Return trace-generation environment variables that are not set."""
    return [name for name in REQUIRED_TRACE_ENV_VARS if not os.getenv(name)]
