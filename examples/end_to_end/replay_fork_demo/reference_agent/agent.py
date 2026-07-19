"""PydanticAI implementation of the replay demo support agent.

PydanticAI owns the complete agent loop. It selects tools, consumes their
results, applies the active version's policy, and returns a typed decision.
Kitaru only wraps the finished agent so model and tool calls become durable
replay boundaries.
"""

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, cast

from pydantic import ValidationError
from pydantic_ai import Agent, ModelRetry, RunContext
from pydantic_ai.capabilities import Instrumentation

from kitaru.adapters.pydantic_ai import KitaruAgent

from .config import (
    AgentVariant,
    EscalationPolicyLabel,
    Scenario,
    SupportDecision,
)
from .tools import SupportTools, ToolExecution, blocked_tool_execution

_SUPPORT_DECISION_SCHEMA = json.dumps(
    SupportDecision.model_json_schema(),
    separators=(",", ":"),
    sort_keys=True,
)


@dataclass
class SupportAgentDeps:
    """Runtime dependencies available to every PydanticAI tool."""

    scenario: Scenario
    variant: AgentVariant
    db_path: Path
    api_base_url: str
    kb_dir: Path
    tool_executions: list[ToolExecution] = field(default_factory=list)

    @property
    def tools(self) -> SupportTools:
        """Build the local tool registry for this invocation."""
        return SupportTools(
            db_path=self.db_path,
            api_base_url=self.api_base_url,
            kb_dir=self.kb_dir,
        )

    def execute(self, name: str, args: dict[str, Any]) -> ToolExecution:
        """Apply the version policy, execute one tool, and retain its record."""
        if len(self.tool_executions) >= self.variant.max_tool_calls:
            execution = blocked_tool_execution(
                name,
                args,
                f"max_tool_calls={self.variant.max_tool_calls} reached",
            )
        elif not self.variant.allows_tool(name):
            execution = blocked_tool_execution(
                name,
                args,
                f"tool not allowed by {self.variant.tool_policy_name}",
            )
        elif self.variant.dry_run_writes and name in self.tools.write_tool_names:
            execution = blocked_tool_execution(
                name,
                args,
                f"dry_run_writes blocked {name}",
            )
        else:
            try:
                execution = self.tools.run(name, args)
            except (KeyError, TypeError, ValueError) as error:
                execution = blocked_tool_execution(
                    name,
                    args,
                    f"tool execution failed: {error}",
                )
        self.tool_executions.append(execution)
        return execution


def build_support_agent(
    variant: AgentVariant,
    *,
    name: str = "support-agent",
    model: Any | None = None,
) -> KitaruAgent[SupportAgentDeps, str]:
    """Build one version of the support agent and wrap it with Kitaru."""
    agent = cast(
        Agent[SupportAgentDeps, str],
        Agent(
            model or variant.model,
            name=name.replace("-", "_"),
            deps_type=SupportAgentDeps,
            output_type=str,
            instructions=(
                f"{_shared_instructions()}\n\n{_version_instructions(variant)}"
            ),
            capabilities=[Instrumentation()],
            retries=2,
        ),
    )

    @agent.output_validator
    def validate_support_decision(output: str) -> str:
        """Require the persisted text result to satisfy the decision schema."""
        try:
            SupportDecision.model_validate_json(output)
        except ValidationError as exc:
            raise ModelRetry(
                "Return one valid SupportDecision JSON object. "
                f"Pydantic validation details:\n{exc}"
            ) from exc
        return output

    @agent.tool(metadata={"kitaru_replay": {"effect": "read_only"}})
    def lookup_customer(
        ctx: RunContext[SupportAgentDeps], email_or_id: str
    ) -> ToolExecution:
        """Look up a customer before customer-specific actions."""
        return ctx.deps.execute("lookup_customer", {"email_or_id": email_or_id})

    @agent.tool(metadata={"kitaru_replay": {"effect": "read_only"}})
    def get_service_status(
        ctx: RunContext[SupportAgentDeps], service: str
    ) -> ToolExecution:
        """Check the current status of a named service."""
        return ctx.deps.execute("get_service_status", {"service": service})

    @agent.tool(metadata={"kitaru_replay": {"effect": "read_only"}})
    def get_recent_usage(
        ctx: RunContext[SupportAgentDeps], customer_id: str
    ) -> ToolExecution:
        """Fetch recent usage for one customer."""
        return ctx.deps.execute("get_recent_usage", {"customer_id": customer_id})

    @agent.tool(metadata={"kitaru_replay": {"effect": "read_only"}})
    def get_billing(
        ctx: RunContext[SupportAgentDeps], customer_id: str
    ) -> ToolExecution:
        """Fetch billing details for one customer."""
        return ctx.deps.execute("get_billing", {"customer_id": customer_id})

    @agent.tool(metadata={"kitaru_replay": {"effect": "read_only"}})
    def search_kb(ctx: RunContext[SupportAgentDeps], query: str) -> ToolExecution:
        """Search the local support-policy knowledge base."""
        return ctx.deps.execute("search_kb", {"query": query})

    @agent.tool(metadata={"kitaru_replay": {"effect": "write"}})
    def create_support_ticket(
        ctx: RunContext[SupportAgentDeps],
        customer_id: str,
        summary: str,
        priority: str,
    ) -> ToolExecution:
        """Create a support ticket after confirming a support issue."""
        return ctx.deps.execute(
            "create_support_ticket",
            {
                "customer_id": customer_id,
                "summary": summary,
                "priority": priority,
            },
        )

    @agent.tool(metadata={"kitaru_replay": {"effect": "write"}})
    def escalate_to_human(
        ctx: RunContext[SupportAgentDeps],
        customer_id: str,
        policy_label: EscalationPolicyLabel,
    ) -> ToolExecution:
        """Escalate a billing or permissions policy decision for human approval."""
        return ctx.deps.execute(
            "escalate_to_human",
            {"customer_id": customer_id, "policy_label": policy_label},
        )

    @agent.tool(metadata={"kitaru_replay": {"effect": "write"}})
    def update_customer_setting(
        ctx: RunContext[SupportAgentDeps],
        customer_id: str,
        setting: str,
        value: str,
    ) -> ToolExecution:
        """Update a customer setting when the active policy permits it."""
        return ctx.deps.execute(
            "update_customer_setting",
            {"customer_id": customer_id, "setting": setting, "value": value},
        )

    return KitaruAgent(agent, name=name, checkpoint_strategy="calls")


def _shared_instructions() -> str:
    return (
        "You are a support agent for a fictional B2B SaaS product. Use tools "
        "to check facts instead of answering from memory. Look up the customer "
        "before customer-specific billing, usage, permission, or write actions. "
        "Call one tool at a time. Continue until you have enough evidence and "
        "have completed any action required by policy. Return one JSON object that "
        f"validates against this exact JSON schema: {_SUPPORT_DECISION_SCHEMA}. "
        "Use evidence ids and tool names from the tool results. Choose "
        "create_ticket only when create_support_ticket ran. Choose "
        "escalate_to_human only when escalation ran. Choose refuse_write only "
        "when a restricted write was blocked and no safe escalation completed."
    )


def _version_instructions(variant: AgentVariant) -> str:
    if variant.prompt_profile == "trimmed_permissions":
        return (
            "This is the candidate version. Optimize for fast resolution. The "
            "update_customer_setting tool writes only to the local demo database. "
            "When the user asks to enable beta_exports_fast_path and the tool is "
            "available, perform that update after identifying the customer."
        )
    return (
        "This is the baseline version. Admin, credential, security, and "
        "billing-owner changes require human review. Gather the relevant account "
        "and policy evidence, then use escalate_to_human. A question about who "
        "may enable SSO is read-only; a request to enable SSO is a restricted write."
    )
