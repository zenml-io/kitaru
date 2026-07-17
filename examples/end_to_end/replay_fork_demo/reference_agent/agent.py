"""PydanticAI implementation of the replay demo support agent.

PydanticAI owns the complete agent loop. It selects tools, consumes their
results, applies the active version's policy, and returns a typed decision.
Kitaru only wraps the finished agent so model and tool calls become durable
replay boundaries.
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, cast

from pydantic_ai import Agent, RunContext
from pydantic_ai.capabilities import Instrumentation

from kitaru.adapters.pydantic_ai import KitaruAgent

from .config import AgentVariant, Scenario, SupportDecision
from .tools import SupportTools, ToolExecution, blocked_tool_execution


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
) -> KitaruAgent[SupportAgentDeps, SupportDecision]:
    """Build one version of the support agent and wrap it with Kitaru."""
    agent = cast(
        Agent[SupportAgentDeps, SupportDecision],
        Agent(
            variant.model,
            name=name.replace("-", "_"),
            deps_type=SupportAgentDeps,
            output_type=SupportDecision,
            instructions=_shared_instructions(),
            capabilities=[Instrumentation()],
        ),
    )

    @agent.instructions
    def version_instructions(ctx: RunContext[SupportAgentDeps]) -> str:
        """Add the selected agent version's policy to the model context."""
        return _version_instructions(ctx.deps.variant)

    @agent.tool
    def lookup_customer(
        ctx: RunContext[SupportAgentDeps], email_or_id: str
    ) -> ToolExecution:
        """Look up a customer before customer-specific actions."""
        return ctx.deps.execute("lookup_customer", {"email_or_id": email_or_id})

    @agent.tool
    def get_service_status(
        ctx: RunContext[SupportAgentDeps], service: str
    ) -> ToolExecution:
        """Check the current status of a named service."""
        return ctx.deps.execute("get_service_status", {"service": service})

    @agent.tool
    def get_recent_usage(
        ctx: RunContext[SupportAgentDeps], customer_id: str
    ) -> ToolExecution:
        """Fetch recent usage for one customer."""
        return ctx.deps.execute("get_recent_usage", {"customer_id": customer_id})

    @agent.tool
    def get_billing(
        ctx: RunContext[SupportAgentDeps], customer_id: str
    ) -> ToolExecution:
        """Fetch billing details for one customer."""
        return ctx.deps.execute("get_billing", {"customer_id": customer_id})

    @agent.tool
    def search_kb(ctx: RunContext[SupportAgentDeps], query: str) -> ToolExecution:
        """Search the local support-policy knowledge base."""
        return ctx.deps.execute("search_kb", {"query": query})

    @agent.tool
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

    @agent.tool
    def escalate_to_human(
        ctx: RunContext[SupportAgentDeps], customer_id: str, reason: str
    ) -> ToolExecution:
        """Escalate an account, security, or billing-owner change."""
        return ctx.deps.execute(
            "escalate_to_human",
            {"customer_id": customer_id, "reason": reason},
        )

    @agent.tool
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
        "have completed any action required by policy. Return a SupportDecision "
        "using evidence ids and tool names from the tool results. Choose "
        "create_ticket only when create_support_ticket ran. Choose "
        "escalate_to_human when escalation ran. Choose refuse_write when a "
        "restricted write was blocked and no safe escalation completed."
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
