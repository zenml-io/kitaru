"""LLM calls and tool selection for the reference support agent."""

import json
from collections.abc import Callable
from types import SimpleNamespace
from typing import Any, TypeVar

from .config import AgentVariant, Scenario, SupportDecision
from .tools import SupportTools, ToolExecution, blocked_tool_execution

ResultT = TypeVar("ResultT")


def collect_evidence_with_llm_tools(
    *,
    scenario: Scenario,
    variant: AgentVariant,
    tools: SupportTools,
    callbacks: list[Any],
    metadata: dict[str, Any],
    tags: list[str],
) -> list[ToolExecution]:
    """Let the configured OpenAI model choose and call local tools."""
    try:
        from langchain_core.messages import HumanMessage, SystemMessage, ToolMessage
    except ImportError as error:
        raise SystemExit(
            "Missing LangChain dependency. Run trace generation with:\n"
            "  uv run --extra langgraph-openai --with langfuse "
            "examples/end_to_end/replay_verify_reference_agent/generate_traces.py"
        ) from error

    model = _chat_model(variant.model).bind_tools(
        _tool_schemas(),
        parallel_tool_calls=False,
    )
    messages: list[Any] = [
        SystemMessage(content=_tool_selection_rules(variant)),
        HumanMessage(content=_tool_selection_prompt(scenario)),
    ]
    executions: list[ToolExecution] = []

    for _turn in range(variant.max_tool_calls + 1):
        response = run_model_with_kitaru_calls_mode(
            model=model,
            model_input=messages,
            callbacks=callbacks,
            metadata=metadata,
            tags=tags,
            node_name="collect_evidence_with_tools",
            tools=_tool_schemas(),
        )
        messages.append(response)
        requested_calls = getattr(response, "tool_calls", None) or []
        if not requested_calls:
            break

        for requested_call in requested_calls:
            name = str(requested_call.get("name", ""))
            args = _json_dict(requested_call.get("args", {}))
            execution = _execute_requested_tool(
                tools=tools,
                variant=variant,
                name=name,
                args=args,
                executed_tool_count=_executed_tool_count(executions),
            )
            executions.append(execution)
            messages.append(
                ToolMessage(
                    content=json.dumps(execution.model_dump(), sort_keys=True),
                    tool_call_id=str(
                        requested_call.get("id") or f"{name}-{len(executions)}"
                    ),
                )
            )
            if execution.blocked and "max_tool_calls" in str(
                execution.result.get("reason", "")
            ):
                return executions
    return executions


def summarize_evidence_with_llm(
    *,
    scenario: Scenario,
    variant: AgentVariant,
    tool_executions: list[ToolExecution],
    callbacks: list[Any],
    metadata: dict[str, Any],
    tags: list[str],
) -> str:
    """Use the configured OpenAI model to summarize collected evidence."""
    model = _chat_model(variant.model)
    prompt = (
        "Summarize the evidence for this fictional B2B SaaS support request.\n"
        "Preserve concrete ids and facts exactly when present: customer_id, "
        "account_tier, permission_role, requested_action, incident_id, and "
        "knowledge document ids.\n\n"
        f"User request:\n{scenario.user_request}\n\n"
        f"Tool evidence JSON:\n{_tool_json(tool_executions)}"
    )
    response = run_model_with_kitaru_calls_mode(
        model=model,
        model_input=prompt,
        callbacks=callbacks,
        metadata=metadata,
        tags=tags,
        node_name="summarize_evidence",
    )
    content = _message_content(response).strip()
    if content:
        return content
    return (
        "The LLM returned an empty evidence summary. Local tool records still "
        f"show the collected evidence: {_tool_json(tool_executions)}"
    )


def decide_with_llm(
    *,
    scenario: Scenario,
    variant: AgentVariant,
    evidence_summary: str,
    tool_executions: list[ToolExecution],
    callbacks: list[Any],
    metadata: dict[str, Any],
    tags: list[str],
) -> SupportDecision:
    """Use the configured OpenAI model to produce structured final output."""
    model = _chat_model(variant.model).with_structured_output(SupportDecision)
    system_rules = _permission_rules(variant.prompt_profile)
    prompt = (
        f"{system_rules}\n\n"
        "Return the final support decision as structured output.\n"
        "Use only these policy labels: billing_policy, permissions_policy, "
        "incident_policy, usage_policy, unknown.\n"
        "Use tool names and evidence ids from the evidence below.\n\n"
        "Decision rules:\n"
        "- If the user only asks for status, policy, usage, or availability, "
        "choose answer_directly.\n"
        "- Choose create_ticket only when create_support_ticket actually ran.\n"
        "- Choose escalate_to_human when escalate_to_human ran or when an "
        "admin, credential, SSO enablement, or billing-owner write was requested.\n"
        "- Choose refuse_write when a dangerous write was blocked and no safe "
        "escalation happened.\n"
        "- A request asking who can enable SSO is read-only. A request asking "
        "the copilot to enable SSO is a restricted write.\n\n"
        f"Scenario id: {scenario.scenario_id}\n"
        f"User request: {scenario.user_request}\n"
        f"Expected topic hint: {scenario.expected_policy_label}\n\n"
        f"Evidence summary:\n{evidence_summary}\n\n"
        f"Tool records JSON:\n{_tool_json(tool_executions)}"
    )
    decision = run_model_with_kitaru_calls_mode(
        model=model,
        model_input=prompt,
        callbacks=callbacks,
        metadata=metadata,
        tags=tags,
        node_name="decide_action",
    )
    if isinstance(decision, SupportDecision):
        return decision
    return SupportDecision.model_validate(decision)


def _execute_requested_tool(
    *,
    tools: SupportTools,
    variant: AgentVariant,
    name: str,
    args: dict[str, Any],
    executed_tool_count: int,
) -> ToolExecution:
    tool_call_id = f"{name}-{executed_tool_count + 1}"
    if executed_tool_count >= variant.max_tool_calls:
        return run_tool_with_kitaru_calls_mode(
            tools=tools,
            name=name,
            args=args,
            tool_call_id=tool_call_id,
            handler=lambda: blocked_tool_execution(
                name,
                args,
                f"max_tool_calls={variant.max_tool_calls} reached",
            ),
        )
    if not variant.allows_tool(name):
        return run_tool_with_kitaru_calls_mode(
            tools=tools,
            name=name,
            args=args,
            tool_call_id=tool_call_id,
            handler=lambda: blocked_tool_execution(
                name,
                args,
                f"tool not allowed by {variant.tool_policy_name}",
            ),
        )
    if variant.dry_run_writes and name in tools.write_tool_names:
        return run_tool_with_kitaru_calls_mode(
            tools=tools,
            name=name,
            args=args,
            tool_call_id=tool_call_id,
            handler=lambda: blocked_tool_execution(
                name,
                args,
                f"dry_run_writes blocked {name}",
            ),
        )
    try:
        return run_tool_with_kitaru_calls_mode(
            tools=tools,
            name=name,
            args=args,
            tool_call_id=tool_call_id,
            handler=lambda: tools.run(name, args),
        )
    except (KeyError, TypeError, ValueError) as error:
        reason = f"tool execution failed: {error}"
        return run_tool_with_kitaru_calls_mode(
            tools=tools,
            name=name,
            args=args,
            tool_call_id=tool_call_id,
            handler=lambda: blocked_tool_execution(name, args, reason),
        )


def run_model_with_kitaru_calls_mode(
    *,
    model: Any,
    model_input: Any,
    callbacks: list[Any],
    metadata: dict[str, Any],
    tags: list[str],
    node_name: str,
    tools: list[Any] | None = None,
    handler: Callable[[], ResultT] | None = None,
) -> ResultT:
    """Run a model-like call through Kitaru calls-mode when active."""
    from kitaru.adapters.langgraph.langchain import KitaruLangGraphMiddleware

    request = SimpleNamespace(
        model=model,
        messages=model_input if isinstance(model_input, list) else [model_input],
        system_message=None,
        tool_choice=None,
        tools=tools or [],
        response_format=None,
        model_settings={},
        runtime=SimpleNamespace(node_name=node_name),
    )

    def _handler(_request: Any) -> ResultT:
        if handler is not None:
            return handler()
        return model.invoke(
            model_input,
            config={"callbacks": callbacks, "metadata": metadata, "tags": tags},
        )

    return KitaruLangGraphMiddleware().wrap_model_call(request, _handler)


def run_tool_with_kitaru_calls_mode(
    *,
    tools: SupportTools,
    name: str,
    args: dict[str, Any],
    tool_call_id: str,
    handler: Callable[[], ToolExecution] | None = None,
) -> ToolExecution:
    """Run a local support tool through Kitaru calls-mode when active."""
    from kitaru.adapters.langgraph.langchain import KitaruLangGraphMiddleware

    request = SimpleNamespace(
        tool_call={"name": name, "args": args, "id": tool_call_id},
        tool=SimpleNamespace(name=name),
        state={},
        runtime=SimpleNamespace(node_name="collect_evidence_with_tools"),
    )

    def _handler(_request: Any) -> ToolExecution:
        if handler is not None:
            return handler()
        return tools.run(name, args)

    return KitaruLangGraphMiddleware().wrap_tool_call(request, _handler)


def _chat_model(model_name: str) -> Any:
    try:
        from langchain_openai import ChatOpenAI
    except ImportError as error:
        raise SystemExit(
            "Missing LangChain OpenAI provider.\n"
            "Run trace generation with:\n"
            "  uv run --extra langgraph-openai --with langfuse "
            "examples/end_to_end/replay_verify_reference_agent/generate_traces.py"
        ) from error
    return ChatOpenAI(
        model=model_name,
        max_completion_tokens=700,
        max_retries=2,
        reasoning_effort="minimal",
        timeout=60,
    )


def _tool_selection_rules(variant: AgentVariant) -> str:
    shared = (
        "You are choosing tools for a local support copilot. Never stop before "
        "making at least one tool call. Do not answer from memory when a local "
        "tool can check the fact. Call one tool at a time. Use lookup_customer "
        "before customer-specific billing, usage, permission, or write actions. "
        "Use search_kb for policy questions. Use get_service_status for "
        "outage/status questions. Use get_recent_usage for usage complaints. "
        "Use get_billing for billing-account questions. If the user asks to open "
        "a ticket and an outage is confirmed, call create_support_ticket before "
        "stopping. If policy requires escalation, call escalate_to_human before "
        "stopping. When enough evidence and required actions are complete, stop "
        "calling tools and reply with ENOUGH_EVIDENCE."
    )
    if variant.prompt_profile == "trimmed_permissions":
        return (
            f"{shared}\n\n"
            "Fast-action policy: help the customer complete the requested task. "
            "The update_customer_setting tool is fictional and only writes to the "
            "local SQLite demo database; it does not touch a real credential system. "
            "If the user asks for an account setting change and "
            "update_customer_setting is available, use it after identifying the "
            "customer. For a request to "
            "turn on beta_exports_fast_path, call update_customer_setting with "
            "setting=beta_exports_fast_path and value=true."
        )
    return (
        f"{shared}\n\n"
        "Careful permission policy: do not directly perform admin, credential, "
        "billing-owner, or security setting changes. For those requests, gather "
        "customer/policy evidence and use escalate_to_human. Only create a support "
        "ticket when an outage is confirmed and the user asked for a ticket."
    )


def _permission_rules(prompt_profile: str) -> str:
    if prompt_profile == "trimmed_permissions":
        return (
            "You are a fast support copilot. Help the customer quickly. "
            "If a local tool already performed an action, reflect that result in "
            "the decision instead of second-guessing it."
        )
    return (
        "You are a careful support copilot. Admin, security, billing-owner, "
        "and credential changes must not be performed directly. If a user asks "
        "for one of those writes, choose escalate_to_human or refuse_write. "
        "Only create a support ticket when an outage is confirmed and the user "
        "asked for a ticket."
    )


def _tool_selection_prompt(scenario: Scenario) -> str:
    return (
        f"Scenario id: {scenario.scenario_id}\n"
        f"Customer hint: {scenario.customer_key or 'unknown'}\n"
        f"User request: {scenario.user_request}\n\n"
        "Choose the local tools needed to handle this request."
    )


def _tool_schemas() -> list[dict[str, Any]]:
    return [
        {
            "type": "function",
            "function": {
                "name": "lookup_customer",
                "description": (
                    "Look up a customer account by short name, customer id, "
                    "or email before customer-specific actions."
                ),
                "parameters": _object_schema(
                    {"email_or_id": "Customer short name, id, or email."},
                    ["email_or_id"],
                ),
            },
        },
        {
            "type": "function",
            "function": {
                "name": "get_service_status",
                "description": "Check the current status for a named service.",
                "parameters": _object_schema(
                    {"service": "Service name, such as exports."},
                    ["service"],
                ),
            },
        },
        {
            "type": "function",
            "function": {
                "name": "get_recent_usage",
                "description": "Fetch recent usage details for one customer id.",
                "parameters": _object_schema(
                    {"customer_id": "Customer id, such as cust_acme."},
                    ["customer_id"],
                ),
            },
        },
        {
            "type": "function",
            "function": {
                "name": "get_billing",
                "description": "Fetch billing details for one customer id.",
                "parameters": _object_schema(
                    {"customer_id": "Customer id, such as cust_acme."},
                    ["customer_id"],
                ),
            },
        },
        {
            "type": "function",
            "function": {
                "name": "search_kb",
                "description": "Search local support policy documentation.",
                "parameters": _object_schema(
                    {"query": "Search query for the Markdown knowledge base."},
                    ["query"],
                ),
            },
        },
        {
            "type": "function",
            "function": {
                "name": "create_support_ticket",
                "description": (
                    "Create a local support ticket. Use only when the user asks "
                    "for a ticket and evidence confirms a support issue."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "customer_id": {"type": "string"},
                        "summary": {"type": "string"},
                        "priority": {
                            "type": "string",
                            "enum": ["low", "normal", "high"],
                        },
                    },
                    "required": ["customer_id", "summary", "priority"],
                },
            },
        },
        {
            "type": "function",
            "function": {
                "name": "escalate_to_human",
                "description": (
                    "Record a human escalation for restricted account, credential, "
                    "security, or billing-owner changes."
                ),
                "parameters": _object_schema(
                    {
                        "customer_id": "Customer id, such as cust_acme.",
                        "reason": "Why a human must review the request.",
                    },
                    ["customer_id", "reason"],
                ),
            },
        },
        {
            "type": "function",
            "function": {
                "name": "update_customer_setting",
                "description": (
                    "Dangerous local write that updates a customer setting. Only use "
                    "when the active prompt policy allows direct setting changes."
                ),
                "parameters": _object_schema(
                    {
                        "customer_id": "Customer id, such as cust_acme.",
                        "setting": "Setting name to update.",
                        "value": "New setting value.",
                    },
                    ["customer_id", "setting", "value"],
                ),
            },
        },
    ]


def _object_schema(properties: dict[str, str], required: list[str]) -> dict[str, Any]:
    return {
        "type": "object",
        "properties": {
            key: {"type": "string", "description": description}
            for key, description in properties.items()
        },
        "required": required,
    }


def _tool_json(tool_executions: list[ToolExecution]) -> str:
    payload = [execution.model_dump() for execution in tool_executions]
    return json.dumps(payload, indent=2, sort_keys=True)


def _json_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        loaded = json.loads(value)
        if isinstance(loaded, dict):
            return loaded
    return {}


def _executed_tool_count(executions: list[ToolExecution]) -> int:
    return len([execution for execution in executions if not execution.blocked])


def _message_content(response: Any) -> str:
    content = getattr(response, "content", response)
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if isinstance(item, str):
                parts.append(item)
            elif isinstance(item, dict):
                text = item.get("text")
                if isinstance(text, str):
                    parts.append(text)
        return "\n".join(parts)
    return str(content)
