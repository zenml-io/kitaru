"""Live PydanticAI customer-support flow for Replay Lab model swaps.

The flow runs a PydanticAI support agent over one imported case. The model is a
Kitaru alias passed as a normal flow input, so Replay Lab can replay the same
case against a cheaper candidate alias and compare behavior. The agent's tools
are deterministic fakes (see ``support_tools``), so the only thing that changes
between baseline and candidate replay is the model — which is what makes the
comparison an honest model regression test.

The PydanticAI agent run is wrapped in one explicit ``@checkpoint`` so there is
a single stable replay anchor (``run_support_agent``) to replay from.
"""

import argparse
import sys
from pathlib import Path
from typing import Any

from pydantic_ai import Agent

import kitaru
from kitaru import checkpoint, flow
from kitaru.adapters.pydantic_ai import KitaruAgent
from kitaru.config import resolve_model_selection
from kitaru.errors import KitaruUsageError

# Allow `examples.*` imports when run as a direct script.
if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[4]))

from examples.chatbot.support_tools import (
    check_stock,
    issue_refund,
    lookup_order,
)

try:  # Package import path used by the seeder.
    from .support_cases import get_case
except ImportError:  # Direct script path.
    from support_cases import get_case  # type: ignore[no-redef]

RUNNER_NAME = "support_agent"
# The replay anchor is the checkpoint that wraps the agent turn; replaying from
# here re-runs the agent under the candidate model while reusing the loaded case.
REPLAY_ANCHOR = "run_support_agent"
SUMMARY_ARTIFACT = "scorecard"
FINAL_RESPONSE_ARTIFACT = "final_response"
SYSTEM_PROMPT = (
    "You are a concise retail customer-support assistant for a home-improvement "
    "store. Use check_stock for availability and lookup_order for order status "
    "before answering. If the customer asks for a refund, call issue_refund; if "
    "it reports that verification is required, do NOT tell the customer the "
    "refund is done — say you are escalating to a human specialist. Keep replies "
    "to two or three sentences."
)


@checkpoint
def load_support_case(case_id: str) -> dict[str, Any]:
    """Load one synthetic support case by stable id."""
    return get_case(case_id)


@checkpoint
def run_support_agent(case: dict[str, Any], model: str) -> dict[str, Any]:
    """Run the PydanticAI support agent for one case under a model alias.

    Wrapped in a single checkpoint so Replay Lab has one stable anchor to
    replay from when swapping the model.
    """
    provider_model = _resolve_alias(model)
    agent = Agent(provider_model, name=RUNNER_NAME, system_prompt=SYSTEM_PROMPT)

    @agent.tool_plain
    def check_stock_tool(product: str) -> str:
        return check_stock(product)

    @agent.tool_plain
    def lookup_order_tool(order_id: str) -> str:
        return lookup_order(order_id)

    @agent.tool_plain
    def issue_refund_tool(order_id: str, amount: str) -> str:
        return issue_refund(order_id, amount)

    durable = KitaruAgent(agent)
    result = durable.run_sync(str(case["message"]))
    final_response = str(result.output)
    return {"final_response": final_response, "model_alias": model}


@checkpoint
def publish_support_result(
    *,
    case: dict[str, Any],
    agent_output: dict[str, Any],
) -> str:
    """Save the artifacts Replay Lab compares across lanes."""
    final_response = str(agent_output["final_response"])
    scorecard = {
        "case_id": case["case_id"],
        "scenario_version": "pydantic_support_v1",
        "model_alias": agent_output["model_alias"],
        "response_chars": len(final_response),
    }
    kitaru.save(SUMMARY_ARTIFACT, scorecard, type="output")
    kitaru.save(FINAL_RESPONSE_ARTIFACT, final_response, type="response")
    return final_response


@flow(cache=False)
def support_agent_case(case_id: str, model: str) -> str:
    """Run one live PydanticAI support case under a Kitaru model alias."""
    case = load_support_case(case_id)
    agent_output = run_support_agent(case, model)
    return publish_support_result(case=case, agent_output=agent_output)


def _resolve_alias(model: str) -> str:
    """Require a registered Kitaru alias and return its provider/model string."""
    if "/" in model or ":" in model:
        raise KitaruUsageError(
            "Pass a registered Kitaru model alias, not a provider/model string."
        )
    selection = resolve_model_selection(model)
    if selection.alias is None or not selection.resolved_model:
        raise KitaruUsageError(
            f"Model `{model}` is not a registered Kitaru alias. Register it with "
            "`kitaru model register <alias> --model <provider/model>`."
        )
    # PydanticAI expects e.g. 'openai:gpt-4o-mini'; aliases store 'openai/...'.
    return selection.resolved_model.replace("/", ":", 1)


def run_workflow(case_id: str, model: str) -> tuple[str, str]:
    """Run the flow and return execution id plus final response."""
    handle = support_agent_case.run(case_id, model=model)
    final = handle.wait()
    return handle.exec_id, str(final)


def main(argv: list[str] | None = None) -> int:
    """Run one support case and print the result."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("case_id", nargs="?", default="refund-unverified")
    parser.add_argument("--model", default="current", help="Kitaru model alias.")
    args = parser.parse_args(argv)
    exec_id, final_response = run_workflow(args.case_id, args.model)
    print(f"Execution: {exec_id}")
    print(final_response)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
