#!/usr/bin/env python
# ruff: noqa: E402,I001
"""Run a local Kitaru/LangGraph fork experiment for the reference agent."""

import argparse
import sys
import tempfile
import time
from pathlib import Path
from typing import Annotated, Any, cast
from uuid import uuid4

REPO_ROOT = Path(__file__).resolve().parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from langgraph.checkpoint.memory import InMemorySaver
from pydantic import BaseModel
from zenml.client import Client
from zenml.types import HTMLString

from kitaru import checkpoint, flow
from kitaru.adapters.langgraph import (
    KitaruGraphRunner,
    LangGraphCapturePolicy,
    LangGraphDurabilityPolicy,
    LangGraphRunRequest,
    LangGraphRunResult,
)

from examples.end_to_end.replay_verify_reference_agent import db
from examples.end_to_end.replay_verify_reference_agent.config import (
    EXAMPLE_DIR,
    AgentVariant,
    Scenario,
    SupportDecision,
    load_scenarios,
    load_variant,
)
from examples.end_to_end.replay_verify_reference_agent.fork_report import (
    ForkDemoReport,
    VariantSummary,
    render_fork_demo_html,
)
from examples.end_to_end.replay_verify_reference_agent.graph import build_graph
from examples.end_to_end.replay_verify_reference_agent.mock_api import MockApiServer
from examples.end_to_end.replay_verify_reference_agent.tools import (
    SupportTools,
    ToolExecution,
)

DEFAULT_SCENARIO_ID = "account_setting_change_request"
DEFAULT_BASELINE_VARIANT = "baseline"
DEFAULT_CANDIDATE_VARIANT = "nano_trimmed_permissions"
DEFAULT_REPORT_PATH = EXAMPLE_DIR / "reports" / "fork-demo.html"
REPORT_ARTIFACT_NAME = "fork_demo_report"


class ForkDemoFlowResult(BaseModel):
    """Small serializable result returned by the demo flow."""

    report_path: str
    report_artifact_name: str
    thread_id: str
    baseline_latest_checkpoint_id: str | None
    selected_checkpoint_id: str | None
    fork_checkpoint_id: str | None
    terminal_fork_checkpoint_id: str | None
    baseline_required_action: str
    forked_required_action: str
    tool_collection_rerun: bool


@checkpoint(cache=False)
def publish_fork_demo_report(
    report: ForkDemoReport,
) -> Annotated[HTMLString, "fork_demo_report"]:
    """Publish the fork demo report as a Kitaru HTML artifact."""
    return HTMLString(render_fork_demo_html(report))


@flow(cache=False)
def replay_verify_fork_demo_flow(
    scenario_id: str = DEFAULT_SCENARIO_ID,
    candidate: str = DEFAULT_CANDIDATE_VARIANT,
    baseline: str = DEFAULT_BASELINE_VARIANT,
    report_path: str = str(DEFAULT_REPORT_PATH),
) -> ForkDemoFlowResult:
    """Run baseline, fork at the post-tool checkpoint, and publish a report."""
    result = run_fork_demo_experiment(
        scenario_id=scenario_id,
        baseline_variant_name=baseline,
        candidate_variant_name=candidate,
        report_path=Path(report_path),
    )
    report_html = publish_fork_demo_report(result.report)
    return finalize_fork_demo_result(
        result.flow_result,
        result.baseline_result,
        result.fork_result,
        report_html,
    )


@checkpoint(cache=False)
def finalize_fork_demo_result(
    flow_result: ForkDemoFlowResult,
    baseline_result: LangGraphRunResult,
    fork_result: LangGraphRunResult,
    report_html: HTMLString,
) -> Annotated[ForkDemoFlowResult, "fork_demo_result"]:
    """Consume all demo branches so `.run().wait()` has one result."""
    _ = baseline_result, fork_result, report_html
    return flow_result


class _ExperimentResult(BaseModel):
    report: ForkDemoReport
    flow_result: ForkDemoFlowResult
    baseline_result: LangGraphRunResult
    fork_result: LangGraphRunResult


def run_fork_demo_experiment(
    *,
    scenario_id: str = DEFAULT_SCENARIO_ID,
    baseline_variant_name: str = DEFAULT_BASELINE_VARIANT,
    candidate_variant_name: str = DEFAULT_CANDIDATE_VARIANT,
    report_path: Path = DEFAULT_REPORT_PATH,
) -> _ExperimentResult:
    """Run the deterministic fork mechanics used by the Kitaru flow and tests."""
    scenario = _select_scenario(scenario_id)
    baseline_variant = load_variant(baseline_variant_name)
    candidate_variant = load_variant(candidate_variant_name)
    thread_id = f"fork-demo-{scenario.scenario_id}-{uuid4().hex[:8]}"

    with tempfile.TemporaryDirectory(prefix="kitaru-fork-demo-") as tmp_dir:
        db_path = Path(tmp_dir) / "reference_agent.sqlite"
        db.reset_database(db_path=db_path)
        with MockApiServer() as api:
            tools = SupportTools(
                db_path=db_path,
                api_base_url=api.base_url,
                kb_dir=EXAMPLE_DIR / "knowledge_base",
            )
            graph = build_graph(
                tools=tools,
                callbacks=[],
                metadata={"scenario_id": scenario.scenario_id},
                tags=["kitaru", "replay-verify", "fork-demo"],
                checkpointer=InMemorySaver(),
                collect_evidence_fn=collect_evidence_deterministically,
                summarize_evidence_fn=summarize_evidence_deterministically,
                decide_fn=decide_deterministically,
            )
            baseline_runner = _graph_runner(graph, name="replay_verify_baseline")
            fork_runner = _graph_runner(graph, name="replay_verify_candidate_fork")

            baseline_result = baseline_runner.invoke(
                LangGraphRunRequest.start(
                    {"scenario": scenario, "variant": baseline_variant},
                    thread_id=thread_id,
                    metadata={
                        "scenario_id": scenario.scenario_id,
                        "variant_name": baseline_variant.name,
                        "run_role": "baseline",
                    },
                )
            )
            selected_snapshot = _post_tool_snapshot(graph, thread_id=thread_id)
            selected_checkpoint_id = _checkpoint_id(selected_snapshot.config)
            fork_config = graph.update_state(
                selected_snapshot.config,
                values={"variant": candidate_variant},
            )
            fork_checkpoint_id = _checkpoint_id(fork_config)
            fork_result = fork_runner.invoke(
                LangGraphRunRequest.start(
                    None,
                    thread_id=thread_id,
                    checkpoint_ns=_checkpoint_ns(fork_config),
                    metadata={
                        "scenario_id": scenario.scenario_id,
                        "variant_name": candidate_variant.name,
                        "run_role": "candidate_fork",
                        "fork_source_checkpoint_id": selected_checkpoint_id,
                        "fork_checkpoint_id": fork_checkpoint_id,
                    },
                )
            )

    report = _build_report(
        scenario=scenario,
        baseline_variant=baseline_variant,
        candidate_variant=candidate_variant,
        thread_id=thread_id,
        baseline_result=baseline_result,
        fork_result=fork_result,
        selected_checkpoint_id=selected_checkpoint_id,
        fork_checkpoint_id=fork_checkpoint_id,
    )
    html = render_fork_demo_html(report)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(html, encoding="utf-8")
    flow_result = ForkDemoFlowResult(
        report_path=str(report_path),
        report_artifact_name=REPORT_ARTIFACT_NAME,
        thread_id=thread_id,
        baseline_latest_checkpoint_id=report.baseline_latest_checkpoint_id,
        selected_checkpoint_id=report.selected_checkpoint_id,
        fork_checkpoint_id=report.fork_checkpoint_id,
        terminal_fork_checkpoint_id=report.terminal_fork_checkpoint_id,
        baseline_required_action=str(
            report.baseline_decision.get("required_action", "unknown")
        ),
        forked_required_action=str(
            report.forked_decision.get("required_action", "unknown")
        ),
        tool_collection_rerun=report.tool_collection_rerun,
    )
    return _ExperimentResult(
        report=report,
        flow_result=flow_result,
        baseline_result=baseline_result,
        fork_result=fork_result,
    )


def collect_evidence_deterministically(
    *,
    scenario: Scenario,
    variant: AgentVariant,
    tools: SupportTools,
    callbacks: list[Any],
    metadata: dict[str, Any],
    tags: list[str],
) -> list[ToolExecution]:
    """Collect stable local evidence without calling an LLM."""
    _ = callbacks, metadata, tags
    executions: list[ToolExecution] = []
    if scenario.customer_key:
        lookup = tools.run("lookup_customer", {"email_or_id": scenario.customer_key})
        executions.append(lookup)
        customer_id = str(lookup.result.get("customer_id", "unknown"))
    else:
        customer_id = "unknown"
    executions.append(
        tools.run(
            "search_kb",
            {"query": "permission policy account setting restricted write"},
        )
    )
    if variant.allows_tool("escalate_to_human"):
        executions.append(
            tools.run(
                "escalate_to_human",
                {
                    "customer_id": customer_id,
                    "reason": (
                        "Restricted account setting changes require human review."
                    ),
                },
            )
        )
    return executions


def summarize_evidence_deterministically(
    *,
    scenario: Scenario,
    variant: AgentVariant,
    tool_executions: list[ToolExecution],
    callbacks: list[Any],
    metadata: dict[str, Any],
    tags: list[str],
) -> str:
    """Produce a stable summary that changes when the active variant changes."""
    _ = scenario, callbacks, metadata, tags
    tool_names = ", ".join(execution.name for execution in tool_executions)
    if variant.prompt_profile == "trimmed_permissions":
        return (
            "Candidate summary: the fork reused the already-collected lookup, "
            "policy, and escalation evidence. The active variant now uses the "
            "faster permissions profile, so downstream handling becomes more "
            "permissive without rerunning tool collection. "
            f"Reused tools: {tool_names}."
        )
    return (
        "Baseline summary: the run collected lookup and policy evidence, then "
        "recorded a human escalation for the restricted account-setting request. "
        f"Collected tools: {tool_names}."
    )


def decide_deterministically(
    *,
    scenario: Scenario,
    variant: AgentVariant,
    evidence_summary: str,
    tool_executions: list[ToolExecution],
    callbacks: list[Any],
    metadata: dict[str, Any],
    tags: list[str],
) -> SupportDecision:
    """Return a stable final decision that reflects the active variant."""
    _ = callbacks, metadata, tags
    evidence_ids = [
        evidence_id
        for execution in tool_executions
        for evidence_id in execution.evidence_ids
    ]
    tool_names = [execution.name for execution in tool_executions]
    if variant.prompt_profile == "trimmed_permissions":
        return SupportDecision(
            policy_label="permissions_policy",
            risk_status="safe",
            required_action="answer_directly",
            summary=(
                "The candidate fork used the faster permission profile after "
                "the post-tool checkpoint. It changed the response posture, but "
                "the fork did not rerun tools or perform a direct setting write."
            ),
            evidence_ids=evidence_ids,
            tool_names=tool_names,
        )
    return SupportDecision(
        policy_label="permissions_policy",
        risk_status="needs_review",
        required_action=cast(Any, scenario.expected_required_action),
        summary=evidence_summary,
        evidence_ids=evidence_ids,
        tool_names=tool_names,
    )


def _graph_runner(graph: Any, *, name: str) -> KitaruGraphRunner:
    return KitaruGraphRunner(
        graph,
        name=name,
        durability=LangGraphDurabilityPolicy(require_checkpointer=True),
        capture=LangGraphCapturePolicy(save_state_values=True),
    )


def _select_scenario(scenario_id: str) -> Scenario:
    for scenario in load_scenarios():
        if scenario.scenario_id == scenario_id:
            return scenario
    raise ValueError(f"Unknown scenario: {scenario_id}")


def _post_tool_snapshot(graph: Any, *, thread_id: str) -> Any:
    history = list(graph.get_state_history({"configurable": {"thread_id": thread_id}}))
    for snapshot in history:
        if tuple(getattr(snapshot, "next", ()) or ()) == ("summarize_evidence",):
            return snapshot
    seen = [tuple(getattr(snapshot, "next", ()) or ()) for snapshot in history]
    raise RuntimeError(
        "Could not find LangGraph checkpoint with next == "
        f"('summarize_evidence',). Saw next values: {seen}"
    )


def _build_report(
    *,
    scenario: Scenario,
    baseline_variant: AgentVariant,
    candidate_variant: AgentVariant,
    thread_id: str,
    baseline_result: LangGraphRunResult,
    fork_result: LangGraphRunResult,
    selected_checkpoint_id: str | None,
    fork_checkpoint_id: str | None,
) -> ForkDemoReport:
    baseline_output = _final_output(baseline_result)
    forked_output = _final_output(fork_result)
    baseline_tools = _tool_execution_names(baseline_output)
    forked_tools = _tool_execution_names(forked_output)
    return ForkDemoReport(
        thread_id=thread_id,
        scenario_id=scenario.scenario_id,
        selected_checkpoint_id=selected_checkpoint_id,
        baseline_latest_checkpoint_id=baseline_result.latest_checkpoint_id,
        fork_checkpoint_id=fork_checkpoint_id,
        terminal_fork_checkpoint_id=fork_result.latest_checkpoint_id,
        baseline_variant=_variant_summary(baseline_variant),
        candidate_variant=_variant_summary(candidate_variant),
        baseline_decision=cast(dict[str, Any], baseline_output["decision"]),
        forked_decision=cast(dict[str, Any], forked_output["decision"]),
        baseline_evidence_summary=str(baseline_output["evidence_summary"]),
        forked_evidence_summary=str(forked_output["evidence_summary"]),
        baseline_audit_relevant_tool_names=list(
            baseline_output["audit_relevant_tool_names"]
        ),
        forked_audit_relevant_tool_names=list(
            forked_output["audit_relevant_tool_names"]
        ),
        baseline_tool_execution_names=baseline_tools,
        forked_tool_execution_names=forked_tools,
        changed_tool_execution_names=_changed_tool_names(baseline_tools, forked_tools),
        warnings=[*baseline_result.warnings, *fork_result.warnings],
    )


def _variant_summary(variant: AgentVariant) -> VariantSummary:
    return VariantSummary(
        name=variant.name,
        model=variant.model,
        prompt_profile=variant.prompt_profile,
        tool_policy_name=variant.tool_policy_name,
    )


def _final_output(result: LangGraphRunResult) -> dict[str, Any]:
    output = cast(dict[str, Any], result.output)
    final_output = output.get("final_output")
    if not isinstance(final_output, dict):
        raise RuntimeError("LangGraph run did not produce a final_output mapping.")
    return final_output


def _tool_execution_names(output: dict[str, Any]) -> list[str]:
    executions = output.get("tool_executions", [])
    if not isinstance(executions, list):
        return []
    return [str(execution.get("name", "unknown")) for execution in executions]


def _changed_tool_names(baseline: list[str], forked: list[str]) -> list[str]:
    return sorted(set(baseline).symmetric_difference(forked))


def _checkpoint_id(config: Any) -> str | None:
    configurable = _configurable(config)
    value = configurable.get("checkpoint_id")
    return str(value) if value is not None else None


def _checkpoint_ns(config: Any) -> str | None:
    configurable = _configurable(config)
    value = configurable.get("checkpoint_ns")
    if value is None:
        return None
    value = str(value)
    return value or None


def _configurable(config: Any) -> dict[str, Any]:
    if isinstance(config, dict):
        configurable = config.get("configurable", {})
        if isinstance(configurable, dict):
            return configurable
    return {}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse fork-demo CLI arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scenario", default=DEFAULT_SCENARIO_ID)
    parser.add_argument("--candidate", default=DEFAULT_CANDIDATE_VARIANT)
    parser.add_argument("--baseline", default=DEFAULT_BASELINE_VARIANT)
    parser.add_argument(
        "--report-path",
        default=str(DEFAULT_REPORT_PATH),
        help="Local HTML output path. Defaults to examples/.../reports/fork-demo.html.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Run the Kitaru flow and print where to inspect the result."""
    args = parse_args(argv)
    handle = replay_verify_fork_demo_flow.run(
        scenario_id=args.scenario,
        candidate=args.candidate,
        baseline=args.baseline,
        report_path=args.report_path,
    )
    _wait_for_execution(handle.exec_id)
    report_path = Path(args.report_path)
    print(f"Kitaru execution id: {handle.exec_id}")
    print(f"HTML report: {report_path}")
    print(f"Report artifact: {REPORT_ARTIFACT_NAME}")
    if report_path.exists():
        print("Report check: local HTML file exists")
    else:
        print("Report check: local HTML file was not found", file=sys.stderr)
        return 1
    return 0


def _wait_for_execution(exec_id: str, *, timeout_seconds: int = 120) -> None:
    deadline = time.monotonic() + timeout_seconds
    last_status = "unknown"
    while time.monotonic() < deadline:
        run = Client().get_pipeline_run(exec_id, allow_name_prefix_match=False)
        last_status = str(run.status)
        if run.status.is_finished:
            if not run.status.is_successful:
                raise RuntimeError(
                    f"Kitaru execution {exec_id} finished as {run.status}."
                )
            return
        time.sleep(0.5)
    raise TimeoutError(
        f"Kitaru execution {exec_id} did not finish within "
        f"{timeout_seconds}s; last status was {last_status}."
    )


if __name__ == "__main__":
    raise SystemExit(main())
