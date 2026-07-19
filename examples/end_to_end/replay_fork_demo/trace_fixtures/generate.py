"""Generate Langfuse traces for the support-agent example scenarios."""

import json
from datetime import UTC, datetime
from uuid import uuid4

import click
from langfuse import get_client  # ty: ignore[unresolved-import]
from reference_agent import db
from reference_agent.agent import SupportAgentDeps, build_support_agent
from reference_agent.config import (
    EXAMPLE_DIR,
    IMPORTED_SOURCE_VARIANT,
    imported_source_version_for_variant,
    load_scenarios,
    load_variant,
    select_scenarios,
)
from reference_agent.mock_api import MockApiServer


def _generate_trace(
    scenario_id: str,
    variant_name: str,
    generation_id: str,
) -> str:
    """Run one scenario and record its inputs, tools, and decision in Langfuse."""
    scenarios = {scenario.scenario_id: scenario for scenario in load_scenarios()}
    scenario = scenarios[scenario_id]
    source_version = imported_source_version_for_variant(variant_name)
    variant = load_variant(variant_name)
    agent = build_support_agent(variant, name="support-agent")
    langfuse = get_client()
    trace_id = uuid4().hex

    db.reset_database()
    with MockApiServer() as api:
        deps = SupportAgentDeps(
            scenario=scenario,
            variant=variant,
            db_path=db.DEFAULT_DB_PATH,
            api_base_url=api.base_url,
            kb_dir=EXAMPLE_DIR / "knowledge_base",
        )
        with langfuse.start_as_current_observation(
            as_type="span",
            name="support-agent",
            input={"request": scenario.user_request},
            version=source_version,
            metadata={
                "intent": scenario.expected_policy_label.removesuffix("_policy"),
                "scenario_id": scenario.scenario_id,
                "case_id": scenario.case_id,
                "agent_version": source_version,
                "variant": variant.name,
                "fixture_generation_id": generation_id,
            },
            trace_context={"trace_id": trace_id},
        ) as root:
            result = agent.wrapped.run_sync(scenario.user_request, deps=deps)
            root.update(output=json.loads(result.output))

    langfuse.flush()
    return trace_id


@click.command()
@click.option("--set", "scenario_set", default="smoke", show_default=True)
@click.option("--scenario")
@click.option(
    "--variant",
    type=click.Choice([IMPORTED_SOURCE_VARIANT]),
    default=IMPORTED_SOURCE_VARIANT,
    show_default=True,
)
@click.option("--generation-id")
def cli(
    scenario_set: str,
    scenario: str | None,
    variant: str,
    generation_id: str | None,
) -> None:
    """Run a scenario set and print its Langfuse trace URIs."""
    all_scenarios = load_scenarios()
    if scenario is None:
        selected = select_scenarios(scenario_set, all_scenarios)
    else:
        selected = [item for item in all_scenarios if item.scenario_id == scenario]
        if not selected:
            raise click.ClickException(f"Unknown scenario: {scenario}")

    resolved_generation_id = generation_id or datetime.now(UTC).strftime(
        "kitaru-replay-example-%Y%m%dT%H%M%SZ"
    )
    click.echo(f"fixture_generation_id={resolved_generation_id}")
    for item in selected:
        trace_id = _generate_trace(
            item.scenario_id,
            variant,
            resolved_generation_id,
        )
        click.echo(f"{item.scenario_id}\tlangfuse://trace/{trace_id}")


if __name__ == "__main__":
    cli()
