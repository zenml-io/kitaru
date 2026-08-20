"""Register the dummy agent and evaluators against a running server."""

import argparse
import asyncio
import sys
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from pathlib import Path
from typing import Protocol, TypeVar

from kitaru.analytics.events import FINISHED_ONBOARDING_SURVEY_KEY
from kitaru.api_models.v1.account import UserUpdateRequest
from kitaru.api_models.v1.agent import AgentCreateRequest
from kitaru.api_models.v1.agent_version import AgentVersionCreateRequest, RunSpec
from kitaru.api_models.v1.evaluator import (
    EvaluatorCreateRequest,
    EvaluatorVersionCreateRequest,
)
from kitaru.api_models.v1.importer import (
    ImporterCreateRequest,
    ImporterVersionCreateRequest,
)
from kitaru.api_models.v1.plugin import ScriptPluginSource
from kitaru.client.api_client import KitaruAPIClient
from kitaru.client.exceptions import APIError

DEVTOOLS_DIR = Path(__file__).resolve().parent
AGENT_FILE = DEVTOOLS_DIR / "agent.py"
EVALUATORS_FILE = DEVTOOLS_DIR / "evaluators.py"
# The built-in kitaru/kitaru-jsonl importer installs its plugin package from
# PyPI at task time. Registering a script plugin with the same parse contract
# keeps imports working in offline or version-pinned environments.
JSONL_IMPORTER_FILE = DEVTOOLS_DIR / "importer.py"

DEFAULT_AGENT_NAME = "dummy-agent"
DEFAULT_IMPORTER_NAME = "dummy-jsonl"
AGENT_TIMEOUT_SECONDS = 120

EVALUATOR_ENTRYPOINTS = {
    "dummy-outcome": "evaluate_outcome",
    "dummy-expected-match": "evaluate_expected_match",
    "dummy-efficiency": "evaluate_efficiency",
    "dummy-grade": "evaluate_grade",
    "dummy-notes": "evaluate_notes",
    "dummy-suite": "evaluate_suite",
}


class _NamedResource(Protocol):
    """Named resource."""

    name: str


T = TypeVar("T", bound=_NamedResource)


async def _get_or_create(
    create: Callable[[], Awaitable[T]],
    iterate: Callable[[], AsyncIterator[T]],
    name: str,
    kind: str,
) -> T:
    """Create a resource, reusing an existing one with the same name."""
    try:
        return await create()
    except APIError as exc:
        if exc.status_code != 409:
            raise
    async for resource in iterate():
        if resource.name == name:
            return resource
    raise RuntimeError(f"{kind} {name!r} conflicts but cannot be found")


async def register_importer(
    client: KitaruAPIClient, name: str = DEFAULT_IMPORTER_NAME
) -> uuid.UUID:
    """Register the jsonl importer parser as a script plugin."""
    blob = await client.blobs.upload(
        JSONL_IMPORTER_FILE.read_bytes(),
        media_type="text/x-python",
        filename=JSONL_IMPORTER_FILE.name,
    )
    importer = await _get_or_create(
        lambda: client.importers.create(
            ImporterCreateRequest(name=name, provider="kitaru-dummy")
        ),
        client.importers.iter,
        name,
        "Importer",
    )
    await client.importers.create_version(
        importer.id,
        ImporterVersionCreateRequest(
            source=ScriptPluginSource(blob_id=blob.id, entrypoint="parse")
        ),
    )
    return importer.id


async def register_evaluators(client: KitaruAPIClient) -> dict[str, uuid.UUID]:
    """Register every dummy evaluator from the shared plugin file."""
    blob = await client.blobs.upload(
        EVALUATORS_FILE.read_bytes(),
        media_type="text/x-python",
        filename=EVALUATORS_FILE.name,
    )
    ids: dict[str, uuid.UUID] = {}
    for name, entrypoint in EVALUATOR_ENTRYPOINTS.items():
        evaluator = await _get_or_create(
            lambda name=name: client.evaluators.create(
                EvaluatorCreateRequest(name=name)
            ),
            client.evaluators.iter,
            name,
            "Evaluator",
        )
        await client.evaluators.create_version(
            evaluator.id,
            EvaluatorVersionCreateRequest(
                source=ScriptPluginSource(blob_id=blob.id, entrypoint=entrypoint)
            ),
        )
        ids[name] = evaluator.id
    return ids


async def register_agent(
    client: KitaruAPIClient,
    name: str = DEFAULT_AGENT_NAME,
    extra_env: dict[str, str] | None = None,
    display_version: str = "dummy-v1",
) -> tuple[uuid.UUID, uuid.UUID]:
    """Register the dummy agent and a runnable version."""
    agent = await _get_or_create(
        lambda: client.agents.create(
            AgentCreateRequest(name=name, description="Dummy research agent")
        ),
        client.agents.iter,
        name,
        "Agent",
    )
    run_spec = RunSpec(
        command=f'"{sys.executable}" "{AGENT_FILE}"',
        working_dir=str(DEVTOOLS_DIR),
        env={"DUMMY_AGENT_ID": str(agent.id), **(extra_env or {})},
        timeout_seconds=AGENT_TIMEOUT_SECONDS,
    )
    version = await client.agents.create_version(
        agent.id,
        AgentVersionCreateRequest(display_version=display_version, run_spec=run_spec),
    )
    return agent.id, version.id


async def finish_onboarding(client: KitaruAPIClient) -> bool:
    """Mark the current account's onboarding survey finished, skipping the UI form."""
    account = await client.accounts.get_current()
    if account.is_service_account:
        return False
    if account.metadata.get(FINISHED_ONBOARDING_SURVEY_KEY):
        return True
    metadata = {**account.metadata, FINISHED_ONBOARDING_SURVEY_KEY: True}
    await client.users.update(account.id, UserUpdateRequest(metadata=metadata))
    return True


async def register_all(
    client: KitaruAPIClient, agent_name: str = DEFAULT_AGENT_NAME
) -> tuple[uuid.UUID, uuid.UUID, dict[str, uuid.UUID]]:
    """Register the dummy agent, importer, and evaluators, returning their ids."""
    await finish_onboarding(client)
    await register_importer(client)
    evaluator_ids = await register_evaluators(client)
    agent_id, agent_version_id = await register_agent(client, agent_name)
    return agent_id, agent_version_id, evaluator_ids


async def _main(args: argparse.Namespace) -> None:
    """Register everything against the configured server."""
    extra_env = dict(pair.split("=", 1) for pair in args.agent_env)
    async with KitaruAPIClient() as client:
        onboarded = await finish_onboarding(client)
        importer_id = await register_importer(client)
        evaluator_ids = await register_evaluators(client)
        agent_id, agent_version_id = await register_agent(
            client,
            args.agent_name,
            extra_env=extra_env,
            display_version=args.display_version,
        )
    if onboarded:
        print("onboarding survey: marked finished")
    else:
        print("onboarding survey: skipped, the credential is a service account")
    print(f"agent {args.agent_name}: {agent_id} (version {agent_version_id})")
    print(f"importer {DEFAULT_IMPORTER_NAME}: {importer_id}")
    for name, evaluator_id in evaluator_ids.items():
        print(f"evaluator {name}: {evaluator_id}")


def main() -> int:
    """Register fixtures from CLI flags."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--agent-name", default=DEFAULT_AGENT_NAME)
    parser.add_argument("--display-version", default="dummy-v1")
    parser.add_argument(
        "--agent-env",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="Extra agent process environment variable, repeatable.",
    )
    asyncio.run(_main(parser.parse_args()))
    return 0


if __name__ == "__main__":
    sys.exit(main())
