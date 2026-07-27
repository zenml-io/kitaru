#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at:
#
#       https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
#  or implied. See the License for the specific language governing
#  permissions and limitations under the License.
"""Score job process entrypoint."""

import asyncio
import sys
import uuid
from collections.abc import Callable
from pathlib import Path

from kitaru.api_models.v1.jobs import (
    JobSpecScorer,
    JobUpdateRequest,
    SourceScorerConfig,
)
from kitaru.client.api_client import KitaruAPIClient
from kitaru.plugin_loader import (
    PluginLoadError,
    load_plugin_module,
    required_env,
    run_harness,
)
from kitaru.scoring import (
    ScoringError,
    SessionView,
    call_scorer,
    load_scorer,
    scorer_attribute,
)

PLUGIN_MODULE_NAME = "kitaru_scorer_plugin"


def load_plugin_scorer(path: Path, entrypoint: str) -> Callable[..., float]:
    """Import a scorer from a materialized code file.

    Args:
        path: Path of the code file.
        entrypoint: Attribute implementing the scorer.

    Raises:
        ScoringError: The file does not import, or the attribute is
            missing or not callable.

    Returns:
        Scorer function.
    """
    try:
        module = load_plugin_module(PLUGIN_MODULE_NAME, path)
    except PluginLoadError as exc:
        raise ScoringError(f"Failed to import scorer code from {path}: {exc}") from exc
    return scorer_attribute(module, entrypoint)


def resolve_scorer(scorer: JobSpecScorer) -> Callable[..., float]:
    """Load the scorer function a score job runs.

    Registered code is imported from the file the worker materialized,
    source references resolve against the ambient environment.

    Args:
        scorer: Scorer of the job spec.

    Raises:
        ScoringError: The code does not import, or the attribute is
            missing or not callable.

    Returns:
        Scorer function.
    """
    if scorer.plugin is not None:
        path = Path(required_env("KITARU_JOB_PLUGIN_PATH", ScoringError))
        return load_plugin_scorer(path, scorer.plugin.entrypoint)
    if not isinstance(scorer.config, SourceScorerConfig):
        raise ScoringError(f"Scorer {scorer.config.name!r} has no code to run")
    return load_scorer(scorer.config.source)


async def score_job(client: KitaruAPIClient, job_id: uuid.UUID) -> float:
    """Score the session of a score job and record the score.

    Args:
        client: API client.
        job_id: Id of the job.

    Raises:
        ScoringError: The job is not a score job, its scorer does not
            load, or the scorer raised or returned an invalid score.
        APIError: A read or the score update failed.

    Returns:
        Recorded score.
    """
    spec = await client.jobs.get_spec(job_id)
    if spec.scorer is None:
        raise ScoringError(f"Job {job_id} is not a score job")
    session, nodes = await asyncio.gather(
        client.sessions.get(spec.scorer.input_session_id),
        client.session_nodes.list(spec.scorer.input_session_id, include_payloads=True),
    )
    scorer = resolve_scorer(spec.scorer)
    score = call_scorer(
        spec.scorer.config.name,
        scorer,
        SessionView(session=session, nodes=nodes),
        spec.scorer.config.params,
    )
    await client.jobs.update(job_id, JobUpdateRequest(score=score))
    return score


async def run() -> None:
    """Run the score job named by the process environment.

    Raises:
        ScoringError: The environment is incomplete or the scoring
            failed.
        APIError: A read or the score update failed.
    """
    job_id = uuid.UUID(required_env("KITARU_JOB_ID", ScoringError))
    async with KitaruAPIClient(
        base_url=required_env("KITARU_API_URL", ScoringError),
        api_key=required_env("KITARU_API_KEY", ScoringError),
    ) as client:
        await score_job(client, job_id)


def main() -> int:
    """Run the score job process.

    Returns:
        Exit code.
    """
    return run_harness(run)


if __name__ == "__main__":
    sys.exit(main())
