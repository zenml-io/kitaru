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
"""Sample data seeding across the resource services."""

import gzip
import uuid
from collections.abc import AsyncIterator
from pathlib import Path

from kitaru.analytics.events import AnalyticsEvent
from kitaru.api_models.v1.annotation import AnnotationSelector
from kitaru.api_models.v1.filter import FilterOp
from kitaru.api_models.v1.investigation import (
    InvestigationSessionHighlight,
    InvestigationSessionQuestion,
)
from kitaru.api_models.v1.tag import TagResourceType
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.cohort import CohortCreate, CohortVersionCreate
from kitaru.server.application.models.evaluation import EvaluationCreate
from kitaru.server.application.models.experiment import ExperimentCreate
from kitaru.server.application.models.investigation import (
    InvestigationCreate,
    InvestigationSessionInput,
)
from kitaru.server.application.models.plugin import PluginCreate
from kitaru.server.application.models.sample_data import (
    SampleData,
    SampleInvestigationSession,
    SeededSession,
)
from kitaru.server.application.models.tag import TagFilter
from kitaru.server.application.services.agent_service import AgentService
from kitaru.server.application.services.blob_service import BlobService
from kitaru.server.application.services.cohort_service import CohortService
from kitaru.server.application.services.cohort_version_service import (
    CohortVersionService,
)
from kitaru.server.application.services.evaluation_service import EvaluationService
from kitaru.server.application.services.experiment_service import ExperimentService
from kitaru.server.application.services.investigation_service import (
    InvestigationService,
)
from kitaru.server.application.services.plugin_service import PluginService
from kitaru.server.application.services.server_analytics import ServerAnalytics
from kitaru.server.application.services.session_node_service import SessionNodeService
from kitaru.server.application.services.session_service import SessionService
from kitaru.server.application.services.tag_service import TagService
from kitaru.server.domain.agent import Agent
from kitaru.server.domain.plugin import ScriptPluginSource
from kitaru.server.domain.tag import DuplicateTagName, Tag
from kitaru.server.filtering import FilterCondition

SAMPLE_DATA_PATH = Path(__file__).with_name("sample_data.json")

COMPRESSED_SAMPLE_DATA_PATH = Path(__file__).with_name("sample_data.json.gz")

EVALUATOR_MEDIA_TYPE = "text/x-python"

EVALUATOR_ENTRYPOINT = "evaluate"


def load_sample_data() -> SampleData:
    """Read the sample data shipped with the package.

    Raises:
        FileNotFoundError: The installation carries no sample data file.

    Returns:
        Parsed sample data.
    """
    # The repository tracks the plain sample data file. The published wheel
    # excludes it and ships the gzipped copy the release workflow writes.
    if SAMPLE_DATA_PATH.is_file():
        return SampleData.model_validate_json(SAMPLE_DATA_PATH.read_bytes())
    if COMPRESSED_SAMPLE_DATA_PATH.is_file():
        return SampleData.model_validate_json(
            gzip.decompress(COMPRESSED_SAMPLE_DATA_PATH.read_bytes())
        )
    raise FileNotFoundError(
        f"Sample data was not found at {SAMPLE_DATA_PATH} or "
        f"{COMPRESSED_SAMPLE_DATA_PATH}."
    )


async def _single_chunk(content: bytes) -> AsyncIterator[bytes]:
    """Yield the content as one upload chunk.

    Args:
        content: Content to yield.

    Yields:
        The content, once.
    """
    yield content


def _investigation_session(
    item: SampleInvestigationSession, seeded: SeededSession
) -> InvestigationSessionInput:
    """Build one investigation session with its highlights pinned to nodes.

    Args:
        item: Questions the sample data asks about the session.
        seeded: Stored session the questions are asked about.

    Returns:
        Investigation session input.
    """
    return InvestigationSessionInput(
        session_id=seeded.id,
        questions=[
            InvestigationSessionQuestion(
                key=question.key,
                question=question.question,
                highlights=[
                    InvestigationSessionHighlight(
                        selector=AnnotationSelector(
                            node_id=seeded.node_ids[highlight.node_index]
                        ),
                        description=highlight.description,
                    )
                    for highlight in question.highlights
                ],
            )
            for question in item.questions
        ],
    )


class SampleDataSeeder:
    """Sample data seeding use case."""

    def __init__(
        self,
        agent_service: AgentService,
        session_service: SessionService,
        session_node_service: SessionNodeService,
        evaluation_service: EvaluationService,
        tag_service: TagService,
        cohort_service: CohortService,
        cohort_version_service: CohortVersionService,
        blob_service: BlobService,
        evaluator_service: PluginService,
        experiment_service: ExperimentService,
        investigation_service: InvestigationService,
        analytics: ServerAnalytics | None,
    ) -> None:
        """Initialize the seeder.

        Args:
            agent_service: Agent service.
            session_service: Session service.
            session_node_service: Session node service.
            evaluation_service: Evaluation service.
            tag_service: Tag service.
            cohort_service: Cohort service.
            cohort_version_service: Cohort version service.
            blob_service: Blob service, to store the evaluator script.
            evaluator_service: Plugin service bound to the evaluator kind.
            experiment_service: Experiment service.
            investigation_service: Investigation service.
            analytics: Analytics tracker, None skips tracking.
        """
        self._agents = agent_service
        self._sessions = session_service
        self._nodes = session_node_service
        self._evaluations = evaluation_service
        self._tags = tag_service
        self._cohorts = cohort_service
        self._cohort_versions = cohort_version_service
        self._blobs = blob_service
        self._evaluators = evaluator_service
        self._experiments = experiment_service
        self._investigations = investigation_service
        self._analytics = analytics

    async def create_sample_agent(self, name: str | None, actor: AuthContext) -> Agent:
        """Create the sample agent.

        Args:
            name: Agent name, None uses the sample data's agent name.
            actor: Caller context.

        Raises:
            DuplicateAgentName: The agent name is already registered.

        Returns:
            Created agent.
        """
        command = load_sample_data().agent
        if name is not None:
            command = command.model_copy(update={"name": name})
        return await self._agents.create_agent(command, actor)

    async def seed(self, agent: Agent, actor: AuthContext) -> None:
        """Seed everything recorded under the sample agent.

        The sample sessions are stored with their nodes and evaluations,
        tagged, and frozen into a cohort version. The evaluator script is
        stored as a blob and registered as a script evaluator, which the
        sample experiment references. The experiment is defined but never
        started, since a run replays agent code through a worker.

        Args:
            agent: Committed sample agent the data is recorded under.
            actor: Caller context.
        """
        data = load_sample_data()
        seeded = await self._seed_sessions(data, agent, actor)
        await self._tag_sessions(
            data.session_tag, [item.id for item in seeded.values()], actor
        )
        await self._seed_cohort(data, agent, seeded, actor)
        await self._seed_experiment(data, agent, actor)
        await self._seed_investigation(data, agent, seeded, actor)
        if self._analytics is not None:
            self._analytics.track(actor.account.id, AnalyticsEvent.SAMPLE_DATA_LOADED)

    async def _seed_sessions(
        self, data: SampleData, agent: Agent, actor: AuthContext
    ) -> dict[str, SeededSession]:
        """Store every sample session with its nodes and evaluations.

        Args:
            data: Sample data.
            agent: Agent the sessions are recorded under.
            actor: Caller context.

        Returns:
            Stored session per external id.
        """
        seeded: dict[str, SeededSession] = {}
        for item in data.sessions:
            session = await self._sessions.create_session(
                item.session.model_copy(update={"agent_id": agent.id}), actor
            )
            nodes = await self._nodes.ingest_nodes(session.id, item.nodes, actor)
            await self._evaluations.create_evaluations(
                session.id,
                [
                    EvaluationCreate(
                        name=result.name,
                        data_type=result.data_type,
                        score=result.score,
                        value=result.value,
                        explanation=result.explanation,
                        passed=result.passed,
                    )
                    for result in item.evaluations
                ],
                actor,
            )
            external_id = item.session.external_id
            assert external_id is not None
            seeded[external_id] = SeededSession(
                id=session.id,
                node_ids={node.index: node.id for node in nodes},
            )
        return seeded

    async def _get_or_create_tag(self, name: str, actor: AuthContext) -> Tag:
        """Read the tag with this name, creating it when it does not exist.

        Args:
            name: Tag name.
            actor: Caller context.

        Raises:
            DuplicateTagName: The name was taken and no longer resolves.

        Returns:
            Existing or created tag.
        """
        try:
            return await self._tags.create_tag(name, actor)
        except DuplicateTagName:
            tags, _ = await self._tags.list_tags(
                TagFilter(
                    expression=FilterCondition(
                        field="name", op=FilterOp.EQ, value=name
                    ),
                    size=1,
                ),
                actor,
            )
            if not tags:
                raise
            return tags[0]

    async def _tag_sessions(
        self, name: str, session_ids: list[uuid.UUID], actor: AuthContext
    ) -> None:
        """Link every seeded session to the sample data tag.

        Args:
            name: Tag name.
            session_ids: Ids of the sessions to link.
            actor: Caller context.
        """
        tag = await self._get_or_create_tag(name, actor)
        for session_id in session_ids:
            await self._tags.create_tag_link(
                tag.id, TagResourceType.SESSION, session_id, actor
            )

    async def _seed_cohort(
        self,
        data: SampleData,
        agent: Agent,
        seeded: dict[str, SeededSession],
        actor: AuthContext,
    ) -> None:
        """Freeze the reviewed sessions into the sample cohort's first version.

        Args:
            data: Sample data.
            agent: Agent the cohort belongs to.
            seeded: Stored session per external id.
            actor: Caller context.
        """
        cohort = await self._cohorts.create_cohort(
            CohortCreate(
                name=data.cohort.name,
                description=data.cohort.description,
                agent_id=agent.id,
            ),
            actor,
        )
        await self._cohort_versions.create_version(
            cohort.id,
            CohortVersionCreate(
                add_session_ids=[
                    seeded[external_id].id
                    for external_id in data.cohort.member_external_ids
                ],
                display_version=data.cohort.display_version,
            ),
            actor,
        )

    async def _seed_experiment(
        self, data: SampleData, agent: Agent, actor: AuthContext
    ) -> None:
        """Register the sample evaluator and define the sample experiment.

        Args:
            data: Sample data.
            agent: Agent the experiment belongs to.
            actor: Caller context.
        """
        blob, _ = await self._blobs.upload_blob(
            _single_chunk(data.evaluator.source.encode()),
            EVALUATOR_MEDIA_TYPE,
            actor,
        )
        evaluator = await self._evaluators.create_plugin(
            PluginCreate(
                name=data.evaluator.name, description=data.evaluator.description
            ),
            actor,
        )
        await self._evaluators.create_version(
            evaluator.id,
            ScriptPluginSource(blob_id=blob.id, entrypoint=EVALUATOR_ENTRYPOINT),
            data.evaluator.display_version,
            actor,
        )
        await self._experiments.create_experiment(
            ExperimentCreate(
                name=data.experiment.name,
                description=data.experiment.description,
                agent_id=agent.id,
                tool_policy=data.experiment.tool_policy,
                evaluators=data.experiment.evaluators,
            ),
            actor,
        )

    async def _seed_investigation(
        self,
        data: SampleData,
        agent: Agent,
        seeded: dict[str, SeededSession],
        actor: AuthContext,
    ) -> None:
        """Open the sample investigation over the reviewed sessions.

        Args:
            data: Sample data.
            agent: Agent the investigation belongs to.
            seeded: Stored session per external id.
            actor: Caller context.
        """
        await self._investigations.create_investigation(
            InvestigationCreate(
                agent_id=agent.id,
                name=data.investigation.name,
                description=data.investigation.description,
                sessions=[
                    _investigation_session(item, seeded[item.external_id])
                    for item in data.investigation.sessions
                ],
            ),
            actor,
        )
