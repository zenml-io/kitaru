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
from contextlib import suppress
from pathlib import Path

from kitaru.analytics.events import AnalyticsEvent
from kitaru.api_models.v1.annotation import AnnotationSelector
from kitaru.api_models.v1.filter import FilterOp
from kitaru.api_models.v1.investigation import (
    InvestigationSessionHighlight,
    InvestigationSessionQuestion,
)
from kitaru.api_models.v1.tag import TagResourceType
from kitaru.server.application.interfaces.agent_repository import AgentRepository
from kitaru.server.application.interfaces.session_repository import SessionRepository
from kitaru.server.application.models.agent import AgentFilter
from kitaru.server.application.models.agent_version import AgentVersionFilter
from kitaru.server.application.models.auth import AuthContext
from kitaru.server.application.models.cohort import (
    CohortCreate,
    CohortFilter,
    CohortVersionCreate,
    CohortVersionFilter,
)
from kitaru.server.application.models.evaluation import (
    EvaluationCreate,
    EvaluationFilter,
)
from kitaru.server.application.models.experiment import (
    ExperimentCreate,
    ExperimentFilter,
)
from kitaru.server.application.models.investigation import (
    InvestigationCreate,
    InvestigationFilter,
    InvestigationSessionInput,
)
from kitaru.server.application.models.plugin import (
    EvaluatorFilter,
    PluginCreate,
    PluginVersionFilter,
)
from kitaru.server.application.models.sample_data import (
    SampleData,
    SampleInvestigationSession,
    SeededSession,
)
from kitaru.server.application.models.session import SessionFilter
from kitaru.server.application.models.tag import TagFilter
from kitaru.server.application.services.agent_service import AgentService
from kitaru.server.application.services.agent_version_service import AgentVersionService
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
from kitaru.server.domain.agent import Agent, DuplicateAgentName
from kitaru.server.domain.agent_version import AgentVersion
from kitaru.server.domain.base import ConflictError
from kitaru.server.domain.cohort import DuplicateCohortName
from kitaru.server.domain.experiment import DuplicateExperimentName
from kitaru.server.domain.investigation import Investigation
from kitaru.server.domain.plugin import (
    DuplicatePluginName,
    PluginKind,
    ScriptPluginSource,
)
from kitaru.server.domain.session import (
    DuplicateSessionExternalId,
    Session,
    SessionNotFound,
)
from kitaru.server.domain.tag import DuplicateTagLink, DuplicateTagName, Tag
from kitaru.server.filtering import AndExpression, FilterCondition
from kitaru.server.utils import paginate_all

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
        session_id=seeded.session.id,
        questions=[
            InvestigationSessionQuestion(
                key=question.key,
                question=question.question,
                highlights=[
                    InvestigationSessionHighlight(
                        selector=AnnotationSelector(
                            node_id=seeded.node_ids[highlight.node_external_id]
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
        session_service: SessionService,
        session_repository: SessionRepository,
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
            session_service: Session service.
            session_repository: Session repository for guarded sample-version repair.
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
        self._sessions = session_service
        self._session_repository = session_repository
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

    async def seed(
        self, agent: Agent, version: AgentVersion, actor: AuthContext
    ) -> tuple[Agent, Session, Investigation]:
        """Seed everything recorded under the sample agent.

        The sample sessions are stored with their nodes and evaluations,
        tagged, and frozen into a cohort version. The evaluator script is
        stored as a blob and registered as a script evaluator, which the
        sample experiment references. The experiment is defined but never
        started, since a run replays agent code through a worker.

        Args:
            agent: Committed sample agent the data is recorded under.
            version: Committed baseline agent version.
            actor: Caller context.
        """
        data = load_sample_data()
        seeded = await self._seed_sessions(data, agent, version, actor)
        await self._tag_sessions(
            data.session_tag, [item.session.id for item in seeded.values()], actor
        )
        await self._seed_cohort(data, agent, seeded, actor)
        await self._seed_experiment(data, agent, actor)
        investigation = await self._seed_investigation(data, agent, seeded, actor)
        if self._analytics is not None:
            self._analytics.track(actor.account.id, AnalyticsEvent.SAMPLE_DATA_LOADED)
        failing = seeded[data.failing_session_external_id]
        return agent, failing.session, investigation

    async def _seed_sessions(
        self, data: SampleData, agent: Agent, version: AgentVersion, actor: AuthContext
    ) -> dict[str, SeededSession]:
        """Store every sample session with its nodes and evaluations.

        Args:
            data: Sample data.
            agent: Agent the sessions are recorded under.
            version: Recorded agent version.
            actor: Caller context.

        Returns:
            Stored session per external id.
        """
        resolved: dict[str, Session] = {}
        for item in data.sessions:
            try:
                session = await self._sessions.create_session(
                    item.session.model_copy(
                        update={"agent_id": agent.id, "agent_version_id": version.id}
                    ),
                    actor,
                )
            except DuplicateSessionExternalId:
                sessions, _ = await self._sessions.list_sessions(
                    SessionFilter(
                        expression=AndExpression(
                            operands=(
                                FilterCondition(
                                    field="external_id",
                                    op=FilterOp.EQ,
                                    value=item.session.external_id,
                                ),
                                FilterCondition(
                                    field="agent_id", op=FilterOp.EQ, value=agent.id
                                ),
                                FilterCondition(
                                    field="imported_from",
                                    op=FilterOp.EQ,
                                    value=item.session.imported_from,
                                ),
                            )
                        ),
                        size=1,
                    ),
                    False,
                    actor,
                )
                if (
                    not sessions
                    or sessions[0].metadata.get("kitaru_sample_data") is not True
                    or sessions[0].imported_from != item.session.imported_from
                ):
                    raise
                session = sessions[0]
            external_id = item.session.external_id
            assert external_id is not None
            resolved[external_id] = session

        # Ingestion and evaluation services re-lock sessions, so acquire the
        # complete set in one ordered batch before any per-session work.
        locked = await self._session_repository.get_many(
            sorted(session.id for session in resolved.values()),
            include_payloads=False,
            exclusive=True,
        )
        seeded: dict[str, SeededSession] = {}
        for item in data.sessions:
            external_id = item.session.external_id
            assert external_id is not None
            session_id = resolved[external_id].id
            session = locked.get(session_id)
            if session is None:
                raise SessionNotFound(session_id)
            if session.link_sample_agent_version(
                version, item.session.imported_from, external_id
            ):
                session = await self._session_repository.update(session)
            nodes = await self._nodes.ingest_nodes(session.id, item.nodes, actor)
            existing = await paginate_all(
                lambda cursor, session_id=session.id: (
                    self._evaluations.list_evaluations(
                        EvaluationFilter(
                            expression=FilterCondition(
                                field="session_id", op=FilterOp.EQ, value=session_id
                            ),
                            cursor=cursor,
                            size=100,
                        ),
                        actor,
                    )
                )
            )
            existing_names = {item.evaluation.name for item in existing}
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
                    if result.name not in existing_names
                ],
                actor,
            )
            seeded[external_id] = SeededSession(
                session=session,
                node_ids={node.external_id: node.id for node in nodes},
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
            with suppress(DuplicateTagLink):
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
        try:
            cohort = await self._cohorts.create_cohort(
                CohortCreate(
                    name=data.cohort.name,
                    description=data.cohort.description,
                    agent_id=agent.id,
                ),
                actor,
            )
        except DuplicateCohortName:
            cohorts, _ = await self._cohorts.list_cohorts(
                CohortFilter(
                    expression=AndExpression(
                        operands=(
                            FilterCondition(
                                field="name", op=FilterOp.EQ, value=data.cohort.name
                            ),
                            FilterCondition(
                                field="agent_id", op=FilterOp.EQ, value=agent.id
                            ),
                        )
                    ),
                    size=1,
                ),
                actor,
            )
            if (
                not cohorts
                or cohorts[0].agent_id != agent.id
                or cohorts[0].description != data.cohort.description
            ):
                raise
            cohort = cohorts[0]
        versions = await paginate_all(
            lambda cursor: self._cohort_versions.list_versions(
                CohortVersionFilter(cohort_id=cohort.id, cursor=cursor, size=100), actor
            )
        )
        if any(
            version.display_version == data.cohort.display_version
            for version in versions
        ):
            return
        await self._cohort_versions.create_version(
            cohort.id,
            CohortVersionCreate(
                add_session_ids=[
                    seeded[external_id].session.id
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
        try:
            evaluator = await self._evaluators.create_plugin(
                PluginCreate(
                    name=data.evaluator.name, description=data.evaluator.description
                ),
                actor,
            )
        except DuplicatePluginName:
            evaluators, _ = await self._evaluators.list_plugins(
                EvaluatorFilter(
                    kind=PluginKind.EVALUATOR,
                    expression=FilterCondition(
                        field="name", op=FilterOp.EQ, value=data.evaluator.name
                    ),
                    size=1,
                ),
                actor,
            )
            if (
                not evaluators
                or evaluators[0].description != data.evaluator.description
                or evaluators[0].agent_id not in (None, agent.id)
            ):
                raise
            evaluator = evaluators[0]
        source = ScriptPluginSource(blob_id=blob.id, entrypoint=EVALUATOR_ENTRYPOINT)
        versions = await paginate_all(
            lambda cursor: self._evaluators.list_versions(
                PluginVersionFilter(plugin_id=evaluator.id, cursor=cursor, size=100),
                actor,
            )
        )
        matching = [
            version
            for version in versions
            if version.display_version == data.evaluator.display_version
        ]
        if matching and any(version.source != source for version in matching):
            raise ConflictError("Sample evaluator version has different source")
        if not matching:
            await self._evaluators.create_version(
                evaluator.id, source, data.evaluator.display_version, actor
            )
        try:
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
        except DuplicateExperimentName:
            experiments, _ = await self._experiments.list_experiments(
                ExperimentFilter(
                    expression=AndExpression(
                        operands=(
                            FilterCondition(
                                field="name", op=FilterOp.EQ, value=data.experiment.name
                            ),
                            FilterCondition(
                                field="agent_id", op=FilterOp.EQ, value=agent.id
                            ),
                        )
                    ),
                    size=1,
                ),
                actor,
            )
            if not experiments:
                raise
            if experiments[0][0].description != data.experiment.description:
                raise ConflictError("Sample experiment has different content") from None

    async def _seed_investigation(
        self,
        data: SampleData,
        agent: Agent,
        seeded: dict[str, SeededSession],
        actor: AuthContext,
    ) -> Investigation:
        """Open the sample investigation over the reviewed sessions.

        Args:
            data: Sample data.
            agent: Agent the investigation belongs to.
            seeded: Stored session per external id.
            actor: Caller context.
        """
        investigations = await paginate_all(
            lambda cursor: self._investigations.list_investigations(
                InvestigationFilter(
                    expression=FilterCondition(
                        field="agent_id", op=FilterOp.EQ, value=agent.id
                    ),
                    cursor=cursor,
                    size=100,
                ),
                actor,
            )
        )
        for investigation in investigations:
            if investigation.name == data.investigation.name:
                if investigation.description != data.investigation.description:
                    raise ConflictError("Sample investigation has different content")
                return investigation
        return await self._investigations.create_investigation(
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


class SampleDataAgentSeeder:
    """Prepare the sample agent and recorded version in one transaction."""

    def __init__(
        self,
        agents: AgentService,
        versions: AgentVersionService,
        repository: AgentRepository,
    ) -> None:
        """Initialize agent preparation.

        Args:
            agents: Agent service.
            versions: Agent version service.
            repository: Agent repository for serializing baseline preparation.
        """
        self._agents = agents
        self._versions = versions
        self._repository = repository

    async def create_sample_agent(
        self, name: str | None, resume_existing: bool, actor: AuthContext
    ) -> tuple[Agent, AgentVersion]:
        """Create or adopt the sample agent and its baseline version.

        Args:
            name: Agent name, None uses the packaged name.
            resume_existing: Adopt only an existing sample agent.
            actor: Caller context.

        Raises:
            DuplicateAgentName: The name belongs to an agent that cannot be adopted.

        Returns:
            Sample agent and its recorded version.
        """
        data = load_sample_data()
        command = (
            data.agent.model_copy(update={"name": name})
            if name is not None
            else data.agent
        )
        try:
            agent = await self._agents.create_agent(command, actor)
        except DuplicateAgentName:
            if not resume_existing:
                raise
            agents, _ = await self._agents.list_agents(
                AgentFilter(
                    expression=FilterCondition(
                        field="name", op=FilterOp.EQ, value=command.name
                    ),
                    size=1,
                ),
                actor,
            )
            if not agents:
                raise
            agent = await self._repository.get(agents[0].id, exclusive=True)
            if (
                agent.description != data.agent.description
                or agent.owner_id != actor.account.id
            ):
                raise
        versions = await paginate_all(
            lambda cursor: self._versions.list_versions(
                AgentVersionFilter(agent_id=agent.id, cursor=cursor, size=100), actor
            )
        )
        for version in versions:
            if version.display_version == data.agent_version.display_version:
                if (
                    version.description != data.agent_version.description
                    or version.run_spec != data.agent_version.run_spec
                    or version.capabilities != data.agent_version.capabilities
                ):
                    raise ConflictError("Sample agent version has different content")
                return agent, version
        version = await self._versions.create_version(
            agent.id,
            data.agent_version.display_version,
            data.agent_version.description,
            data.agent_version.run_spec,
            data.agent_version.capabilities,
            actor,
        )
        return agent, version
