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
"""In-memory fakes for tags and sessions."""

import uuid
from collections.abc import Sequence
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from kitaru.api_models.v1.filter import FilterOp
from kitaru.api_models.v1.session import SessionOrigin, TokenUsage
from kitaru.api_models.v1.tag import TagResourceType
from kitaru.server.application.models.session import SessionFilter
from kitaru.server.application.models.tag import TagFilter
from kitaru.server.domain.payload import Payload
from kitaru.server.domain.session import (
    DuplicateSessionExternalId,
    Session,
    SessionInUse,
    SessionNotFound,
    SessionRollups,
)
from kitaru.server.domain.tag import (
    DuplicateTagLink,
    DuplicateTagName,
    Tag,
    TagLink,
    TagLinkNotFound,
    TagNotFound,
)
from kitaru.server.filtering import FilterCondition
from kitaru_test_support.common import (
    _evaluate_filter_expression,
    _paginate_fake,
    _refuse_unresolvable_fields,
    _renewed_timestamp,
)

# These fakes import FakeSessionRepository or FakeTagRepository at module
# level, so importing them back here at runtime would be circular.
if TYPE_CHECKING:
    from kitaru_test_support.cohorts_workers import FakeCohortVersionRepository
    from kitaru_test_support.evaluations import FakeEvaluationRepository
    from kitaru_test_support.experiments_replays import FakeReplayRepository
    from kitaru_test_support.investigations import FakeInvestigationRepository
    from kitaru_test_support.jobs_tasks import FakeTaskRepository


class FakeTagRepository:
    """In-memory tag repository."""

    def __init__(self) -> None:
        """Initialize the repository."""
        self._tags: dict[uuid.UUID, Tag] = {}
        self._links: dict[uuid.UUID, TagLink] = {}

    def _check_duplicate_name(self, tag: Tag) -> None:
        for other in self._tags.values():
            if other.id != tag.id and other.name == tag.name:
                raise DuplicateTagName(tag.name)

    def _check_duplicate_link(self, link: TagLink) -> None:
        for other in self._links.values():
            if (
                other.id != link.id
                and other.tag_id == link.tag_id
                and other.resource_type == link.resource_type
                and other.resource_id == link.resource_id
            ):
                raise DuplicateTagLink(
                    link.tag_id, link.resource_type, link.resource_id
                )

    async def create(self, tag: Tag) -> Tag:
        """Persist a new tag.

        Args:
            tag: Tag to store.

        Raises:
            DuplicateTagName: The tag name is already registered.

        Returns:
            Stored tag with timestamps set.
        """
        self._check_duplicate_name(tag)
        now = datetime.now(UTC)
        stored = tag.model_copy(update={"created": now, "updated": now})
        self._tags[stored.id] = stored
        return stored.model_copy()

    async def get(self, tag_id: uuid.UUID) -> Tag:
        """Load a tag by id.

        Args:
            tag_id: Id of the tag.

        Raises:
            TagNotFound: No tag has this id.

        Returns:
            Stored tag.
        """
        tag = self._tags.get(tag_id)
        if tag is None:
            raise TagNotFound(tag_id)
        return tag.model_copy()

    async def query(self, tag_filter: TagFilter) -> tuple[list[Tag], str | None]:
        """Query tags matching a filter.

        Args:
            tag_filter: Filter and pagination parameters.

        Returns:
            Page of matching tags and the next cursor.
        """
        tags = list(self._tags.values())
        if tag_filter.expression is not None:
            tags = [
                tag
                for tag in tags
                if _evaluate_filter_expression(tag, tag_filter.expression)
            ]
        page, next_cursor = _paginate_fake(tags, tag_filter)
        return [tag.model_copy() for tag in page], next_cursor

    async def update(self, tag: Tag) -> Tag:
        """Persist changes to an existing tag.

        Args:
            tag: Tag with modified fields.

        Raises:
            TagNotFound: No tag has this id.
            DuplicateTagName: The tag name is already registered.

        Returns:
            Stored tag with the updated timestamp renewed.
        """
        stored = self._tags.get(tag.id)
        if stored is None:
            raise TagNotFound(tag.id)
        self._check_duplicate_name(tag)
        now = _renewed_timestamp(stored.updated)
        updated = tag.model_copy(update={"created": stored.created, "updated": now})
        self._tags[tag.id] = updated
        return updated.model_copy()

    async def delete(self, tag_id: uuid.UUID) -> None:
        """Delete a tag and its links.

        Args:
            tag_id: Id of the tag.

        Raises:
            TagNotFound: No tag has this id.
        """
        if tag_id not in self._tags:
            raise TagNotFound(tag_id)
        del self._tags[tag_id]
        cascaded = [
            link_id for link_id, link in self._links.items() if link.tag_id == tag_id
        ]
        for link_id in cascaded:
            del self._links[link_id]

    async def create_link(self, link: TagLink) -> TagLink:
        """Persist a new tag link.

        Args:
            link: Tag link to store.

        Raises:
            TagNotFound: No tag has the link's tag id.
            DuplicateTagLink: The tag is already linked to the resource.

        Returns:
            Stored tag link with timestamps set.
        """
        if link.tag_id not in self._tags:
            raise TagNotFound(link.tag_id)
        self._check_duplicate_link(link)
        now = datetime.now(UTC)
        stored = link.model_copy(update={"created": now, "updated": now})
        self._links[stored.id] = stored
        return stored.model_copy()

    async def delete_link(
        self,
        tag_id: uuid.UUID,
        resource_type: TagResourceType,
        resource_id: uuid.UUID,
    ) -> None:
        """Delete a tag link by tag and resource.

        Args:
            tag_id: Id of the tag.
            resource_type: Kind of the linked resource.
            resource_id: Id of the linked resource.

        Raises:
            TagLinkNotFound: No link matches the tag and resource.
        """
        for link_id, link in self._links.items():
            if (
                link.tag_id == tag_id
                and link.resource_type == resource_type
                and link.resource_id == resource_id
            ):
                del self._links[link_id]
                return
        raise TagLinkNotFound(tag_id, resource_type, resource_id)

    def has_link(
        self,
        resource_type: TagResourceType,
        resource_id: uuid.UUID,
        tag_name: str,
    ) -> bool:
        """Report whether a resource carries a tag, mirroring the tag EXISTS join.

        Args:
            resource_type: Kind of the linked resource.
            resource_id: Id of the linked resource.
            tag_name: Tag name to look up.

        Returns:
            Whether a link ties the resource to a tag with this name.
        """
        tag_ids = {tag.id for tag in self._tags.values() if tag.name == tag_name}
        return any(
            link.resource_type == resource_type
            and link.resource_id == resource_id
            and link.tag_id in tag_ids
            for link in self._links.values()
        )


async def create_tag(
    repository: FakeTagRepository, owner_id: uuid.UUID, name: str = "smoke-test"
) -> Tag:
    """Store a tag in the fake repository.

    Args:
        repository: Fake tag repository.
        owner_id: Id of the owning account.
        name: Tag name.

    Returns:
        Stored tag.
    """
    return await repository.create(Tag(owner_id=owner_id, name=name))


class FakeSessionRepository:
    """In-memory session repository."""

    def __init__(
        self,
        tags: FakeTagRepository | None = None,
        evaluations: "FakeEvaluationRepository | None" = None,
    ) -> None:
        """Initialize the repository.

        Args:
            tags: Fake tag repository, consulted by the ``tag`` filter.
            evaluations: Fake evaluation repository, consulted by the
                ``has_evaluation`` filter. Defaults to an empty, unwired
                repository so the filter always matches something sensible.
        """
        from kitaru_test_support.evaluations import FakeEvaluationRepository

        self._sessions: dict[uuid.UUID, Session] = {}
        self._tags = tags
        self._evaluations = (
            evaluations if evaluations is not None else FakeEvaluationRepository()
        )
        self._cohort_membership_counts: dict[uuid.UUID, int] = {}
        self._cohort_versions: FakeCohortVersionRepository | None = None
        # Wired back by FakeInvestigationRepository, FakeReplayRepository,
        # and FakeTaskRepository, to check restriction and clear pointers on
        # delete.
        self._investigations: FakeInvestigationRepository | None = None
        self._replays: FakeReplayRepository | None = None
        self._tasks: FakeTaskRepository | None = None
        self._session_numbers: dict[uuid.UUID, int] = {}

    def _mark_cohort_member(self, session_id: uuid.UUID) -> None:
        """Record that one more cohort version references this session.

        Mirrors the SQL repository's restricting foreign key from
        ``cohort_version_session.session_id``.

        Args:
            session_id: Id of the session.
        """
        self._cohort_membership_counts[session_id] = (
            self._cohort_membership_counts.get(session_id, 0) + 1
        )

    def _unmark_cohort_member(self, session_id: uuid.UUID) -> None:
        """Record that one fewer cohort version references this session.

        Args:
            session_id: Id of the session.
        """
        self._cohort_membership_counts[session_id] -= 1

    def _null_agent_version(self, agent_version_id: uuid.UUID) -> None:
        """Clear a deleted agent version's session pointers.

        Mirrors the SQL repository's SET NULL foreign key from
        ``session.agent_version_id``.

        Args:
            agent_version_id: Id of the agent version being deleted.
        """
        for session_id, session in self._sessions.items():
            if session.agent_version_id == agent_version_id:
                self._sessions[session_id] = session.model_copy(
                    update={"agent_version_id": None}
                )

    def _check_duplicate_external_id(self, session: Session) -> None:
        if session.imported_from is None or session.external_id is None:
            return
        for other in self._sessions.values():
            if (
                other.id != session.id
                and other.agent_id == session.agent_id
                and other.imported_from == session.imported_from
                and other.external_id == session.external_id
            ):
                raise DuplicateSessionExternalId(
                    session.imported_from, session.external_id
                )

    async def allocate_session_number(self, agent_id: uuid.UUID) -> int:
        """Bump the agent's session counter and return the new value.

        Mirrors the SQL repository's ``UPDATE ... RETURNING`` bump.

        Args:
            agent_id: Id of the agent to bump.

        Returns:
            New session number.
        """
        self._session_numbers[agent_id] = self._session_numbers.get(agent_id, 0) + 1
        return self._session_numbers[agent_id]

    async def create(self, session: Session) -> Session:
        """Persist a new session.

        Args:
            session: Session to store.

        Raises:
            DuplicateSessionExternalId: The imported_from and external id pair is
                already registered.

        Returns:
            Stored session with timestamps set, without payloads.
        """
        self._check_duplicate_external_id(session)
        now = datetime.now(UTC)
        stored = session.model_copy(update={"created": now, "updated": now})
        self._sessions[stored.id] = stored
        return self._copy(stored, include_payloads=False)

    @staticmethod
    def _copy(session: Session, include_payloads: bool) -> Session:
        """Copy a stored session, dropping payloads unless requested.

        Args:
            session: Stored session.
            include_payloads: Whether to keep the inputs and outputs.

        Returns:
            Copied session.
        """
        if include_payloads:
            return session.model_copy()
        # Rebuild without the payload fields so they stay unset, like a load
        # that excluded the payload columns.
        data = dict(session)
        del data["inputs"]
        del data["outputs"]
        return Session(**data)

    async def get(
        self, session_id: uuid.UUID, include_payloads: bool, exclusive: bool = False
    ) -> Session:
        """Load a session by id.

        Args:
            session_id: Id of the session.
            include_payloads: Whether to read the inputs and outputs
                columns.
            exclusive: Ignored, the fake has no concurrent callers to lock
                against.

        Raises:
            SessionNotFound: No session has this id.

        Returns:
            Stored session.
        """
        _ = exclusive
        session = self._sessions.get(session_id)
        if session is None:
            raise SessionNotFound(session_id)
        return self._copy(session, include_payloads)

    async def get_by_task_id(
        self, task_id: uuid.UUID, include_payloads: bool, exclusive: bool = False
    ) -> Session | None:
        """Load the session a task produced, if any.

        Args:
            task_id: Id of the producing task.
            include_payloads: Whether to read the inputs and outputs
                columns.
            exclusive: Ignored, the fake has no concurrent callers to lock
                against.

        Returns:
            Stored session, or ``None`` when no session links the task.
        """
        _ = exclusive
        for session in self._sessions.values():
            if session.task_id == task_id:
                return self._copy(session, include_payloads)
        return None

    def _session_ids_tagged(self, tag_name: str) -> set[uuid.UUID]:
        """Resolve the ids of sessions linked to a tag by name.

        Args:
            tag_name: Name of the tag to resolve.

        Returns:
            Ids of sessions linked to the tag.
        """
        if self._tags is None:
            return set()
        tag_id = next(
            (tag.id for tag in self._tags._tags.values() if tag.name == tag_name),
            None,
        )
        if tag_id is None:
            return set()
        return {
            link.resource_id
            for link in self._tags._links.values()
            if link.tag_id == tag_id and link.resource_type == TagResourceType.SESSION
        }

    def _session_ids_in_cohort_version(
        self, cohort_version_id: uuid.UUID
    ) -> set[uuid.UUID]:
        """Resolve the ids of sessions that are members of a cohort version.

        Args:
            cohort_version_id: Id of the cohort version.

        Returns:
            Ids of member sessions.
        """
        if self._cohort_versions is None:
            return set()
        return set(self._cohort_versions._members.get(cohort_version_id, []))

    async def query(
        self, session_filter: SessionFilter, include_payloads: bool
    ) -> tuple[list[Session], str | None]:
        """Query sessions matching a filter.

        Args:
            session_filter: Filter and pagination parameters.
            include_payloads: Whether to read the inputs and outputs
                columns.

        Returns:
            Page of matching sessions and the next cursor.
        """
        _refuse_unresolvable_fields(session_filter.expression, ("experiment_run_id",))
        sessions = list(self._sessions.values())
        if session_filter.expression is not None:
            resolvers = {
                "tag": self._evaluate_tag_condition,
                "cohort_version_id": self._evaluate_cohort_version_condition,
                "has_evaluation": self._evaluate_has_evaluation_condition,
            }
            sessions = [
                s
                for s in sessions
                if _evaluate_filter_expression(s, session_filter.expression, resolvers)
            ]
        page, next_cursor = _paginate_fake(sessions, session_filter)
        return [self._copy(s, include_payloads) for s in page], next_cursor

    def _evaluate_tag_condition(
        self, session: Session, condition: FilterCondition
    ) -> bool:
        """Evaluate a tag filter condition against a session.

        Args:
            session: Session to evaluate.
            condition: Validated tag condition.

        Returns:
            Whether the session has a matching tag.
        """
        names = condition.value if condition.op is FilterOp.IN else (condition.value,)
        return any(session.id in self._session_ids_tagged(name) for name in names)

    def _evaluate_cohort_version_condition(
        self, session: Session, condition: FilterCondition
    ) -> bool:
        """Evaluate a cohort version condition against a session.

        Args:
            session: Session to evaluate.
            condition: Validated cohort version condition.

        Returns:
            Whether the session belongs to a matching cohort version.
        """
        ids = condition.value if condition.op is FilterOp.IN else (condition.value,)
        return any(
            session.id in self._session_ids_in_cohort_version(version_id)
            for version_id in ids
        )

    def _evaluate_has_evaluation_condition(
        self, session: Session, condition: FilterCondition
    ) -> bool:
        """Evaluate a has-evaluation condition against a session.

        Args:
            session: Session to evaluate.
            condition: Validated has-evaluation condition.

        Returns:
            Whether the session's evaluation state matches the condition.
        """
        expected = (
            condition.value if condition.op is FilterOp.EQ else not condition.value
        )
        return self._evaluations.has_evaluation(session.id) == expected

    async def get_many(
        self, session_ids: Sequence[uuid.UUID], include_payloads: bool
    ) -> dict[uuid.UUID, Session]:
        """Bulk-load sessions by id, keyed by id, missing ids omitted.

        Args:
            session_ids: Ids of the sessions to load.
            include_payloads: Whether to read the inputs and outputs
                columns.

        Returns:
            Stored sessions keyed by id.
        """
        return {
            session_id: self._copy(self._sessions[session_id], include_payloads)
            for session_id in session_ids
            if session_id in self._sessions
        }

    async def update(self, session: Session) -> Session:
        """Persist changes to an existing session.

        Args:
            session: Session with modified fields.

        Raises:
            SessionNotFound: No session has this id.
            DuplicateSessionExternalId: The imported_from and external id pair is
                already registered.

        Returns:
            Stored session with the updated timestamp renewed, without
            payloads.
        """
        stored = self._sessions.get(session.id)
        if stored is None:
            raise SessionNotFound(session.id)
        self._check_duplicate_external_id(session)
        now = _renewed_timestamp(stored.updated)
        updated = session.model_copy(
            update={
                "created": stored.created,
                "updated": now,
                "inputs": stored.inputs,
                "outputs": session.outputs
                if "outputs" in session.model_fields_set
                else stored.outputs,
            }
        )
        self._sessions[session.id] = updated
        return self._copy(updated, include_payloads=False)

    async def finalize_replay_inputs(self, session: Session) -> Session:
        """Persist final replay input and status in one fake repository update."""
        stored = self._sessions.get(session.id)
        if stored is None:
            raise SessionNotFound(session.id)
        self._check_duplicate_external_id(session)
        updated = session.model_copy(
            update={
                "created": stored.created,
                "updated": _renewed_timestamp(stored.updated),
                "outputs": session.outputs
                if "outputs" in session.model_fields_set
                else stored.outputs,
            }
        )
        self._sessions[session.id] = updated
        return self._copy(updated, include_payloads=False)

    async def delete(self, session_id: uuid.UUID) -> None:
        """Delete a session by id.

        Args:
            session_id: Id of the session.

        Raises:
            SessionNotFound: No session has this id.
            SessionInUse: The session is referenced by a cohort version,
                investigation, or replay and cannot be deleted.
        """
        if session_id not in self._sessions:
            raise SessionNotFound(session_id)
        if self._cohort_membership_counts.get(session_id, 0) > 0:
            raise SessionInUse(session_id)
        if self._investigations is not None and any(
            link.session_id == session_id
            for link in self._investigations._sessions.values()
        ):
            raise SessionInUse(session_id)
        if self._replays is not None and any(
            session_id in (replay.baseline_session_id, replay.result_session_id)
            for replay in self._replays._replays.values()
        ):
            raise SessionInUse(session_id)
        del self._sessions[session_id]
        if self._tasks is not None:
            self._tasks._unlink_session(session_id)

    async def apply_rollups(
        self, session_id: uuid.UUID, deltas: SessionRollups
    ) -> None:
        """Apply rollup deltas to a session's cost, tokens, and call counts.

        Args:
            session_id: Id of the session.
            deltas: Rollup deltas to add.

        Raises:
            SessionNotFound: No session has this id.
        """
        stored = self._sessions.get(session_id)
        if stored is None:
            raise SessionNotFound(session_id)
        tokens = stored.tokens if stored.tokens is not None else TokenUsage()
        new_tokens = TokenUsage(
            input_tokens=(tokens.input_tokens or 0) + deltas.input_tokens,
            output_tokens=(tokens.output_tokens or 0) + deltas.output_tokens,
            cached_input_tokens=(tokens.cached_input_tokens or 0)
            + deltas.cached_input_tokens,
            reasoning_tokens=(tokens.reasoning_tokens or 0) + deltas.reasoning_tokens,
        )
        now = _renewed_timestamp(stored.updated)
        updated = stored.model_copy(
            update={
                "cost": (stored.cost if stored.cost is not None else Decimal(0))
                + deltas.cost,
                "tokens": new_tokens,
                "llm_call_count": stored.llm_call_count + deltas.llm_call_count,
                "tool_call_count": stored.tool_call_count + deltas.tool_call_count,
                "updated": now,
            }
        )
        self._sessions[session_id] = updated


async def create_session(
    repository: FakeSessionRepository,
    owner_id: uuid.UUID,
    agent_id: uuid.UUID,
    **overrides: Any,
) -> Session:
    """Store a session in the fake repository.

    Args:
        repository: Fake session repository.
        owner_id: Id of the owning account.
        agent_id: Id of the agent the session belongs to.
        **overrides: Additional session fields.

    Returns:
        Stored session.
    """
    values: dict[str, Any] = {
        "owner_id": owner_id,
        "agent_id": agent_id,
        "origin": SessionOrigin.RECORDED,
    }
    values.update(overrides)
    if "number" not in values:
        values["number"] = await repository.allocate_session_number(values["agent_id"])
    for field in ("inputs", "outputs"):
        value = values.get(field)
        if value is not None and not isinstance(value, Payload):
            values[field] = Payload.from_json(value)
    stored = await repository.create(Session(**values))
    return await repository.get(stored.id, include_payloads=True)
