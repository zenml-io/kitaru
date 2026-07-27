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
"""Kitaru recording and job adapter for the mock agent."""

import asyncio
import json
import os
import threading
import uuid
from collections.abc import Coroutine
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, TypeVar

from adapter_example.agent import (  # ty: ignore[unresolved-import]
    AgentHooks,
    LLMCall,
    MockAgent,
    ToolCall,
    ToolExecutor,
)
from kitaru.api_models.v1.jobs import (
    HistoryPolicy,
    JobSpecResponse,
    PassthroughPolicy,
    ReplayOverride,
    StaticCase,
    StaticMatchMode,
    StaticPolicy,
    ToolLookupRequest,
    ToolPolicyOnMiss,
)
from kitaru.api_models.v1.session_nodes import (
    NodeStatus,
    NodeType,
    SessionNodeBatchRequest,
    SessionNodeCreateRequest,
)
from kitaru.api_models.v1.sessions import (
    SessionCreateRequest,
    SessionOrigin,
    SessionStatus,
    SessionUpdateRequest,
    TokenUsage,
)
from kitaru.client import KitaruAPIClient
from kitaru.hashing import tool_call_cache_key
from kitaru.ids import uuid7
from kitaru.job import job_id as get_job_id

ADAPTER_VERSION = "0.1.0"
FRAMEWORK = "mock"

T = TypeVar("T")


class ToolPolicyMissError(RuntimeError):
    """Raised when a tool policy lookup misses and on_miss is fail."""


class ToolPolicyError(RuntimeError):
    """Raised when a tool policy cannot be applied."""


def _case_matches(case: StaticCase, arguments: dict[str, Any]) -> bool:
    """Check a static case against tool call arguments."""
    if case.match is None:
        return True
    if case.match_mode is StaticMatchMode.EXACT:
        return arguments == case.match
    return all(
        name in arguments and arguments[name] == value
        for name, value in case.match.items()
    )


@dataclass
class _MockedCall:
    """Policy outcome of the tool call currently executing."""

    policy: str
    failed: bool = False


class KitaruAdapter(AgentHooks):
    """Adapter recording mock agent runs as Kitaru sessions."""

    def __init__(
        self,
        agent: MockAgent,
        agent_id: uuid.UUID,
        agent_version_id: uuid.UUID | None = None,
        api_url: str | None = None,
        api_key: str | None = None,
        job_id: uuid.UUID | None = None,
        session_name: str | None = None,
        api_client: KitaruAPIClient | None = None,
        batch_size: int = 20,
    ) -> None:
        """Initialize the adapter, wire the agent, and apply any job spec."""
        self._agent = agent
        self._agent_id = agent_id
        self._agent_version_id = agent_version_id
        self._session_name = session_name or os.environ.get("KITARU_JOB_SESSION_NAME")
        self._batch_size = batch_size
        if job_id is None:
            job_env = get_job_id()
            job_id = uuid.UUID(job_env) if job_env else None
        self._job_id = job_id
        if api_client is None:
            api_url = api_url or os.environ.get("KITARU_API_URL")
            if not api_url:
                raise ValueError("KITARU_API_URL is not set")
            api_key = api_key or os.environ.get("KITARU_API_KEY")
            api_client = KitaruAPIClient(base_url=api_url, api_key=api_key)
        self._client = api_client
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._loop.run_forever, daemon=True)
        self._thread.start()
        self._session_id: uuid.UUID | None = None
        self._root_id: uuid.UUID | None = None
        self._last_llm_id: uuid.UUID | None = None
        self._sequence = 0
        self._buffer: list[SessionNodeCreateRequest] = []
        self._llm_started_at: datetime | None = None
        self._tool_started_at: datetime | None = None
        self._mocked_call: _MockedCall | None = None
        self._run_started_at: datetime | None = None
        self._run_inputs: Any = None
        self._spec: JobSpecResponse | None = None
        if self._job_id is not None:
            self._spec = self._run(self._client.jobs.get_spec(self._job_id))
            if self._spec.override is not None:
                self._apply_override(self._spec.override)
            if self._spec.tool_policy is not None:
                agent.tool_interceptor = self._intercept_tool
        agent.register_hooks(self)

    @property
    def session_id(self) -> uuid.UUID | None:
        """Id of the most recently started session."""
        return self._session_id

    def _run(self, coro: Coroutine[Any, Any, T]) -> T:
        """Run a coroutine on the event loop thread and wait for the result."""
        return asyncio.run_coroutine_threadsafe(coro, self._loop).result()

    def close(self) -> None:
        """Close the API client and stop the event loop thread."""
        self._run(self._client.close())
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join()
        self._loop.close()

    def resolve_inputs(self, default: Any = None) -> Any:
        """Resolve run inputs from the environment, the job spec, or a default."""
        raw = os.environ.get("KITARU_JOB_INPUTS")
        if raw is not None:
            return json.loads(raw)
        if self._spec is not None:
            return self._spec.inputs
        return default

    def _apply_override(self, override: ReplayOverride) -> None:
        """Apply an execution override to the agent configuration."""
        model: str | None = None
        if isinstance(override.model, str):
            model = override.model
        elif isinstance(override.model, dict):
            model = override.model.get(self._agent.model)
        self._agent.configure(
            model=model,
            system_prompt=override.system_prompt,
            model_params=override.model_params,
        )

    def _intercept_tool(
        self, tool_name: str, arguments: dict[str, Any], execute: ToolExecutor
    ) -> Any:
        """Resolve a tool call against the job tool policy."""
        assert self._spec is not None
        config = self._spec.tool_policy
        assert config is not None
        policy = config.tools.get(tool_name, config.default)
        if isinstance(policy, PassthroughPolicy):
            return execute(tool_name, arguments)
        if isinstance(policy, StaticPolicy):
            for case in policy.cases:
                if _case_matches(case, arguments):
                    self._mocked_call = _MockedCall(policy=policy.type)
                    return case.result
            return self._handle_miss(
                policy.type, policy.on_miss, tool_name, arguments, execute
            )
        if isinstance(policy, HistoryPolicy):
            assert self._job_id is not None
            request = ToolLookupRequest(
                tool_name=tool_name,
                inputs=arguments,
                cache_key=tool_call_cache_key(tool_name, arguments),
            )
            response = self._run(self._client.jobs.tool_lookup(self._job_id, request))
            if response.found:
                self._mocked_call = _MockedCall(policy=policy.type)
                return response.result
            return self._handle_miss(
                policy.type, policy.on_miss, tool_name, arguments, execute
            )
        raise ToolPolicyError(f"Unsupported tool policy '{policy.type}'")

    def _handle_miss(
        self,
        policy_type: str,
        on_miss: ToolPolicyOnMiss,
        tool_name: str,
        arguments: dict[str, Any],
        execute: ToolExecutor,
    ) -> Any:
        """Apply the on_miss behavior of a tool policy."""
        if on_miss is ToolPolicyOnMiss.PASSTHROUGH:
            return execute(tool_name, arguments)
        if on_miss is ToolPolicyOnMiss.ERROR_RESULT:
            self._mocked_call = _MockedCall(policy=policy_type, failed=True)
            return {"error": f"No {policy_type} result for tool '{tool_name}'"}
        raise ToolPolicyMissError(f"No {policy_type} result for tool '{tool_name}'")

    def _buffer_node(self, node: SessionNodeCreateRequest) -> None:
        """Buffer a node and flush when the batch is full."""
        self._buffer.append(node)
        if len(self._buffer) >= self._batch_size:
            self._flush()

    def _flush(self) -> None:
        """Send the buffered nodes to the server."""
        if not self._buffer or self._session_id is None:
            return
        batch = SessionNodeBatchRequest(nodes=self._buffer)
        self._buffer = []
        self._run(self._client.session_nodes.upsert(self._session_id, batch))

    def _next_sequence(self) -> int:
        """Return the next node sequence number."""
        sequence = self._sequence
        self._sequence += 1
        return sequence

    def on_run_start(self, inputs: Any) -> None:
        """Create the session and its root span node."""
        started_at = datetime.now(UTC)
        session = self._run(
            self._client.sessions.create(
                SessionCreateRequest(
                    agent_id=self._agent_id,
                    agent_version_id=self._agent_version_id,
                    origin=SessionOrigin.RECORDED,
                    name=self._session_name,
                    inputs=inputs,
                    started_at=started_at,
                    framework=FRAMEWORK,
                    adapter_version=ADAPTER_VERSION,
                    job_id=self._job_id,
                )
            )
        )
        self._session_id = session.id
        self._root_id = uuid7()
        self._last_llm_id = None
        self._sequence = 0
        self._run_started_at = started_at
        self._run_inputs = inputs
        self._buffer = [
            SessionNodeCreateRequest(
                id=self._root_id,
                parent_id=None,
                sequence=self._next_sequence(),
                node_type=NodeType.SPAN,
                name="run",
                status=NodeStatus.IN_PROGRESS,
                started_at=started_at,
                inputs=inputs,
            )
        ]
        self._flush()

    def on_llm_call_start(self, call: LLMCall) -> None:
        """Record the LLM call start time."""
        self._llm_started_at = datetime.now(UTC)

    def on_llm_call_end(self, call: LLMCall) -> None:
        """Buffer an llm_call node."""
        node_id = uuid7()
        self._buffer_node(
            SessionNodeCreateRequest(
                id=node_id,
                parent_id=self._root_id,
                sequence=self._next_sequence(),
                node_type=NodeType.LLM_CALL,
                name=call.name,
                status=NodeStatus.COMPLETED,
                started_at=self._llm_started_at,
                ended_at=datetime.now(UTC),
                inputs=call.messages,
                outputs=call.output,
                requested_model=call.model,
                model=call.model,
                tokens=TokenUsage(
                    input_tokens=call.input_tokens, output_tokens=call.output_tokens
                ),
                cost=None if call.cost is None else Decimal(str(call.cost)),
                model_params=call.model_params,
            )
        )
        self._last_llm_id = node_id

    def on_tool_call_start(self, call: ToolCall) -> None:
        """Record the tool call start time and reset the policy outcome."""
        self._tool_started_at = datetime.now(UTC)
        self._mocked_call = None

    def on_tool_call_end(self, call: ToolCall) -> None:
        """Buffer a tool_call node under the requesting LLM call."""
        mocked = self._mocked_call
        self._mocked_call = None
        status = NodeStatus.COMPLETED
        error = call.error
        attributes: dict[str, Any] = {}
        if mocked is not None:
            attributes = {"mocked": True, "policy": mocked.policy}
            if mocked.failed:
                status = NodeStatus.FAILED
                error = json.dumps(call.result, sort_keys=True, default=str)
        if call.error is not None:
            status = NodeStatus.FAILED
        self._buffer_node(
            SessionNodeCreateRequest(
                id=uuid7(),
                parent_id=self._last_llm_id or self._root_id,
                sequence=self._next_sequence(),
                node_type=NodeType.TOOL_CALL,
                name=call.tool_name,
                status=status,
                error=error,
                started_at=self._tool_started_at,
                ended_at=datetime.now(UTC),
                inputs=call.arguments,
                outputs=call.result,
                tool_name=call.tool_name,
                attributes=attributes,
            )
        )

    def on_run_end(self, outputs: Any) -> None:
        """Complete the root span and the session."""
        ended_at = datetime.now(UTC)
        self._finish_root(NodeStatus.COMPLETED, outputs, None, ended_at)
        assert self._session_id is not None
        self._run(
            self._client.sessions.update(
                self._session_id,
                SessionUpdateRequest(
                    status=SessionStatus.COMPLETED, outputs=outputs, ended_at=ended_at
                ),
            )
        )

    def on_run_error(self, error: BaseException) -> None:
        """Fail the root span and the session."""
        if self._session_id is None:
            return
        ended_at = datetime.now(UTC)
        self._finish_root(NodeStatus.FAILED, None, str(error), ended_at)
        self._run(
            self._client.sessions.update(
                self._session_id,
                SessionUpdateRequest(
                    status=SessionStatus.FAILED, error=str(error), ended_at=ended_at
                ),
            )
        )

    def _finish_root(
        self,
        status: NodeStatus,
        outputs: Any,
        error: str | None,
        ended_at: datetime,
    ) -> None:
        """Re-upsert the root span in its terminal state and flush."""
        assert self._root_id is not None
        self._buffer_node(
            SessionNodeCreateRequest(
                id=self._root_id,
                parent_id=None,
                sequence=0,
                node_type=NodeType.SPAN,
                name="run",
                status=status,
                error=error,
                started_at=self._run_started_at,
                ended_at=ended_at,
                inputs=self._run_inputs,
                outputs=outputs,
            )
        )
        self._flush()
