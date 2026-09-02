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
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
"""One-shot public Claude Agent SDK query facade."""

import asyncio
import contextlib
import json
import os
import uuid
from collections.abc import AsyncGenerator, Awaitable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from typing import Any, cast

from claude_agent_sdk import (
    ClaudeAgentOptions,
    HookCallback,
    HookContext,
    HookInput,
    HookJSONOutput,
    HookMatcher,
    Message,
    ResultMessage,
    SdkMcpTool,
    create_sdk_mcp_server,
)
from claude_agent_sdk import (
    query as sdk_query,
)

from kitaru.api_models.v1.replay import ReplayResponse, ToolLookupRequest
from kitaru.api_models.v1.replay_config import (
    HistoryConfig,
    HistoryScope,
    LLMConfig,
    PassthroughConfig,
    ReplayOverride,
    StaticCase,
    StaticConfig,
    StaticMatchMode,
    ToolConfig,
    ToolPolicyOnMiss,
)
from kitaru.api_models.v1.session_node import NodeStatus
from kitaru.cache_keys import compute_tool_cache_key

from . import recording as recording_module
from .capability import (
    KitaruRecordingError,
    ReplayableSdkMcpServer,
    ToolPolicyError,
    ToolPolicyMissError,
    UnsupportedReplayError,
)
from .codec import decode_tool_result, normalize_tool_result
from .recording import (
    InvocationRecorder,
    finalize_failure,
    finalize_terminal,
    resolve_run_input,
)

_FINALIZATION_FAILURE_NOTE = "Kitaru could not finalize the failed recording."
_INNER_CLOSE_FAILURE_NOTE = "Kitaru could not close the Claude query iterator."


@dataclass
class _HistoryState:
    """Track history occurrences and reject concurrent identical calls."""

    occurrences: dict[str, int] = field(default_factory=dict)
    active_keys: set[str] = field(default_factory=set)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)


@dataclass
class _ReplayAbortState:
    """Retain the first policy failure hidden by the SDK MCP boundary."""

    error: Exception | None = None

    def capture(self, error: Exception) -> None:
        """Retain the first adapter failure for the outer query boundary."""
        if self.error is None:
            self.error = error

    def raise_if_captured(self) -> None:
        """Raise an adapter failure after the SDK returns control to Kitaru."""
        if self.error is not None:
            raise self.error

    async def run_kitaru(self, operation: Awaitable[Any]) -> Any:
        """Retain a Kitaru operation failure that the SDK may swallow."""
        try:
            return await operation
        except Exception as error:
            self.capture(error)
            raise


class KitaruClaudeRunner:
    """Run one-shot Claude queries while recording native message streams."""

    def __init__(
        self,
        *,
        agent_id: uuid.UUID | None = None,
        agent_version_id: uuid.UUID | None = None,
        session_name: str | None = None,
    ) -> None:
        """Configure Kitaru identity for independently recorded queries."""
        self._agent_id = agent_id
        self._agent_version_id = agent_version_id
        self._session_name = session_name or os.environ.get("KITARU_SESSION_NAME")

    async def query(
        self,
        *,
        prompt: str,
        options: ClaudeAgentOptions | None = None,
        replayable_servers: Sequence[ReplayableSdkMcpServer] = (),
        transport: Any = None,
    ) -> AsyncGenerator[Message, None]:
        """Yield exact native messages from a one-shot public SDK query.

        Use ``contextlib.aclosing()`` when stopping consumption early so the
        Claude iterator and partial Kitaru session are closed promptly.
        """
        if not isinstance(prompt, str):
            raise TypeError(
                "KitaruClaudeRunner.query() supports string prompts only; "
                "async prompt iterables cannot be replayed safely."
            )
        self._validate_identity()

        client = recording_module.KitaruAPIClient()
        recorder: InvocationRecorder | None = None
        inner: AsyncGenerator[Message, None] | None = None
        replay_abort = _ReplayAbortState()
        primary_error: BaseException | None = None
        try:
            try:
                resolved = await resolve_run_input(client, prompt)
                copied_options = replace(options or ClaudeAgentOptions())
                run_prompt, copied_options = _prepare_replay(
                    resolved.claude, copied_options, resolved.replay
                )
                replayable_tools = _preflight_replayable_servers(
                    replayable_servers,
                    copied_options,
                    resolved.replay,
                )
                safe_options = _get_safe_options(copied_options)
                recorder = await InvocationRecorder.start(
                    client=client,
                    inputs=resolved.recorded,
                    agent_id=None if resolved.task_bound else self._agent_id,
                    agent_version_id=(
                        None if resolved.task_bound else self._agent_version_id
                    ),
                    session_name=self._session_name,
                    replay=resolved.replay is not None,
                    safe_options=safe_options,
                    replayable_tool_names=frozenset(replayable_tools),
                )
            except BaseException as error:
                primary_error = error
                if recorder is None:
                    await _close_client_preserving_error(client, error)
                raise

            try:
                copied_options.mcp_servers = _materialize_replayable_servers(
                    replayable_servers,
                    copied_options.mcp_servers,
                    resolved.replay,
                    client,
                    recorder,
                    replay_abort,
                )
                copied_options.hooks = _compose_hooks(copied_options.hooks, recorder)
                inner = cast(
                    AsyncGenerator[Message, None],
                    sdk_query(
                        prompt=run_prompt,
                        options=copied_options,
                        transport=transport,
                    ),
                )
                async for message in inner:
                    replay_abort.raise_if_captured()
                    try:
                        await recorder.record_message(message)
                    except asyncio.CancelledError:
                        raise
                    except BaseException as error:
                        recording_error = KitaruRecordingError(
                            terminal_message=(
                                message if isinstance(message, ResultMessage) else None
                            ),
                            session_id=recorder.session_id,
                            phase="record",
                        )
                        failure = await finalize_failure(recorder, recording_error)
                        if failure is not None:
                            _add_failure_note(
                                recording_error, _FINALIZATION_FAILURE_NOTE
                            )
                        raise recording_error from error

                    if isinstance(message, ResultMessage):
                        await finalize_terminal(recorder, message)
                    yield message
                replay_abort.raise_if_captured()
                if not recorder.finalized:
                    raise RuntimeError(
                        "Claude query ended without a terminal ResultMessage."
                    )
            except BaseException as error:
                primary_error = error
                if not recorder.finalized:
                    failure = await finalize_failure(recorder, error)
                    if failure is not None:
                        _add_failure_note(error, _FINALIZATION_FAILURE_NOTE)
                raise
        finally:
            if inner is not None:
                try:
                    await inner.aclose()
                except BaseException as close_error:
                    if primary_error is not None:
                        _add_failure_note(primary_error, _INNER_CLOSE_FAILURE_NOTE)
                    else:
                        if recorder is not None and not recorder.finalized:
                            await finalize_failure(recorder, close_error)
                        raise

    def _validate_identity(self) -> None:
        """Require an explicit identity outside Kitaru task execution."""
        if (
            self._agent_id is None
            and self._agent_version_id is None
            and not os.environ.get("KITARU_TASK_ID")
        ):
            raise ValueError(
                "Standalone queries require agent_id or agent_version_id; "
                "task-bound queries may infer identity from KITARU_TASK_ID."
            )


def _prepare_replay(
    prompt: str,
    options: ClaudeAgentOptions,
    replay: ReplayResponse | None,
) -> tuple[str, ClaudeAgentOptions]:
    """Apply root-input replay to the private options copy."""
    if replay is None:
        return prompt, options

    unsupported_session_options = [
        name
        for name, enabled in (
            ("resume", options.resume is not None),
            ("continue_conversation", options.continue_conversation),
            ("fork_session", options.fork_session),
            ("resume_session_at", options.resume_session_at is not None),
            ("resume_drops_turn", options.resume_drops_turn is not None),
        )
        if enabled
    ]
    if unsupported_session_options:
        names = ", ".join(unsupported_session_options)
        raise UnsupportedReplayError(
            f"Kitaru replay cannot be combined with Claude {names}."
        )

    override = replay.override
    if override is not None and override.model_params:
        raise UnsupportedReplayError(
            "Claude Agent SDK replay does not support model_params."
        )
    if override is None:
        return prompt, options
    if override.prompt is not None:
        prompt = override.prompt
    if override.system_prompt is not None:
        options.system_prompt = override.system_prompt
    replacement_model = _resolve_model_override(override, options.model)
    if replacement_model is not None:
        options.model = replacement_model
    return prompt, options


def _resolve_model_override(
    override: ReplayOverride, current_model: str | None
) -> str | None:
    """Resolve a direct or current-model-keyed replacement."""
    model = override.model
    if model is None or isinstance(model, str):
        return model
    if current_model is None:
        raise UnsupportedReplayError(
            "A mapped model override requires ClaudeAgentOptions.model."
        )
    replacement = model.get(current_model)
    if replacement is None:
        raise UnsupportedReplayError(
            f"The mapped model override has no entry for '{current_model}'."
        )
    return replacement


def _get_qualified_tool_name(server_name: str, tool_name: str) -> str:
    """Build Claude's exact public MCP tool identity."""
    return f"mcp__{server_name}__{tool_name}"


def _preflight_replayable_servers(
    servers: Sequence[ReplayableSdkMcpServer],
    options: ClaudeAgentOptions,
    replay: ReplayResponse | None,
) -> dict[str, SdkMcpTool[Any]]:
    """Validate definitions and harden substituting replay options."""
    if servers and not isinstance(options.mcp_servers, Mapping):
        raise UnsupportedReplayError(
            "Replayable SDK MCP servers require mapping-based mcp_servers options."
        )
    existing_servers = (
        set(cast(Mapping[str, Any], options.mcp_servers))
        if isinstance(options.mcp_servers, Mapping)
        else set()
    )
    tools: dict[str, SdkMcpTool[Any]] = {}
    server_names: set[str] = set()
    for server in servers:
        if not server.name or not server.version:
            raise UnsupportedReplayError(
                "Replayable SDK MCP server names and versions must be non-empty."
            )
        if server.name in server_names or server.name in existing_servers:
            raise UnsupportedReplayError(
                f"Replayable SDK MCP server '{server.name}' is duplicated."
            )
        server_names.add(server.name)
        for tool in server.tools:
            identity = _get_qualified_tool_name(server.name, tool.name)
            if identity in tools:
                raise UnsupportedReplayError(
                    f"Replayable SDK MCP tool '{identity}' is duplicated."
                )
            tools[identity] = tool

    if replay is None:
        return tools
    policy = replay.tool_policy
    has_substitution = not isinstance(policy.default, PassthroughConfig) or any(
        not isinstance(config, PassthroughConfig) for config in policy.tools.values()
    )
    if not has_substitution:
        return tools
    if isinstance(policy.default, LLMConfig):
        raise UnsupportedReplayError(
            "LLM substitution is not supported by the Claude Agent SDK adapter."
        )
    if not (
        isinstance(policy.default, StaticConfig)
        and not policy.default.cases
        and policy.default.on_miss is ToolPolicyOnMiss.FAIL
    ):
        raise UnsupportedReplayError(
            "Claude SDK MCP replay requires an empty static fail default policy."
        )
    if not tools:
        raise UnsupportedReplayError(
            "Tool substitution requires adapter-wrapped SDK MCP tools."
        )
    for target, config in policy.tools.items():
        if isinstance(config, LLMConfig):
            raise UnsupportedReplayError(
                f"LLM substitution is not supported for tool '{target}'."
            )
        if isinstance(config, StaticConfig | HistoryConfig) and target not in tools:
            raise UnsupportedReplayError(
                f"Tool policy target '{target}' is not a wrapped SDK MCP tool."
            )
        if isinstance(config, StaticConfig):
            for case in config.cases:
                try:
                    normalize_tool_result(case.result)
                except ToolPolicyError as error:
                    raise UnsupportedReplayError(
                        f"Static result for tool '{target}' cannot be replayed: {error}"
                    ) from error
        if (
            isinstance(config, PassthroughConfig)
            and any(target.startswith(f"mcp__{name}__") for name in server_names)
            and target not in tools
        ):
            raise UnsupportedReplayError(
                f"Tool policy target '{target}' is not defined by its wrapped SDK "
                "MCP server."
            )
    if options.tools != []:
        raise UnsupportedReplayError(
            "Claude SDK MCP substitution requires ClaudeAgentOptions(tools=[]) "
            "so unwrapped built-in tools cannot execute. Kitaru injects wrapped "
            "SDK MCP tools separately."
        )
    if existing_servers:
        names = ", ".join(sorted(existing_servers))
        raise UnsupportedReplayError(
            "Claude SDK MCP substitution cannot run with pre-existing MCP servers "
            f"because unwrapped tools cannot be denied safely: {names}. Remove "
            "them or use an all-passthrough tool policy."
        )
    unwrapped_allowed = [
        target for target in options.allowed_tools if target not in tools
    ]
    if unwrapped_allowed:
        names = ", ".join(unwrapped_allowed)
        raise UnsupportedReplayError(
            "Claude SDK MCP substitution requires allowed_tools to contain only "
            f"exact wrapped tool identities; remove: {names}."
        )
    topology_options = [
        ("settings", options.settings is not None, "None"),
        ("setting_sources", bool(options.setting_sources), "None or []"),
        ("plugins", bool(options.plugins), "[]"),
        ("extra_args", bool(options.extra_args), "{}"),
        ("skills", options.skills is not None, "None"),
        ("agents", options.agents is not None, "None"),
    ]
    for name, configured, safe_value in topology_options:
        if configured:
            raise UnsupportedReplayError(
                "Claude SDK MCP substitution cannot verify unwrapped tools "
                f"introduced through ClaudeAgentOptions.{name}; set {name}="
                f"{safe_value} or use an all-passthrough tool policy."
            )
    options.setting_sources = []
    options.strict_mcp_config = True
    return tools


def _materialize_replayable_servers(
    definitions: Sequence[ReplayableSdkMcpServer],
    current_servers: Any,
    replay: ReplayResponse | None,
    client: Any,
    recorder: InvocationRecorder,
    replay_abort: _ReplayAbortState,
) -> Any:
    """Create fresh handler-bound public SDK MCP servers for one query."""
    if not definitions:
        return current_servers
    servers = dict(cast(Mapping[str, Any], current_servers))
    history = _HistoryState()
    for definition in definitions:
        wrapped = [
            replace(
                tool,
                handler=_wrap_tool_handler(
                    identity=_get_qualified_tool_name(definition.name, tool.name),
                    original=tool.handler,
                    replay=replay,
                    client=client,
                    history=history,
                    recorder=recorder,
                    replay_abort=replay_abort,
                ),
            )
            for tool in definition.tools
        ]
        servers[definition.name] = create_sdk_mcp_server(
            name=definition.name,
            version=definition.version,
            tools=wrapped,
        )
    return servers


def _normalize_arguments(identity: str, arguments: Any) -> dict[str, Any]:
    """Round-trip handler arguments through strict canonical JSON."""
    if not isinstance(arguments, Mapping):
        raise ToolPolicyError(f"Arguments for tool '{identity}' must be an object")
    try:
        serialized = json.dumps(
            arguments,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
        normalized = json.loads(serialized)
        serialized.encode("utf-8")
    except (TypeError, ValueError, UnicodeError, RecursionError) as error:
        raise ToolPolicyError(
            f"Arguments for tool '{identity}' cannot be replayed safely"
        ) from error
    if not isinstance(normalized, dict):
        raise ToolPolicyError(f"Arguments for tool '{identity}' must be an object")
    return cast(dict[str, Any], normalized)


def _case_matches(case: StaticCase, arguments: dict[str, Any]) -> bool:
    """Match exact or shallow-subset static arguments."""
    if case.match is None:
        return True
    if case.match_mode is StaticMatchMode.EXACT:
        return arguments == case.match
    if not isinstance(case.match, dict):
        return False
    return all(
        key in arguments and arguments[key] == value
        for key, value in case.match.items()
    )


def _get_tool_policy(replay: ReplayResponse, identity: str) -> ToolConfig:
    """Resolve one exact identity against the shared Kitaru policy."""
    return replay.tool_policy.tools.get(identity, replay.tool_policy.default)


def _create_error_result(policy_name: str, identity: str) -> dict[str, Any]:
    """Return a valid Claude-readable MCP error result."""
    return {
        "content": [
            {
                "type": "text",
                "text": f"No {policy_name} result for tool '{identity}'",
            }
        ],
        "is_error": True,
    }


async def _handle_miss(
    *,
    policy_name: str,
    on_miss: ToolPolicyOnMiss,
    identity: str,
    arguments: dict[str, Any],
    original: Any,
    recorder: InvocationRecorder,
    replay_abort: _ReplayAbortState,
) -> dict[str, Any]:
    """Apply the exact shared miss behavior at the handler boundary."""
    passthrough = on_miss is ToolPolicyOnMiss.PASSTHROUGH
    await replay_abort.run_kitaru(
        recorder.record_tool_policy(
            tool_name=identity,
            arguments=arguments,
            policy=policy_name,
            live=passthrough,
        )
    )
    if passthrough:
        return cast(dict[str, Any], await original(arguments))
    if on_miss is ToolPolicyOnMiss.ERROR_RESULT:
        return _create_error_result(policy_name, identity)
    raise ToolPolicyMissError(f"No {policy_name} result for tool '{identity}'")


def _wrap_tool_handler(
    *,
    identity: str,
    original: Any,
    replay: ReplayResponse | None,
    client: Any,
    history: _HistoryState,
    recorder: InvocationRecorder,
    replay_abort: _ReplayAbortState,
) -> Any:
    """Bind one original public handler to one query's replay state."""

    async def handler(arguments: Any) -> dict[str, Any]:
        try:
            normalized = _normalize_arguments(identity, arguments)
            if replay is None:
                await replay_abort.run_kitaru(
                    recorder.record_tool_policy(
                        tool_name=identity,
                        arguments=normalized,
                        policy="passthrough",
                        live=True,
                    )
                )
                return cast(dict[str, Any], await original(normalized))
            policy = _get_tool_policy(replay, identity)
            if isinstance(policy, PassthroughConfig):
                await replay_abort.run_kitaru(
                    recorder.record_tool_policy(
                        tool_name=identity,
                        arguments=normalized,
                        policy=policy.type,
                        live=True,
                    )
                )
                return cast(dict[str, Any], await original(normalized))
            if isinstance(policy, StaticConfig):
                matching = next(
                    (case for case in policy.cases if _case_matches(case, normalized)),
                    None,
                )
                if matching is not None:
                    await replay_abort.run_kitaru(
                        recorder.record_tool_policy(
                            tool_name=identity,
                            arguments=normalized,
                            policy=policy.type,
                            live=False,
                        )
                    )
                    return normalize_tool_result(matching.result)
                return await _handle_miss(
                    policy_name=policy.type,
                    on_miss=policy.on_miss,
                    identity=identity,
                    arguments=normalized,
                    original=original,
                    recorder=recorder,
                    replay_abort=replay_abort,
                )
            if isinstance(policy, HistoryConfig):
                return await _history_result(
                    identity=identity,
                    arguments=normalized,
                    original=original,
                    policy=policy,
                    replay=replay,
                    client=client,
                    state=history,
                    recorder=recorder,
                    replay_abort=replay_abort,
                )
            raise ToolPolicyError(
                f"Tool policy '{policy.type}' is not supported for '{identity}'"
            )
        except ToolPolicyError as error:
            replay_abort.capture(error)
            raise

    return handler


async def _history_result(
    *,
    identity: str,
    arguments: dict[str, Any],
    original: Any,
    policy: HistoryConfig,
    replay: ReplayResponse,
    client: Any,
    state: _HistoryState,
    recorder: InvocationRecorder,
    replay_abort: _ReplayAbortState,
) -> dict[str, Any]:
    """Resolve a history result without scheduler-dependent occurrence claims."""
    cache_key = compute_tool_cache_key(identity, arguments)
    if cache_key is None:
        return await _handle_miss(
            policy_name=policy.type,
            on_miss=policy.on_miss,
            identity=identity,
            arguments=arguments,
            original=original,
            recorder=recorder,
            replay_abort=replay_abort,
        )
    track_occurrence = policy.scope is HistoryScope.BASELINE
    if track_occurrence:
        async with state.lock:
            if cache_key in state.active_keys:
                raise ToolPolicyError(
                    f"Concurrent identical history calls for tool '{identity}' are "
                    "ambiguous; use static replay or distinct identities or arguments"
                )
            state.active_keys.add(cache_key)
            occurrence = state.occurrences.get(cache_key, 0)
    else:
        occurrence = None
    try:
        response = await replay_abort.run_kitaru(
            client.replays.tool_lookup(
                replay.id,
                ToolLookupRequest(
                    tool_name=identity,
                    cache_key=cache_key,
                    occurrence=occurrence,
                ),
            )
        )
        if "match" not in response.model_fields_set:
            raise ToolPolicyError(
                "Kitaru server tool lookup response does not include 'match'; "
                "upgrade the server before using history replay"
            )
        match = response.match
        if match is None:
            return await _handle_miss(
                policy_name=policy.type,
                on_miss=policy.on_miss,
                identity=identity,
                arguments=arguments,
                original=original,
                recorder=recorder,
                replay_abort=replay_abort,
            )
        if match.status not in {NodeStatus.COMPLETED, NodeStatus.FAILED}:
            raise ToolPolicyError(
                f"History lookup for tool '{identity}' returned unexpected status "
                f"'{match.status.value}'"
            )
        if track_occurrence:
            async with state.lock:
                state.occurrences[cache_key] = cast(int, occurrence) + 1
        await replay_abort.run_kitaru(
            recorder.record_tool_policy(
                tool_name=identity,
                arguments=arguments,
                policy=policy.type,
                live=False,
            )
        )
        if match.status is NodeStatus.FAILED:
            raise ToolPolicyError(
                match.error or f"Recorded tool call '{identity}' failed"
            )
        return decode_tool_result(match.result)
    finally:
        if track_occurrence:
            async with state.lock:
                state.active_keys.discard(cache_key)


def _get_safe_options(options: ClaudeAgentOptions) -> dict[str, Any]:
    """Select the option fields the recorder's bounded capture permits."""
    return {
        "allowed_tools": options.allowed_tools,
        "disallowed_tools": options.disallowed_tools,
        "model": options.model,
        "permission_mode": options.permission_mode,
        "system_prompt": options.system_prompt,
    }


def _compose_hooks(
    caller_hooks: Any,
    recorder: InvocationRecorder,
) -> Any:
    """Append run-local observers without mutating caller hook collections."""
    hooks = {name: list(matchers) for name, matchers in (caller_hooks or {}).items()}

    async def before_tool(
        hook_input: HookInput,
        _tool_use_id: str | None,
        _context: HookContext,
    ) -> HookJSONOutput:
        await recorder.record_tool_hook(
            cast(Mapping[str, Any], hook_input), event="before"
        )
        return {}

    async def after_tool(
        hook_input: HookInput,
        _tool_use_id: str | None,
        _context: HookContext,
    ) -> HookJSONOutput:
        await recorder.record_tool_hook(
            cast(Mapping[str, Any], hook_input), event="after"
        )
        return {}

    hooks.setdefault("PreToolUse", []).append(
        HookMatcher(hooks=[cast(HookCallback, before_tool)])
    )
    hooks.setdefault("PostToolUse", []).append(
        HookMatcher(hooks=[cast(HookCallback, after_tool)])
    )
    hooks.setdefault("PostToolUseFailure", []).append(
        HookMatcher(hooks=[cast(HookCallback, after_tool)])
    )
    return hooks


async def _close_client_preserving_error(client: Any, error: BaseException) -> None:
    """Close a preflight client without replacing the requested failure."""
    try:
        await client.close()
    except BaseException:
        _add_failure_note(error, "Kitaru could not close the preflight client.")


def _add_failure_note(error: BaseException, note: str) -> None:
    """Attach a payload-free secondary failure without replacing the primary."""
    with contextlib.suppress(BaseException):
        error.add_note(note)


__all__ = ["KitaruClaudeRunner"]
