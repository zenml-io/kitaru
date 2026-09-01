#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
"""One-shot public Claude Agent SDK query facade."""

import asyncio
import contextlib
import os
import uuid
from collections.abc import AsyncGenerator, AsyncIterable, Mapping
from dataclasses import replace
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
)
from claude_agent_sdk import (
    query as sdk_query,
)

from kitaru.api_models.v1.replay import ReplayResponse
from kitaru.api_models.v1.replay_config import PassthroughConfig, ReplayOverride

from . import recording as recording_module
from .capability import KitaruRecordingError, UnsupportedReplayError
from .recording import (
    InvocationRecorder,
    finalize_failure,
    finalize_terminal,
    resolve_run_input,
)

_FINALIZATION_FAILURE_NOTE = "Kitaru could not finalize the failed recording."
_INNER_CLOSE_FAILURE_NOTE = "Kitaru could not close the Claude query iterator."


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
        prompt: str | AsyncIterable[dict[str, Any]],
        options: ClaudeAgentOptions | None = None,
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
        primary_error: BaseException | None = None
        try:
            try:
                resolved = await resolve_run_input(client, prompt)
                copied_options = replace(options or ClaudeAgentOptions())
                run_prompt, copied_options = _prepare_replay(
                    resolved.claude, copied_options, resolved.replay
                )
                safe_options = _safe_options(copied_options)
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
                )
            except BaseException as error:
                primary_error = error
                if recorder is None:
                    await _close_client_preserving_error(client, error)
                raise

            copied_options.hooks = _compose_hooks(copied_options.hooks, recorder)
            inner = cast(
                AsyncGenerator[Message, None],
                sdk_query(
                    prompt=run_prompt,
                    options=copied_options,
                    transport=transport,
                ),
            )
            try:
                async for message in inner:
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
    """Preflight and apply root-input replay without mutating caller options."""
    if replay is None:
        return prompt, options

    unsupported_session_options = [
        name
        for name, enabled in (
            ("resume", options.resume is not None),
            ("continue_conversation", options.continue_conversation),
            ("fork_session", options.fork_session),
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
    if not isinstance(replay.tool_policy.default, PassthroughConfig) or any(
        not isinstance(policy, PassthroughConfig)
        for policy in replay.tool_policy.tools.values()
    ):
        raise UnsupportedReplayError(
            "Tool substitution requires adapter-wrapped SDK MCP tools."
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
    if isinstance(override.model, str):
        return override.model
    if isinstance(override.model, dict) and current_model is not None:
        return override.model.get(current_model)
    return None


def _safe_options(options: ClaudeAgentOptions) -> dict[str, Any]:
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
