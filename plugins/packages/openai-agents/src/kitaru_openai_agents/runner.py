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
"""Non-streaming OpenAI Agents SDK runner facade."""

import asyncio
import contextlib
import os
import uuid
import warnings
from typing import Any, Generic, TypeVar

from agents import (
    Agent,
    AgentsException,
    RunConfig,
    RunErrorHandlers,
    RunHooks,
    Runner,
    RunResult,
    RunState,
    Session,
    TResponseInputItem,
)

from . import recording as recording_module
from .recording import (
    KitaruRecordingError,
    RunRecorder,
    SessionObserver,
    UnsupportedInterruptionError,
    finalize_failure,
    resolve_run_input,
)
from .replay import prepare_replay

TContext = TypeVar("TContext")

_PARTIAL_RECONCILIATION_FAILURE_NOTE = (
    "Kitaru could not reconcile partial OpenAI run data."
)
_FAILURE_FINALIZATION_FAILURE_NOTE = "Kitaru could not finalize the failed recording."
_CLIENT_CLOSE_FAILURE_NOTE = "Kitaru could not close the recording client."


class KitaruRunner(Generic[TContext]):
    """Run OpenAI agents while recording each run as a Kitaru session."""

    def __init__(
        self,
        *,
        agent_id: uuid.UUID | None = None,
        agent_version_id: uuid.UUID | None = None,
        session_name: str | None = None,
        batch_size: int = 20,
        session_observer: SessionObserver | None = None,
    ) -> None:
        """Configure Kitaru identity and recording behavior.

        Args:
            agent_id: Optional Kitaru agent identifier. Task-bound runs infer
                this from the task's agent version.
            agent_version_id: Optional Kitaru agent-version identifier.
            session_name: Optional name for the recorded Kitaru session.
            batch_size: Number of child nodes sent per upsert batch.
            session_observer: Optional sync or async callback invoked after the
                session and root node exist.

        Raises:
            ValueError: If batch size is invalid.
        """
        if batch_size < 1:
            raise ValueError("batch_size must be at least 1")
        self._agent_id = agent_id
        self._agent_version_id = agent_version_id
        self._session_name = session_name or os.environ.get("KITARU_SESSION_NAME")
        self._batch_size = batch_size
        self._session_observer = session_observer

    async def run(
        self,
        starting_agent: Agent[TContext],
        input: str | list[TResponseInputItem] | RunState[TContext],
        *,
        context: TContext | None = None,
        max_turns: int | None = 10,
        hooks: RunHooks[TContext] | None = None,
        run_config: RunConfig | dict[str, Any] | None = None,
        error_handlers: RunErrorHandlers[TContext] | None = None,
        previous_response_id: str | None = None,
        auto_previous_response_id: bool = False,
        conversation_id: str | None = None,
        session: Session | None = None,
    ) -> RunResult:
        """Run an agent asynchronously and return its exact native result."""
        validated_input = self._validate_run(input)
        return await self._run(
            starting_agent,
            validated_input,
            context=context,
            max_turns=max_turns,
            hooks=hooks,
            run_config=run_config,
            error_handlers=error_handlers,
            previous_response_id=previous_response_id,
            auto_previous_response_id=auto_previous_response_id,
            conversation_id=conversation_id,
            session=session,
        )

    def run_sync(
        self,
        starting_agent: Agent[TContext],
        input: str | list[TResponseInputItem] | RunState[TContext],
        *,
        context: TContext | None = None,
        max_turns: int | None = 10,
        hooks: RunHooks[TContext] | None = None,
        run_config: RunConfig | dict[str, Any] | None = None,
        error_handlers: RunErrorHandlers[TContext] | None = None,
        previous_response_id: str | None = None,
        auto_previous_response_id: bool = False,
        conversation_id: str | None = None,
        session: Session | None = None,
    ) -> RunResult:
        """Run an agent synchronously on the persistent thread-default loop."""
        validated_input = self._validate_run(input)
        try:
            running_loop = asyncio.get_running_loop()
        except RuntimeError:
            running_loop = None
        if running_loop is not None:
            raise RuntimeError(
                "KitaruRunner.run_sync() cannot be called when an event loop is "
                "already running; use KitaruRunner.run() instead."
            )

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            policy = asyncio.get_event_loop_policy()  # ty: ignore[deprecated]
            try:
                default_loop = policy.get_event_loop()
            except RuntimeError:
                default_loop = policy.new_event_loop()
                policy.set_event_loop(default_loop)
        if default_loop.is_closed():
            default_loop = policy.new_event_loop()
            policy.set_event_loop(default_loop)

        task = default_loop.create_task(
            self._run(
                starting_agent,
                validated_input,
                context=context,
                max_turns=max_turns,
                hooks=hooks,
                run_config=run_config,
                error_handlers=error_handlers,
                previous_response_id=previous_response_id,
                auto_previous_response_id=auto_previous_response_id,
                conversation_id=conversation_id,
                session=session,
            )
        )
        try:
            return default_loop.run_until_complete(task)
        except BaseException:
            if not task.done():
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    default_loop.run_until_complete(task)
            raise
        finally:
            if not default_loop.is_closed():
                with contextlib.suppress(RuntimeError):
                    default_loop.run_until_complete(default_loop.shutdown_asyncgens())

    async def _run(
        self,
        starting_agent: Agent[TContext],
        input: str | list[TResponseInputItem],
        **options: Any,
    ) -> RunResult:
        """Run one complete Kitaru lifecycle around the public OpenAI runner."""
        recorder = RunRecorder(
            client=recording_module.KitaruAPIClient(),
            batch_size=self._batch_size,
            observer=self._session_observer,
        )
        try:
            resolved = await resolve_run_input(recorder.client, input)
            task_bound = resolved.task_id is not None
            run_agent = starting_agent
            run_input = resolved.openai
            if resolved.replay is not None:
                prepared = prepare_replay(
                    starting_agent,
                    resolved.openai,
                    options.get("run_config"),
                    resolved.replay,
                )
                run_agent = prepared.starting_agent
                run_input = prepared.input
                options["run_config"] = prepared.run_config
            await recorder.start(
                inputs=resolved.recorded,
                agent_id=None if task_bound else self._agent_id,
                agent_version_id=None if task_bound else self._agent_version_id,
                session_name=self._session_name,
                replay=resolved.replay is not None,
                task_id=resolved.task_id,
            )
        except BaseException as error:
            await _finalize_failure_and_close(recorder, error)
            raise

        options["hooks"] = recorder.compose_hooks(options.get("hooks"))
        try:
            result = await Runner.run(run_agent, run_input, **options)
        except AgentsException as error:
            if error.run_data is not None:
                try:
                    await recorder.reconcile(error.run_data)
                except asyncio.CancelledError as cancellation:
                    await _finalize_failure_and_close(recorder, cancellation)
                    raise
                except BaseException:
                    _add_failure_note(error, _PARTIAL_RECONCILIATION_FAILURE_NOTE)
            await _finalize_failure_and_close(recorder, error)
            raise
        except BaseException as error:
            await _finalize_failure_and_close(recorder, error)
            raise

        try:
            await recorder.reconcile(result)
        except asyncio.CancelledError as error:
            await _finalize_failure_and_close(recorder, error)
            raise
        except BaseException as error:
            await _finalize_failure_and_close(recorder, error)
            raise KitaruRecordingError(
                result=result,
                session_id=recorder.session.id if recorder.session else None,
                phase="reconcile",
            ) from error

        if result.interruptions:
            error = UnsupportedInterruptionError(result)
            await _finalize_failure_and_close(recorder, error)
            raise error

        try:
            await recorder.complete(result.final_output)
        except asyncio.CancelledError as error:
            await _finalize_failure_and_close(recorder, error)
            raise
        except BaseException as error:
            await _finalize_failure_and_close(recorder, error)
            raise KitaruRecordingError(
                result=result,
                session_id=recorder.session.id if recorder.session else None,
                phase="finalize",
            ) from error
        try:
            await recorder.close()
        except asyncio.CancelledError:
            raise
        except BaseException as error:
            raise KitaruRecordingError(
                result=result,
                session_id=recorder.session.id if recorder.session else None,
                phase="close",
            ) from error
        return result

    def _validate_run(
        self, input: str | list[TResponseInputItem] | RunState[TContext]
    ) -> str | list[TResponseInputItem]:
        """Reject inputs that cannot enter the Kitaru v2 lifecycle."""
        if isinstance(input, RunState):
            raise TypeError(
                "RunState input is not supported because Kitaru v2 cannot resume "
                "an interrupted OpenAI run."
            )
        if (
            self._agent_id is None
            and self._agent_version_id is None
            and not os.environ.get("KITARU_TASK_ID")
        ):
            raise ValueError(
                "Standalone runs require agent_id or agent_version_id; task-bound "
                "runs may infer identity from KITARU_TASK_ID."
            )
        return input


async def _finalize_failure_and_close(
    recorder: RunRecorder, error: BaseException
) -> None:
    """Persist a failed run and close its client without replacing its error."""
    finalization_error = await finalize_failure(recorder, error)
    if finalization_error is not None:
        _add_failure_note(error, _FAILURE_FINALIZATION_FAILURE_NOTE)
    try:
        await recorder.close()
    except BaseException:
        _add_failure_note(error, _CLIENT_CLOSE_FAILURE_NOTE)


def _add_failure_note(error: BaseException, note: str) -> None:
    """Add a payload-free secondary failure note without replacing the error."""
    with contextlib.suppress(BaseException):
        error.add_note(note)
