#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
"""Public LangChain callback translation for LangGraph invocations."""

import uuid
from typing import Any

from langchain_core.callbacks import AsyncCallbackHandler, BaseCallbackHandler
from langchain_core.outputs import LLMResult

from kitaru.api_models.v1.session_node import NodeType

from .recording import InvocationRecorder, SyncBridge


def _name(serialized: dict[str, Any] | None, fallback: str) -> str:
    if not serialized:
        return fallback
    name = serialized.get("name")
    if isinstance(name, str) and name:
        return name
    identifier = serialized.get("id")
    if isinstance(identifier, list) and identifier:
        return str(identifier[-1])
    return fallback


class SyncKitaruCallback(BaseCallbackHandler):
    """Submit synchronous callback writes to one invocation's loop bridge."""

    def __init__(self, recorder: InvocationRecorder, bridge: SyncBridge) -> None:
        self._recorder = recorder
        self._bridge = bridge

    def on_chain_start(
        self,
        serialized: dict[str, Any],
        inputs: dict[str, Any],
        *,
        run_id: uuid.UUID,
        parent_run_id: uuid.UUID | None = None,
        **_: Any,
    ) -> None:
        self._bridge.run(
            self._recorder.start_chain(
                run_id=run_id,
                parent_run_id=parent_run_id,
                name=_name(serialized, "graph"),
                inputs=inputs,
            )
        )

    def on_chain_end(
        self,
        outputs: dict[str, Any],
        *,
        run_id: uuid.UUID,
        **_: Any,
    ) -> None:
        self._bridge.run(
            self._recorder.finish_chain(run_id=run_id, outputs=outputs, error=None)
        )

    def on_chain_error(
        self,
        error: BaseException,
        *,
        run_id: uuid.UUID,
        **_: Any,
    ) -> None:
        self._bridge.run(
            self._recorder.finish_chain(run_id=run_id, outputs=None, error=error)
        )

    def on_chat_model_start(
        self,
        serialized: dict[str, Any],
        messages: list[list[Any]],
        *,
        run_id: uuid.UUID,
        parent_run_id: uuid.UUID | None = None,
        **_: Any,
    ) -> None:
        self._start_model(serialized, messages, run_id, parent_run_id)

    def on_llm_start(
        self,
        serialized: dict[str, Any],
        prompts: list[str],
        *,
        run_id: uuid.UUID,
        parent_run_id: uuid.UUID | None = None,
        **_: Any,
    ) -> None:
        self._start_model(serialized, prompts, run_id, parent_run_id)

    def _start_model(
        self,
        serialized: dict[str, Any],
        inputs: Any,
        run_id: uuid.UUID,
        parent_run_id: uuid.UUID | None,
    ) -> None:
        self._bridge.run(
            self._recorder.start_call(
                run_id=run_id,
                parent_run_id=parent_run_id,
                name=_name(serialized, "model"),
                inputs=inputs,
                node_type=NodeType.LLM_CALL,
            )
        )

    def on_llm_end(self, response: LLMResult, *, run_id: uuid.UUID, **_: Any) -> None:
        self._bridge.run(
            self._recorder.finish_call(run_id=run_id, outputs=response, error=None)
        )

    def on_llm_error(
        self, error: BaseException, *, run_id: uuid.UUID, **_: Any
    ) -> None:
        self._bridge.run(
            self._recorder.finish_call(run_id=run_id, outputs=None, error=error)
        )

    def on_tool_start(
        self,
        serialized: dict[str, Any],
        input_str: str,
        *,
        run_id: uuid.UUID,
        parent_run_id: uuid.UUID | None = None,
        inputs: dict[str, Any] | None = None,
        **_: Any,
    ) -> None:
        name = _name(serialized, "tool")
        self._bridge.run(
            self._recorder.start_call(
                run_id=run_id,
                parent_run_id=parent_run_id,
                name=name,
                inputs=inputs if inputs is not None else input_str,
                node_type=NodeType.TOOL_CALL,
            )
        )

    def on_tool_end(self, output: Any, *, run_id: uuid.UUID, **_: Any) -> None:
        self._bridge.run(
            self._recorder.finish_call(
                run_id=run_id,
                outputs=output,
                error=None,
            )
        )

    def on_tool_error(
        self, error: BaseException, *, run_id: uuid.UUID, **_: Any
    ) -> None:
        self._bridge.run(
            self._recorder.finish_call(
                run_id=run_id,
                outputs=None,
                error=error,
            )
        )


class AsyncKitaruCallback(AsyncCallbackHandler):
    """Await callback writes on the caller's async invocation loop."""

    def __init__(self, recorder: InvocationRecorder) -> None:
        self._recorder = recorder

    async def on_chain_start(
        self,
        serialized: dict[str, Any],
        inputs: dict[str, Any],
        *,
        run_id: uuid.UUID,
        parent_run_id: uuid.UUID | None = None,
        **_: Any,
    ) -> None:
        await self._recorder.start_chain(
            run_id=run_id,
            parent_run_id=parent_run_id,
            name=_name(serialized, "graph"),
            inputs=inputs,
        )

    async def on_chain_end(
        self, outputs: dict[str, Any], *, run_id: uuid.UUID, **_: Any
    ) -> None:
        await self._recorder.finish_chain(run_id=run_id, outputs=outputs, error=None)

    async def on_chain_error(
        self, error: BaseException, *, run_id: uuid.UUID, **_: Any
    ) -> None:
        await self._recorder.finish_chain(run_id=run_id, outputs=None, error=error)

    async def on_chat_model_start(
        self,
        serialized: dict[str, Any],
        messages: list[list[Any]],
        *,
        run_id: uuid.UUID,
        parent_run_id: uuid.UUID | None = None,
        **_: Any,
    ) -> None:
        await self._start_model(serialized, messages, run_id, parent_run_id)

    async def on_llm_start(
        self,
        serialized: dict[str, Any],
        prompts: list[str],
        *,
        run_id: uuid.UUID,
        parent_run_id: uuid.UUID | None = None,
        **_: Any,
    ) -> None:
        await self._start_model(serialized, prompts, run_id, parent_run_id)

    async def _start_model(
        self,
        serialized: dict[str, Any],
        inputs: Any,
        run_id: uuid.UUID,
        parent_run_id: uuid.UUID | None,
    ) -> None:
        await self._recorder.start_call(
            run_id=run_id,
            parent_run_id=parent_run_id,
            name=_name(serialized, "model"),
            inputs=inputs,
            node_type=NodeType.LLM_CALL,
        )

    async def on_llm_end(
        self, response: LLMResult, *, run_id: uuid.UUID, **_: Any
    ) -> None:
        await self._recorder.finish_call(run_id=run_id, outputs=response, error=None)

    async def on_llm_error(
        self, error: BaseException, *, run_id: uuid.UUID, **_: Any
    ) -> None:
        await self._recorder.finish_call(run_id=run_id, outputs=None, error=error)

    async def on_tool_start(
        self,
        serialized: dict[str, Any],
        input_str: str,
        *,
        run_id: uuid.UUID,
        parent_run_id: uuid.UUID | None = None,
        inputs: dict[str, Any] | None = None,
        **_: Any,
    ) -> None:
        await self._recorder.start_call(
            run_id=run_id,
            parent_run_id=parent_run_id,
            name=_name(serialized, "tool"),
            inputs=inputs if inputs is not None else input_str,
            node_type=NodeType.TOOL_CALL,
        )

    async def on_tool_end(self, output: Any, *, run_id: uuid.UUID, **_: Any) -> None:
        await self._recorder.finish_call(
            run_id=run_id,
            outputs=output,
            error=None,
        )

    async def on_tool_error(
        self, error: BaseException, *, run_id: uuid.UUID, **_: Any
    ) -> None:
        await self._recorder.finish_call(
            run_id=run_id,
            outputs=None,
            error=error,
        )
