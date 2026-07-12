"""Installed Google ADK public API contract tests.

These tests use real ``google-adk`` classes when the optional dependency is
installed. They do not instantiate hosted provider clients or make network
calls. The runner smoke uses only a local in-memory ADK runner plus local dummy
``BaseLlm`` / ``BaseTool`` objects.
"""

from __future__ import annotations

import asyncio
import importlib
import inspect
import os
import socket
import sys
from collections.abc import AsyncIterator, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import pytest


def _import_adk_module(module_path: str) -> Any:
    try:
        return importlib.import_module(module_path)
    except ModuleNotFoundError as exc:
        missing_requested_module = exc.name is not None and (
            module_path == exc.name or module_path.startswith(f"{exc.name}.")
        )
        if not missing_requested_module:
            raise
        if os.environ.get("KITARU_REQUIRE_GOOGLE_ADK_CONTRACT") == "1":
            pytest.fail(
                "google-adk contract tests were required, but "
                f"`{module_path}` could not be imported. Install the optional "
                "extra in a no-dev environment, for example: "
                "uv run --no-dev --extra google-adk --with pytest pytest "
                "tests/test_google_adk_installed_contract.py"
            )
        pytest.skip("google-adk optional dependency is not installed")


def _adk_contract_classes() -> tuple[type[Any], type[Any], type[Any]]:
    _import_adk_module("google.adk")
    base_llm_module = _import_adk_module("google.adk.models.base_llm")
    base_tool_module = _import_adk_module("google.adk.tools.base_tool")
    try:
        agents_module = _import_adk_module("google.adk.agents")
        llm_agent = agents_module.LlmAgent
    except (AttributeError, ImportError):
        llm_agent_module = _import_adk_module("google.adk.agents.llm_agent")
        llm_agent = llm_agent_module.LlmAgent
    return base_llm_module.BaseLlm, base_tool_module.BaseTool, llm_agent


@dataclass(frozen=True)
class _ADKRuntimeAPI:
    runner_cls: type[Any] | None
    in_memory_runner_cls: type[Any] | None
    in_memory_session_service_cls: type[Any] | None
    genai_types: Any
    llm_response_cls: type[Any]


def _adk_runtime_api() -> _ADKRuntimeAPI:
    runners_module = _import_adk_module("google.adk.runners")
    sessions_module = _import_adk_module("google.adk.sessions")
    genai_types = _import_adk_module("google.genai.types")
    llm_response_module = _import_adk_module("google.adk.models.llm_response")
    return _ADKRuntimeAPI(
        runner_cls=getattr(runners_module, "Runner", None),
        in_memory_runner_cls=getattr(runners_module, "InMemoryRunner", None),
        in_memory_session_service_cls=getattr(
            sessions_module,
            "InMemorySessionService",
            None,
        ),
        genai_types=genai_types,
        llm_response_cls=llm_response_module.LlmResponse,
    )


def _adapter_module() -> Any:
    for cached in list(sys.modules):
        if cached.startswith("kitaru.adapters.google_adk"):
            del sys.modules[cached]
    return importlib.import_module("kitaru.adapters.google_adk")


def _adapter_wrappers() -> tuple[type[Any], type[Any]]:
    adapter = _adapter_module()
    return adapter.KitaruADKModel, adapter.KitaruADKTool


async def _collect_model_events(model: Any, request: Any) -> list[Any]:
    return [event async for event in model.generate_content_async(request)]


def _text_content(genai_types: Any, text: str, *, role: str) -> Any:
    part_cls = genai_types.Part
    if hasattr(part_cls, "from_text"):
        part = part_cls.from_text(text=text)
    else:
        part = part_cls(text=text)
    return genai_types.Content(role=role, parts=[part])


def _function_call_content(
    genai_types: Any,
    *,
    name: str,
    args: dict[str, Any],
) -> Any:
    part_cls = genai_types.Part
    if hasattr(part_cls, "from_function_call"):
        part = part_cls.from_function_call(name=name, args=args)
    else:
        function_call = genai_types.FunctionCall(name=name, args=args)
        part = part_cls(function_call=function_call)
    return genai_types.Content(role="model", parts=[part])


def _llm_response(api: _ADKRuntimeAPI, content: Any) -> Any:
    return api.llm_response_cls(content=content)


def _local_lookup_declaration(genai_types: Any) -> Any:
    return genai_types.FunctionDeclaration(
        name="local_lookup",
        description="Local lookup used by Kitaru's installed ADK runner smoke.",
        parameters=genai_types.Schema(
            type=genai_types.Type.OBJECT,
            properties={"query": genai_types.Schema(type=genai_types.Type.STRING)},
            required=["query"],
        ),
    )


def _build_in_memory_runner(
    api: _ADKRuntimeAPI,
    *,
    agent: Any,
    app_name: str,
) -> Any:
    if api.in_memory_runner_cls is not None:
        return api.in_memory_runner_cls(agent=agent, app_name=app_name)

    if api.runner_cls is None or api.in_memory_session_service_cls is None:
        pytest.fail(
            "Installed google-adk does not expose InMemoryRunner or "
            "Runner + InMemorySessionService for local runner smoke."
        )

    session_service = api.in_memory_session_service_cls()
    return api.runner_cls(
        app_name=app_name,
        agent=agent,
        session_service=session_service,
    )


async def _create_runner_session(
    runner: Any,
    *,
    app_name: str,
    user_id: str,
    session_id: str,
) -> Any:
    session_service = getattr(runner, "session_service", None)
    if session_service is None:
        pytest.fail("Installed ADK runner does not expose `.session_service`.")
    create_session = getattr(session_service, "create_session", None)
    if not callable(create_session):
        pytest.fail("Installed ADK session service has no `create_session(...)`.")

    parameters = inspect.signature(create_session).parameters
    kwargs: dict[str, Any] = {}
    if "app_name" in parameters:
        kwargs["app_name"] = app_name
    if "user_id" in parameters:
        kwargs["user_id"] = user_id
    if "session_id" in parameters:
        kwargs["session_id"] = session_id
    elif "id" in parameters:
        kwargs["id"] = session_id

    session = create_session(**kwargs)
    if inspect.isawaitable(session):
        return await session
    return session


async def _run_in_memory_adapter_turn(
    api: _ADKRuntimeAPI,
    adapter: Any,
    *,
    agent: Any,
    app_name: str,
    session_id: str,
    message: Any,
    user_id: str = "local-user",
) -> Any:
    runner = _build_in_memory_runner(api, agent=agent, app_name=app_name)
    await _create_runner_session(
        runner,
        app_name=app_name,
        user_id=user_id,
        session_id=session_id,
    )
    return await adapter.KitaruADKRunner(runner, name=app_name).run(
        adapter.ADKRunRequest(
            user_id=user_id,
            session_id=session_id,
            message=message,
        )
    )


async def _get_runner_session(
    runner: Any,
    *,
    app_name: str,
    user_id: str,
    session_id: str,
) -> Any:
    session_service = getattr(runner, "session_service", None)
    if session_service is None:
        pytest.fail("Installed ADK runner does not expose `.session_service`.")
    get_session = getattr(session_service, "get_session", None)
    if not callable(get_session):
        pytest.fail("Installed ADK session service has no `get_session(...)`.")

    parameters = inspect.signature(get_session).parameters
    kwargs: dict[str, Any] = {}
    if "app_name" in parameters:
        kwargs["app_name"] = app_name
    if "user_id" in parameters:
        kwargs["user_id"] = user_id
    if "session_id" in parameters:
        kwargs["session_id"] = session_id
    elif "id" in parameters:
        kwargs["id"] = session_id

    session = get_session(**kwargs)
    if inspect.isawaitable(session):
        return await session
    return session


def _session_state(session: Any) -> Mapping[str, Any]:
    state = getattr(session, "state", None)
    if isinstance(state, Mapping):
        return state
    model_dump = getattr(state, "model_dump", None)
    if callable(model_dump):
        dumped = model_dump(mode="json")
        if isinstance(dumped, Mapping):
            return dumped
    pytest.fail("Installed ADK session does not expose mapping-like `.state`.")


def _contains_marker(value: Any, marker: str) -> bool:
    if isinstance(value, str):
        return marker in value
    if isinstance(value, Mapping):
        return any(
            _contains_marker(key, marker) or _contains_marker(item, marker)
            for key, item in value.items()
        )
    if isinstance(value, Sequence) and not isinstance(value, bytes | bytearray):
        return any(_contains_marker(item, marker) for item in value)
    model_dump = getattr(value, "model_dump", None)
    if callable(model_dump):
        return _contains_marker(model_dump(mode="json"), marker)
    return False


def _install_no_hosted_provider_guard(monkeypatch: pytest.MonkeyPatch) -> None:
    real_connect = socket.socket.connect

    def guarded_connect(sock: socket.socket, address: Any) -> Any:
        host = address[0] if isinstance(address, tuple) and address else address
        if isinstance(host, bytes):
            host = host.decode()
        if host not in {"127.0.0.1", "::1", "localhost"}:
            raise AssertionError(
                f"Installed ADK smoke must not open network connections: {address!r}"
            )
        return real_connect(sock, address)

    monkeypatch.setattr(socket.socket, "connect", guarded_connect)

    try:
        genai_module = importlib.import_module("google.genai")
    except ModuleNotFoundError:
        return

    if hasattr(genai_module, "Client"):
        monkeypatch.setattr(
            genai_module,
            "Client",
            lambda *args, **kwargs: pytest.fail(
                "Installed ADK smoke must not construct google.genai.Client."
            ),
        )


def test_kitaru_adk_model_preserves_real_base_llm_public_behavior() -> None:
    BaseLlm, _BaseTool, _LlmAgent = _adk_contract_classes()
    KitaruADKModel, _KitaruADKTool = _adapter_wrappers()

    class ContractLlm(BaseLlm):  # type: ignore[misc, valid-type]
        @classmethod
        def supported_models(cls) -> list[str]:
            return ["kitaru-contract-model"]

        async def generate_content_async(
            self,
            llm_request: Any,
            stream: bool = False,
        ) -> AsyncIterator[Any]:
            yield {"request": llm_request, "stream": stream}

    wrapped = KitaruADKModel(ContractLlm(model="kitaru-contract-model"))

    assert wrapped.model == "kitaru-contract-model"
    assert wrapped.supported_models() == ["kitaru-contract-model"]
    signature = inspect.signature(wrapped.generate_content_async)
    assert "llm_request" in signature.parameters
    assert "stream" in signature.parameters

    events = asyncio.run(_collect_model_events(wrapped, {"prompt": "local only"}))
    assert events == [{"request": {"prompt": "local only"}, "stream": False}]


def test_real_llm_agent_accepts_kitaru_model_wrapper() -> None:
    BaseLlm, _BaseTool, LlmAgent = _adk_contract_classes()
    KitaruADKModel, _KitaruADKTool = _adapter_wrappers()

    class ContractLlm(BaseLlm):  # type: ignore[misc, valid-type]
        @classmethod
        def supported_models(cls) -> list[str]:
            return ["kitaru-contract-model"]

        async def generate_content_async(
            self,
            llm_request: Any,
            stream: bool = False,
        ) -> AsyncIterator[Any]:
            yield {"request": llm_request, "stream": stream}

    wrapped = KitaruADKModel(ContractLlm(model="kitaru-contract-model"))

    assert isinstance(wrapped, BaseLlm)
    agent = LlmAgent(name="contract_agent", model=wrapped)
    assert agent.model is wrapped
    assert isinstance(agent.model, BaseLlm)

    events = asyncio.run(_collect_model_events(agent.model, {"prompt": "local only"}))
    assert events == [{"request": {"prompt": "local only"}, "stream": False}]


def test_kitaru_adk_tool_preserves_real_base_tool_public_behavior() -> None:
    _BaseLlm, BaseTool, _LlmAgent = _adk_contract_classes()
    _KitaruADKModel, KitaruADKTool = _adapter_wrappers()

    class ContractTool(BaseTool):  # type: ignore[misc, valid-type]
        async def run_async(self, *, args: dict[str, Any], tool_context: Any) -> Any:
            return {"args": args, "context": type(tool_context).__name__}

    class SyncProcessTool(ContractTool):
        def __init__(self) -> None:
            super().__init__(
                name="contract_tool",
                description="Local contract tool",
                custom_metadata={"contract": True},
            )
            self.processed: list[tuple[Any, Any]] = []

        def process_llm_request(
            self,
            *,
            tool_context: Any,
            llm_request: Any,
        ) -> None:
            self.processed.append((tool_context, llm_request))

    class AsyncProcessTool(ContractTool):
        def __init__(self) -> None:
            super().__init__(
                name="async_contract_tool",
                description="Local async contract tool",
                custom_metadata={"contract": True},
            )
            self.processed: list[tuple[Any, Any]] = []

        async def process_llm_request(
            self,
            *,
            tool_context: Any,
            llm_request: Any,
        ) -> None:
            self.processed.append((tool_context, llm_request))

    sync_tool = SyncProcessTool()
    wrapped = KitaruADKTool(sync_tool)
    context = object()
    request = {"prompt": "local only"}

    assert wrapped.name == "contract_tool"
    assert wrapped.description == "Local contract tool"
    assert wrapped.is_long_running is False
    assert wrapped.custom_metadata == {"contract": True}
    assert asyncio.run(
        wrapped.run_async(args={"query": "cats"}, tool_context=context)
    ) == {"args": {"query": "cats"}, "context": "object"}
    assert (
        asyncio.run(
            wrapped.process_llm_request(tool_context=context, llm_request=request)
        )
        is None
    )
    assert sync_tool.processed == [(context, request)]

    async_tool = AsyncProcessTool()
    wrapped_async = KitaruADKTool(async_tool)
    asyncio.run(
        wrapped_async.process_llm_request(tool_context=context, llm_request=request)
    )
    assert async_tool.processed == [(context, request)]


def test_real_llm_agent_accepts_kitaru_tool_wrapper() -> None:
    BaseLlm, BaseTool, LlmAgent = _adk_contract_classes()
    _KitaruADKModel, KitaruADKTool = _adapter_wrappers()

    class ContractLlm(BaseLlm):  # type: ignore[misc, valid-type]
        @classmethod
        def supported_models(cls) -> list[str]:
            return ["kitaru-contract-model"]

        async def generate_content_async(
            self,
            llm_request: Any,
            stream: bool = False,
        ) -> AsyncIterator[Any]:
            if False:
                yield {"request": llm_request, "stream": stream}

    class ContractTool(BaseTool):  # type: ignore[misc, valid-type]
        async def run_async(self, *, args: dict[str, Any], tool_context: Any) -> Any:
            return {"args": args, "context": type(tool_context).__name__}

    model = ContractLlm(model="kitaru-contract-model")
    tool = ContractTool(name="contract_tool", description="Local contract tool")
    wrapped = KitaruADKTool(tool)

    assert isinstance(wrapped, BaseTool)
    agent = LlmAgent(name="contract_agent", model=model, tools=[wrapped])
    assert agent.tools[0] is wrapped
    assert asyncio.run(
        agent.tools[0].run_async(args={"query": "cats"}, tool_context=object())
    ) == {"args": {"query": "cats"}, "context": "object"}


def test_adk_tool_confirmation_events_become_typed_handoffs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_no_hosted_provider_guard(monkeypatch)

    BaseLlm, _BaseTool, LlmAgent = _adk_contract_classes()
    api = _adk_runtime_api()
    adapter = _adapter_module()
    function_tool_module = _import_adk_module("google.adk.tools.function_tool")

    class ConfirmationLlm(BaseLlm):  # type: ignore[misc, valid-type]
        def __init__(self) -> None:
            super().__init__(model="kitaru-confirmation-model")

        @classmethod
        def supported_models(cls) -> list[str]:
            return ["kitaru-confirmation-model"]

        async def generate_content_async(
            self,
            llm_request: Any,
            stream: bool = False,
        ) -> AsyncIterator[Any]:
            yield _llm_response(
                api,
                _function_call_content(
                    api.genai_types,
                    name="dangerous_lookup",
                    args={"query": "cats"},
                ),
            )

    def dangerous_lookup(query: str) -> dict[str, Any]:
        return {"query": query, "answer": "should-not-run-before-confirmation"}

    agent = LlmAgent(
        name="confirmation_agent",
        model=ConfirmationLlm(),
        tools=[
            function_tool_module.FunctionTool(
                dangerous_lookup,
                require_confirmation=True,
            )
        ],
    )
    result = asyncio.run(
        _run_in_memory_adapter_turn(
            api,
            adapter,
            agent=agent,
            app_name="kitaru_confirmation_app",
            session_id="confirmation-session",
            message=_text_content(api.genai_types, "confirm it", role="user"),
        )
    )

    assert result.status == "requires_action"
    assert len(result.handoffs) == 1
    handoff = result.handoffs[0]
    assert handoff.kind == "tool_confirmation"
    assert handoff.tool_name == "dangerous_lookup"
    assert handoff.tool_args == {"query": "cats"}
    assert handoff.function_call_id is not None
    assert handoff.request_function_call_id is not None
    assert handoff.invocation_id is not None
    assert isinstance(handoff.message, str) and handoff.message
    assert _contains_marker(result.events, "adk_request_confirmation")
    assert _contains_marker(result.events, "requestedToolConfirmations")


def test_adk_tool_confirmation_helper_resumes_real_runner_without_provider(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_no_hosted_provider_guard(monkeypatch)

    BaseLlm, _BaseTool, LlmAgent = _adk_contract_classes()
    api = _adk_runtime_api()
    adapter = _adapter_module()
    function_tool_module = _import_adk_module("google.adk.tools.function_tool")
    tool_calls: list[str] = []

    class ConfirmationLoopLlm(BaseLlm):  # type: ignore[misc, valid-type]
        def __init__(self) -> None:
            super().__init__(model="kitaru-confirmation-loop-model")
            object.__setattr__(self, "calls", [])

        @classmethod
        def supported_models(cls) -> list[str]:
            return ["kitaru-confirmation-loop-model"]

        async def generate_content_async(
            self,
            llm_request: Any,
            stream: bool = False,
        ) -> AsyncIterator[Any]:
            self.calls.append(llm_request)
            for content in getattr(llm_request, "contents", []) or []:
                for part in getattr(content, "parts", []) or []:
                    function_response = getattr(part, "function_response", None)
                    if (
                        function_response is not None
                        and function_response.name == "dangerous_lookup"
                    ):
                        yield _llm_response(
                            api,
                            _text_content(
                                api.genai_types,
                                "final answer after approval",
                                role="model",
                            ),
                        )
                        return

            yield _llm_response(
                api,
                _function_call_content(
                    api.genai_types,
                    name="dangerous_lookup",
                    args={"query": "cats"},
                ),
            )

    def dangerous_lookup(query: str) -> dict[str, Any]:
        tool_calls.append(query)
        return {"query": query, "answer": "ran-after-confirmation"}

    async def run_round_trip() -> tuple[Any, Any, list[str]]:
        app_name = "kitaru_confirmation_resume_app"
        user_id = "local-user"
        session_id = "confirmation-resume-session"
        model = ConfirmationLoopLlm()
        agent = LlmAgent(
            name="confirmation_resume_agent",
            model=model,
            tools=[
                function_tool_module.FunctionTool(
                    dangerous_lookup,
                    require_confirmation=True,
                )
            ],
        )
        runner = _build_in_memory_runner(api, agent=agent, app_name=app_name)
        await _create_runner_session(
            runner,
            app_name=app_name,
            user_id=user_id,
            session_id=session_id,
        )
        kitaru_runner = adapter.KitaruADKRunner(runner, name=app_name)
        first = await kitaru_runner.run(
            adapter.ADKRunRequest(
                user_id=user_id,
                session_id=session_id,
                message=_text_content(api.genai_types, "confirm it", role="user"),
            )
        )
        assert tool_calls == []
        followup = adapter.build_tool_confirmation_request(
            first.handoffs[0],
            confirmed=True,
            user_id=user_id,
            session_id=session_id,
        )
        second = await kitaru_runner.run(followup)
        return first, second, model.calls

    first, second, model_calls = asyncio.run(run_round_trip())

    assert first.status == "requires_action"
    assert len(first.handoffs) == 1
    handoff = first.handoffs[0]
    assert handoff.kind == "tool_confirmation"
    assert handoff.request_function_call_id is not None
    assert tool_calls == ["cats"]
    assert second.status == "completed"
    assert second.handoffs == []
    assert adapter.final_output_preview(second.final_output) == (
        "final answer after approval"
    )
    assert len(model_calls) >= 2


def test_adk_credential_request_events_become_typed_handoffs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_no_hosted_provider_guard(monkeypatch)

    BaseLlm, _BaseTool, LlmAgent = _adk_contract_classes()
    api = _adk_runtime_api()
    adapter = _adapter_module()
    auth_module = _import_adk_module("google.adk.auth")
    function_tool_module = _import_adk_module("google.adk.tools.function_tool")
    tool_context_module = _import_adk_module("google.adk.tools.tool_context")
    fastapi_models = _import_adk_module("fastapi.openapi.models")

    class CredentialLlm(BaseLlm):  # type: ignore[misc, valid-type]
        def __init__(self) -> None:
            super().__init__(model="kitaru-credential-model")

        @classmethod
        def supported_models(cls) -> list[str]:
            return ["kitaru-credential-model"]

        async def generate_content_async(
            self,
            llm_request: Any,
            stream: bool = False,
        ) -> AsyncIterator[Any]:
            yield _llm_response(
                api,
                _function_call_content(
                    api.genai_types,
                    name="needs_api_key",
                    args={"query": "cats"},
                ),
            )

    def needs_api_key(
        query: str,
        tool_context: Any,  # ADK injects this because of the concrete annotation.
    ) -> dict[str, Any]:
        scheme = fastapi_models.APIKey(
            type="apiKey",
            **{"in": fastapi_models.APIKeyIn.header},
            name="X-Kitaru-Test",
        )
        tool_context.request_credential(auth_module.AuthConfig(authScheme=scheme))
        return {"query": query, "status": "credential-requested"}

    needs_api_key.__annotations__["tool_context"] = tool_context_module.ToolContext

    agent = LlmAgent(
        name="credential_agent",
        model=CredentialLlm(),
        tools=[function_tool_module.FunctionTool(needs_api_key)],
    )
    result = asyncio.run(
        _run_in_memory_adapter_turn(
            api,
            adapter,
            agent=agent,
            app_name="kitaru_credential_app",
            session_id="credential-session",
            message=_text_content(api.genai_types, "need auth", role="user"),
        )
    )

    assert result.status == "requires_action"
    assert len(result.handoffs) == 1
    handoff = result.handoffs[0]
    assert handoff.kind == "credential_request"
    assert handoff.tool_name == "needs_api_key"
    assert handoff.function_call_id is not None
    assert handoff.request_function_call_id is not None
    assert handoff.auth_config is not None
    assert handoff.auth_config["authScheme"]["name"] == "X-Kitaru-Test"
    assert _contains_marker(result.events, "adk_request_credential")
    assert _contains_marker(result.events, "requestedAuthConfigs")


def test_serialized_adk_request_input_event_becomes_typed_handoff() -> None:
    _import_adk_module("google.adk")
    adapter = _adapter_module()

    request_event = {
        "id": "request-input-event",
        "invocationId": "request-input-invocation",
        "author": "workflow_agent",
        "content": {
            "parts": [
                {
                    "functionCall": {
                        "id": "request-input-call",
                        "name": "adk_request_input",
                        "args": {
                            "interruptId": "request-input-interrupt",
                            "message": "Choose the next step",
                            "payload": {"question": "continue?"},
                            "responseSchema": {
                                "type": "object",
                                "properties": {"ok": {"type": "boolean"}},
                            },
                        },
                    }
                }
            ]
        },
    }

    class StaticRunner:
        app_name = "request-input-app"

        async def run_async(self, **_kwargs: Any) -> AsyncIterator[Any]:
            yield request_event

    result = asyncio.run(
        adapter.KitaruADKRunner(StaticRunner()).run(
            adapter.ADKRunRequest(
                user_id="local-user",
                session_id="request-input-session",
                message={"text": "start workflow"},
            )
        )
    )

    assert result.status == "requires_action"
    assert len(result.handoffs) == 1
    handoff = result.handoffs[0]
    assert handoff.kind == "human_input"
    assert handoff.event_id == "request-input-event"
    assert handoff.invocation_id == "request-input-invocation"
    assert handoff.author == "workflow_agent"
    assert handoff.function_call_id == "request-input-interrupt"
    assert handoff.request_function_call_id == "request-input-call"
    assert handoff.message == "Choose the next step"
    assert handoff.payload == {"question": "continue?"}
    assert handoff.response_schema == {
        "type": "object",
        "properties": {"ok": {"type": "boolean"}},
    }
    assert _contains_marker(result.events, "adk_request_input")


def test_adk_mcp_toolset_import_contract_documents_v1_limit() -> None:
    _import_adk_module("google.adk.tools.mcp_tool")
    try:
        mcp_toolset_module = importlib.import_module(
            "google.adk.tools.mcp_tool.mcp_toolset"
        )
    except ImportError as exc:
        # google-adk 2.3.x exposes the MCP package path, but the no-dev
        # `kitaru[google-adk]` environment cannot instantiate McpToolset here:
        # without an MCP package the import says `No module named 'mcp'`; with
        # the current standalone `mcp` package it fails on `SamplingCapability`.
        # Kitaru v1 therefore documents ADK-hosted MCP only at the ADK-exposed
        # tool call/result level, not as restored MCP process/session state.
        message = str(exc)
        assert "mcp" in message or "SamplingCapability" in message
        return

    mcp_toolset = getattr(mcp_toolset_module, "McpToolset", None)
    assert mcp_toolset is not None
    signature = inspect.signature(mcp_toolset)
    assert "connection_params" in signature.parameters
    assert any("tool" in name.lower() for name in dir(mcp_toolset))


def test_real_adk_runner_invokes_kitaru_wrapped_local_model_and_tool_without_network(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _install_no_hosted_provider_guard(monkeypatch)

    BaseLlm, BaseTool, LlmAgent = _adk_contract_classes()
    api = _adk_runtime_api()
    adapter = _adapter_module()
    marker = "local-cat-fact"

    class LocalToolLoopLlm(BaseLlm):  # type: ignore[misc, valid-type]
        def __init__(self) -> None:
            super().__init__(model="kitaru-local-runner-smoke-model")
            object.__setattr__(self, "calls", [])
            object.__setattr__(self, "saw_tool_response", False)

        @classmethod
        def supported_models(cls) -> list[str]:
            return ["kitaru-local-runner-smoke-model"]

        async def generate_content_async(
            self,
            llm_request: Any,
            stream: bool = False,
        ) -> AsyncIterator[Any]:
            self.calls.append(llm_request)
            if len(self.calls) == 1:
                yield _llm_response(
                    api,
                    _function_call_content(
                        api.genai_types,
                        name="local_lookup",
                        args={"query": "cats"},
                    ),
                )
                return

            for content in llm_request.contents:
                for part in content.parts or []:
                    function_response = getattr(part, "function_response", None)
                    if function_response is not None:
                        object.__setattr__(self, "saw_tool_response", True)
            yield _llm_response(
                api,
                _text_content(
                    api.genai_types,
                    f"final local answer: {marker}",
                    role="model",
                ),
            )

    class LocalLookupTool(BaseTool):  # type: ignore[misc, valid-type]
        def __init__(self) -> None:
            super().__init__(
                name="local_lookup",
                description="Local lookup used by Kitaru's ADK smoke.",
            )
            self.calls: list[dict[str, Any]] = []

        def _get_declaration(self) -> Any:
            return _local_lookup_declaration(api.genai_types)

        async def run_async(self, *, args: dict[str, Any], tool_context: Any) -> Any:
            self.calls.append(dict(args))
            tool_context.state["kitaru_lookup_marker"] = marker
            return {"query": args.get("query"), "answer": marker}

    async def run_smoke() -> Any:
        app_name = "kitaru_runner_smoke_app"
        user_id = "local-user"
        session_id = "local-session"
        local_model = LocalToolLoopLlm()
        local_tool = LocalLookupTool()
        wrapped_model = adapter.KitaruADKModel(local_model)
        wrapped_tool = adapter.KitaruADKTool(local_tool)
        agent = LlmAgent(
            name="kitaru_runner_smoke_agent",
            model=wrapped_model,
            tools=[wrapped_tool],
        )
        runner = _build_in_memory_runner(api, agent=agent, app_name=app_name)
        await _create_runner_session(
            runner,
            app_name=app_name,
            user_id=user_id,
            session_id=session_id,
        )
        kitaru_runner = adapter.KitaruADKRunner(runner, name=app_name)
        result = await kitaru_runner.run(
            adapter.ADKRunRequest(
                user_id=user_id,
                session_id=session_id,
                message=_text_content(
                    api.genai_types,
                    "please look up cats locally",
                    role="user",
                ),
            )
        )
        session = await _get_runner_session(
            runner,
            app_name=app_name,
            user_id=user_id,
            session_id=session_id,
        )
        return result, local_model, local_tool, session

    result, local_model, local_tool, session = asyncio.run(run_smoke())

    assert result.status == "completed"
    assert len(local_model.calls) >= 2
    assert local_tool.calls == [{"query": "cats"}]
    assert _session_state(session)["kitaru_lookup_marker"] == marker
    assert local_model.saw_tool_response is True
    assert result.events
    assert _contains_marker(result.events, marker)
    assert type(result.final_output).__module__ == "google.genai.types"
    assert type(result.final_output).__qualname__ == "Content"
    assert adapter.final_output_preview(result.final_output) == (
        f"final local answer: {marker}"
    )
