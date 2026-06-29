"""Google ADK + Kitaru adapter example.

Story:
- The default local mode uses a real installed Google ADK InMemoryRunner, a
  local dummy ADK model, and a local dummy ADK tool. No hosted provider or
  network call is made.
- `KitaruADKRunner(checkpoint_strategy="runner_call")` wraps the whole ADK
  runner turn.
- `KitaruADKModel` and `KitaruADKTool` wrap the concrete ADK model/tool objects,
  which is the path Kitaru uses for model/tool checkpoints when this code runs
  from inside a Kitaru flow.
- `--mode live` swaps in ADK's Gemini model path and keeps the same runner-level
  wrapper. It accepts either Gemini Developer API key credentials or a
  Vertex AI / Application Default Credentials environment. Kitaru only checks
  the environment shape; ADK and Google GenAI perform the real authentication.

Run the deterministic local mode from an isolated no-dev ADK environment:
    UV_PROJECT_ENVIRONMENT=.venv-google-adk \
      uv run --python 3.12 --no-dev --extra google-adk \
      python examples/integrations/google_adk_agent/google_adk_adapter.py

Run the optional live model mode with an API key:
    export GEMINI_API_KEY=<your-gemini-api-key>
    # GOOGLE_API_KEY=<your-google-api-key> also works.
    UV_PROJECT_ENVIRONMENT=.venv-google-adk \
      uv run --python 3.12 --no-dev --extra google-adk \
      python examples/integrations/google_adk_agent/google_adk_adapter.py --mode live

Run the optional live model mode with Vertex AI / ADC:
    export GOOGLE_GENAI_USE_VERTEXAI=true
    export GOOGLE_CLOUD_PROJECT=<your-gcp-project-id>
    export GOOGLE_CLOUD_LOCATION=<your-region>
    # Authenticate outside Kitaru, for example with:
    #   gcloud auth application-default login
    UV_PROJECT_ENVIRONMENT=.venv-google-adk \
      uv run --python 3.12 --no-dev --extra google-adk \
      python examples/integrations/google_adk_agent/google_adk_adapter.py --mode live
"""

import argparse
import asyncio
import importlib
import inspect
import json
import sys
from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass
from typing import Any, Literal

from kitaru._google_auth_env import (
    CLOUD_LOCATION_ENV,
    CLOUD_PROJECT_ENV,
    GEMINI_API_KEY_ENV,
    GOOGLE_API_KEY_ENV,
    VERTEXAI_ENV,
    require_google_live_auth_env,
)

DEFAULT_APP_NAME = "kitaru_google_adk_example"
DEFAULT_USER_ID = "local-user"
DEFAULT_LOCAL_SESSION_ID = "local-session"
DEFAULT_LIVE_SESSION_ID = "live-session"
DEFAULT_LOCAL_PROMPT = "Please look up one local cat fact."
DEFAULT_LIVE_PROMPT = (
    "Explain one Kitaru checkpoint in one short sentence. Do not use tools."
)
DEFAULT_LIVE_MODEL = "gemini-flash-latest"
LOCAL_MARKER = "local-cat-fact"
Mode = Literal["local", "live"]


@dataclass(frozen=True)
class ADKRuntimeAPI:
    """Small bundle of Google ADK classes this example needs."""

    BaseLlm: type[Any]
    BaseTool: type[Any]
    LlmAgent: type[Any]
    Runner: type[Any] | None
    InMemoryRunner: type[Any] | None
    InMemorySessionService: type[Any] | None
    LlmResponse: type[Any]
    genai_types: Any


def _import_adk_module(module_path: str) -> Any:
    try:
        return importlib.import_module(module_path)
    except ModuleNotFoundError as exc:
        missing_requested_module = exc.name is not None and (
            module_path == exc.name or module_path.startswith(f"{exc.name}.")
        )
        if not missing_requested_module:
            raise
        raise SystemExit(
            "This example needs the optional `google-adk` extra. Run it in an "
            "isolated no-dev environment, for example:\n"
            "  UV_PROJECT_ENVIRONMENT=.venv-google-adk \\\n"
            "    uv run --python 3.12 --no-dev --extra google-adk \\\n"
            "    python examples/integrations/google_adk_agent/"
            "google_adk_adapter.py\n\n"
            "Do not combine `--extra google-adk` with `--extra local` while the "
            "current FastAPI dependency conflict remains."
        ) from exc


def _load_adk_api() -> ADKRuntimeAPI:
    _import_adk_module("google.adk")
    base_llm_module = _import_adk_module("google.adk.models.base_llm")
    base_tool_module = _import_adk_module("google.adk.tools.base_tool")
    runners_module = _import_adk_module("google.adk.runners")
    sessions_module = _import_adk_module("google.adk.sessions")
    llm_response_module = _import_adk_module("google.adk.models.llm_response")
    genai_types = _import_adk_module("google.genai.types")
    try:
        agents_module = _import_adk_module("google.adk.agents")
        llm_agent = agents_module.LlmAgent
    except (AttributeError, ImportError):
        llm_agent_module = _import_adk_module("google.adk.agents.llm_agent")
        llm_agent = llm_agent_module.LlmAgent

    return ADKRuntimeAPI(
        BaseLlm=base_llm_module.BaseLlm,
        BaseTool=base_tool_module.BaseTool,
        LlmAgent=llm_agent,
        Runner=getattr(runners_module, "Runner", None),
        InMemoryRunner=getattr(runners_module, "InMemoryRunner", None),
        InMemorySessionService=getattr(
            sessions_module,
            "InMemorySessionService",
            None,
        ),
        LlmResponse=llm_response_module.LlmResponse,
        genai_types=genai_types,
    )


def _load_kitaru_adk_adapter() -> Any:
    # Unit tests install fake google.adk modules, then import the adapter against
    # those fake base classes. The runnable examples need a fresh adapter import
    # after `_load_adk_api()` has loaded the real installed ADK classes.
    for cached in list(sys.modules):
        if cached.startswith("kitaru.adapters.google_adk"):
            del sys.modules[cached]
    return importlib.import_module("kitaru.adapters.google_adk")


def _text_content(api: ADKRuntimeAPI, text: str, *, role: str) -> Any:
    part_cls = api.genai_types.Part
    if hasattr(part_cls, "from_text"):
        part = part_cls.from_text(text=text)
    else:
        part = part_cls(text=text)
    return api.genai_types.Content(role=role, parts=[part])


def _function_call_content(
    api: ADKRuntimeAPI,
    *,
    name: str,
    args: dict[str, Any],
) -> Any:
    part_cls = api.genai_types.Part
    if hasattr(part_cls, "from_function_call"):
        part = part_cls.from_function_call(name=name, args=args)
    else:
        function_call = api.genai_types.FunctionCall(name=name, args=args)
        part = part_cls(function_call=function_call)
    return api.genai_types.Content(role="model", parts=[part])


def _local_lookup_declaration(api: ADKRuntimeAPI) -> Any:
    return api.genai_types.FunctionDeclaration(
        name="local_lookup",
        description="Return a deterministic local lookup result for the ADK example.",
        parameters=api.genai_types.Schema(
            type=api.genai_types.Type.OBJECT,
            properties={
                "query": api.genai_types.Schema(type=api.genai_types.Type.STRING)
            },
            required=["query"],
        ),
    )


def _build_in_memory_runner(
    api: ADKRuntimeAPI,
    *,
    agent: Any,
    app_name: str,
) -> Any:
    if api.InMemoryRunner is not None:
        return api.InMemoryRunner(agent=agent, app_name=app_name)

    if api.Runner is None or api.InMemorySessionService is None:
        raise RuntimeError(
            "Installed google-adk exposes neither InMemoryRunner nor "
            "Runner + InMemorySessionService."
        )

    return api.Runner(
        app_name=app_name,
        agent=agent,
        session_service=api.InMemorySessionService(),
    )


async def _create_runner_session(
    runner: Any,
    *,
    app_name: str,
    user_id: str,
    session_id: str,
) -> None:
    session_service = getattr(runner, "session_service", None)
    if session_service is None:
        raise RuntimeError("Installed ADK runner does not expose `.session_service`.")
    create_session = getattr(session_service, "create_session", None)
    if not callable(create_session):
        raise RuntimeError(
            "Installed ADK session service has no `create_session(...)`."
        )

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

    created = create_session(**kwargs)
    if inspect.isawaitable(created):
        await created


def _build_local_agent(api: ADKRuntimeAPI, adapter: Any, *, query: str) -> Any:
    class LocalToolLoopLlm(api.BaseLlm):  # type: ignore[misc, valid-type]
        def __init__(self) -> None:
            super().__init__(model="kitaru-local-adk-example-model")
            object.__setattr__(self, "calls", [])
            object.__setattr__(self, "query", query)

        @classmethod
        def supported_models(cls) -> list[str]:
            return ["kitaru-local-adk-example-model"]

        async def generate_content_async(
            self,
            llm_request: Any,
            stream: bool = False,
        ) -> AsyncIterator[Any]:
            self.calls.append(llm_request)
            if len(self.calls) == 1:
                yield api.LlmResponse(
                    content=_function_call_content(
                        api,
                        name="local_lookup",
                        args={"query": self.query},
                    )
                )
                return

            yield api.LlmResponse(
                content=_text_content(
                    api,
                    f"final local answer: {LOCAL_MARKER} for {self.query}",
                    role="model",
                )
            )

    class LocalLookupTool(api.BaseTool):  # type: ignore[misc, valid-type]
        def __init__(self) -> None:
            super().__init__(
                name="local_lookup",
                description=(
                    "Deterministic local lookup used by the Kitaru ADK example."
                ),
            )
            self.calls: list[dict[str, Any]] = []

        def _get_declaration(self) -> Any:
            return _local_lookup_declaration(api)

        async def run_async(self, *, args: dict[str, Any], tool_context: Any) -> Any:
            self.calls.append(dict(args))
            tool_context.state["kitaru_lookup_marker"] = LOCAL_MARKER
            return {"query": args.get("query"), "answer": LOCAL_MARKER}

    wrapped_model = adapter.KitaruADKModel(LocalToolLoopLlm())
    wrapped_tool = adapter.KitaruADKTool(LocalLookupTool())
    return api.LlmAgent(
        name="kitaru_local_adk_agent",
        model=wrapped_model,
        tools=[wrapped_tool],
    )


def _build_live_agent(api: ADKRuntimeAPI, *, model: str) -> Any:
    return api.LlmAgent(
        name="kitaru_live_adk_agent",
        model=model,
        instruction="Answer briefly. Do not call tools.",
        tools=[],
    )


def _incomplete_vertex_message(missing: tuple[str, ...]) -> str:
    return (
        f"{VERTEXAI_ENV} is enabled (Vertex AI / ADC mode), so no API key "
        "is required when Vertex config is complete, but "
        f"{' and '.join(missing)} must also be set:\n"
        f"  export {CLOUD_PROJECT_ENV}='<your-gcp-project-id>'\n"
        f"  export {CLOUD_LOCATION_ENV}='<your-region>'\n"
        "ADK / Google GenAI will authenticate the provider call."
    )


def _missing_credentials_message() -> str:
    return (
        "Missing Google/Gemini credentials for --mode live.\n"
        "Pick one authentication path before a real ADK run.\n"
        "API key (Gemini Developer API):\n"
        f"  export {GEMINI_API_KEY_ENV}='<your-gemini-api-key>'\n"
        f"  export {GOOGLE_API_KEY_ENV}='<your-google-api-key>'   # alternative name\n"
        "Application Default Credentials (Vertex AI, no API key):\n"
        f"  export {VERTEXAI_ENV}=true\n"
        f"  export {CLOUD_PROJECT_ENV}='<your-gcp-project-id>'\n"
        f"  export {CLOUD_LOCATION_ENV}='<your-region>'\n"
        "  gcloud auth application-default login\n"
        "Kitaru only checks this environment shape; ADK / Google GenAI "
        "authenticates the provider call."
    )


def prepare_live_google_credentials() -> None:
    """Validate the live ADK Google auth environment without authenticating."""
    require_google_live_auth_env(
        alias_direction="gemini_to_google",
        incomplete_vertex_message=_incomplete_vertex_message,
        missing_credentials_message=_missing_credentials_message(),
    )


async def _run_adk_turn(args: argparse.Namespace) -> Any:
    api = _load_adk_api()
    adapter = _load_kitaru_adk_adapter()
    app_name = str(args.app_name)
    user_id = str(args.user_id)
    session_id = str(args.session_id)

    if args.mode == "live":
        prepare_live_google_credentials()
        agent = _build_live_agent(api, model=str(args.model))
        message = _text_content(api, str(args.prompt), role="user")
    else:
        agent = _build_local_agent(api, adapter, query=str(args.query))
        message = _text_content(api, str(args.prompt), role="user")

    runner = _build_in_memory_runner(api, agent=agent, app_name=app_name)
    await _create_runner_session(
        runner,
        app_name=app_name,
        user_id=user_id,
        session_id=session_id,
    )

    kitaru_runner = adapter.KitaruADKRunner(
        runner,
        name=app_name,
        checkpoint_strategy="runner_call",
    )
    request = adapter.ADKRunRequest(
        user_id=user_id,
        session_id=session_id,
        message=message,
        metadata={"example": "google_adk_agent", "mode": args.mode},
    )
    return await kitaru_runner.run(request)


def _json_block(value: Any) -> str:
    if value is None:
        return "(none)"
    if isinstance(value, Mapping):
        return json.dumps(value, indent=2, sort_keys=True, default=str)
    model_dump = getattr(value, "model_dump", None)
    if callable(model_dump):
        return json.dumps(
            model_dump(mode="python"),
            indent=2,
            sort_keys=True,
            default=str,
        )
    return json.dumps(value, indent=2, sort_keys=True, default=str)


def _print_result(result: Any, *, mode: Mode) -> None:
    adapter = _load_kitaru_adk_adapter()
    preview = adapter.final_output_preview(result.final_output)

    print("\n=== What happened ===")
    if mode == "local":
        print(
            "A real Google ADK runner used a local dummy model and tool. "
            "This direct script shows the Kitaru ADK wrappers around the "
            "runner/model/tool objects. Put the same calls inside a Kitaru "
            "flow to persist checkpoints for replay."
        )
    else:
        print(
            "A real Google ADK runner used Gemini through ADK. This direct "
            "script shows the Kitaru runner wrapper; put the same call inside "
            "a Kitaru flow to persist the runner_call checkpoint."
        )

    print("\n=== ADK result ===")
    print("Checkpoint strategy: runner_call")
    print(f"Status: {result.status}")
    print(f"Final output preview: {preview or '(empty final output)'}")
    print(f"Event count: {len(result.events)}")
    print(f"Handoff count: {len(result.handoffs)}")

    if result.handoffs:
        print("\n=== Handoffs ===")
        for handoff in result.handoffs:
            print(_json_block(handoff))

    if result.usage is not None:
        print("\n=== Usage ===")
        print(_json_block(result.usage))

    if result.warnings:
        print("\n=== Warnings ===")
        for warning in result.warnings:
            print(f"- {warning}")


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run a Google ADK agent through Kitaru's experimental ADK adapter."
        )
    )
    parser.add_argument(
        "--mode",
        choices=("local", "live"),
        default="local",
        help=(
            "Use a deterministic local ADK model/tool loop, or call Gemini through "
            "ADK. Defaults to local."
        ),
    )
    parser.add_argument(
        "--prompt",
        default=None,
        help="User message. Defaults to a local or live prompt depending on --mode.",
    )
    parser.add_argument(
        "--query",
        default="cats",
        help="Local lookup query used by --mode local. Defaults to cats.",
    )
    parser.add_argument(
        "--model",
        default=DEFAULT_LIVE_MODEL,
        help=f"Gemini model for --mode live. Defaults to {DEFAULT_LIVE_MODEL}.",
    )
    parser.add_argument(
        "--app-name",
        default=DEFAULT_APP_NAME,
        help=f"ADK app name. Defaults to {DEFAULT_APP_NAME}.",
    )
    parser.add_argument(
        "--user-id",
        default=DEFAULT_USER_ID,
        help=f"Stable ADK user id. Defaults to {DEFAULT_USER_ID}.",
    )
    parser.add_argument(
        "--session-id",
        default=None,
        help="Stable ADK session id. Defaults depend on --mode.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)
    if args.prompt is None:
        args.prompt = (
            DEFAULT_LIVE_PROMPT if args.mode == "live" else DEFAULT_LOCAL_PROMPT
        )
    if args.session_id is None:
        args.session_id = (
            DEFAULT_LIVE_SESSION_ID if args.mode == "live" else DEFAULT_LOCAL_SESSION_ID
        )

    result = asyncio.run(_run_adk_turn(args))
    _print_result(result, mode=args.mode)


if __name__ == "__main__":
    main()
