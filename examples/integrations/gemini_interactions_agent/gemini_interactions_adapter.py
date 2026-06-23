"""Real Gemini Interactions API + Kitaru adapter example.

Story:
- A Kitaru flow sends one Gemini Interactions API request.
- `KitaruGeminiInteractionsRunner` wraps that stable interaction response in one
  Kitaru checkpoint for normal model and Antigravity modes.
- `--mode sandbox-function` uses three visible checkpoints: ask Gemini for a
  function call, run the sandbox command, then send the function result back.
- `--dry-run` prints the same kind of result summary without credentials or a
  network call, so smoke tests can exercise the example safely.
- `--stream` uses direct model streaming for model mode and create-once,
  observe-same-id background streaming for Antigravity mode, then shows the same
  final stable result after the stream finishes.

Run (API key):
    uv sync --extra local --extra gemini
    uv run kitaru init
    export GEMINI_API_KEY=<your-gemini-api-key>
    uv run python \
        examples/integrations/gemini_interactions_agent/gemini_interactions_adapter.py

Run (Application Default Credentials via Vertex AI, no API key):
    gcloud auth application-default login
    export GOOGLE_GENAI_USE_VERTEXAI=true
    export GOOGLE_CLOUD_PROJECT=<your-gcp-project-id>
    export GOOGLE_CLOUD_LOCATION=global   # Vertex serves the agent backend here
    uv run python \
        examples/integrations/gemini_interactions_agent/gemini_interactions_adapter.py \
        --mode antigravity   # Vertex supports agent interactions, not raw models
"""

import argparse
import json
import os
import sys
import threading
from typing import Any, Literal

from kitaru import checkpoint, flow
from kitaru.adapters._result_identity import canonicalize_result_model
from kitaru.adapters.gemini import (
    GEMINI_STREAM_EVENT_KINDS,
    GEMINI_STREAM_TERMINAL_EVENT_KINDS,
    GeminiInteractionCapturePolicy,
    GeminiInteractionRequest,
    GeminiInteractionResult,
    GeminiInteractionStepSummary,
    GeminiSandboxFunctionExecution,
    GeminiSandboxFunctionSpec,
    KitaruGeminiInteractionsRunner,
    execute_gemini_sandbox_function_call,
)
from kitaru.client import KitaruClient
from kitaru.config import SandboxCommandResult
from kitaru.errors import KitaruBackendError, KitaruFeatureNotAvailableError

GOOGLE_API_KEY_ENV = "GOOGLE_API_KEY"
GEMINI_API_KEY_ENV = "GEMINI_API_KEY"
VERTEXAI_ENV = "GOOGLE_GENAI_USE_VERTEXAI"
CLOUD_PROJECT_ENV = "GOOGLE_CLOUD_PROJECT"
CLOUD_LOCATION_ENV = "GOOGLE_CLOUD_LOCATION"
# The google-genai SDK treats these (case-insensitively) as "use Vertex AI".
_TRUTHY_VALUES = {"1", "true", "yes", "on"}
DEFAULT_MODEL = "gemini-3.5-flash"
DEFAULT_MODEL_PROMPT = (
    "In three plain sentences, explain why checkpointing one AI interaction is "
    "useful in a long-running workflow."
)
DEFAULT_ANTIGRAVITY_PROMPT = (
    "Inspect the task at a high level and explain what you would check first. "
    "Do not edit files or run destructive commands."
)
SANDBOX_FUNCTION_NAME = "sandbox_python_version"
DEFAULT_SANDBOX_FUNCTION_PROMPT = (
    "Call the sandbox_python_version function to learn the Python version in "
    "Kitaru's active sandbox, then explain the result in one sentence."
)
SANDBOX_FUNCTION_TOOL = {
    "type": "function",
    "name": SANDBOX_FUNCTION_NAME,
    "description": "Return the Python version from Kitaru's active sandbox.",
    "parameters": {
        "type": "object",
        "properties": {},
        "additionalProperties": False,
    },
}
Mode = Literal["model", "antigravity", "sandbox-function"]
DEFAULT_PROMPTS_BY_MODE: dict[Mode, str] = {
    "model": DEFAULT_MODEL_PROMPT,
    "antigravity": DEFAULT_ANTIGRAVITY_PROMPT,
    "sandbox-function": DEFAULT_SANDBOX_FUNCTION_PROMPT,
}


def _vertex_mode_enabled() -> bool:
    """Vertex AI mode authenticates with ADC instead of an API key."""
    return os.getenv(VERTEXAI_ENV, "").strip().lower() in _TRUTHY_VALUES


def _require_vertex_settings() -> None:
    """Vertex AI supplies credentials via ADC, but still needs project + region."""
    missing = [
        name for name in (CLOUD_PROJECT_ENV, CLOUD_LOCATION_ENV) if not os.getenv(name)
    ]
    if not missing:
        return
    raise SystemExit(
        f"{VERTEXAI_ENV} is enabled (Vertex AI mode), so no API key is needed, "
        f"but {' and '.join(missing)} must also be set:\n"
        f"  export {CLOUD_PROJECT_ENV}='<your-gcp-project-id>'\n"
        f"  export {CLOUD_LOCATION_ENV}='<your-region>'   # e.g. europe-north1\n"
        "Authenticate once with: gcloud auth application-default login"
    )


def _prepare_google_credentials() -> None:
    """Confirm the SDK can authenticate, via Vertex AI ADC or an API key."""
    if _vertex_mode_enabled():
        _require_vertex_settings()
        return
    if os.getenv(GEMINI_API_KEY_ENV):
        return
    google_api_key = os.getenv(GOOGLE_API_KEY_ENV)
    if google_api_key:
        os.environ[GEMINI_API_KEY_ENV] = google_api_key
        return
    raise SystemExit(
        "Missing Google/Gemini credentials.\n"
        "Pick one authentication path before a real run.\n"
        "API key (Gemini Developer API):\n"
        "  export GEMINI_API_KEY='<your-gemini-api-key>'\n"
        "  export GOOGLE_API_KEY='<your-google-api-key>'   # alternative name\n"
        "Application Default Credentials (Vertex AI, no API key):\n"
        f"  export {VERTEXAI_ENV}=true\n"
        f"  export {CLOUD_PROJECT_ENV}='<your-gcp-project-id>'\n"
        f"  export {CLOUD_LOCATION_ENV}='<your-region>'\n"
        "  gcloud auth application-default login\n"
        "Use --dry-run to preview the example without credentials or network."
    )


def _build_runner(
    *,
    include_stream_text_deltas: bool = False,
) -> KitaruGeminiInteractionsRunner:
    return KitaruGeminiInteractionsRunner(
        name="gemini_interactions_example",
        capture=GeminiInteractionCapturePolicy(
            include_stream_text_deltas=include_stream_text_deltas
        ),
        checkpoint_config={"cache": False},
        allow_direct_execution_inside_checkpoint=True,
    )


RUNNER = _build_runner()


def _runner_for_options(*, show_text_deltas: bool) -> KitaruGeminiInteractionsRunner:
    if show_text_deltas:
        return _build_runner(include_stream_text_deltas=True)
    return RUNNER


@flow
def run_gemini_interaction(
    request: GeminiInteractionRequest,
    *,
    stream: bool = False,
    show_text_deltas: bool = False,
) -> GeminiInteractionResult:
    """Run one Gemini interaction as one Kitaru checkpoint."""
    runner = _runner_for_options(show_text_deltas=show_text_deltas)
    if stream:
        return runner.run_stream_sync(request)
    return runner.run_sync(request)


def _build_request(args: argparse.Namespace) -> GeminiInteractionRequest:
    prompt = str(args.prompt)
    if args.mode == "model":
        return GeminiInteractionRequest.start(
            prompt,
            model=str(args.model),
            metadata={"example": "gemini_interactions_agent", "mode": "model"},
        )
    if args.mode == "sandbox-function":
        return GeminiInteractionRequest.start(
            prompt,
            model=str(args.model),
            tools=[SANDBOX_FUNCTION_TOOL],
            metadata={
                "example": "gemini_interactions_agent",
                "mode": "sandbox-function",
            },
        )
    # Antigravity defaults to background mode because the Vertex/Chiliagon
    # managed-agent path can require it. Keep --foreground-antigravity as a
    # preview-backend escape hatch if a specific endpoint rejects background mode.
    return GeminiInteractionRequest.antigravity(
        prompt,
        background=not args.foreground_antigravity,
        timeout_s=float(args.timeout),
        metadata={
            "example": "gemini_interactions_agent",
            "mode": "antigravity",
        },
    )


def _guard_vertex_mode(mode: Mode) -> None:
    """Fail fast on combinations the Vertex AI Interactions API cannot serve."""
    if not _vertex_mode_enabled():
        return
    if mode in {"model", "sandbox-function"}:
        raise SystemExit(
            f"{VERTEXAI_ENV}=true (Vertex AI mode), but the Vertex Interactions "
            "API does not serve raw model interactions yet: every model returns "
            "'Unsupported model interaction'.\n"
            "Use an agent instead, or switch to an API key for model or "
            "sandbox-function mode:\n"
            f"  --mode antigravity   (and export {CLOUD_LOCATION_ENV}=global)\n"
            f"  unset {VERTEXAI_ENV}; "
            f"export {GEMINI_API_KEY_ENV}='<key>'  # model/sandbox-function"
        )
    location = os.getenv(CLOUD_LOCATION_ENV, "")
    if location and location != "global":
        print(
            f"Note: {CLOUD_LOCATION_ENV}={location!r}. The Vertex Interactions agent "
            f"backend is currently only available in 'global'. If this run fails, "
            f"export {CLOUD_LOCATION_ENV}=global and retry.",
            file=sys.stderr,
        )


def _sandbox_python_version_spec() -> GeminiSandboxFunctionSpec:
    return GeminiSandboxFunctionSpec(
        function_name=SANDBOX_FUNCTION_NAME,
        command="python --version",
    )


@checkpoint
def request_sandbox_python_version_function_call(
    request: GeminiInteractionRequest,
    *,
    stream: bool = False,
    show_text_deltas: bool = False,
) -> GeminiInteractionResult:
    """Ask Gemini to call the showcased sandbox function."""
    runner = _runner_for_options(show_text_deltas=show_text_deltas)
    return runner.run_stream_sync(request) if stream else runner.run_sync(request)


@checkpoint
def run_sandbox_python_version_function(
    result: GeminiInteractionResult,
) -> GeminiSandboxFunctionExecution:
    """Execute the showcased Gemini custom function in Kitaru's sandbox."""
    if result.status != "requires_action":
        raise RuntimeError(
            "The sandbox-function showcase expected Gemini to request "
            "sandbox_python_version, but Gemini returned "
            f"status={result.status!r}. Try the default prompt or ask explicitly "
            "for that function call."
        )
    return execute_gemini_sandbox_function_call(
        result,
        {SANDBOX_FUNCTION_NAME: _sandbox_python_version_spec()},
        tools=[SANDBOX_FUNCTION_TOOL],
        metadata={**result.metadata, "mode": "sandbox-function"},
    )


@checkpoint
def finish_sandbox_python_version_function(
    execution: GeminiSandboxFunctionExecution,
    *,
    stream: bool = False,
    show_text_deltas: bool = False,
) -> GeminiInteractionResult:
    """Send the sandbox function result back to Gemini."""
    runner = _runner_for_options(show_text_deltas=show_text_deltas)
    request = execution.function_result_request
    return runner.run_stream_sync(request) if stream else runner.run_sync(request)


@flow
def run_gemini_sandbox_function_showcase(
    request: GeminiInteractionRequest,
    *,
    stream: bool = False,
    show_text_deltas: bool = False,
) -> GeminiInteractionResult:
    """Run Gemini, answer one sandbox function call, then resume Gemini."""
    first_result = request_sandbox_python_version_function_call(
        request,
        stream=stream,
        show_text_deltas=show_text_deltas,
    )
    execution = run_sandbox_python_version_function(first_result)
    return finish_sandbox_python_version_function(
        execution,
        stream=stream,
        show_text_deltas=show_text_deltas,
    )


def _fake_result(
    mode: Mode,
    model: str,
    *,
    stream: bool = False,
) -> GeminiInteractionResult:
    target = model if mode == "model" else "antigravity-preview-05-2026"
    return GeminiInteractionResult(
        status="completed",
        interaction_id="dry-run-interaction-id",
        previous_interaction_id=None,
        output_text=(
            "Dry run only: no Google request was made. In a real run, this field "
            "would contain Gemini's stable interaction output."
        ),
        model=model if mode == "model" else None,
        agent=target if mode == "antigravity" else None,
        environment_id="dry-run-environment" if mode == "antigravity" else None,
        steps=[
            GeminiInteractionStepSummary(
                index=0,
                step_id="dry-run-user-input",
                type="user_input",
                status="completed",
                text_preview=None,
                raw_keys=["type", "text"],
            ),
            GeminiInteractionStepSummary(
                index=1,
                step_id="dry-run-model-output",
                type="model_output",
                status="completed",
                text_preview="Dry-run output preview",
                raw_keys=["type", "text"],
            ),
        ],
        usage={"dry_run": True},
        duration_ms=0.0,
        poll_count=0,
        sdk_version="dry-run",
        input_artifact_name=None,
        request_manifest_artifact_name="gemini_request_manifest_dry_run",
        raw_interaction_artifact_name=None,
        steps_artifact_name=None,
        output_artifact_name="gemini_output_dry_run",
        usage_artifact_name="gemini_usage_dry_run",
        event_log_artifact_name="gemini_events_dry_run",
        run_summary_artifact_name="gemini_run_summary_dry_run",
        warnings=["Dry run: no credentials, network call, or Kitaru flow execution."],
        metadata={
            "example": "gemini_interactions_agent",
            "mode": mode,
            "surface": "run_stream_sync" if stream else "run_sync",
            **(
                {
                    "stream": {
                        "event_count": 0,
                        "counts_by_event_type": {},
                        "last_event_id": None,
                        "final_status": "completed",
                        "reconstruction": "stream_accumulator_v1",
                    }
                }
                if stream
                else {}
            ),
        },
    )


def _fake_sandbox_requires_action_result(model: str) -> GeminiInteractionResult:
    return GeminiInteractionResult(
        status="requires_action",
        interaction_id="dry-run-function-interaction-id",
        model=model,
        steps=[
            GeminiInteractionStepSummary(
                index=0,
                step_id="dry-run-function-call",
                type="function_call",
                status="requires_action",
                call_id="dry-run-call-id",
                tool_name=SANDBOX_FUNCTION_NAME,
                raw_keys=["type", "id", "name"],
            )
        ],
        usage={"dry_run": True},
        duration_ms=0.0,
        sdk_version="dry-run",
        warnings=[
            "Dry run: no Google request, sandbox command, or Kitaru flow execution."
        ],
        metadata={
            "example": "gemini_interactions_agent",
            "mode": "sandbox-function",
        },
    )


def _fake_sandbox_command_result() -> SandboxCommandResult:
    return SandboxCommandResult(
        command="python --version",
        cwd=None,
        stdout="Python 3.12.0\n",
        stderr="",
        exit_code=0,
        stdout_truncated=False,
        stderr_truncated=False,
        stack_id="dry-run-stack-id",
        stack_name="dry-run-stack",
        sandbox_id="dry-run-sandbox-id",
        sandbox_name="dry-run-sandbox",
        session_id="dry-run-session-id",
        cleanup="destroy",
        cleanup_succeeded=True,
        cleanup_error=None,
    )


def _fake_sandbox_function_result_request(
    result: GeminiInteractionResult,
) -> GeminiInteractionRequest:
    call = result.function_calls[0]
    payload = _sandbox_python_version_spec().build_payload(
        call,
        _fake_sandbox_command_result(),
    )
    return GeminiInteractionRequest.function_result(
        previous_interaction_id=result.interaction_id
        or "dry-run-function-interaction-id",
        function_call_id=call.call_id,
        function_name=call.function_name,
        function_result=payload,
        model=result.model,
        metadata={
            "example": "gemini_interactions_agent",
            "mode": "sandbox-function",
            "dry_run": True,
        },
    )


def _print_sandbox_function_dry_run(model: str) -> None:
    result = _fake_sandbox_requires_action_result(model)
    request = _fake_sandbox_function_result_request(result)

    print("\n=== Sandbox function dry run ===")
    print("No Google request, sandbox command, or Kitaru flow execution was run.")
    print("This previews the caller-owned custom function sequence.")
    print("In a real flow, run the sandbox command from a @checkpoint.")

    print("\n1. Fake Gemini requires_action result")
    _print_result(result, mode="sandbox-function")

    print("\n2. Fake Kitaru sandbox command result")
    print(
        _json_block(
            {
                "registered_function": SANDBOX_FUNCTION_NAME,
                "sandbox_command": "python --version",
                "result_payload_sent_to_gemini": request.function_result_payload,
            }
        )
    )

    print("\n3. Fake Gemini function_result request")
    print(_json_block(request.model_dump(by_alias=True)))


def _coerce_result(value: Any) -> GeminiInteractionResult:
    return canonicalize_result_model(value, GeminiInteractionResult)


def _json_block(value: Any) -> str:
    if value is None:
        return "(not reported by SDK)"
    return json.dumps(value, indent=2, sort_keys=True, default=str)


def _event_data(payload: dict[str, Any]) -> dict[str, Any]:
    data = payload.get("data")
    return data if isinstance(data, dict) else {}


def _stream_event_display_lines(kind: str, payload: dict[str, Any]) -> list[str]:
    data = _event_data(payload)
    display = data.get("display") or kind
    category = data.get("category")
    text_delta = data.get("text_delta")
    if (
        isinstance(text_delta, str)
        and text_delta
        and (category == "text_delta" or display == text_delta)
    ):
        display = "Gemini text delta"

    prefix = f"[{category}] " if isinstance(category, str) else ""
    lines = [f"- {prefix}{display}"]
    if isinstance(text_delta, str) and text_delta:
        lines.append(f"  text_delta: {text_delta}")
    return lines


def _watch_gemini_stream(exec_id: str, stop_event: threading.Event) -> None:
    print("\n=== live Gemini stream events ===")
    try:
        for event in KitaruClient().executions.events(
            exec_id,
            kinds=list(GEMINI_STREAM_EVENT_KINDS),
        ):
            if stop_event.is_set():
                return
            for line in _stream_event_display_lines(event.kind, event.payload):
                print(line)
            if event.kind in GEMINI_STREAM_TERMINAL_EVENT_KINDS:
                return
    except (KitaruBackendError, KitaruFeatureNotAvailableError) as error:
        print("\nLive event watching is unavailable on this backend.")
        print(f"The durable result will still be read with .wait(): {error}")


def _print_result(
    result: GeminiInteractionResult,
    *,
    mode: Mode | None = None,
) -> None:
    print("\n=== What happened ===")
    result_mode = mode or result.metadata.get("mode")
    if result_mode == "sandbox-function":
        print(
            "Kitaru used three checkpoints: ask Gemini for a sandbox function "
            "call, run the sandbox command, then send the result back to Gemini."
        )
    else:
        print(
            "Kitaru records one stable Gemini interaction response as one checkpoint."
        )
    if "stream" in result.metadata:
        print(
            "Streaming was enabled: Kitaru published best-effort live events while "
            "Gemini worked, then returned this same stable final result."
        )

    print("\n=== Interaction details ===")
    print(f"Status: {result.status}")
    print(f"Interaction ID: {result.interaction_id or '(not reported by SDK)'}")
    print(
        "Previous interaction ID: "
        f"{result.previous_interaction_id or '(not reported by SDK)'}"
    )
    print(f"Model: {result.model or '(not a model interaction)'}")
    print(f"Agent: {result.agent or '(not an agent interaction)'}")
    print(f"Environment ID: {result.environment_id or '(not reported by SDK)'}")
    print(f"Poll count: {result.poll_count}")
    print(f"SDK version: {result.sdk_version}")

    print("\n=== Output preview ===")
    output = result.output_text or "(empty output)"
    print(output[:800])

    print("\n=== Step summaries ===")
    if not result.steps:
        print("(no steps reported by SDK)")
    for step in result.steps:
        print(
            f"- #{step.index} type={step.type or '(unknown)'} "
            f"status={step.status or '(unknown)'} "
            f"call_id={step.call_id or '(none)'} "
            f"tool={step.tool_name or '(none)'} "
            f"preview={step.text_preview or '(none)'}"
        )

    print("\n=== Usage ===")
    print(_json_block(result.usage))

    stream_metadata = result.metadata.get("stream")
    if stream_metadata is not None:
        print("\n=== Stream metadata ===")
        print(_json_block(stream_metadata))

    print("\n=== Kitaru artifact names ===")
    print(f"Input: {result.input_artifact_name or '(disabled)'}")
    print(f"Request manifest: {result.request_manifest_artifact_name or '(disabled)'}")
    print(f"Raw interaction: {result.raw_interaction_artifact_name or '(disabled)'}")
    print(f"Steps: {result.steps_artifact_name or '(disabled)'}")
    print(f"Output: {result.output_artifact_name or '(disabled)'}")
    print(f"Usage: {result.usage_artifact_name or '(not captured)'}")
    print(f"Events: {result.event_log_artifact_name or '(disabled)'}")
    print(f"Run summary: {result.run_summary_artifact_name or '(disabled)'}")

    if result.warnings:
        print("\n=== Warnings ===")
        for warning in result.warnings:
            print(f"- {warning}")


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run one Gemini Interactions API response inside one Kitaru checkpoint."
        )
    )
    parser.add_argument(
        "--mode",
        choices=("model", "antigravity", "sandbox-function"),
        default="model",
        help=(
            "Use a cheap Gemini model interaction, the Antigravity managed-agent "
            "preset, or the caller-owned sandbox-function showcase. Defaults "
            "to model."
        ),
    )
    parser.add_argument(
        "--model",
        default=DEFAULT_MODEL,
        help="Model for --mode model. Defaults to gemini-3.5-flash.",
    )
    parser.add_argument(
        "--prompt",
        default=None,
        help="Prompt to send. Defaults depend on --mode.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=300.0,
        help=(
            "Seconds to let the Antigravity background job and same-id "
            "observation/polling path run. The first Vertex AI call is slow "
            "while Google provisions the sandbox. Defaults to 300."
        ),
    )
    parser.add_argument(
        "--stream",
        action="store_true",
        help=(
            "Use KitaruGeminiInteractionsRunner.run_stream_sync(...) for the "
            "provider call. Model mode uses direct create streaming; "
            "Antigravity mode creates one background job and observes/polls "
            "that same interaction id."
        ),
    )
    text_delta_group = parser.add_mutually_exclusive_group()
    text_delta_group.add_argument(
        "--show-text-deltas",
        dest="show_text_deltas",
        action="store_true",
        default=True,
        help=(
            "When used with --stream, include clipped Gemini output text chunks "
            "in live event display. This is the example default so manual "
            "streaming runs visibly show model output."
        ),
    )
    text_delta_group.add_argument(
        "--hide-text-deltas",
        dest="show_text_deltas",
        action="store_false",
        help=(
            "When used with --stream, hide actual Gemini output chunks and show "
            "event labels only. Use this if live event logs should not include "
            "model output text."
        ),
    )
    parser.add_argument(
        "--foreground-antigravity",
        action="store_true",
        help=(
            "Force --mode antigravity to pass background=False. Use only if a "
            "preview backend explicitly rejects background mode."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Print a fake result summary without credentials, network, or Kitaru "
            "flow execution."
        ),
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)
    if args.prompt is None:
        args.prompt = DEFAULT_PROMPTS_BY_MODE[args.mode]

    if args.dry_run:
        if args.mode == "sandbox-function":
            _print_sandbox_function_dry_run(str(args.model))
        else:
            _print_result(
                _fake_result(args.mode, str(args.model), stream=bool(args.stream)),
                mode=args.mode,
            )
        return

    _prepare_google_credentials()
    _guard_vertex_mode(args.mode)
    request = _build_request(args)
    flow_entrypoint = (
        run_gemini_sandbox_function_showcase
        if args.mode == "sandbox-function"
        else run_gemini_interaction
    )
    handle = flow_entrypoint.run(
        request,
        stream=bool(args.stream),
        show_text_deltas=bool(args.stream and args.show_text_deltas),
    )
    print(f"Submitted execution: {handle.exec_id}")

    stop_watching = threading.Event()
    watcher: threading.Thread | None = None
    if args.stream:
        watcher = threading.Thread(
            target=_watch_gemini_stream,
            args=(handle.exec_id, stop_watching),
            daemon=True,
        )
        watcher.start()

    result = _coerce_result(handle.wait())
    stop_watching.set()
    if watcher is not None:
        watcher.join(timeout=1.0)
        if watcher.is_alive():
            print("\nLive watcher is still open; showing the durable result now.")
    _print_result(result, mode=args.mode)


if __name__ == "__main__":
    main()
