"""Real Gemini Interactions API + Kitaru adapter example.

Story:
- A Kitaru flow sends one Gemini Interactions API request.
- `KitaruGeminiInteractionsRunner` wraps that stable interaction response in one
  Kitaru checkpoint.
- `--dry-run` prints the same kind of result summary without credentials or a
  network call, so smoke tests can exercise the example safely.

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
from typing import Any, Literal

from kitaru import flow
from kitaru.adapters._result_identity import canonicalize_result_model
from kitaru.adapters.gemini import (
    GeminiInteractionRequest,
    GeminiInteractionResult,
    GeminiInteractionStepSummary,
    KitaruGeminiInteractionsRunner,
)

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
Mode = Literal["model", "antigravity"]


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


def _build_runner() -> KitaruGeminiInteractionsRunner:
    return KitaruGeminiInteractionsRunner(
        name="gemini_interactions_example",
        checkpoint_config={"cache": False},
    )


RUNNER = _build_runner()


@flow
def run_gemini_interaction(
    request: GeminiInteractionRequest,
) -> GeminiInteractionResult:
    """Run one Gemini interaction as one Kitaru checkpoint."""
    return RUNNER.run_sync(request)


def _build_request(args: argparse.Namespace) -> GeminiInteractionRequest:
    prompt = str(args.prompt)
    if args.mode == "model":
        return GeminiInteractionRequest.start(
            prompt,
            model=str(args.model),
            metadata={"example": "gemini_interactions_agent", "mode": "model"},
        )
    # Antigravity is an async managed agent. On Vertex AI the Interactions API
    # rejects synchronous agent calls ("Chiliagon path must set background to
    # true") and the first call is slow while Google provisions the remote
    # sandbox, so submit as a background job and poll within `timeout_s`.
    return GeminiInteractionRequest.antigravity(
        prompt,
        background=True,
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
    if mode == "model":
        raise SystemExit(
            f"{VERTEXAI_ENV}=true (Vertex AI mode), but the Vertex Interactions "
            "API does not serve raw model interactions yet: every model returns "
            "'Unsupported model interaction'.\n"
            "Use an agent instead, or switch to an API key for model mode:\n"
            f"  --mode antigravity   (and export {CLOUD_LOCATION_ENV}=global)\n"
            f"  unset {VERTEXAI_ENV}; export {GEMINI_API_KEY_ENV}='<key>'  # model"
        )
    location = os.getenv(CLOUD_LOCATION_ENV, "")
    if location and location != "global":
        print(
            f"Note: {CLOUD_LOCATION_ENV}={location!r}. The Vertex Interactions agent "
            f"backend is currently only available in 'global'. If this run fails, "
            f"export {CLOUD_LOCATION_ENV}=global and retry.",
            file=sys.stderr,
        )


def _fake_result(mode: Mode, model: str) -> GeminiInteractionResult:
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
                text_preview="Dry-run prompt preview",
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
        metadata={"example": "gemini_interactions_agent", "mode": mode},
    )


def _coerce_result(value: Any) -> GeminiInteractionResult:
    return canonicalize_result_model(value, GeminiInteractionResult)


def _json_block(value: Any) -> str:
    if value is None:
        return "(not reported by SDK)"
    return json.dumps(value, indent=2, sort_keys=True, default=str)


def _print_result(result: GeminiInteractionResult) -> None:
    print("\n=== What happened ===")
    print("Kitaru records one stable Gemini interaction response as one checkpoint.")

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
        choices=("model", "antigravity"),
        default="model",
        help=(
            "Use a cheap Gemini model interaction or the Antigravity managed-agent "
            "preset. Defaults to model."
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
            "Seconds to wait for an antigravity background job (create + poll). "
            "The first Vertex AI call is slow while Google provisions the sandbox. "
            "Defaults to 300."
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
        args.prompt = (
            DEFAULT_MODEL_PROMPT if args.mode == "model" else DEFAULT_ANTIGRAVITY_PROMPT
        )

    if args.dry_run:
        _print_result(_fake_result(args.mode, str(args.model)))
        return

    _prepare_google_credentials()
    _guard_vertex_mode(args.mode)
    request = _build_request(args)
    handle = run_gemini_interaction.run(request)
    result = _coerce_result(handle.wait())
    _print_result(result)


if __name__ == "__main__":
    main()
