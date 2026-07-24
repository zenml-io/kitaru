# PydanticAI adapter example

This example runs a real PydanticAI `Agent("openai:gpt-5-nano")` through
`KitaruAgent`. The prompt requires the model to call two local Python tools:
`get_current_utc_time` reads the process clock and `multiply` performs arithmetic.

## Set up and run

From the repository root:

```bash
uv sync --extra pydantic-ai

export OPENAI_API_KEY="..."
export KITARU_API_URL="http://localhost:8000"
export KITARU_API_KEY="..."
export KITARU_AGENT_ID="..."
# Optional:
export KITARU_AGENT_VERSION_ID="..."

# If these values are stored in the repository's ignored .env file:
set -a; source .env; set +a

uv run python -m v2_examples.pydantic_ai_agent
```

The example prints the final answer and the recorded `kitaru_session_id`.

## Record the same run in Kitaru and Langfuse

Set the Langfuse credentials and enable the optional instrumentation:

```bash
export LANGFUSE_PUBLIC_KEY="..."
export LANGFUSE_SECRET_KEY="..."
export LANGFUSE_BASE_URL="https://cloud.langfuse.com"
export KITARU_EXAMPLE_LANGFUSE=1

uv run --with langfuse python -m v2_examples.pydantic_ai_agent
```

This uses PydanticAI's OpenTelemetry instrumentation and prints both the
`kitaru_session_id` and `langfuse_trace_id`. The explicit parent Langfuse span
records the original prompt and final output, while its child observations capture
the model and tool activity. Calling `flush()` before exit makes the short-lived
script wait until its trace has been exported.

The Kitaru run records the session input and final output, model requests and
responses, token usage, and both tool calls with their arguments and results.

Use `uv run python -m v2_examples.pydantic_ai_agent` as the agent version's run
command. The current local Kitaru Runner launches that registered command for
replay and experiment execution, then supplies replay inputs, overrides, tool
policies, and the replay identifier through its environment. This example does
not depend on an unimplemented remote worker.
