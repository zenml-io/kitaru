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

The run records the session input and final output, model requests and responses,
token usage, and both tool calls with their arguments and results.

Use `uv run python -m v2_examples.pydantic_ai_agent` as the agent version's run
command. The current local Kitaru Runner launches that registered command for
replay and experiment execution, then supplies replay inputs, overrides, tool
policies, and the replay identifier through its environment. This example does
not depend on an unimplemented remote worker.
