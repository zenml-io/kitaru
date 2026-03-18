# Basic Coding Agent

A general-purpose interactive agent built with **kitaru primitives + LiteLLM** — no PydanticAI, no LangChain, no agent framework.

Demonstrates:

- **LiteLLM tool calling** — manual tool-call loop with typed Pydantic responses
- **`kitaru.wait()`** — durable human-in-the-loop via `ask_user` and `hand_back` tools
- **Custom materializers** — dynamic ZenML dashboard visualizations per tool type
- **Generated file persistence** — HTML/Markdown/CSV files saved as artifacts via `kitaru.save()`
- **LLM-named checkpoints** — `_display_name` tool parameter for descriptive step names

## Setup

Register a model alias (one-time):

```bash
kitaru secrets set anthropic-creds --ANTHROPIC_API_KEY=sk-ant-...
kitaru model register coding-agent --model anthropic/claude-sonnet-4-20250514 --secret anthropic-creds
```

## Usage

```bash
# Start the agent with a task
uv run python -m flow "Create a Plotly population pyramid for South Korea"
```

The agent works on the task, then calls `hand_back` with a summary and a question for you. You respond with a follow-up task or cancel the execution to stop.

### Remote execution

```bash
# Send follow-up input
kitaru executions input <eid> --wait follow_up_0 --value '{"message": "Now add a legend"}'
kitaru executions resume <eid>

# Cancel to stop
kitaru executions cancel <eid>
```

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `CODING_AGENT_MODEL` | `coding-agent` | Model alias or LiteLLM identifier |
| `CODING_AGENT_MAX_TOOL_ROUNDS` | `30` | Max tool-calling rounds per task |
