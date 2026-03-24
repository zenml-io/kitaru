# Coding Agent

A general-purpose interactive agent built with **kitaru primitives + direct provider SDKs** — no PydanticAI, no LangChain, no agent framework.

Demonstrates:

- **Parallel tool execution** — when the LLM returns multiple tool calls, they run concurrently via `checkpoint.submit()`
- **Provider SDK tool calling** — manual tool-call loop with typed Pydantic responses (OpenAI or Anthropic)
- **`kitaru.wait()`** — durable human-in-the-loop via `ask_user` and `hand_back` tools
- **Custom materializers** — dynamic ZenML dashboard visualizations per tool type
- **Generated file persistence** — HTML/Markdown/CSV files saved as artifacts via `kitaru.save()`
- **LLM-named checkpoints** — `_display_name` tool parameter for descriptive step names

## Setup

```bash
cd examples/coding_agent
uv pip install 'kitaru[local]'   # Install Kitaru with local runtime
kitaru init                  # Initialize a Kitaru project in this directory
```

Register a model alias (one-time):

```bash
kitaru secrets set anthropic-creds --ANTHROPIC_API_KEY=sk-ant-...
kitaru model register coding-agent --model anthropic/claude-sonnet-4-20250514 --secret anthropic-creds
```

## Usage

```bash
python agent.py "Create a Plotly population pyramid for South Korea"
```

The agent works on the task, then calls `hand_back` with a summary and a question for you. You respond with a follow-up task or cancel the execution to stop.

### Remote execution

```bash
# Send follow-up input
kitaru executions input <exec_id> --value '{"message": "Now add a legend"}'
kitaru executions resume <exec_id>

# Cancel to stop
kitaru executions cancel <exec_id>
```

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `CODING_AGENT_MODEL` | `coding-agent` | Model alias or provider/model identifier |
| `CODING_AGENT_MAX_TOOL_ROUNDS` | `30` | Max tool-calling rounds per task |
