# Durable Harness

A planner-builder-evaluator harness built with **Kitaru primitives** — inspired
by Anthropic's ["Harness design for long-running application development"](https://www.anthropic.com/engineering/harness-design-long-running-apps)
(March 2026), rebuilt with crash recovery, replay, and human-in-the-loop gates.

Demonstrates:

- **Four-agent durable harness** — planner -> builder -> evaluator -> summarizer, each in its own `@checkpoint`
- **Tracked LLM calls** via `kitaru.llm()` with cost/token metadata
- **Human-in-the-loop review gates** via `kitaru.wait()` after QA failures
- **Crash recovery** via checkpoint-level replay
- **Feedback summarization** across build/evaluate iterations
- **Named artifact persistence** (`spec`, `code_round_N`, `qa_report_round_N`) for inspection
- **Post-run execution summary** via `KitaruClient`

## How durability works

Durability in Kitaru applies at **checkpoint boundaries**, not continuously.
Code between checkpoints (variable assignments, loop control, etc.) runs
normally and is not intercepted. If the process dies between two checkpoints,
the successfully completed checkpoint's output is persisted, but flow-body
variable assignments are lost. On replay, cached checkpoints return their
stored outputs and `.load()` re-materializes them.

This is why each "expensive" operation (LLM call) is inside its own checkpoint.

## Setup

```bash
uv sync --extra local --extra llm
kitaru init

# Store Anthropic API key
kitaru secrets set anthropic-creds --ANTHROPIC_API_KEY=sk-ant-...

# Register main model (builder + evaluator — needs strong code gen)
kitaru model register harness --model anthropic/claude-sonnet-4-6 --secret anthropic-creds

# Register fast model (planner + summarizer — lighter tasks)
kitaru model register harness-fast --model anthropic/claude-haiku-4-5-20251001 --secret anthropic-creds

# Alternative: use Opus for the main model (highest quality, higher cost)
# kitaru model register harness --model anthropic/claude-opus-4-6 --secret anthropic-creds

# Alternative: Ollama (free, local — less reliable JSON from evaluator)
# kitaru model register harness --model ollama/qwen3:8b

kitaru model list
```

## Usage

```bash
cd examples/durable_harness
uv run python harness.py "A personal dashboard with weather widget, todo list, and quote of the day"

# Use Opus for builder/evaluator, Haiku for planner/summarizer:
uv run python harness.py "..." --model anthropic/claude-opus-4-6 --fast-model anthropic/claude-haiku-4-5-20251001
```

## Demo scenarios

### Scenario 1: Happy path

Run to completion. Inspect the generated `outputs/*.html`. Show execution
details:

```bash
kitaru executions list
kitaru executions get <exec_id>
kitaru executions logs <exec_id> --grouped -v
```

### Scenario 2: Human-in-the-loop review

When QA fails, the flow pauses. The watcher thread prints fallback commands.
Resolve from another terminal:

```bash
# Include --wait with the wait name for explicitness (the CLI auto-selects
# when there's only one pending wait, but being explicit is safer).
kitaru executions input <exec_id> --wait review_round_0 \
  --value '{"action": "approve", "feedback": ""}'
kitaru executions input <exec_id> --wait review_round_0 \
  --value '{"action": "revise", "feedback": "Add keyboard navigation"}'
kitaru executions input <exec_id> --wait review_round_0 \
  --value '{"action": "abort", "feedback": ""}'

# Resume if the runner has exited. Some backends auto-resume after input.
kitaru executions resume <exec_id>
```

### Scenario 3: Replay from a checkpoint

```bash
kitaru executions replay <exec_id> --from builder_round_0
kitaru executions replay <exec_id> --from planner --args '{"task": "A portfolio website"}'
kitaru executions replay <exec_id> --from evaluator_round_0 \
  --overrides '{"checkpoint.builder_round_0": "<html>edited</html>"}'

# Replay with a stronger model for builder/evaluator (only re-executed checkpoints use it)
kitaru executions replay <exec_id> --from builder_round_0 \
  --args '{"model": "anthropic/claude-opus-4-6"}'

# Replay from planner with a different task entirely
kitaru executions replay <exec_id> --from planner \
  --args '{"task": "A portfolio website with project cards and contact form"}'
```

### Scenario 4: Crash recovery

Kill the process during the build phase (Ctrl+C). Then replay:

```bash
kitaru executions replay <exec_id> --from builder_round_0
```

Every checkpoint before the crash point is reused on replay.

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `DURABLE_HARNESS_MODEL` | `harness` | Model alias for builder + evaluator |
| `DURABLE_HARNESS_FAST_MODEL` | same as `DURABLE_HARNESS_MODEL` | Model alias for planner + summarizer (cheaper/faster) |
| `KITARU_LLM_MOCK_RESPONSE` | *(unset)* | Set to any string to skip real LLM calls (for testing) |

## Extending this example

- **Swap `kitaru.llm()` for Claude Agent SDK sessions** in the builder
  checkpoint. This enables tool calling, multi-file output, and real code
  editing, while Kitaru still provides the durable checkpoint wrapper around
  each phase.
- **Add Playwright-based evaluation** in the evaluator checkpoint. Open the
  generated HTML in a headless browser, screenshot it, check for JS errors.
  This closes the gap with Anthropic's original Playwright MCP evaluator.
- **Split the builder into multiple checkpoints** (scaffold -> widgets ->
  styling -> polish) to get finer-grained crash recovery on long builds.
- **Add `@checkpoint(runtime="isolated")` on the builder** to run it in its
  own container on remote orchestrators like Kubernetes.
