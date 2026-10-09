# Multi-turn delivery support demo

A PydanticAI delivery-support agent, a responsive customer simulator, and an MCP App for turning captured conversations into regression cases. The editor reads actual Kitaru sessions, shows their customer messages and shipping evidence, proposes one reviewed variation, runs it, and saves the executed case in a versioned test set. The same interface runs in a local browser.

The orders and shipping state are synthetic. An order can be fulfilled while the carrier still reports it in transit. The missing-date fixture has no delivery estimate; the positive control has an estimate of `2026-10-09`. The control prompt prioritizes reassurance. The fixed prompt requires tool evidence and offers tracking/support instructions when the date is unknown.

## Prepare captured conversations

From this checkout:

```bash
cd examples/python/pydantic_ai_delivery_date
uv sync --frozen --extra langfuse
```

Put `OPENAI_API_KEY`, `LANGFUSE_PUBLIC_KEY`, and `LANGFUSE_SECRET_KEY` in this directory's uncommitted `.env`. Set `LANGFUSE_BASE_URL` there when using a specific Langfuse host. Kitaru uses saved login credentials or `KITARU_API_KEY`; choose its API URL explicitly in the preparation command:

```bash
uv run --env-file .env --frozen --extra langfuse python scripts/prepare_sessions.py \
  --server-url http://127.0.0.1:8000 \
  --output /tmp/delivery-sessions.json
```

Replace the URL with the Kitaru server you use. The command runs missing-date/control, missing-date/fix, and known-date/fix conversations using OpenAI `gpt-6-luna`. Native PydanticAI instrumentation records agent runs, model calls, and the local shipping tool in Langfuse. Each trace also captures the resolved scenario, native seed history, selected policy and model, and the actual `RunResult`, including the visible transcript and simulator attempts.

Preparation flushes Langfuse, exports complete traces, uploads them through Kitaru's existing Langfuse importer, and associates the imported sessions with `delivery-date-demo`. The Kitaru importer must be registered and its worker running. The receipt lists Kitaru session IDs, import IDs, source trace links, and execution outcomes. Native trace exports are written beside the receipt in `/tmp`; no scenario files need to be maintained.

For provider-free preparation, choose direct recording explicitly:

```bash
uv run --env-file .env --frozen python scripts/prepare_sessions.py \
  --source kitaru --backend scripted \
  --server-url http://127.0.0.1:8000 \
  --output /tmp/delivery-sessions.json
```

Scripted responses use PydanticAI's `FunctionModel`. They demonstrate the intended failure and correction without model inference and are marked `fixture_only` in the receipt. Langfuse preparation never silently switches to direct Kitaru recording when credentials or imports fail.

## Open the editor

The local preview uses the same editor operations as the MCP App. Pass the session IDs from the preparation receipt:

```bash
uv run --env-file .env --frozen python -m delivery_date.preview \
  --server http://127.0.0.1:8000 \
  --sessions SESSION_UUID_1 SESSION_UUID_2 SESSION_UUID_3
```

Open `http://127.0.0.1:8876` in a browser. Select a captured conversation, inspect its evidence, and edit the customer goal, known facts, opening message, tone, persistence, shipping status, estimate, tracking link, acceptance criteria, turn budget, or starting boundary. The editor supports this delivery schema and its `check_shipping` tool; it does not infer arbitrary agents' business state or tools.

The three proposal directions change customer pressure, customer knowledge, or tool evidence. Proposal generation changes only the fields allowed for the selected direction, shows the patch for review, and leaves the case unchanged until you accept it. Clicking Run executes the reviewed case against the selected control or fixed policy and records the resolved inputs and result in Kitaru. Generation, policy suggestions, delivery replies, customer continuations, and experiment comparisons default to OpenAI `gpt-6-luna`. `SCENARIO_GENERATION_MODEL` selects the model for these App operations. Model inference is hosted and incurs API charges; the conversation loop and mocked shipping tool run locally.

For an MCP Apps host, launch this stdio server from the example directory:

```bash
uv run --env-file .env --frozen python -m delivery_date.mcp_app \
  --server http://127.0.0.1:8000
```

For a host that accepts an MCP server configuration, use an absolute checkout path:

```json
{
  "mcpServers": {
    "kitaru-simulation": {
      "command": "uv",
      "args": [
        "--directory", "/absolute/path/to/kitaru/examples/python/pydantic_ai_delivery_date",
        "run", "--env-file", ".env", "--frozen",
        "python", "-m", "delivery_date.mcp_app",
        "--server", "http://127.0.0.1:8000"
      ]
    }
  }
}
```

Ask the host to call `kitaru_simulation_open` with `request.session_ids` containing the exact source session UUIDs. It returns the interactive editor resource `ui://kitaru/scenario-editor.html`. The server also exposes the native Kitaru MCP tools. Its proposal, run, save, set-read, and comparison actions are available to the App.

## Save and rerun regression cases

The editor opens on **Original trace**, showing the recorded customer, agent, and tool messages before the customer brief. Source traces stay unchanged while you edit a scenario. Saved scenarios open on their run, and the Original trace step returns to the source conversation.

After running an edited case, keep its recorded execution in the regression test set. Kitaru stores the exact editor fields, executable scenario snapshot and hash, seed history, start mode, continuation flag, and runner revision on the session. Each addition creates a cohort version with frozen membership; saving the same execution again does not duplicate it. Automatic sets are separated by runner revision, so new code creates a compatible set without changing the old one. A set can contain at most 25 cases from one agent.

Use View test set to reload the editor's current pinned version. To reopen a saved version in a new MCP App session, supply `request.cohort_version_id` alongside source session IDs when opening the App.

## Compare policies in Kitaru

Conversation offers the existing fixed and control policies, an expandable editor, and **Suggest policies** beside **Run scenario**. Suggest policies generates three options from the selected policy and current scenario without requiring a written request. The table shows each policy's name and rationale; select one to inspect its full prompt, then accept it before running. An optional direction lets you regenerate options with a specific goal. The recording preserves the exact resolved prompt, display name, and prompt SHA256. Single-scenario runs show preview checks; those results are stored on the session.

After saving cases, choose **Compare policies**. The App registers the reviewed baseline and candidate as runnable versions of the same agent and creates a native Kitaru experiment. Both runs use the same pinned cohort and versioned Python evaluator. The evaluator independently reads new assistant messages and frozen shipping evidence; it does not trust the preview verdict. It also checks that the simulation completed. Comparisons support one to five saved cases.

Start a worker from this example directory before comparing policies:

```bash
uv run --env-file .env --frozen kitaru worker start \
  --name delivery-policy-worker --claim agent --claim evaluator --concurrency 2
```

The worker uses the selected Kitaru server. Set `KITARU_API_URL` in the environment or use the connection selected by `kitaru login`. Both the App and worker need `OPENAI_API_KEY` in the local `.env`. Native task commands inherit the worker's project directory, so the same command works in a local checkout or CI checkout. Task-scoped credentials associate each new recording with its replay and agent version. The conversation loop and mocked shipping tool run locally; model inference uses OpenAI.

The editor polls actual experiment results and offers **Open experiment in Kitaru**. A failed assertion is different from an incomplete replay or missing evaluator result. The regression handoff becomes available only after both policy runs finish and every candidate case passes its configured evaluator.

## Hand the regression back to Codex

Click **Prepare regression PR**, then **Continue in Codex**. The App rechecks the pinned experiment evidence and sends a user message to the MCP host with the validated reference receipt and local skill path. Direct messaging is available after the MCP host initializes and advertises text-message support. Handoff details stay collapsed; the standalone browser provides the same text to copy. Neither button exports conversations or publishes a PR itself.

For this checkout, the local `kitaru-regression-pr` skill lives under `.agents/skills/kitaru-regression-pr/SKILL.md` and is ignored by Git. It tells Codex to verify the evidence, prepare a focused reference configuration and CI change, complete repository checks, and open the requested PR. The runner infrastructure must already exist in the PR's base branch. The skill remains outside the commit.

The reference configuration contains IDs, scenario hashes, policy hashes, evaluator source hash, and runner revision. The cases remain stored in the pinned cohort. Verify a saved reference file without running models:

```bash
uv run --env-file .env --frozen python -m delivery_date.experiments \
  --receipt tests/regressions/delivery-policy.json --verify-only
```

With the worker running, execute the exact candidate as a new native experiment run:

```bash
uv run --env-file .env --frozen python -m delivery_date.experiments \
  --receipt tests/regressions/delivery-policy.json
```

Both commands accept `--server-url`. The CI command prints a receipt and exits nonzero for incomplete executions, failed checks, missing independent evaluation results, or changed pins. Supply Kitaru and OpenAI credentials through CI secrets. Keep the worker running until the check finishes and stop it afterward. Model output can vary between runs; the pinned cohort and evaluator preserve the comparison inputs, not deterministic model responses.

The direct `delivery_date.test_set` command remains available for checking the scenario library without creating an experiment; it is not the native experiment regression gate.

## Run the agent directly

The terminal demo remains available independently of the editor:

```bash
uv run --env-file .env --frozen python -m delivery_date \
  --scenario missing-date --variant fix --model gpt-6-luna
```

The terminal command's default model remains `gpt-5-nano`; pass `--model gpt-6-luna` to match App operations. Both the delivery agent and customer simulator use the selected model. `--variant both --scenario all` compares both policies and the supported-date control. Add `--backend scripted` for programmed responses, `--verbose` for native messages and validation feedback, and `--check` to fail the command on a complete conversation's failed date check. Incomplete conversations always return a nonzero exit code.

The customer simulator receives visible dialogue, fixed known facts, its goal, behavior policy, and acceptance criteria. It does not receive raw shipping-tool results or expected-answer checks. It asks one clarification when the budget permits, then can end through `end_conversation` or continue through `reply_as_customer`. New recognized dates absent from its known facts and visible dialogue trigger one validation retry. A depleted turn budget never forces a successful outcome. Calls have request limits and 60-second timeouts; simulation failures, agent failures, and turn limits remain distinct outcomes.

### Choose the starting boundary

```bash
uv run --env-file .env --frozen python -m delivery_date --model gpt-6-luna --mode full
uv run --env-file .env --frozen python -m delivery_date --model gpt-6-luna --mode n-minus-one
uv run --env-file .env --frozen python -m delivery_date --model gpt-6-luna --mode tool-boundary
```

`full` starts from the opening customer message. `n-minus-one` supplies a prefix ending with the customer's next question and generates one new reply, following [Hamel's N−1 method](https://hamel.dev/blog/posts/evals-faq/#q-how-do-i-debug-multi-turn-conversation-traces). The terminal fixture's prefix is synthetic; the editor derives available boundaries from captured native messages.

`tool-boundary` supplies a native assistant `ToolCallPart` and its matching `ToolReturnPart`, then continues without adding a user message. Historical tools remain supplied history; later calls execute the isolated fixture. Malformed histories are rejected before inference. Add `--continue` to either prefix mode for a subsequent customer rollout. The editor uses continuation when running its prefix modes.

These are fresh executions using PydanticAI message history and reconstructed mock shipping state. They do not resume a suspended process or restore hidden provider state, audio, timing, or an arbitrary application's database or Python memory. The optional Ollama backend uses `--backend ollama --model YOUR_LOCAL_MODEL`; `--base-url` applies only to that backend.

### Record and reuse a scenario directly

```bash
uv run --env-file .env --frozen python -m delivery_date \
  --scenario missing-date --variant fix --model gpt-6-luna --record

uv run --env-file .env --frozen python -m delivery_date \
  --from-session YOUR_SESSION_UUID \
  --variant fix --mode tool-boundary --model gpt-6-luna --check
```

Without `--server-url`, the SDK resolves `KITARU_API_URL` first, then the server selected by `kitaru login`. Direct recording creates or reuses `delivery-date-demo`, retains the complete resolved scenario and actual output, and prints the session ID. Newly executed shipping tools become tool nodes; historical tools remain in the seed. Scripted responses are spans. No worker is required for direct recording, and it does not register a worker replay command. The direct scenario loader validates its recorded snapshot and hash; the editor additionally accepts native Langfuse imports with the canonical delivery capture.

## What the checks establish

`no_unsupported_date` checks that recognized explicit dates in new agent replies match the fixture. `uses_supported_date` requires the positive-control estimate to appear. The checker normalizes ISO dates and English month-name dates with a numeric day and explicit year, including `2026-10-09`, `October 9, 2026`, and `9 October 2026`. Historical replies are excluded.

These are narrow fixture checks. Relative dates, dates without a year, spelled-out day numbers, ambiguous numeric dates, unsupported monitoring promises, and invented calculation methods remain outside the checker. Mentioning a date to deny it can still be flagged. The fixed prompt prohibits unsupported capabilities and explanations, but the date checks do not prove that every answer follows those rules or that simulated customers behave realistically. Incomplete recordings receive unavailable checks rather than passing scores.

## Verify the example

Fast tests need no server or model:

```bash
uv run --frozen --extra langfuse pytest -q
```

For the provider-free recording and saved-set round trip, install the repository's server dependencies and ensure local test Postgres is reachable on port 5433. From the repository root:

```bash
uv sync --frozen --extra server
uv run --no-sync python examples/python/pydantic_ai_delivery_date/scripts/run_ci_e2e.py
```

The harness starts a unique database and server, runs the example tests, and removes both afterward. It does not replace other stacks or start importer workers. Hosted generation, Langfuse capture, and an MCP host's actual rendering need their respective live services.
