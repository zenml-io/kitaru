# Replay overrides demo

This is the main demo for Kitaru's replay override API. It runs a small support copilot once, then replays that execution from `lookup_policy_tool` while changing different parts of the run.

The baseline run uses `openai:gpt-5-mini` with the careful prompt. The replay runs try changes such as a cheaper model, a different prompt profile, an injected checkpoint result, and a replacement policy lookup function.

```text
support_copilot_model_request
  -> gather_context_tool
  -> lookup_policy_tool                 replay starts here
  -> support_copilot_model_request_2
  -> publish_support_decision
  -> record_replay_observation
```

Use this demo when you want to understand the public replay API: SDK, CLI, and JSON result shapes.

## What this demo covers

- `flow_overrides`: change flow inputs for the replay run, such as `model` and `prompt_profile`.
- `checkpoint_overrides`: target every invocation of a checkpoint name. The demo uses this to replace `lookup_policy_tool` with `mocks.lookup_policy`.
- `invocation_overrides`: target one recorded invocation. The demo injects one `publish_support_decision` output and separately changes `support_copilot_model_request_2` to `openai:gpt-5-nano`.
- `skip`: keep a checkpoint result from the source run instead of recomputing it.
- tagged batch replay: send several explicit execution IDs through the same replay request and write a diff-matrix report.

The point is scope. A flow override changes the replayed flow inputs. A checkpoint override changes every matching checkpoint name. An invocation override changes one recorded call.

## Requirements

From the repository root:

```bash
uv sync --extra local --extra pydantic-ai
```

Connect to the Kitaru server you want to use and make sure the OpenAI key is available to the process:

```bash
kitaru status
echo "${OPENAI_API_KEY:+OPENAI_API_KEY is set}"
```

The live model comparison uses:

- baseline: `openai:gpt-5-mini`
- variant: `openai:gpt-5-nano`

## Run the demo

Run commands from this directory:

```bash
cd examples/end_to_end/replay_overrides_demo
uv run python demo.py seed
uv run python demo.py flow-override
uv run python demo.py inject-output
uv run python demo.py code-swap
uv run python demo.py model-override
uv run python demo.py explicit-skip
uv run python demo.py seed-batch --count 4
uv run python demo.py tagged-batch
uv run python demo.py diff-report
uv run python demo.py diff-matrix-report
```

Or run the whole sequence:

```bash
uv run python demo.py run-all
```

The demo writes execution IDs under `fixtures/` and JSON reports under `reports/`. Git ignores those generated files.

## CLI equivalents

Flow override:

```bash
kitaru executions replay "$EXEC_ID" \
  --at lookup_policy_tool \
  --flow-overrides '{"model":"openai:gpt-5-nano","prompt_profile":"trimmed_permissions"}' \
  --wait \
  -o json
```

Invocation output injection:

```bash
kitaru executions replay "$EXEC_ID" \
  --at lookup_policy_tool \
  --invocation-overrides '{"publish_support_decision":{"output":{"policy_label":"injected_support_decision","risk_status":"safe_to_answer","required_action":"answer_directly_with_safety_note","summary":"Injected during replay"}}}' \
  --wait \
  -o json
```

Checkpoint code swap:

```bash
kitaru executions replay "$EXEC_ID" \
  --at lookup_policy_tool \
  --checkpoint-overrides '{"lookup_policy_tool":{"code":"mocks.lookup_policy"}}' \
  --wait \
  -o json
```

Targeted model override:

```bash
kitaru executions replay "$EXEC_ID" \
  --at lookup_policy_tool \
  --invocation-overrides '{"support_copilot_model_request_2":{"model":"openai:gpt-5-nano"}}' \
  --wait \
  -o json
```

Explicit skip:

```bash
kitaru executions replay "$EXEC_ID" \
  --at lookup_policy_tool \
  --flow-overrides '{"prompt_profile":"trimmed_permissions"}' \
  --skip publish_support_decision \
  --wait \
  -o json
```

Tagged batch replay:

```bash
kitaru executions replay "$EXEC_ID_1" "$EXEC_ID_2" "$EXEC_ID_3" \
  --at lookup_policy_tool \
  --flow-overrides '{"model":"openai:gpt-5-nano","prompt_profile":"trimmed_permissions"}' \
  --tag replay-overrides-demo \
  --wait \
  --on-error collect \
  -o json
```

Diff reports:

```bash
kitaru executions diff "$EXEC_ID" "$REPLAY_ID" -o json
kitaru executions diff-matrix "$EXEC_ID_1" "$EXEC_ID_2" "$EXEC_ID_3" -o json
```
