# Replay overrides demo

This example is a live, end-to-end operator demo for Kitaru replay overrides.

The story is concrete: a support copilot answers account-administration
requests. The original production-like run uses `openai:gpt-5-mini` and the
careful baseline prompt. Then you replay the same execution from
`lookup_policy_tool` with different override scopes:

```text
support_copilot_model_request → gather_context_tool → lookup_policy_tool → support_copilot_model_request_2 → publish_support_decision → record_replay_observation
                                                      ↑ replay starts here
```

## What this demonstrates

- **Flow overrides** change flow inputs for the replay run, such as `model` and
  `prompt_profile`.
- **Checkpoint overrides** target every invocation of a checkpoint name. The demo
  uses this to swap the policy lookup callable with `mocks.lookup_policy`.
- **Invocation overrides** target one recorded invocation. The demo injects one
  `publish_support_decision` output and separately targets
  `support_copilot_model_request_2` with `openai:gpt-5-nano`.
- **Explicit skip** asks replay to keep a checkpoint result from the source run.
- **Tagged batch replay** sends several explicit execution IDs through the same
  replay request and writes a diff-matrix report.

## Requirements

From the repository root:

```bash
uv sync --extra local --extra pydantic-ai
```

Then connect to the Kitaru server you want to use and make sure the OpenAI key is
available to the process:

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

The demo writes execution IDs under `fixtures/` and JSON reports under
`reports/`. Those generated files are ignored by git.

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
