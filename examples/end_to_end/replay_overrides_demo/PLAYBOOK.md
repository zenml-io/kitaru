# Replay overrides operator playbook

Use this playbook when you want to prove replay overrides against a connected
Kitaru server with real OpenAI calls.

## Setup check

```bash
cd examples/end_to_end/replay_overrides_demo
kitaru status
echo "${OPENAI_API_KEY:+OPENAI_API_KEY is set}"
```

If the key is not visible, start a shell that loads your normal zsh profile, then
run the commands again.

## Act 1 — seed one original

```bash
uv run python demo.py seed
```

Expected result:

1. One `support_copilot_flow` execution completes.
2. The dashboard shows `lookup_policy_tool`.
3. `fixtures/prod_exec_id` stores the execution ID.

## Act 2 — flow override replay

```bash
uv run python demo.py flow-override
```

What happens:

1. Kitaru loads the original execution from `fixtures/prod_exec_id`.
2. It starts replay at `lookup_policy_tool`.
3. It changes flow inputs to `model=openai:gpt-5-nano` and
   `prompt_profile=trimmed_permissions`.
4. It writes `reports/flow_override.json`.

## Act 3 — invocation output injection

```bash
uv run python demo.py inject-output
```

What happens:

1. The exact `publish_support_decision` invocation does not run its function.
2. Replay injects a JSON support decision.
3. The downstream reporting checkpoint sees that injected decision.

## Act 4 — checkpoint code swap

```bash
uv run python demo.py code-swap
```

What happens:

1. Every matching `lookup_policy_tool` checkpoint in the replay uses
   `mocks.lookup_policy`.
2. The model gets policy facts from the replacement function.
3. The report shows whether the final support decision changed.

## Act 5 — targeted model override

```bash
uv run python demo.py model-override
```

What happens:

1. Replay targets `support_copilot_model_request_2`.
2. The requested model for that invocation is `openai:gpt-5-nano`.
3. The first model request remains cached from the original run because replay
   starts later at `lookup_policy_tool`.

## Act 6 — explicit skip

```bash
uv run python demo.py explicit-skip
```

What happens:

1. Replay still starts at `lookup_policy_tool`.
2. The final publish checkpoint is listed in `skip`.
3. The submission JSON records the explicit skip request.

## Act 7 — tagged batch replay

```bash
uv run python demo.py seed-batch --count 4
uv run python demo.py tagged-batch
```

What happens:

1. The seed command creates several original execution IDs.
2. The batch replay passes those explicit IDs to one
   `client.executions.replay(...)` request.
3. Each replay child receives the tag `replay-overrides-demo`.
4. `reports/tagged_batch.json` records completed, failed, and skipped rows.

## Act 8 — diff reports

```bash
uv run python demo.py diff-report
uv run python demo.py diff-matrix-report
```

`diff-report` compares one original with the replay children created earlier.
`diff-matrix-report` asks Kitaru for a matrix across the batch originals and any
replays linked back to them.

## Ship/no-ship reading

Look at the JSON reports and compare URLs:

- Ship if the cheaper model keeps `risk_status` and `required_action` acceptable
  across the batch.
- Do not ship if sensitive account changes move from `needs_review` to a direct
  account change.
- Investigate if the batch has failures or skipped rows before using the result
  as release evidence.
