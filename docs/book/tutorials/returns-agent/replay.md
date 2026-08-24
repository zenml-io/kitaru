---
description: Register one investigation-derived candidate and replay the frozen cohort safely.
icon: rotate-left
---

# 4. Replay one bounded change

Observe → Judge → Define → **Replay** → Compare

This is the first phase that runs the agent and can make paid model calls. You will make one change justified by the investigation, register its run specification, review tool safety, and replay the frozen cohort.

## Make one investigation-derived change

Change `returns_agent/agent.py` only after the review has identified one behavior worth changing. Keep the candidate narrow enough that you can explain how it is expected to affect the evaluator and counterexamples.

The example does not include a prewritten candidate or environment switch. That is intentional: the candidate should follow from the behavior you accepted, not from a hidden fixture answer key. If you want coding-agent help, ask it for the smallest code change that implements only that behavior, require it to explain the expected effect on every reviewed target and counterexample, and review the patch before registering it.

Record the source revision or working-tree state you intend the worker to execute. Then register version 2:

```bash
uv run kitaru agent version register \
  returns-resolver \
  --command "python -m returns_agent.agent" \
  --description "Test one investigation-derived behavior change." \
  --display-version candidate-v1 \
  --working-dir . \
  --timeout-seconds 180 \
  --tool lookup_order \
  --tool get_return_policy \
  --tool check_shipping \
  --tool issue_refund \
  --tool create_replacement \
  --tool escalate_to_human
```

Registration does not run the agent. It creates `returns-resolver@2`, an immutable run specification. The specification does not snapshot a mutable `--working-dir`, so reproducibility also requires the worker to use the intended checkout, commit, or container image.

Save the exact candidate reference:

```bash
CANDIDATE_AGENT="returns-resolver@2"
```

## Give the worker model credentials

The agent uses `openai:gpt-5-nano`. Each replay may make more than one paid OpenAI API request.

In Terminal 2, stop the existing worker with `Ctrl-C`. Export the key in that same shell, then restart the worker:

```bash
printf 'OpenAI API key: '
IFS= read -r -s OPENAI_API_KEY
printf '\n'
export OPENAI_API_KEY
uv run kitaru worker start --name canonical-example-worker
```

Restarting matters because the worker launches the registered agent as a subprocess. A running process cannot inherit environment variables added to another terminal later.

{% hint style="warning" %} The commands below create remote Kitaru resources and paid model calls. Confirm the candidate version, cohort membership, evaluator versions, and expected number of replays before starting the run. {% endhint %}

## Choose the replay tool policy

When agent code runs again, its tools need an explicit relationship to the outside world. The tool policy determines whether a call uses recorded history, a static result, or the live tool.

This synthetic example uses passthrough:

```json
{"default":{"type":"passthrough"},"tools":{}}
```

Passthrough is safe here because every action tool writes only to a fresh in-memory store created for the replay. Do not copy this choice for tools that charge cards, send messages, change production data, or trigger other side effects. For those, prefer recorded history with `on_miss=fail` or a reviewed static result. See [Replay and overrides](../../guides/replay-and-overrides.md).

Replay safety comes from the configured policy and the actual tool implementations, not from the word "replay." Review both before starting the run.

## Create the experiment

An [**experiment**](../../concepts/experiments.md) fixes the replay configuration and evaluator versions for an agent. An **experiment run** supplies the exact candidate agent version and immutable cohort version.

Create an experiment with the accepted behavior evaluator and operational measurements:

```bash
uv run kitaru experiment create \
  returns-candidate \
  --agent returns-resolver \
  --description "Test one accepted behavior change against the reviewed cohort." \
  --tool-policy '{"default":{"type":"passthrough"},"tools":{}}' \
  --evaluator "$BEHAVIOR_EVALUATOR" \
  --evaluator kitaru/tool-health@latest \
  --evaluator kitaru/timing-profile@latest
```

The experiment fixes the agent parent, tool policy, and evaluator versions. Reusing it does not by itself preserve the candidate code or population; each run supplies those exact versions.

Resolve the cohort-version UUID:

```bash
COHORT_VERSION_ID="$(
  uv run kitaru --output json cohort version get "$COHORT_REFERENCE" \
  | jq -r '.item.id'
)"
```

Before continuing, inspect `$COHORT_REFERENCE` again and count its members. The run creates one replay per cohort session.

## Start the bounded run

```bash
uv run kitaru experiment run start \
  returns-candidate \
  --cohort-version "$COHORT_VERSION_ID" \
  --agent "$CANDIDATE_AGENT" \
  --evaluate-baselines \
  --wait \
  --timeout 1800
```

Save the experiment-run UUID printed in the receipt:

```bash
RUN_ID="YOUR_EXPERIMENT_RUN_UUID"
```

The worker launches the candidate command once for each cohort session and stores every new run as a session with `origin: replay`. `--evaluate-baselines` applies the same evaluator versions to both the imported sessions and their replays. Without it, you would have candidate measurements but no like-for-like baseline.

`--timeout-seconds 180` limits each agent subprocess. `--timeout 1800` limits how long the CLI waits for the complete experiment run.

If a replay fails or times out, keep it in the denominator. A completed subset is not the complete experiment result.

## Checkpoint

You now have:

- `CANDIDATE_AGENT`, set to the registered candidate run specification;
- `returns-candidate`, the experiment definition;
- `RUN_ID`, identifying one experiment run over `$COHORT_REFERENCE`; and
- explicit terminal states for every attempted replay, with like-for-like evaluations for completed pairs and preserved failures for incomplete pairs.

The worker has made paid model requests. Continue to [5. Compare the paired evidence](compare.md).
