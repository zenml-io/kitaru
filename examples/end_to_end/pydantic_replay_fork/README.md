# PydanticAI replay demo — debug a production run, then prove the fix across a cohort

You built a support agent with **PydanticAI** and made each run a durable Kitaru flow. Every run is a durable execution made of checkpoints. Something looks wrong in a production run. This example shows the loop:

1. **Original recorded run** — the agent already ran in production.
2. **Unchanged replay / reproduction** — replay from `decide` with no edits. Kitaru reuses `gather_context` from checkpoints, then re-runs `decide` and `finalize` live with the recorded config.
3. **Edited replay** — replay from the same checkpoint with a cheaper model and looser prompt profile.
4. **Cohort** — apply that same edit across recent production runs and measure what improved or regressed.

## Fast path: replay with the Kitaru CLI

The replay operation is already a first-class Kitaru CLI command. There is no demo-specific replay command to learn.

```bash
# Unchanged reproduction: original recorded run → unchanged replay
kitaru executions replay <EXEC-ID> --from decide

# Edited replay: unchanged replay → edited replay with new flow-input values
kitaru executions replay <EXEC-ID> --from decide \
  --args '{"model": "openai:gpt-5-nano", "prompt_profile": "trimmed_permissions"}'
```

The first command asks, “If we run the live tail again with the same recorded config, do we get the same decision?” The second command asks, “After reproduction works, does this model/prompt edit change the decision?”

Add `--output json` if you want to capture the new execution id programmatically.

## What the demo script adds

`demo.py` narrates the same operations with SDK calls, prints the decisions, writes a three-way HTML report, and can run a small cohort experiment.

```python
from support_copilot import support_copilot_flow

# Original recorded run.
handle = support_copilot_flow.run(prompt, customer, "openai:gpt-5-mini", "baseline")
handle.wait()
exec_id = handle.exec_id

# Unchanged replay / reproduction.
reproduced = support_copilot_flow.replay(exec_id, from_="decide", cache=False)
reproduced.wait()

# Edited replay with new flow-input values.
edited = support_copilot_flow.replay(
    exec_id,
    from_="decide",
    cache=False,
    model="openai:gpt-5-nano",
    prompt_profile="trimmed_permissions",
)
edited.wait()
```

The important safety check is sequential:

- If **original recorded run → unchanged replay** changes, reproduction failed. Do not trust the edited comparison yet.
- If **unchanged replay → edited replay** changes, the edit changed the decision.

## Run it

The agent calls OpenAI through PydanticAI, so set your key. The demo does not load `.env` for you.

```bash
cd examples/end_to_end/pydantic_replay_fork
export OPENAI_API_KEY=sk-...        # or: set -a && source .env && set +a

uv run python demo.py run-all       # the full narrated arc
```

Individual commands:

| command | what it does |
|---|---|
| `uv run python demo.py run` | Run the agent once; print the original execution id, decision, and CLI replay commands. |
| `uv run python demo.py replay <EXEC-ID>` | Load the original decision, run an unchanged replay, run an edited replay, then write `replay_three_way.html`. |
| `uv run python demo.py cohort` | For recent runs, first check original→unchanged replay, then measure unchanged replay→edited replay. |

## Running on a remote stack (Kubernetes)

The same demo runs unchanged on a containerized stack. The flow declares its image needs in `support_copilot.py`, installs `pydantic-ai`, and pulls `OPENAI_API_KEY` from a Kitaru secret named `openai-creds`.

Create that secret once. The key must be named `OPENAI_API_KEY` so PydanticAI picks it up automatically:

```bash
kitaru secrets set openai-creds --private --OPENAI_API_KEY=sk-...
```

Then point at your stack and run as usual:

```bash
kitaru stack use <your-k8s-stack>
uv run python demo.py run-all
```

> **Troubleshooting:** if you hit `ApiClient.call_api() got an unexpected keyword argument 'response_type'` when submitting to Kubernetes, your local `kubernetes` client is too new for the stack's connector. Pin it with `uv pip install "kubernetes<26"`.

## The agent

The flow has three checkpointed steps:

```text
gather_context  →  decide  →  finalize
                   ↑ replay starts here
```

- `decide` is the intermediate step you replay from (`CUT = "decide"`).
- The `baseline` prompt profile treats permission, SSO, and admin changes as `needs_review`.
- The `trimmed_permissions` profile is looser and more likely to answer directly.
- Output is a typed `SupportDecision`: policy label, risk status, required action, and summary.
- “Did the decision move?” is judged on `risk_status` and `required_action`. The model may reword `policy_label` or `summary`, so those do not count as decision drift.

Config (`model` + `prompt_profile`) travels as flow inputs. That is why both the SDK and `kitaru executions replay` can rebuild the agents from a fresh process.

## Cohort metrics

`run_cohort(...)` handles many original execution ids. For each original execution, it:

1. Loads the original recorded decision from artifacts.
2. Runs an unchanged replay and checks original→unchanged replay drift.
3. Runs the edited replay and checks unchanged replay→edited replay drift.
4. Applies bring-your-own metrics to the unchanged replay and edited replay.

Three metrics are provided in `utils.py`:

- `cost` — `display_cost_usd` from Kitaru's usage tracking (lower is better)
- `latency` — wall-clock seconds (lower is better)
- `quality_judge` — an LLM judge scoring the answer 1–5 (higher is better)

`report.summary()` prints metric means, original→reproduction drift count, reproduction→edited drift count, and an improvement verdict. `report.regressions()` returns the metrics and decision checks that got worse.

## Files

| file | purpose |
|---|---|
| `agent.py` | The PydanticAI support agent: typed outputs and per-step prompt profiles. |
| `support_copilot.py` | The durable flow: three `@checkpoint` steps plus `@flow`, and `recent_exec_ids()`. |
| `demo.py` | The walkthrough: `run`, `replay`, and `cohort` as plain functions over SDK primitives. |
| `cohort.py` | `run_cohort(...) -> Report` with `summary()` and `regressions()`. |
| `utils.py` | Analysis helpers: metrics, quality judge, decision extraction, and `ReplayRun`. |
| `comparison_html.py` | Three-way original/reproduction/edited HTML report. |

## Validating

This example is validated by real runs: a real model and the real Kitaru backend, not mocks.

```bash
uv run python demo.py run-all
```
