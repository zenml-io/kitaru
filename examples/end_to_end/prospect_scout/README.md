# Prospect scout — durable prospect research with human approval

A sales-intelligence sweep, the kind a staffing or B2B sales team runs before
outreach: research a list of target companies on the web, qualify each one
with a typed PydanticAI agent, pause for a human to approve the shortlist,
then draft outreach emails for the approved prospects.

The point of the example is the failure mode it removes. A research sweep is
a long row of paid network and model calls — if your process dies at company
47 of 60, you do not want to pay for (or wait on) the first 47 again. Here
every company is its own Kitaru checkpoint, so a crashed run resumes from
where it stopped.

## What it demonstrates

- **One checkpoint per unit of paid work.** `research_prospect` runs once per
  company inside a plain Python `for` loop — no graph DSL. Completed
  checkpoints are persisted, so `kitaru executions retry <id>` skips them and
  only the unfinished companies re-run.
- **Typed agent outputs.** The qualifier agent must return a
  `ProspectAssessment` with an enum `fit` level (`hot` / `warm` / `cold`).
  Free-text or out-of-vocabulary answers fail validation and are retried by
  PydanticAI before anything reaches downstream checkpoints.
- **Human-in-the-loop without idle compute.** `kitaru.wait()` pauses the flow
  for shortlist approval. Locally it prompts in your terminal; on a remote
  stack the execution snapshots its state, releases compute after the
  timeout, and resumes in a fresh pod when you answer via the CLI.
- **Checkpoint retries.** `@checkpoint(retries=2)` retries transient search
  or provider failures in place, separately from PydanticAI's output
  validation retries.

## Run it

```bash
uv sync --extra local --extra pydantic-ai
cd examples/end_to_end/prospect_scout
kitaru init                      # one-time project marker
export OPENAI_API_KEY=sk-...
python prospector.py
```

Without an `EXA_API_KEY` the search step uses bundled fixture snippets, so
the flow runs end to end with just an OpenAI key. Set `EXA_API_KEY` to
research real companies via [Exa](https://exa.ai):

```bash
export EXA_API_KEY=...
python prospector.py --companies "Acme Robotics,Initech,Globex"
```

No provider key at all? Run the mechanics on PydanticAI's deterministic
test model first:

```bash
PROSPECT_SCOUT_MODEL=test python prospector.py
```

## The durability demo

Simulate a crash partway through the sweep, then resume it:

```bash
PROSPECT_SCOUT_CRASH_AFTER=4 python prospector.py
# RuntimeError: Simulated crash after 4 of 8 companies...

kitaru executions list                   # find the failed execution id
kitaru executions retry <execution-id>
```

On the retry, the four researched companies come back instantly from their
cached checkpoint outputs — watch the logs or open the dashboard trace to see
the cache hits — and only the remaining companies actually search and call
the model.

## Approving the shortlist

When the flow reaches the approval gate it prints the exact commands:

```bash
kitaru executions input <execution-id> --value true   # or false to reject
kitaru executions resume <execution-id>
```

Locally you can simply answer the terminal prompt. Approved prospects each
get an outreach draft (again one checkpoint per draft), and the flow returns
the assembled report.

## Remote stacks

On a remote stack (Kubernetes, Vertex, SageMaker, AzureML) the checkpoint
pods cannot read your shell environment. Store the provider keys in a Kitaru
secret and attach it to the image instead:

```bash
kitaru secrets set prospect-scout-keys \
    --OPENAI_API_KEY=sk-... \
    --EXA_API_KEY=...
```

```python
PROSPECTOR_IMAGE = ImageSettings(
    requirements=["pydantic-ai-slim[openai]>=1.75,<1.80"],
    secret_environment_from=["prospect-scout-keys"],
)
```
