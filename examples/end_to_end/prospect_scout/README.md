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

## Example output

With a real model (`gpt-5-nano` here) and a live Exa search, the qualifier
returns a typed `ProspectAssessment` for each company and the writer drafts one
email per approved prospect. One prospect from a real run, abridged:

Qualifier verdict (the typed agent output):

```text
[HOT] Helios Energy
  hiring_signals:
    - "We here at Helios Energy are growing - fast ... we are looking to add four (4) Energy Consultants"
    - "Solar Energy Consultant Job at Helios Energy Inc. Van Nuys, CA 91411"
    - "Helios Energy Secures $3.6 Million for Presidio Project Expansion"
  summary: "Recent signals show explicit hiring momentum at Helios Energy in 2026,
    with a May 28 posting for Solar Energy Consultant in Van Nuys and a plan to add
    four Energy Consultants [...] active growth and hiring activity rather than
    stagnation, making it a hot prospect for staffing outreach."
```

Outreach draft for that prospect:

```text
Hi Helios Energy Team,

BrightPath Talent specializes in energy and sustainability roles, and we're seeing
strong fit with your hot growth signals. You posted a Solar Energy Consultant in
Van Nuys on May 28 and plan to add four Energy Consultants to meet rising demand.
Your $3.6 million Presidio Project expansion and the fully underwritten entitlement
offer signal solid capital to back growth. We can accelerate your hiring with
pre-screened Solar and Energy Consultant candidates in Van Nuys, matched to your
culture and timeline.

Best regards,
BrightPath Talent
```

The output is model-generated over live search results, so the exact companies,
signals, and wording vary from run to run.

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
    requirements=["pydantic-ai-slim[openai]>=1.89,<1.104"],
    secret_environment_from=["prospect-scout-keys"],
)
```

The agents are built inside the checkpoints (via `new_qualifier()` /
`new_outreach_writer()`), not at module scope. Constructing
`Agent("openai:...")` builds the OpenAI client and reads `OPENAI_API_KEY`
immediately — and the runner pod imports this module *before* the secret is
applied, so a module-scope agent would crash at import. Building it inside the
checkpoint defers that until the secret is present.

### The durability demo on a remote stack

`PROSPECT_SCOUT_CRASH_AFTER` is read inside the flow body. Locally that body
runs in the same process you launched, so it sees the variable from your shell.
On a remote stack the body runs in a pod that never sees your shell — so
deliver the marker through the same secret the pod already reads, and flip it
off before retrying:

```bash
# Arm the crash by adding the marker to the secret (existing keys are kept):
kitaru secrets set prospect-scout-keys --PROSPECT_SCOUT_CRASH_AFTER=4
python prospector.py                       # crashes after 4 companies, in the pod

# Disarm before retrying, then resume:
kitaru secrets set prospect-scout-keys --PROSPECT_SCOUT_CRASH_AFTER=0
kitaru executions list                     # find the failed execution id
kitaru executions retry <execution-id>     # the 4 done companies come back cached
```

This works because the execution stores the secret *name*, but Kitaru resolves
its *value* at run time on every attempt. The first attempt reads `4` and
crashes; once you set it to `0`, the retry reads `0`, skips the crash, and runs
to completion with the already-researched companies served from their cached
checkpoints. (Setting it to `0` disarms the marker — `done` never equals `0` —
without removing the key.)
