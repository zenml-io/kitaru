# Prospect scout — a durable, agentic prospecting sweep

A sales-intelligence sweep, the kind a staffing or B2B sales team runs before
outreach: for each target company an **agent researches the web**, classifies
the company against the team's lines of business, and judges whether it is
worth pursuing. A human approves the shortlist, and only then is outreach
drafted.

It is a real **agent**, not a workflow. The qualifier is handed a company name
and a `search_web` tool and decides for itself which searches to run — hiring,
funding, expansion, layoffs — and how many. Nothing is pre-fetched and spoon-fed
into the prompt; the model contemplates tool choice, which is what makes it an
agent.

Search uses [Exa](https://exa.ai) when `EXA_API_KEY` is set and bundled fixtures
otherwise, so the whole thing runs end to end without any accounts.

## The aha moments

Each aha moment below is framed the same way: the failure mode you hit
**without** Kitaru, then exactly what Kitaru (or the PydanticAI it wraps) adds
to remove it. See ["What's Kitaru vs what's PydanticAI"](#whats-kitaru-vs-whats-pydanticai)
at the end for the honest boundary.

### 1. It's an agent — it chooses its own searches

`research_prospect` builds a PydanticAI agent and hands it a `search_web` tool.
The model, not your code, decides what to look up:

```python
@checkpoint(retries=2)
def research_prospect(company: str) -> ProspectAssessment:
    qualifier = new_qualifier()                 # agent owns the search_web tool
    return qualifier.run_sync(
        f"Research and qualify {company} as a staffing prospect."
    ).output
```

**Without it:** if you hard-code the searches, it isn't an agent — it's a
workflow — and once it *is* an agent, its internal tool calls are an opaque box
you can't inspect, debug, or replay.

**What Kitaru adds:** running through `KitaruAgent`, **every `search_web` call
the agent makes is tracked as a child event under that company's checkpoint**,
so you see the queries it actually ran, not just its verdict.
`CapturePolicy(tool_capture="full")` records the arguments and results too.

### 2. Crash at company 7 of 10 → don't pay for the first 7 again

**Without it:** a research sweep is a long row of paid search and model calls;
a crash at company 7 re-runs all 7. **What Kitaru adds:** each company is its
own `@checkpoint`, so a crash loses only the company in flight:

```bash
PROSPECT_SCOUT_MODEL=test PROSPECT_SCOUT_CRASH_AFTER=3 python prospector.py
# RuntimeError: Simulated crash after 3 of 8 companies...

kitaru executions list                                   # find the failed id
PROSPECT_SCOUT_MODEL=test kitaru executions retry <id>   # resume it
```

On the retry the already-researched companies return from their **cached
checkpoint outputs** — no repeated Exa calls, no repeated model tokens — and
only the unfinished companies actually search and call the model. Confirm it:

```bash
kitaru executions get <id>
#   Checkpoints: research_prospect (completed), research_prospect_2 (completed), ...
```

> The retry process re-imports this module to continue the flow, so it needs the
> same model configuration your first run had: set `PROSPECT_SCOUT_MODEL=test`
> (or export `OPENAI_API_KEY`) in the shell you run `retry` from.

### 3. Bad classifications never reach your data

The agent must return a `LineOfBusiness` **and** a `FitLevel` — both enums:

```python
class LineOfBusiness(StrEnum):
    FINANCE_ACCOUNTING = "finance_accounting"
    TECHNOLOGY = "technology"
    MARKETING_CREATIVE = "marketing_creative"
    ...

class ProspectAssessment(BaseModel):
    company: str
    line_of_business: LineOfBusiness
    fit: FitLevel
    ...
```

**Without it:** a free-text or out-of-vocabulary classification silently corrupts
your shortlist and downstream desk routing.

**What PydanticAI adds** (not Kitaru): the answer fails validation and the model
is retried **before** anything reaches `build_shortlist`. **What Kitaru adds on
top:** the validated, typed result is persisted as the checkpoint's artifact, so
a retry or replay serves back *exactly* the value that passed validation rather
than re-rolling the dice.

### 4. A human approves — without a pod idling on the clock

**Without it:** a worker pod keeps billing while a human takes an hour — or a day
— to approve. **What Kitaru adds:** `kitaru.wait()` pauses the flow for approval;
on a remote stack the execution snapshots its state and **releases compute**
while it waits, then resumes in a fresh pod exactly where it paused.

```bash
kitaru executions input <execution-id> --value true   # or false to reject
kitaru executions resume <execution-id>
```

## Run it

```bash
uv sync --extra local --extra pydantic-ai
cd examples/end_to_end/prospect_scout
kitaru init                      # one-time project marker
export OPENAI_API_KEY=sk-...
python prospector.py
```

Set `EXA_API_KEY` to research real companies; without it the `search_web` tool
returns bundled fixture snippets so the flow still runs end to end:

```bash
export EXA_API_KEY=...
python prospector.py --companies "Acme Robotics,Initech,Globex"
```

No provider key at all? Run the mechanics on PydanticAI's deterministic test
model first — it exercises the tool calls, checkpoints, and approval gate
without any network:

```bash
PROSPECT_SCOUT_MODEL=test python prospector.py
```

## How it maps to ZenML

| Example primitive            | What it does                                              |
| ---------------------------- | -------------------------------------------------------- |
| `@flow prospect_scout`       | The sweep — a `@pipeline(dynamic=True)`, plain `for` loop |
| `@checkpoint research_prospect` | One durable unit of paid work per company             |
| `KitaruAgent` + `search_web` | Agentic search, tracked per tool call under the checkpoint |
| `kitaru.wait("approve_shortlist")` | Human approval that releases compute               |
| `build_shortlist` / `publish_report` | Ranking and the single report-producing sink     |

## What's Kitaru vs what's PydanticAI

It's worth being precise about where the value comes from, because they compose
rather than overlap:

- **PydanticAI** owns the agent: the reasoning loop, tool calling, and the typed
  output validation + retry. You could swap it for LangGraph, the OpenAI Agents
  SDK, or a raw provider — Kitaru is framework-agnostic.
- **Kitaru core** (`@flow`, `@checkpoint`, `kitaru.wait`) adds what the agent
  framework does not: durability and caching across crashes/retries/replays, and
  human-in-the-loop that releases compute instead of idling a pod.
- **The `KitaruAgent` adapter** is the thin bridge between the two: it runs the
  PydanticAI agent inside a checkpoint and tracks each model request and tool
  call as a child event, so the agent stops being an opaque box.

So the durability and cost wins are Kitaru's; the agent and its type safety are
PydanticAI's; the adapter is what makes the agent's internals observable and
replayable under Kitaru.

## Remote stacks

On a remote stack (Kubernetes, Vertex, SageMaker, AzureML) the checkpoint pods
cannot read your shell environment. Store the provider keys in a Kitaru secret
and attach it to the image:

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

The agents are built inside the checkpoints (via `new_qualifier()` /
`new_outreach_writer()`), not at module scope. Constructing `Agent("openai:...")`
reads `OPENAI_API_KEY` immediately, and the runner pod imports this module
*before* the secret is applied — so a module-scope agent would crash at import.
Building it inside the checkpoint defers that until the secret is present.

### The crash demo on a remote stack

`PROSPECT_SCOUT_CRASH_AFTER` is read inside the flow body. Locally that body runs
in your process and sees your shell. On a remote stack the body runs in a pod
that never sees your shell, so deliver the marker through the same secret the pod
already reads, and flip it off before retrying:

```bash
# Arm the crash by adding the marker to the secret (existing keys are kept):
kitaru secrets set prospect-scout-keys --PROSPECT_SCOUT_CRASH_AFTER=3
python prospector.py                       # crashes after 3 companies, in the pod

# Disarm before retrying, then retry:
kitaru secrets set prospect-scout-keys --PROSPECT_SCOUT_CRASH_AFTER=0
kitaru executions retry <execution-id>     # the done companies come back cached
```

The execution stores the secret *name*, but Kitaru resolves its *value* at run
time on every attempt: the first attempt reads `3` and crashes; the retry reads
`0`, skips the crash (`done` never equals `0`), and runs to completion with the
already-researched companies served from their cached checkpoints.
