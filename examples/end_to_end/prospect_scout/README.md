# Prospect scout — a durable, agentic prospecting sweep

A sales-intelligence sweep, the kind a staffing or B2B sales team runs before
outreach: for each target company an **agent researches the web**, classifies
the company against the team's lines of business, and judges whether it is worth
pursuing. A human approves the shortlist, and only then is outreach drafted.

The qualifier is a real agent: it's handed a `search_web` tool and a company
name, and decides for itself which searches to run — hiring, funding, expansion,
layoffs — and how many. Nothing is pre-fetched for it.

## Quickstart — run it locally, no API keys

Every command uses `uv run` so it resolves against the project's environment.
You can run the whole thing on PydanticAI's deterministic test model and bundled
search fixtures — no OpenAI or Exa account needed.

**1. Install and start a local Kitaru server.** The server records your runs and
gives you a dashboard to inspect them.

```bash
uv sync --extra local --extra pydantic-ai
uv run kitaru init       # one-time project marker (creates .kitaru/)
uv run kitaru login      # starts a local server; open the printed dashboard URL
```

**2. Start a small sweep on the test model.** Run from the repo root:

```bash
PROSPECT_SCOUT_MODEL=test uv run python \
    examples/end_to_end/prospect_scout/prospector.py \
    --companies "Apex BioLabs,Helios Energy"
```

It researches each company, prints an **`Execution URL`**, then **pauses at the
approval gate** and waits — the process will sit there until you approve. This is
the human-in-the-loop step, and it is intentional.

**3. Approve the shortlist from a second terminal.** The paused run prints the
exact command with its execution id; copy it into another terminal:

```bash
uv run kitaru executions input <execution-id> --value true   # or false to reject
```

The first terminal resumes, drafts outreach for each approved prospect, and
prints the final report.

**4. See what the agent actually did.** Open the `Execution URL` the run printed.
In the dashboard you can see each company's `research_prospect` checkpoint and,
under it, **every `search_web` call the agent chose to make** — the queries, not
just the verdict. (`kitaru executions get <id>` shows the same checkpoints from
the CLI.)

That is the whole loop: agentic research → human approval → outreach, durable at
every step. The rest of this README explains why each part matters.

## Run it for real

Point it at a real model, and optionally a real [Exa](https://exa.ai) search key:

```bash
export OPENAI_API_KEY=sk-...
export EXA_API_KEY=...                # optional; falls back to fixtures if unset
uv run python examples/end_to_end/prospect_scout/prospector.py \
    --companies "Acme Robotics,Initech,Globex"
```

Without `--companies` it runs the bundled list of eight. Without `EXA_API_KEY`
the `search_web` tool returns fixture snippets, so the agent still does real
tool-choice — it just searches a small offline corpus.

## The aha moments

Each is framed the same way: the failure mode you hit **without** Kitaru, then
what Kitaru (or the PydanticAI it wraps) adds. See
["What's Kitaru vs what's PydanticAI"](#whats-kitaru-vs-whats-pydanticai) for the
honest boundary.

### 1. You can see what the agent searched for

`research_prospect` hands a PydanticAI agent a `search_web` tool; the model, not
your code, decides what to look up:

```python
@checkpoint(retries=2)
def research_prospect(company: str) -> ProspectAssessment:
    qualifier = new_qualifier()                 # agent owns the search_web tool
    return qualifier.run_sync(
        f"Research and qualify {company} as a staffing prospect."
    ).output
```

Because it runs through `KitaruAgent`, each `search_web` call is recorded under
that company's checkpoint. Open the `Execution URL` and you can see exactly which
queries the agent ran and what came back — so when a verdict looks wrong, you can
tell whether the search or the reasoning was at fault.

### 2. Crash at company 7 of 10 → don't pay for the first 7 again

**Without it:** a research sweep is a long row of paid search and model calls; a
crash at company 7 re-runs all 7. **What Kitaru adds:** each company is its own
`@checkpoint`, so a crash loses only the company in flight. Try it:

```bash
PROSPECT_SCOUT_MODEL=test PROSPECT_SCOUT_CRASH_AFTER=1 uv run python \
    examples/end_to_end/prospect_scout/prospector.py \
    --companies "Apex BioLabs,Helios Energy"
# RuntimeError: Simulated crash after 1 of 2 companies...

uv run kitaru executions list                                   # find the failed id
PROSPECT_SCOUT_MODEL=test uv run kitaru executions retry <id>   # resume it
```

On the retry the already-researched company returns from its **cached checkpoint
output** — no repeated search, no repeated model tokens — and only the unfinished
one re-runs. `uv run kitaru executions get <id>` shows `research_prospect`
already `completed`, and the dashboard marks it a cache hit.

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
your shortlist and downstream desk routing. **What PydanticAI adds** (not
Kitaru): the answer fails validation and the model is retried **before** anything
reaches `build_shortlist`. **What Kitaru adds on top:** the validated, typed
result is persisted as the checkpoint's artifact, so a retry or replay serves
back *exactly* the value that passed validation rather than re-rolling the dice.

### 4. A human approves — without a pod idling on the clock

**Without it:** a worker pod keeps billing while a human takes an hour — or a day
— to approve. **What Kitaru adds:** `kitaru.wait()` pauses the flow for approval
(the gate you hit in the quickstart); on a remote stack the execution snapshots
its state and **releases compute** while it waits, then resumes in a fresh pod
exactly where it paused.

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
uv run kitaru secrets set prospect-scout-keys \
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
