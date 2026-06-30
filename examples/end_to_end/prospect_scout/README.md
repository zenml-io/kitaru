# Prospect scout — a durable, agentic prospecting sweep

A sales-intelligence sweep, the kind a staffing or B2B sales team runs before
outreach: for each target company an **agent researches the web** (with a
`search_web` tool it controls), classifies it against the team's lines of
business, and judges whether it's worth pursuing. A human approves the
shortlist, then outreach is drafted.

A sweep is a long row of paid search and model calls. The point of the example
is what happens when it dies halfway through: **you don't redo the work you
already paid for.**

## Quickstart: crash it, then resume it

Runs entirely on PydanticAI's test model and bundled search fixtures — no OpenAI
or Exa account needed. Every command uses `uv run`.

```bash
# 1. Install and start a local Kitaru server (records runs + gives you a dashboard)
uv sync --extra local --extra pydantic-ai
uv run kitaru init
uv run kitaru login

# 2. Run a sweep that crashes after the first of two companies
PROSPECT_SCOUT_MODEL=test PROSPECT_SCOUT_CRASH_AFTER=1 uv run python \
    examples/end_to_end/prospect_scout/prospector.py \
    --companies "Apex BioLabs,Helios Energy"
# -> RuntimeError: Simulated crash after 1 of 2 companies...

# 3. Resume it — the finished company comes back from cache
uv run kitaru executions list                                   # grab the failed id
PROSPECT_SCOUT_MODEL=test uv run kitaru executions retry <id>
```

On the retry, `research_prospect` for the first company returns from its **cached
checkpoint** — no repeated search, no repeated model call — and only the second
company is researched. The run then **pauses at the approval gate** and waits;
approve it from a second terminal:

```bash
uv run kitaru executions input <id> --value true   # or false to reject
```

It resumes, drafts outreach, and prints the report. Open the **`Execution URL`**
the run printed to see each company's checkpoint and the individual `search_web`
calls the agent chose to make.

> The retry re-imports the module, so run it with the same model config as your
> first run — here, `PROSPECT_SCOUT_MODEL=test` (or export `OPENAI_API_KEY`).

## Run it for real

```bash
export OPENAI_API_KEY=sk-...
export EXA_API_KEY=...        # optional; falls back to search fixtures if unset
uv run python examples/end_to_end/prospect_scout/prospector.py \
    --companies "Acme Robotics,Initech,Globex"
```

Without `--companies` it runs the bundled list of eight.

## What you just saw

- **Durable and cheap.** Each company is its own `@checkpoint`. A crash loses
  only the company in flight; retry serves the rest from cache, so you don't pay
  twice for search or tokens.
- **A real agent.** The qualifier decides which searches to run (hiring, funding,
  layoffs) and how many — nothing is pre-fetched. Through `KitaruAgent`, every
  `search_web` call is recorded under its checkpoint, so a wrong verdict is
  debuggable: you can see whether the search or the reasoning was at fault.
- **Type-safe outputs.** The agent must return a `LineOfBusiness` and a
  `FitLevel` enum. A bad classification fails PydanticAI validation and is
  retried before it can corrupt the shortlist, and the validated value is what
  Kitaru persists.
- **Human-in-the-loop without idle cost.** `kitaru.wait()` is the approval gate.
  On a remote stack it snapshots and releases compute while it waits, then
  resumes in a fresh pod — no worker billing while a human takes a day to answer.

## Deploy on your stack

Locally this runs on Kitaru's default stack. To run it on your own
infrastructure — Kubernetes, Vertex, SageMaker, AzureML — point Kitaru at a
[stack](https://docs.zenml.io/kitaru/stacks) (a stack is where your flow's
execution actually runs) and deploy the flow onto it:

```bash
uv run kitaru deploy \
    examples/end_to_end/prospect_scout/prospector.py:prospect_scout --stack <your-stack>
uv run kitaru invoke prospect_scout --input '{"companies": ["Acme Robotics", "Initech"]}'
```

See [Deploy and Invoke Flows](https://docs.zenml.io/kitaru/guides/deployments)
for the full path.

One deployment detail worth knowing: the checkpoint pods can't read your shell,
so store provider keys in a Kitaru secret and attach it to the image.

```bash
uv run kitaru secrets set prospect-scout-keys --OPENAI_API_KEY=sk-... --EXA_API_KEY=...
```

```python
PROSPECTOR_IMAGE = ImageSettings(
    requirements=["pydantic-ai-slim[openai]>=1.102.0,<1.104"],
    secret_environment_from=["prospect-scout-keys"],
)
```

The agents are built inside the checkpoints (via `new_qualifier()` /
`new_outreach_writer()`), not at module scope: constructing `Agent("openai:...")`
reads `OPENAI_API_KEY` immediately, and the pod imports this module *before* the
secret is applied, so a module-scope agent would crash at import.
