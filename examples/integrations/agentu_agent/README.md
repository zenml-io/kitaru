# agentu integration example

This directory shows how to make an existing [agentu](https://github.com/hemanth/agentu)
workflow durable and forkable with Kitaru — no adapter and no changes to the
agents themselves.

agentu chains agents into workflows (`>>` sequential, `&` parallel) and its
sessions support `checkpoint(fork=True)` to A/B-test a conversation from a
decision point. Kitaru applies that fork idea to the whole workflow: every
step of a run is recorded as a durable checkpoint, and a finished run can be
replayed ("forked") from any step — across processes, machines, and days —
with edited step outputs or a different model, without re-running or
re-paying for the steps before it.

## Getting started

```bash
cd examples/integrations/agentu_agent
uv sync --extra local
uv pip install agentu
uv run kitaru init
```

agentu talks to any OpenAI-compatible API and defaults to a local Ollama
server — the example keeps that default. To use a hosted provider instead,
and to enable the model-swap fork, set:

```bash
export AGENTU_API_BASE=https://api.openai.com/v1
export AGENTU_API_KEY=sk-...
export AGENTU_MODEL=gpt-4o
export AGENTU_ALT_MODEL=gpt-4o-mini   # optional: enables Fork 2
```

Anthropic's OpenAI-compatible endpoint works too — set
`AGENTU_API_BASE=https://api.anthropic.com/v1` with an `sk-ant-...` key and
models such as `claude-haiku-4-5-20251001` / `claude-sonnet-4-5`. (Claude 5
models currently reject agentu's fixed `temperature` parameter, so stick to
the 4.x family here.)

Then run:

```bash
uv run python agentu_record_replay.py
```

The example uses your current Kitaru connection context. If you want the run
to use a deployed Kitaru server, connect first with `kitaru login <server>`
and confirm with `kitaru status`.

## agentu_record_replay.py

The scenario mirrors agentu's own workflow example: researcher agents fan out
over three topics in parallel, an analyst compares the findings, and a writer
drafts an executive report. The script demonstrates four things:

1. **Durable checkpoints.** Each agentu call — `infer()` tool-calling turns
   for the researchers, `stream()` drafting for the analyst and writer — runs
   inside a `@kitaru.checkpoint`; the researcher checkpoints run concurrently
   via `.submit()`.
2. **Inner-call tracing.** The `KitaruTrace` middleware plugs into agentu's
   standard middleware pipeline (`agent.use(...)`) and calls `kitaru.save()` /
   `kitaru.log()` on every model round-trip. After a run, each checkpoint
   carries `<label>_llm_<n>_prompt` and `<label>_llm_<n>_response` artifacts
   plus per-call latency metadata — the dashboard shows what happened
   *inside* each agent turn.
3. **Fork 1 — counterfactual.** The finished run is replayed from the
   `write_report` checkpoint with the analyst's recorded conclusion swapped
   for a contrary take. The researchers and analyst are *not* re-executed;
   only the writer re-runs, against facts that never happened.
4. **Fork 2 — model swap** (needs `AGENTU_ALT_MODEL`). The writer step is
   replayed on a different model with byte-identical recorded inputs: a true
   A/B comparison of models on one step, rather than two fresh runs that may
   have taken different tool paths.

Afterwards, inspect the runs:

```bash
kitaru executions list
```

All executions (source and forks) appear, and the artifact view shows the
traced inner LLM prompts and responses under each checkpoint.

See the [examples overview](../../README.md) for more Kitaru examples.
