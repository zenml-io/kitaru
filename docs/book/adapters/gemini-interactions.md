---
description: Make Gemini Interactions API turns replayable and observable with Kitaru checkpoints, including Antigravity managed-agent runs
icon: gem
---

# Gemini Interactions Adapter

The [Google Gemini Interactions API](https://ai.google.dev/) gives you a hosted
interaction runtime: you send a request to Gemini or a Google-managed agent,
Google runs the interaction, and you receive an interaction response.

Kitaru does **not** replace that runtime. It adds an outer durable workflow
boundary around it:

```text
one stable Gemini interaction response = one Kitaru checkpoint
```

That boundary is useful when a Gemini interaction is one step in a larger flow.
Imagine this workflow:

```text
collect input → ask Gemini to analyze it → write report → wait for approval → publish
```

If Gemini finishes the analysis and the later `write report` checkpoint fails,
Kitaru can replay the flow and reuse the completed Gemini interaction result
instead of calling Google again. You keep the interaction ID, output text,
status, usage when available, safe step summaries, and Kitaru artifacts that help
you inspect what happened.

The important word is **stable**. The adapter only saves a successful checkpoint
when Gemini reaches a durable status:

- `completed`: Gemini produced a final response.
- `requires_action`: Gemini reached a handoff point and needs your code, a
  human, or another system to do something before the interaction can continue.

In-progress background states are not saved as successful checkpoints. Poll them
again by interaction ID instead of treating them as finished work.

## The mental model

Think of Gemini Interactions as the **hosted worker** and Kitaru as the **outer
workflow recorder**.

Gemini still runs the inside of the interaction:

```text
Gemini interaction
  ├─ Gemini model or Google-managed agent planning
  ├─ interaction steps
  ├─ built-in tools, web, code execution, or hosted MCP work
  ├─ Antigravity sandbox files and environment reuse
  └─ completed / requires_action response
```

Kitaru records the outside:

```text
@kitaru.flow
  └─ gemini_summary_gemini_interaction checkpoint
      └─ one call to client.interactions.create(...) or .get(...)
```

So Kitaru can honestly say:

> “This Gemini interaction reached a stable response. On replay, I can return
> that saved boundary result without asking Google to run the same interaction
> again.”

Kitaru cannot honestly say:

> “I can rewind to Gemini's third internal step, restore Google's hosted sandbox,
> and replay only the remaining tool work.”

A concrete failure story:

```text
1. Antigravity inspects a repository in a Google-managed environment.
2. Gemini returns a completed interaction with a summary.
3. Kitaru stores that result as a checkpoint.
4. A later Kitaru checkpoint fails while writing your own report.
5. You replay the flow.
```

On replay, Kitaru can return the saved `GeminiInteractionResult` without starting
another Antigravity job. But Kitaru did not snapshot Google's remote sandbox
filesystem. If a file, report, or decision must be durable in your own workflow,
return it from the interaction or write it in a later Kitaru-owned checkpoint.

## What you get

The adapter gives existing Gemini Interactions users:

- one durable Kitaru checkpoint around each stable Gemini interaction response
- replay-skip for completed Gemini turns inside larger Kitaru flows
- a typed `GeminiInteractionResult` with status, interaction IDs, output text,
  model/agent, environment ID when reported, usage, timing, and warnings
- safe step summaries from `interaction.steps` so your flow can respond to
  handoff points without storing raw provider payloads by default. Step
  summaries keep metadata such as type, status, and call IDs; `text_preview` is
  only filled for clearly identified final assistant/model output.
- a clear `requires_action` path for function results and human approval gates
- explicit `cache_identity` support when the same runner/request can point at
  different Google projects, regions, credential aliases, or client setups
- redacted request manifests, output, usage, event-log, and run-summary artifacts
  by default
- opt-in raw provider payload capture when you need deeper debugging
- a convenience Antigravity request preset used by the adapter example

Google's Interactions API is still a preview/Beta-style surface. Treat schemas,
agent names, and hosted-agent behavior as more likely to change than stable
Gemini text-generation APIs.

## Install

Add the Gemini extra. Include `local` if you want the local Kitaru server and
dashboard:

```bash
uv add "kitaru[gemini,local]"
```

Initialize the project once:

```bash
kitaru init
kitaru login        # local server; add a URL for a deployed one
kitaru status
```

For real Google calls, authenticate one of two ways.

Most users set a Gemini API key (the AI Studio / Developer API backend):

```bash
export GEMINI_API_KEY='<your-gemini-api-key>'
# GOOGLE_API_KEY is also accepted by the example wrapper.
```

If your organization blocks raw API keys, use Application Default Credentials
(ADC) through Vertex AI instead. ADC is picked up automatically after you log in
with `gcloud`, so you set the backend, project, and region rather than a key:

```bash
gcloud auth application-default login
export GOOGLE_GENAI_USE_VERTEXAI=true
export GOOGLE_CLOUD_PROJECT='<your-gcp-project-id>'
export GOOGLE_CLOUD_LOCATION=global
```

On Vertex AI the Interactions API currently serves agent interactions (`agent=`,
such as Antigravity), not raw model interactions, and only in the `global`
location. Use `model=` interactions with an API key, and `agent=` interactions
on Vertex. Antigravity does not support `background=True`, so bound longer agent
runs with the provider call timeout rather than background polling.

{% hint style="info" %}
Migrating an existing Gemini Interactions or Antigravity managed-agent project?
The [`zenml-io/kitaru-skills`](https://github.com/zenml-io/kitaru-skills)
package includes `/kitaru:kitaru-gemini-interactions-migration` for wrapping stable
interaction responses and checking polling, `requires_action`, function-result,
and Google-owned-internals boundaries. See
[Agent Skills](../agent-native/claude-code-skill.md).
{% endhint %}

## Minimal flow

Use `GeminiInteractionRequest.start(...)` for a fresh interaction. Exactly one of
`model=` or `agent=` must be set.

```python
from kitaru import flow
from kitaru.adapters.gemini import (
    GeminiInteractionRequest,
    GeminiInteractionResult,
    KitaruGeminiInteractionsRunner,
)

runner = KitaruGeminiInteractionsRunner(
    name="gemini_summary",
    # Set this when the same request/runner name may target different Google
    # projects, regions, credential aliases, or client configurations.
    cache_identity="my-project/us-central1",
)

@flow
def summarize(topic: str) -> GeminiInteractionResult:
    request = GeminiInteractionRequest.start(
        f"Explain {topic} in three plain sentences.",
        model="gemini-3.5-flash",
        metadata={"example": "minimal_gemini_interaction"},
    )
    return runner.run_sync(request)

result = summarize.run("Kitaru checkpoints").wait()
print(result.output_text)
print(result.interaction_id)
```

The checkpoint name is derived from the runner name. In this example, the
adapter-created checkpoint is named:

```text
gemini_summary_gemini_interaction
```

On replay, if that checkpoint is already complete and cache/replay rules allow
reuse, Kitaru serves the saved `GeminiInteractionResult`. Gemini is not called
again for that completed interaction boundary.

## How a run works, step by step

When `runner.run_sync(GeminiInteractionRequest.start(...))` executes inside a
Kitaru flow, this is the concrete sequence:

```text
1. Kitaru opens one synthetic checkpoint.
2. The adapter builds a Gemini Interactions API request.
3. The adapter calls client.interactions.create(...).
4. Gemini runs the hosted interaction.
5. The adapter waits for a stable status: completed or requires_action.
6. It extracts output text, IDs, status, usage, environment, and step summaries.
7. It saves configured artifacts: manifest, output, usage, event log, and summary.
8. It returns GeminiInteractionResult as the checkpoint output.
```

For poll requests, step 3 is `client.interactions.get(...)` instead. That matters
for background jobs: polling an existing interaction checks the job you already
started; it does not create a duplicate remote job.

## Requests and continuation

### Start a new interaction

Use `GeminiInteractionRequest.start(...)` when you want a fresh Gemini turn:

```python
request = GeminiInteractionRequest.start(
    "Draft a customer-support reply for this incident.",
    model="gemini-3.5-flash",
    metadata={"ticket_type": "support"},
)
result = runner.run_sync(request)
```

### Continue an interaction

Google's Interactions API can continue server-side history with
`previous_interaction_id`. When you continue an interaction, re-specify the model
or agent and any tools, system instruction, generation config, or response format
that should apply to the new turn.

```python
follow_up = GeminiInteractionRequest.resume(
    "Now turn that reply into a five-item checklist.",
    previous_interaction_id=result.interaction_id,
    model="gemini-3.5-flash",
)
follow_up_result = runner.run_sync(follow_up)
```

Set `store=True` when you need continuation. If `store=False`, Google should not
be expected to keep enough server-side state for a later
`previous_interaction_id` turn.

Continuation and Kitaru replay are related but different:

| Thing | What it means |
|---|---|
| Gemini `interaction_id` | Google's handle for continuing or polling an interaction. |
| Kitaru checkpoint | Kitaru's durable workflow boundary for skipping completed workflow work. |
| Gemini hosted environment | Google's runtime/sandbox state. Kitaru records IDs and summaries, not a filesystem snapshot. |
| Kitaru artifact | Data Kitaru saved around the boundary for audit/debugging. |

## Antigravity managed-agent runs

`GeminiInteractionRequest.antigravity(...)` is the convenience path for the
Google Antigravity managed-agent preview. It uses the adapter's centralized
Antigravity agent ID, defaults `environment="remote"`, forces `store=True`, keeps
`background=False`, and adds non-sensitive preview metadata.

```python
request = GeminiInteractionRequest.antigravity(
    "Inspect this repository and summarize the main test strategy. Do not edit files."
)
result = runner.run_sync(request)
```

This is still one coarse interaction checkpoint. Kitaru records the stable
Antigravity response and capture envelope. Google still owns the hosted agent
loop, sandbox, web/code/tool execution, and environment reuse.

The runnable adapter example includes Antigravity support so you can exercise
that environment path explicitly:

```bash
uv run python examples/integrations/gemini_interactions_agent/gemini_interactions_adapter.py --mode antigravity
```

Use Antigravity mode intentionally. It may be slower, costlier, and more
preview-shaped than a simple model interaction. If a run is slow, raise the
request timeout; do not switch this preset to `background=True`, because Google's
Antigravity preview rejects background mode.

## `requires_action` and function results

A Gemini interaction can return `status="requires_action"`. That is not a final
answer; it is a durable handoff point.

A typical flow looks like this:

```text
1. Gemini asks for a function result or approval.
2. Kitaru saves the requires_action response as a completed checkpoint boundary.
3. Your flow performs the action, or pauses with kitaru.wait() for a human.
4. Your flow sends a follow-up function_result interaction.
```

The result's `steps` list summarizes the interaction steps so your flow can find
a function call ID or action cue without putting raw prompt/tool payloads into
top-level metadata.

```python
if result.status == "requires_action":
    call = next(step for step in result.steps if step.call_id)
    answer = GeminiInteractionRequest.function_result(
        previous_interaction_id=result.interaction_id,
        function_call_id=call.call_id,
        function_name=call.tool_name,
        function_result={"approved": True},
        model="gemini-3.5-flash",
    )
    result = runner.run_sync(answer)
```

Keep human-in-the-loop waits at flow scope. For example, let the flow inspect the
`requires_action` result, call `kitaru.wait()` if a person must decide, then send
the later `function_result` interaction after the wait resumes. That keeps the
pause visible to Kitaru instead of hiding it inside a provider-owned turn.

## Polling background interactions

For background interactions, avoid accidentally starting duplicate remote jobs.
If an interaction is already created and you need to check it later, use
`GeminiInteractionRequest.poll(interaction_id=...)`. Poll requests call
`client.interactions.get(...)`; they do not call `client.interactions.create(...)`.

```python
poll_request = GeminiInteractionRequest.poll(
    interaction_id="interaction_123",
    timeout_s=30,
)
result = runner.run_sync(poll_request)
```

If a background interaction has not reached `completed` or `requires_action`, the
adapter raises `KitaruRuntimeError` instead of saving an unfinished response as a
successful checkpoint. Continue polling the same `interaction_id`; do not retry
by creating a fresh background request unless you intentionally want another
remote job.

## Cache identity

The cache key uses the request, runner name, Kitaru strategy, installed
`google-genai` SDK version, and optional `cache_identity`. Kitaru does not inspect
live Google client internals such as project, region, or credentials.

That means this situation is risky without `cache_identity`:

```text
same runner name + same request
  ├─ client A points at project/dev
  └─ client B points at project/prod
```

From Kitaru's view, those can look identical. If they should not share replay or
cache behavior, give them different identities:

```python
dev_runner = KitaruGeminiInteractionsRunner(
    name="gemini_summary",
    cache_identity="dev-project/us-central1",
)
prod_runner = KitaruGeminiInteractionsRunner(
    name="gemini_summary",
    cache_identity="prod-project/us-central1",
)
```

Use `cache_identity` whenever project, region, credential alias, endpoint, or
other client configuration changes the meaning of the same logical request. It
must be a stable, non-secret string such as `"project/region"`; do not pass a
live client object or anything whose `repr()` can change between processes.

## Result shape

`KitaruGeminiInteractionsRunner.run_sync(...)` returns a
`GeminiInteractionResult` with fields such as:

- `status`
- `interaction_id`
- `previous_interaction_id`
- `output_text` when Gemini exposes text
- `model` or `agent`
- `environment_id` when Google reports one
- `steps`, a list of `GeminiInteractionStepSummary` records derived primarily
  from `interaction.steps`
- `usage` when reported by the SDK
- `poll_count`, `duration_ms`, `sdk_version`, and non-sensitive `metadata`
- artifact names for captured request manifest, output, usage, event log, and
  run summary
- artifact names for raw input, raw interaction, and raw steps only when those
  captures are explicitly enabled
- `warnings` for best-effort capture or SDK-shape compatibility issues

The adapter treats `interaction.steps` as the primary response shape. If an older
SDK exposes `outputs` instead, Kitaru can summarize those for compatibility and
adds a warning. If the SDK omits `output_text`, Kitaru only derives fallback
output from a clearly identified final assistant/model step. Prompt, tool,
sandbox, or ambiguous timeline text is not merged into `output_text`.

Failed Gemini interactions raise an exception instead of returning a successful
`GeminiInteractionResult`. If the failure happens inside a Kitaru checkpoint, the
adapter records best-effort failure metadata before the exception propagates.

## Capture policy

By default, Kitaru saves the boundary data that is useful for replay inspection
and audits:

- redacted request manifest
- output text
- usage when reported
- event log
- run summary
- safe step summaries on the returned result. Their `text_preview` field is
  disabled for prompt, tool, sandbox, and ambiguous timeline content by default.

It does **not** save raw prompts, raw interaction payloads, or raw step payloads
unless you opt in.

```python
from kitaru.adapters.gemini import GeminiInteractionCapturePolicy

runner = KitaruGeminiInteractionsRunner(
    name="gemini_summary",
    capture=GeminiInteractionCapturePolicy(
        # Raw prompt/provider payload capture is opt-in.
        save_input=False,
        save_raw_interaction=False,
        save_steps=False,
        # These remain enabled by default.
        save_output=True,
        save_usage=True,
        redact_request_manifest=True,
    ),
)
```

Turn on raw capture only when you need it for debugging or audit review:

```python
runner = KitaruGeminiInteractionsRunner(
    name="debug_gemini_run",
    capture=GeminiInteractionCapturePolicy(
        save_input=True,
        save_raw_interaction=True,
        save_steps=True,
    ),
)
```

Treat raw provider payloads as conversation data. They may contain prompts,
retrieved snippets, tool arguments, model output, or other sensitive material.
The redacted request manifest removes common secret-like keys such as API keys,
authorization headers, tokens, credentials, cookies, and passwords, but raw
payload artifacts are not a secret store.

Capture failures are non-fatal by default because Google may already have
succeeded by the time Kitaru tries to save artifacts. Retrying after a strict
capture failure can duplicate provider-side work, so only enable
`fail_on_artifact_capture_error=True` or `fail_on_event_persistence_error=True`
when you understand that trade-off.

## Checkpoint strategy

The Gemini Interactions adapter currently supports one public strategy:

```python
KitaruGeminiInteractionsRunner(
    name="gemini_task",
    checkpoint_strategy="interaction",
)
```

`"interaction"` is also the default. It means one stable Gemini interaction
response becomes one Kitaru checkpoint.

Strategies such as per-step, per-tool, web-call, code-execution, hosted-MCP, or
Antigravity-internal checkpointing are not exposed because Kitaru does not own
those call bodies. Google runs them inside its hosted interaction runtime. The
adapter would be lying if it claimed it could replay those internals
independently.

A future client-tool mode may be possible for tools that your Python process
executes itself: Gemini reaches `requires_action`, Kitaru runs the local tool
body, and a later interaction sends the matching function result. That would
only cover the local tool body Kitaru actually runs. It would still not make
Google-owned managed-agent internals replayable.

## Recommended durability pattern

Put the Gemini runner call directly in the flow body so the adapter can create
its own checkpoint around the interaction. Put side effects that must be durable
in separate Kitaru checkpoints after Gemini returns.

```python
from pathlib import Path

import kitaru

@kitaru.checkpoint
def write_report(text: str, path: str) -> str:
    Path(path).write_text(text)
    return path

@kitaru.flow
def report_flow(topic: str) -> str:
    request = GeminiInteractionRequest.start(
        f"Write a short report about {topic}.",
        model="gemini-3.5-flash",
    )
    gemini_result = runner.run_sync(request)
    return write_report(gemini_result.output_text or "", "report.md")
```

That gives you a concrete sequence:

```text
flow body asks Gemini
  -> adapter-created Gemini interaction checkpoint completes
  -> Kitaru-owned write_report checkpoint writes a durable file
```

If a later checkpoint fails, Kitaru can reuse the completed Gemini interaction
result instead of calling Google again.

## Constraints and gotchas

- **Stable statuses only.** The adapter only saves successful checkpoints for
  `completed` and `requires_action`. Background work that is still running should
  be polled again by interaction ID.
- **Coarse durability.** One Gemini interaction response is the replay unit.
  Google-owned internal steps, hosted tools, hosted MCP work, web/code execution,
  and Antigravity sandbox mutations are not separate Kitaru checkpoints.
- **`requires_action` is a handoff point.** Use it to move work back into your
  flow: run local code, call `kitaru.wait()` for a human, then send a matching
  `function_result` interaction.
- **Use `cache_identity` for cross-client disambiguation.** If the same runner
  name and request can point at different projects, regions, credential aliases,
  endpoints, or client setups, set an explicit identity.
- **Raw provider capture is off by default.** Safe summaries and redacted
  manifests are captured by default; raw prompts/interactions/steps require an
  explicit opt-in.
- **Antigravity environment support is adapter-example level.** The adapter gives
  you a preset for the Google-managed Antigravity environment path, but Google
  still owns the remote environment lifecycle and behavior.

## Runnable example

Run the educational integration example:

```bash
uv sync --extra local --extra gemini
uv run kitaru init
export GEMINI_API_KEY='<your-gemini-api-key>'
uv run python examples/integrations/gemini_interactions_agent/gemini_interactions_adapter.py
```

To inspect the example without making a Gemini API call:

```bash
uv run python examples/integrations/gemini_interactions_agent/gemini_interactions_adapter.py --help
```

The example includes:

- `--help` for smoke tests
- `--dry-run` for a no-network preview
- `--mode model` using `gemini-3.5-flash`
- `--mode antigravity` as an explicit slower/costlier managed-agent demo

## Troubleshooting

### “Why did replay not resume inside Antigravity's internal work?”

Because Kitaru's boundary is the stable Gemini interaction response. Antigravity
internal planning, sandbox files, hosted tools, and environment reuse happen
inside Google's runtime. Kitaru can reuse the saved result of a completed
interaction; it cannot restore an arbitrary midpoint inside Google's hosted
agent loop.

### “Why did a background interaction raise instead of saving a checkpoint?”

It had not reached `completed` or `requires_action` yet. Poll the same
`interaction_id` again with `GeminiInteractionRequest.poll(...)`. Do not create a
fresh background interaction unless you deliberately want another Google job.

### “Why do I need `cache_identity`?”

Kitaru can see the request and runner name, but it does not inspect live Google
client internals. If two clients use the same logical request but point at
different projects, regions, credentials, or endpoints, `cache_identity` tells
Kitaru those are different replay/cache worlds.

### “Where are the raw Gemini payloads?”

They are off by default. Enable `save_input=True`, `save_raw_interaction=True`,
or `save_steps=True` in `GeminiInteractionCapturePolicy` when you deliberately
want raw provider artifacts.

### “Can I checkpoint every Gemini internal step?”

Not in this adapter version. Kitaru only checkpoints work it can replay honestly.
Gemini-hosted internal steps are observations from Kitaru's point of view, not
Python call bodies Kitaru can rerun independently.

## Related guides

<table data-view="cards"><thead><tr><th></th><th></th><th data-hidden data-card-target data-type="content-ref"></th></tr></thead><tbody><tr><td><strong>Replay and overrides</strong></td><td>Re-run a flow with cached outputs for completed checkpoints</td><td><a href="../guides/replay-and-overrides.md">../guides/replay-and-overrides.md</a></td></tr><tr><td><strong>Wait, Input, and Resume</strong></td><td>How paused flows are resolved from Python, CLI, and MCP</td><td><a href="../guides/wait-and-resume.md">../guides/wait-and-resume.md</a></td></tr><tr><td><strong>Artifacts</strong></td><td>Save named outputs from Kitaru-owned checkpoints</td><td><a href="../guides/artifacts.md">../guides/artifacts.md</a></td></tr></tbody></table>
