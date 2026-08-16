---
description: No adapter for your framework? Record directly against the API, generate a project-local adapter, or skip the adapter entirely and let Kitaru call your agent.
icon: screwdriver-wrench
---

# No adapter for your framework

Kitaru ships adapters for a handful of frameworks. If yours isn't one of
them you are not stuck, and you do not have to wait for us. There are
three ways forward, and the right one depends on how much control you
have over the agent's code.

| Your situation | Do this |
|---|---|
| You can wrap the agent, and want native recording | [Build a project-local adapter](#build-a-project-local-adapter) |
| You already emit traces somewhere | [Import instead](../guides/importing-sessions.md) — no adapter needed |
| You cannot or will not change the agent's code | [Let Kitaru call your agent](#let-kitaru-call-your-agent) |

Importing is the cheapest of the three and the one to reach for first. An
imported session replays and evaluates exactly like a recorded one, so
"no adapter" costs you nothing except live recording — see
[Import any trace format](../guides/importing-sessions.md).

## Build a project-local adapter

An adapter is not a privileged plugin. It is ordinary code that calls the
recording API, and it can live in your repository forever — there is no
requirement to contribute it upstream.

The job of an adapter is narrow: observe the seams your framework already
exposes, and write each model request and tool call as a node on a
session. What makes an adapter *honest* is that it reports what it can
and cannot see. A wrapper that silently misses nested tool calls is worse
than one that declares the gap.

The fastest path is the `kitaru-adapter-builder`
[agent skill](../agent-native/skills.md), which is built for exactly this:

```bash
npx skills add zenml-io/kitaru-skills
```

Point your coding assistant at it and it will build the smallest adapter
that works inside your project, in Python or TypeScript, and tell you
what it observed and what it could not. It deliberately preserves your
framework's public entrypoint rather than replacing it, and it finishes
locally — nothing is registered until you approve it.

Two rules worth keeping whichever way you build it:

* **Wrap the public entrypoint, change nothing else.** The shipped
  adapters do not recompile graphs, replace checkpointers, or alter
  results. Yours should not either — a recording that changes behavior
  is not a recording.
* **Recording and replaying are one wrapper, not two.** The same code
  that records must apply the override at the model boundary and answer
  tool calls per the [tool policy](../guides/tool-policies.md) during a
  replay. Splitting them is how baselines stop reproducing.

Read the [PydanticAI adapter](pydantic-ai.md) as the reference
implementation, and the [LangGraph capability matrix](langgraph.md#capability-matrix)
for how to express partial support honestly.

## Let Kitaru call your agent

<!-- TODO(v2-launch): function agent runs land with #698
     (feature/replays-without-adapter), which is still a draft. Confirm it
     merged before publish; if it slips, cut this section. -->

Sometimes wrapping is not on the table. The agent runs inside a service
you don't control, or in another language, or behind a queue. For that
there is a third mode: register the agent version as a **function** and
Kitaru asks *your* system to run it.

An agent version's run spec is one of two shapes. The familiar one is a
command Kitaru executes on a worker:

```python
CommandRunSpec(command="python support.py", timeout_seconds=3600)
```

The other points at a Python function, as `module:attribute`:

```python
FunctionRunSpec(
    entrypoint="my_service.kitaru_hooks:run_replay",
)
```

The flow is inverted, and that is the point:

1. Kitaru calls your function instead of running your agent itself.
2. Your function runs the agent however your system already runs it —
   your process, your language, your infrastructure.
3. It returns the **external id** of the session that run produced.
4. Kitaru creates a placeholder session in `pending_import` status
   against that id.
5. When you import the trace for that external id, it **adopts the
   placeholder**. Evaluations run against the imported content, and the
   replay and experiment settle as normal.

There is no deadline on step 5. Until the import arrives the replay stays open, visible as a pending replay whose job is still running, and canceling the replay releases it.

The trade-off is honest: you get replay and experiments with no adapter
and no code change inside the agent, but the fidelity of the replay is
the fidelity of whatever your system exports. Kitaru cannot substitute a
tool result it was never shown.

## Which to choose

If you can wrap the agent, wrap it — native recording sees the most and
needs the least from you. If you cannot, import; it is a first-class path
and most of Kitaru works identically on imported sessions. Reach for
function runs when the agent is genuinely out of reach and you still want
its runs inside experiments.
