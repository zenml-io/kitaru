---
description: "No adapter for your framework? Import the traces you already have, or generate a project-local adapter with the adapter-builder skill."
icon: screwdriver-wrench
---

# No adapter for your framework

Kitaru ships adapters for a handful of frameworks. If yours isn't one of them you are not stuck, and you do not have to wait for us. There are two ways forward:

| Your situation | Do this |
|---|---|
| You already emit traces somewhere | [Import instead](../guides/importing-sessions.md); no adapter needed |
| You want native recording, or replay | [Build a project-local adapter](#build-a-project-local-adapter) |

Importing is the cheaper path and the one to reach for first. An imported session inspects, investigates, and evaluates exactly like a recorded one, so "no adapter" costs you nothing on the review side. The adapter earns its keep at replay: experiments re-run your agent's code, and the adapter is what applies overrides and answers tool calls from the recording. If you plan to run experiments, you will build one eventually; import your backlog now and let the adapter come with that step.

## Build a project-local adapter

An adapter is not a privileged plugin. It is ordinary code that calls the recording API, and it can live in your repository forever. There is no requirement to contribute it upstream.

The job of an adapter is narrow: observe the seams your framework already exposes, and write each model request and tool call as a node on a session. What makes an adapter *honest* is that it reports what it can and cannot see. A wrapper that silently misses nested tool calls is worse than one that declares the gap.

You are not meant to write it by hand. The `kitaru-adapter-builder` [agent skill](../agent-native/setup.md) is built for exactly this:

```bash
npx skills add zenml-io/kitaru-skills
```

Point your coding assistant at it and it will build the smallest adapter that works inside your project, in Python or TypeScript, and tell you what it observed and what it could not. It deliberately preserves your framework's public entrypoint rather than replacing it, and it finishes locally: nothing is registered until you approve it.

Two rules worth keeping whichever way you build it:

* **Wrap the public entrypoint, change nothing else.** The shipped adapters do not recompile graphs, replace checkpointers, or alter results. Yours should not either: a recording that changes behavior is not a recording.
* **Recording and replaying are one wrapper, not two.** The same code that records must apply the override at the model boundary and answer tool calls per the [tool policy](../guides/tool-policies.md) during a replay. Splitting them is how baselines stop reproducing.

Read the [PydanticAI adapter](pydantic-ai.md) as the reference implementation, and the [LangGraph capability matrix](langgraph.md) for how to express partial support honestly.

## Your agent is a CLI harness

If your production agent is a coding harness such as Claude Code or the Gemini CLI rather than code you wrote, the same two paths apply, and the import one works today with no changes to how the harness runs: export its session logs and [convert them to Kitaru JSONL](../guides/custom-importer.md), and you get inspection, investigations, and evaluators over everything the harness did.

For replay and experiments, the adapter wraps the harness invocation the same way the shipped adapters wrap a framework entrypoint: it runs the CLI with the recorded input and applies the experiment's overrides. The `kitaru-adapter-builder` skill drafts that wrapper too. One honest limit: a coding harness keeps a lot of state outside the trace (the repository it edited, the sandbox it ran in), and Kitaru can only show and replay what the session recorded.

## Which to choose

If you can wrap the agent, wrap it: native recording sees the most and needs the least from you. If you cannot yet, import; most of Kitaru works identically on imported sessions, and the adapter can come later, with your first experiment.
