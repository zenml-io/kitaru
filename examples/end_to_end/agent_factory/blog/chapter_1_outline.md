# Chapter 1 — Building blocks for your internal agent factory

**Status:** outline only (writing pass after all stages land)
**Stage file:** `stage_1_basic_agent.py`
**Length target:** ~1500–2000 words, one hero screenshot, runnable code
**Audience:** platform engineers building their org's internal agent platform

---

## The thesis (the chapter's reason to exist)

> **Pydantic-ai gives you an agent loop. Kitaru gives you durable execution. Together: durable agents without learning a graph DSL or rewriting your control flow as a state machine.**

Both halves of the integration earn their keep on page one. By the end of the post the reader has:

- A 30-line agent that runs end-to-end
- A working `kitaru executions resume` demo (kill the process mid-run, resume from where it stopped)
- An understanding of why this is the foundation for an internal agent factory

## Opening hook (~150 words)

Two-paragraph framing:

1. **The problem.** Your team is shipping LLM features. Each one wants its own agent: a researcher, a triage bot, a code reviewer. Every team reinvents the same plumbing — retries, observability, HITL, deployment. You want a *factory*: one foundation, many agents, fast spin-up.
2. **The integration.** Pydantic-ai is the agent-loop layer the team should use; it has typed tools, model abstraction, MCP support, structured outputs. Kitaru is the durability layer; it makes every model call, tool call, and HITL pause persistent and replayable. Combined, you get durable agents without writing a state machine.

## The 30-line agent (~400 words + code)

Walk through `stage_1_basic_agent.py` top to bottom. The five files in `agent_factory/` are tiny — show each one inline:

- `Profile` (Pydantic model: name, system_prompt, model, allowed_tools)
- `PermissionHandler` (gate every tool against the profile)
- `build_tools(permission_handler)` returns the pydantic-ai toolset filtered by `allowed_tools`
- `build_agent(profile)` wires Profile → pydantic-ai `Agent` → `KitaruAgent`
- The flow: `@kitaru.flow def agent_factory_flow(prompt: str) -> str` runs the agent, returns the output

Total ~120 lines of library code, ~30 lines of stage file.

Run it once:

```bash
$ python stage_1_basic_agent.py
Kitaru: Starting flow `agent_factory_flow`.
Kitaru: Stack: local
Kitaru: Checkpoint `default` started.
Kitaru: HTTP Request: POST https://api.openai.com/v1/chat/completions "HTTP/1.1 200 OK"
Kitaru: HTTP Request: POST https://api.openai.com/v1/chat/completions "HTTP/1.1 200 OK"
Kitaru: Checkpoint `default` finished in 11.2s.
Kitaru: Flow completed.

In `/etc/hosts`, the configured hostnames are:
- localhost (mapped to 127.0.0.1 and ::1)
- broadcasthost (mapped to 255.255.255.255)
```

Annotate the log lines: each `Checkpoint started/finished` line is a kitaru durability boundary; each `HTTP Request` is the agent's model call going through pydantic-ai → KitaruAgent (which tracks it as a child event).

## The hero demo: kill-and-resume (~400 words)

This is the chapter's payoff. Three steps, all in the terminal.

```bash
# 1. Run with a longer prompt that needs multiple tool calls
$ python stage_1_basic_agent.py
# (running... watch a few HTTP requests, kill it before it finishes)
$ kill %1

# 2. The execution is now orphaned
$ kitaru executions list
ID         FLOW                  STATUS
abc123     agent_factory_flow    running (orphaned)

# 3. Resume picks up exactly where it stopped
$ kitaru executions resume abc123
Kitaru: Checkpoint `default` cached (resuming from where the previous run stopped)
Kitaru: Flow completed.
```

What just happened, in three sentences: kitaru persisted every checkpoint output as the run progressed; the kill left an orphaned execution; resuming re-runs from the last incomplete checkpoint. Pydantic-ai's agent loop didn't need to know about any of it.

The competing alternatives — hand-rolled state machine, LangGraph nodes, raw pydantic-ai with manual checkpointing — would each take 50–500 lines to get the same property. We got it from a one-line wrap.

## Why turn mode (the default), not granular (~200 words)

The kitaru pydantic-ai adapter has two checkpoint modes: **turn** (default — one checkpoint per `agent.run_sync()`) and **granular** (one checkpoint per LLM/tool/MCP call). Stage 1 uses turn mode.

- Turn mode → kill mid-turn → resume re-runs the whole turn (re-pays the LLM cost for the calls that already completed). Simple. One artifact per run.
- Granular mode → kill mid-turn → resume serves completed calls from cache, picks up at the next incomplete one. More cache fidelity. Costs an explicit `kitaru.save(..., type="output")` to disambiguate the flow's terminal output.

For a chapter-1 agent with 1–3 tool calls, the turn-mode trade-off is fine — the demo still lands ("kill it, resume it") and the code stays simple. Later chapters with longer agent runs introduce granular mode where it earns its keep.

## What this chapter does NOT cover (~150 words)

Tease the next 7 chapters in one paragraph each:

- **Chapter 2:** the `exec` tool runs *in your host process* right now. That's fine for a tutorial. Production agents need a sandbox so they can't `rm -rf /`. Docker worker + persistent shell + workspace volumes.
- **Chapter 3:** the agent has no credentials yet. When it does (chapter 5+ services), you'll want them isolated from the worker so a prompt-injected agent can't exfiltrate them. Two-process credential isolation via mitmproxy.
- **Chapter 4:** the agent's behavior is hardcoded in the system prompt right now. Real agents have *playbooks* — operator-edited markdown files — that describe their procedure.
- **Chapters 5–7:** typed services, HITL, cross-execution memory.
- **Chapter 8:** when you need to re-run an agent against updated context without re-asking the user for their preferences. Replay with checkpoint output overrides.

## Closing (~100 words)

Recap: the foundation is in place. Pydantic-ai for the agent loop, kitaru for durability, Profile as the per-agent unit. From here, every chapter introduces a capability your platform needs and shows how kitaru's primitives give it to you for free.

Link forward to chapter 2 ("Your agents need a sandbox").

---

## Hero artifact

The kill-and-resume terminal screenshot. Two side-by-side panels:
- Left: the killed run + `kitaru executions list` showing it as orphaned
- Right: `kitaru executions resume <id>` completing the run from the cache

## Code excerpts to include verbatim

- All of `stage_1_basic_agent.py` (~30 lines)
- The `Profile` Pydantic model (~10 lines)
- The flow body (~5 lines)

## Open questions / things to nail down before writing

- The exact `kitaru executions resume` UX (does it stream, does it pick up after the resume completes?)
- Whether the post should mention granular mode at all or defer entirely to a later chapter

---

*This outline is a planning artifact. The full post gets written in a separate pass once all 8 chapters' stages are working end-to-end so the cross-chapter narrative is consistent.*
