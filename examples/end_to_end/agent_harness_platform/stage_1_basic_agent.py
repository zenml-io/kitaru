"""Stage 1 — a durable PydanticAI agent.

The stage 1 hero demo: pydantic-ai gives the agent loop, kitaru gives
durable execution. Together: durable agents without learning a graph DSL
or rewriting your control flow as a state machine.

The flow runs the agent **twice** with a deliberate failure point between
the two turns — two checkpoints, both LLM-heavy:

    1. `default`     — first agent turn: investigate the machine
    2. `default_2`   — second agent turn: build a follow-up summary based
                       on what the first turn found

The two-step tour to feel durability:

    # 1. Run with FORCE_FAILURE=1. The first agent turn completes (~15s,
    #    real LLM + tool calls) and is checkpointed. The flow body raises
    #    before the second turn even starts.
    FORCE_FAILURE=1 python stage_1_basic_agent.py

    # 2. Re-run without the flag. kitaru sees `default` in cache (instant,
    #    no LLM call), runs the second turn fresh (~10s of new LLM work),
    #    and returns the combined result.
    python stage_1_basic_agent.py

Without kitaru, step 1's failure would have wasted the first turn's work
and you'd pay for *both* turns on the retry. With kitaru, only the work
that didn't complete the first time gets re-paid for.
"""

import os
import time

from agent_harness_platform.agent import build_agent
from agent_harness_platform.profile import (
    Profile,
)

import kitaru
from kitaru.adapters.pydantic_ai import KitaruAgent

# Toggle: simulate a transient failure between the two agent turns.
# Real platform engineers' failures come from the network / filesystem /
# database — this just stands in for any of those.
FORCE_FAILURE = bool(os.environ.get("FORCE_FAILURE"))

# Toggle: disable kitaru's checkpoint cache for this invocation. Useful
# when developing the flow itself — set DISABLE_CACHE=1 to force every
# checkpoint to re-execute regardless of prior cached outputs.
DISABLE_CACHE = bool(os.environ.get("DISABLE_CACHE"))

DEFAULT_PROFILE = Profile(
    name="default",
    system_prompt=(
        "You are a helpful assistant with a single tool: `exec`, which runs "
        "shell commands in the host process. Use it to investigate questions "
        "the user asks. Explain what you find concisely."
    ),
    model="openai:gpt-5-nano",
    allowed_tools={"exec"},
)


@kitaru.flow
def agent_harness_platform_flow() -> str:
    # The kitaru ↔ pydantic-ai integration seam, in plain sight: build_agent()
    # returns a vanilla pydantic-ai Agent; KitaruAgent wraps it for durable
    # execution. Each agent.run_sync() below becomes its own kitaru checkpoint.
    agent = build_agent(DEFAULT_PROFILE)
    # Turn mode (`granular_checkpoints=False`) for log-clarity in this
    # tour: the stage's hero is the cross-run cache hit, and a single
    # `default` / `default_2` checkpoint per turn keeps the trace
    # readable. Drop the kwarg (or pass `True`) in production for
    # per-model-call / per-tool-call cache hits and finer dashboard rows.
    agent = KitaruAgent(agent, granular_checkpoints=False)

    # Checkpoint 1 — first turn, expensive (multiple LLM + tool calls).
    investigation = agent.run_sync(
        "Investigate this machine: what's the OS, the kernel version, the "
        "current user, and how many processes are running? Use one shell "
        "command per question."
    ).output

    if FORCE_FAILURE:
        raise RuntimeError(
            "Simulated downstream blip between the two agent turns. The "
            "first turn is cached — re-run without FORCE_FAILURE and "
            "kitaru will skip it."
        )

    # Checkpoint 2 — second turn, also expensive.
    summary = agent.run_sync(
        f"Earlier you found:\n\n{investigation}\n\n"
        "Now: write a one-paragraph summary for someone who's never seen "
        "this machine before. No new shell commands needed."
    ).output

    final = f"# Investigation\n\n{investigation}\n\n# Summary\n\n{summary}"
    print(f"\n{final}\n")
    return final


if __name__ == "__main__":
    started = time.perf_counter()
    try:
        agent_harness_platform_flow.run(cache=False if DISABLE_CACHE else None)
        print(f"[took {time.perf_counter() - started:.1f}s]")
    except Exception as exc:
        print(f"\nFlow failed after {time.perf_counter() - started:.1f}s: {exc}")
