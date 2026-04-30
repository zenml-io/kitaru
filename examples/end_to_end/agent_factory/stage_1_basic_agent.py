"""Stage 1 — a durable PydanticAI agent.

The chapter 1 hero demo: pydantic-ai gives the agent loop, kitaru gives
durable execution. Together: durable agents without learning a graph DSL
or rewriting your control flow as a state machine.

The flow runs the agent **twice** with a deliberate failure point between
the two turns — three checkpoints total, both LLM-heavy turns plus a
join step that surfaces the final result:

    1. `default`     — first agent turn: investigate the machine
    2. `default_2`   — second agent turn: build a follow-up summary based
                       on what the first turn found
    3. `join_turns`  — final checkpoint; joins both turns and prints
                       the combined result. The print lives here (not
                       in __main__) because `agent.run_sync().output`
                       strips kitaru's artifact link, leaving multiple
                       terminal checkpoints that `handle.wait()` can't
                       disambiguate.

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

from agent_factory.agent import build_agent
from agent_factory.profile import (
    Profile,
)

import kitaru
from kitaru.adapters.pydantic_ai import KitaruAgent

# Toggle: simulate a transient failure between the two agent turns.
# Real platform engineers' failures come from the network / filesystem /
# database — this just stands in for any of those.
FORCE_FAILURE = bool(os.environ.get("FORCE_FAILURE"))

DEFAULT_PROFILE = Profile(
    name="default",
    system_prompt=(
        "You are a helpful assistant with a single tool: `exec`, which runs "
        "shell commands in the host process. Use it to investigate questions "
        "the user asks. Explain what you find concisely."
    ),
    model="openai:gpt-5.4-nano",
    allowed_tools={"exec"},
)


@kitaru.checkpoint
def join_turns(investigation: str, summary: str) -> str:
    """Final checkpoint that joins both agent turns and prints the result.

    The agent's `.output` strips the artifact link, so kitaru can't see
    `default` or `default_2` as upstream of this step in its artifact
    graph — `handle.wait()` would still call extraction ambiguous. We
    sidestep that by printing the final result here, in a real
    @kitaru.checkpoint, then returning it.
    """
    final = f"# Investigation\n\n{investigation}\n\n# Summary\n\n{summary}"
    print(f"\n{final}\n")
    return final


@kitaru.flow
def agent_factory_flow() -> str:
    # The kitaru ↔ pydantic-ai integration seam, in plain sight: build_agent()
    # returns a vanilla pydantic-ai Agent; KitaruAgent wraps it for durable
    # execution. Each agent.run_sync() below becomes its own kitaru checkpoint.
    agent = build_agent(DEFAULT_PROFILE)
    agent = KitaruAgent(agent)

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

    # Checkpoint 3 — join both turns into the flow's terminal output.
    return join_turns(investigation, summary)


if __name__ == "__main__":
    started = time.perf_counter()
    try:
        agent_factory_flow.run()
        print(f"[took {time.perf_counter() - started:.1f}s]")
    except Exception as exc:
        print(f"\nFlow failed after {time.perf_counter() - started:.1f}s: {exc}")
