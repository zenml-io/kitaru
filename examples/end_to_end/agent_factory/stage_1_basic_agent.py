"""Stage 1 — a durable PydanticAI agent.

The chapter 1 hero demo: pydantic-ai gives the agent loop, kitaru gives
durable execution. Together: durable agents without learning a graph DSL
or rewriting your control flow as a state machine.

This script runs the flow twice:

    Run 1: FORCE_FAILURE=True. The agent does real work (multiple LLM +
           tool calls, ~20s). After the agent's turn completes and its
           checkpoint is persisted, the flow body raises a simulated
           failure. The run is `failed`, but the agent's work is saved.

    Run 2: FORCE_FAILURE=False. Same prompt. kitaru sees the cached
           checkpoint from run 1, serves the agent's whole turn from
           cache (~3s, zero LLM calls), runs the flow body past the
           failure check, and returns the output.

Without kitaru, run 1's failure would have lost ~20s of agent work and
$0.0X of LLM cost. With kitaru, run 2 picks up the saved work for free.
"""

from agent_factory.agent import build_agent
from agent_factory.profile import Profile

import kitaru
from kitaru.adapters.pydantic_ai import KitaruAgent

# Demo flag — flipped by __main__ between run 1 and run 2 to simulate a
# transient post-agent failure. Real platform engineers' failures come
# from the network/filesystem/database; this stands in for any of those.
FORCE_FAILURE = True

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


@kitaru.flow
def agent_factory_flow(prompt: str) -> str:
    # The kitaru ↔ pydantic-ai integration seam, in plain sight:
    # build_agent() returns a vanilla pydantic-ai Agent; KitaruAgent
    # wraps it for durable execution, capture, and HITL bridging.
    agent = build_agent(DEFAULT_PROFILE)
    agent = KitaruAgent(agent)

    # The agent's whole run is a single kitaru checkpoint (turn mode).
    # If anything below this point raises, the checkpoint is already
    # cached and a retry won't re-pay for it.
    output = agent.run_sync(prompt).output

    # The agent's whole turn is now persisted as a kitaru checkpoint — even
    # if the next line raises, that work is saved and the next attempt
    # with the same input will get it from cache.
    if FORCE_FAILURE:
        raise RuntimeError(
            "Simulated downstream blip. The agent's turn is already cached; "
            "the next run with the same prompt skips the LLM + tool calls."
        )
    return output


if __name__ == "__main__":
    import time

    # Unique-per-script-run suffix so run 1 isn't already cached from a
    # previous session — we want to see kitaru actually checkpoint the
    # agent's turn before run 2 picks it up.
    PROMPT = (
        "Inspect this machine: what's the OS, the kernel version, the current "
        "user, and how many processes are running? Use one shell command per "
        f"question. Summarize at the end. (id={int(time.time())})"
    )

    print("=== Run 1 (FORCE_FAILURE=True — agent runs, flow raises after) ===")
    started = time.perf_counter()
    try:
        agent_factory_flow.run(PROMPT)
    except Exception as exc:
        print(f"Flow failed (expected): {type(exc).__name__}: {exc}")
    print(f"[Run 1 took {time.perf_counter() - started:.1f}s — agent did real work]")

    # Flip the flag — same Python process, module state shared between
    # the two runs. (The agent's checkpoint cache is in kitaru, not in
    # this Python process.)
    FORCE_FAILURE = False

    print("\n=== Run 2 (same prompt — agent's turn served from cache) ===")
    started = time.perf_counter()
    handle = agent_factory_flow.run(PROMPT)
    print(handle.wait().output)
    print(
        f"\n[Run 2 took {time.perf_counter() - started:.1f}s — zero LLM calls; "
        "kitaru replayed the agent's checkpoint from run 1]"
    )
