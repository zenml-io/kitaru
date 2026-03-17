"""Basic interactive coding agent — no framework, just kitaru primitives + LiteLLM.

General-purpose agent loop: each wait accepts a task from the user. The agent
can solve coding tasks, math problems, create visualizations, browse the web,
and more.

    PYTHONPATH=. uv run python -m examples.coding_agent_basic.flow

Send commands from another terminal:

    kitaru executions input <exec-id> --wait step_0 --value "Create a plotly chart of..."
    kitaru executions resume <exec-id>
"""

import click

import kitaru
from kitaru import flow

try:
    from .agents import solve
except ImportError:
    from agents import solve

# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------

_QUIT_COMMANDS = {"quit", "exit", "done", "q"}


@flow(
    image={
        "base_image": "strickvl/kitaru-dev:latest",
        "requirements": ["litellm"],
        "apt_packages": ["curl", "ca-certificates"],
    },
)
def coding_agent_basic() -> str:
    """General-purpose agent loop driven by user tasks.

    Each iteration waits for input. The user can:
      - Send any task → the agent solves it using available tools
      - Send "quit" → exits
    """
    results: list[str] = []
    step = 0

    while True:
        msg = kitaru.wait(
            name=f"step_{step}",
            timeout=600,
            schema=str,
            question="Send a task or 'quit' to finish.",
        )
        step += 1
        cmd = msg.strip().lower()

        if cmd in _QUIT_COMMANDS:
            break

        kitaru.log(task=msg.strip())
        result = solve(msg.strip(), id=f"solve_{step - 1}")
        results.append(f"## {msg.strip()}\n\n{result}")

    if results:
        return "\n\n---\n\n".join(results)
    return "No tasks completed."


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


@click.command(help="General-purpose interactive agent.")
def main() -> None:
    coding_agent_basic.run()


if __name__ == "__main__":
    main()
