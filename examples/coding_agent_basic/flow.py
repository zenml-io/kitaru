"""Basic interactive coding agent — no framework, just kitaru primitives + LiteLLM.

Free-form command loop: each wait accepts a user message that drives what
happens next. Send a task description to plan, "implement" to execute the
last plan, or "quit" to finish.

    PYTHONPATH=. uv run python -m examples.coding_agent_basic.flow --cwd /path/to/repo

Send commands from another terminal:

    kitaru executions input <exec-id> --wait step_0 --value "Add type hints"
    kitaru executions resume <exec-id>
"""

import click

import kitaru
from kitaru import flow

try:
    from .agents import implement, plan
except ImportError:
    from agents import implement, plan

# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------

_IMPLEMENT_COMMANDS = {"implement", "go", "do it", "execute", "yes"}
_QUIT_COMMANDS = {"quit", "exit", "done", "q"}


@flow(
    image={
        "base_image": "strickvl/kitaru-dev:latest",
        "requirements": ["litellm"],
    },
)
def coding_agent_basic(cwd: str = ".") -> str:
    """Free-form agent loop driven entirely by user commands.

    Each iteration waits for input. The user can:
      - Send a task description → runs the planner
      - Send "implement" / "go" → executes the last plan
      - Send "quit" → exits
    """
    results: list[str] = []
    current_task: str | None = None
    current_plan: str | None = None
    step = 0
    plan_count = 0
    impl_count = 0

    while True:
        msg = kitaru.wait(
            name=f"step_{step}",
            timeout=600,
            schema=str,
            question=(
                "Send a task to plan, 'implement' to execute the last plan, "
                "or 'quit' to finish."
            ),
        )
        step += 1
        cmd = msg.strip().lower()

        if cmd in _QUIT_COMMANDS:
            break

        if cmd in _IMPLEMENT_COMMANDS:
            if current_plan is None:
                kitaru.log(warning="implement requested but no plan exists")
                continue
            result = implement(
                current_task, current_plan, cwd, id=f"implement_{impl_count}"
            )
            results.append(f"## {current_task}\n\n{result}")
            impl_count += 1
            current_plan = None
        else:
            current_task = msg.strip()
            kitaru.log(task=current_task)
            current_plan = plan(current_task, cwd, id=f"plan_{plan_count}")
            plan_count += 1

    if results:
        return "\n\n---\n\n".join(results)
    return "No tasks completed."


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


@click.command(help="Basic interactive coding agent (no framework).")
@click.option("--cwd", default=".", help="Working directory for the agent")
def main(cwd: str) -> None:
    coding_agent_basic.run(cwd)


if __name__ == "__main__":
    main()
