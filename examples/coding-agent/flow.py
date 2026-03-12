import click

import kitaru
from kitaru import checkpoint, flow

from .utils import coder, reader


@checkpoint(type="llm_call")
def research(task: str, cwd: str) -> str:
    """Read the codebase and build context."""
    result = reader.run_sync(
        f"Analyze this codebase for the following task. Read relevant files, "
        f"identify which files are involved, current behavior, and constraints. "
        f"Do NOT make changes.\n\nTASK: {task}",
        deps=cwd,
    )
    kitaru.log(phase="research", result_length=len(result.output))
    return result.output


@checkpoint(type="llm_call")
def plan(task: str, analysis: str, cwd: str) -> str:
    """Create an implementation plan from the research output."""
    result = reader.run_sync(
        f"Create a numbered implementation plan for this task.\n\n"
        f"TASK: {task}\n\nANALYSIS:\n{analysis}\n\n"
        f"Include: files to modify, specific changes, order of operations, "
        f"verification steps.",
        deps=cwd,
    )
    kitaru.log(phase="plan", result_length=len(result.output))
    return result.output


@checkpoint(type="llm_call")
def implement(task: str, implementation_plan: str, cwd: str) -> str:
    """Execute the approved plan. Most expensive phase — replay target."""
    result = coder.run_sync(
        f"Execute this plan. Make the code changes, then verify.\n\n"
        f"TASK: {task}\n\nPLAN:\n{implementation_plan}",
        deps=cwd,
    )
    kitaru.log(phase="implement", result_length=len(result.output))
    return result.output


@flow
def coding_agent(task: str, cwd: str = ".") -> str:
    """research -> plan -> [human approval] -> implement"""
    analysis = research(task, cwd)
    implementation_plan = plan(task, analysis, cwd)

    approved = kitaru.wait(
        schema=bool,
        name="approve_plan",
        question=f"Approve this plan?\n\n{implementation_plan}",
        metadata={"task": task},
    )
    if not approved:
        return f"Plan rejected for: {task}"

    return implement(task, implementation_plan, cwd)


@click.command(help="Durable coding agent (PydanticAI).")
@click.option('--task', required=True, help='The coding task to execute')
@click.option('--cwd', default='.', help='The current working directory')
def main(task: str, cwd: str) -> None:
    handle = coding_agent.run(task, cwd)
    print(f"exec_id: {handle.exec_id}")
    print(handle.wait())


if __name__ == "__main__":
    main()
