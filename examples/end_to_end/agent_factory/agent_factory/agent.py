"""Wires a Profile into a vanilla pydantic-ai Agent.

Kitaru's KitaruAgent wrap stays at *flow scope* (in the stage file), not
in this helper, so readers see the kitaru ↔ pydantic-ai integration as a
visible, deliberate seam — not as something the library hides.
"""

from pydantic_ai import Agent

from .permissions import PermissionHandler
from .profile import Profile
from .tools import _Sandbox, build_tools


def build_agent(profile: Profile, *, sandbox: _Sandbox | None = None) -> Agent:
    """Build a pydantic-ai Agent from a profile (no kitaru wrap).

    Pass a `sandbox` to isolate the `exec` tool (stage 2+); omit it to run
    shell commands in-process (stage 1). The `skill` tool (stage 3+) is
    enabled when `profile.skill_source` is set.
    """
    permission_handler = PermissionHandler(profile)
    skills_directory = (
        profile.skill_source.resolve() if profile.skill_source is not None else None
    )
    return Agent(
        profile.model,
        name=profile.name,
        system_prompt=profile.system_prompt,
        tools=build_tools(
            permission_handler,
            sandbox=sandbox,
            skills_directory=skills_directory,
        ),
    )
