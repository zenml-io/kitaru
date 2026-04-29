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
    shell commands in-process (stage 1).
    """
    permission_handler = PermissionHandler(profile)
    return Agent(
        profile.model,
        name=profile.name,
        system_prompt=profile.system_prompt,
        tools=build_tools(permission_handler, sandbox=sandbox),
    )
