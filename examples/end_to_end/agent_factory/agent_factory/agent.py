"""Wires a Profile into a durable, pydantic-ai-backed KitaruAgent."""

from kitaru.adapters.pydantic_ai import KitaruAgent
from pydantic_ai import Agent

from .permissions import PermissionHandler
from .profile import Profile
from .tools import build_tools


def build_agent(profile: Profile) -> KitaruAgent:
    """Build a durable PydanticAI agent from a profile.

    Turn mode (default): each agent.run_sync() is one aggregating checkpoint.
    Kill the flow mid-turn and resume → kitaru re-runs the turn. Granular
    per-call caching is introduced in a later chapter where it earns its
    keep (longer agent runs, expensive model calls).
    """
    permission_handler = PermissionHandler(profile)
    agent = Agent(
        profile.model,
        name=profile.name,
        system_prompt=profile.system_prompt,
        tools=build_tools(permission_handler),
    )
    return KitaruAgent(agent)
