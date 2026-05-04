"""Profile-backed permission evaluator."""

from kitaru.errors import KitaruRuntimeError

from .profile import Profile


class PermissionHandler:
    """Decides which tools a profile can use; the runtime backstop for build_tools()."""

    def __init__(self, profile: Profile) -> None:
        self._profile = profile

    def can_use_tool(self, tool_name: str) -> bool:
        return tool_name in self._profile.allowed_tools

    def require_tool(self, tool_name: str) -> None:
        # Bug-tripwire — should never fire in practice because build_tools() filters
        # by allowed_tools at construction time. If it does fire, the profile was
        # mutated mid-flow and the agent should not continue.
        if not self.can_use_tool(tool_name):
            raise KitaruRuntimeError(
                f"Tool {tool_name!r} is not in this profile's allowed_tools"
            )
