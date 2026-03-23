"""Internal CLI app wiring for Kitaru."""

from __future__ import annotations

import cyclopts

_UNKNOWN_VERSION = "unknown"

app = cyclopts.App(
    name="kitaru",
    help="Durable execution for AI agents.",
    version=_UNKNOWN_VERSION,
    version_flags=["--version", "-V"],
)

log_store_app = cyclopts.App(
    name="log-store",
    help="Manage global runtime log-store settings.",
)
stack_app = cyclopts.App(
    name="stack",
    help="Inspect, create, delete, and switch stacks.",
)
secrets_app = cyclopts.App(
    name="secrets",
    help="Manage centralized runtime secrets.",
)
model_app = cyclopts.App(
    name="model",
    help="Manage local model aliases for kitaru.llm().",
)
executions_app = cyclopts.App(
    name="executions",
    help="Inspect and manage flow executions.",
)
clean_app = cyclopts.App(
    name="clean",
    help="Reset Kitaru state.",
)

app.command(log_store_app)
app.command(stack_app)
app.command(secrets_app)
app.command(model_app)
app.command(executions_app)
app.command(clean_app)


@app.default
def main() -> None:
    """Show help when invoked without arguments."""
    app.help_print()


from . import (  # noqa: F401,E402
    _clean,
    _executions,
    _init,
    _models,
    _secrets,
    _stacks,
    _status,
)

__all__ = [
    "_UNKNOWN_VERSION",
    "app",
    "clean_app",
    "executions_app",
    "log_store_app",
    "main",
    "model_app",
    "secrets_app",
    "stack_app",
]
