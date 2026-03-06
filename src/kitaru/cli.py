"""Kitaru command-line interface."""

from importlib.metadata import version

import cyclopts

app = cyclopts.App(
    name="kitaru",
    help="Durable execution for AI agents, built on ZenML.",
    version=version("kitaru"),
    version_flags=["--version", "-V"],
)


@app.default
def main() -> None:
    """Show help when invoked without arguments."""
    app.help_print()


def cli() -> None:
    """Entry point for the `kitaru` console script."""
    app()
