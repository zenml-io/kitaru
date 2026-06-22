"""Light CLI smoke tests for examples/end_to_end/pydantic_replay_fork/demo.py.

Validates:
- The ``cli`` group exposes exactly the expected commands.
- ``--help`` on the group and each command exits 0.

No API calls, no model invocations, no Kitaru server required.
"""
from __future__ import annotations

import sys
import pathlib

import pytest

_EXAMPLES_PATH = str(pathlib.Path(__file__).parent.parent / "examples" / "end_to_end")
if _EXAMPLES_PATH not in sys.path:
    sys.path.insert(0, _EXAMPLES_PATH)

pytest.importorskip("click")
pytest.importorskip("pydantic_ai")


@pytest.fixture(scope="module")
def demo_cli():
    """Import and return the cli group from demo.py."""
    from pydantic_replay_fork import demo
    return demo.cli


def test_cli_command_names(demo_cli) -> None:
    """The cli group exposes exactly the five required commands."""
    names = set(demo_cli.commands.keys())
    assert names == {"run", "rerun", "replay", "cohort", "run-all"}, (
        f"Unexpected command set: {sorted(names)}"
    )


@pytest.mark.parametrize("args", [
    ["--help"],
    ["run", "--help"],
    ["rerun", "--help"],
    ["replay", "--help"],
    ["cohort", "--help"],
    ["run-all", "--help"],
])
def test_help_exits_zero(demo_cli, args) -> None:
    """``--help`` on the group and each command exits 0."""
    from click.testing import CliRunner

    runner = CliRunner()
    result = runner.invoke(demo_cli, args)
    assert result.exit_code == 0, (
        f"`demo.py {' '.join(args)}` exited {result.exit_code}.\n"
        f"Output:\n{result.output}"
    )
