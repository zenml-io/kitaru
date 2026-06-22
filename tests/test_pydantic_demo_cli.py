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

# Make the pydantic_replay_fork package importable.
_EXAMPLES_PATH = str(pathlib.Path(__file__).parent.parent / "examples" / "end_to_end")
if _EXAMPLES_PATH not in sys.path:
    sys.path.insert(0, _EXAMPLES_PATH)

pytest.importorskip("click")
pytest.importorskip("pydantic_ai")


@pytest.fixture(scope="module")
def demo_cli():
    """Import and return the cli group from demo.py.

    Adds ``examples/end_to_end`` to sys.path so ``pydantic_replay_fork`` is a
    resolvable package (satisfying the relative imports in pipeline.py / demo.py).
    """
    examples_path = str(pathlib.Path(__file__).parent.parent / "examples" / "end_to_end")
    if examples_path not in sys.path:
        sys.path.insert(0, examples_path)
    from pydantic_replay_fork import demo  # noqa: PLC0415
    return demo.cli


def test_cli_command_names(demo_cli) -> None:
    """The cli group exposes exactly the five required commands."""
    names = set(demo_cli.commands.keys())
    assert names == {"run", "reproduce", "experiment", "cohort", "run-all"}, (
        f"Unexpected command set: {sorted(names)}"
    )


@pytest.mark.parametrize("args", [
    ["--help"],
    ["run", "--help"],
    ["reproduce", "--help"],
    ["experiment", "--help"],
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
