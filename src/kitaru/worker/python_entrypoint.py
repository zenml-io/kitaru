#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
"""Run one zero-argument Python agent wrapper by module reference."""

import argparse
import asyncio
import importlib
import inspect
import sys
from collections.abc import Awaitable, Sequence
from typing import Any

from kitaru.source_refs import parse_python_source_ref


def run_reference(reference: str) -> None:
    """Import and invoke a zero-argument sync or async wrapper.

    The wrapper owns Kitaru task input retrieval and output handling. Its
    return value is ignored; failures propagate so the worker observes a
    non-zero process exit.

    Args:
        reference: Callable reference formatted as ``MODULE:ATTRIBUTE``.

    Raises:
        ValueError: The reference is malformed or does not resolve to a callable.
    """
    module_name, attribute = _parse_reference(reference)
    module = importlib.import_module(module_name)
    callable_ = getattr(module, attribute, None)
    if not callable(callable_):
        raise ValueError(f"Entrypoint {reference!r} is missing or is not callable")
    result = callable_()
    if inspect.isawaitable(result):
        asyncio.run(_await_result(result))


async def _await_result(result: Awaitable[Any]) -> None:
    """Await a wrapper result while deliberately discarding its value."""
    await result


def _parse_reference(reference: str) -> tuple[str, str]:
    """Parse a top-level module attribute reference."""
    try:
        return parse_python_source_ref(reference)
    except ValueError as error:
        raise ValueError(
            f"Invalid entrypoint {reference!r}; expected MODULE:ATTRIBUTE"
        ) from error


def main(argv: Sequence[str] | None = None) -> int:
    """Run the reference provided on the process command line."""
    parser = argparse.ArgumentParser(
        description="Run a zero-argument Kitaru agent wrapper."
    )
    parser.add_argument("entrypoint", help="Wrapper reference as MODULE:ATTRIBUTE.")
    args = parser.parse_args(argv)
    run_reference(args.entrypoint)
    return 0


if __name__ == "__main__":
    sys.exit(main())
