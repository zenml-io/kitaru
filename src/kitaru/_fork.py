"""Thin public fork dispatcher."""

from typing import Any, Protocol, TypeVar

from kitaru.errors import KitaruUsageError

ResultT = TypeVar("ResultT")


class SupportsFork(Protocol[ResultT]):
    """Runtime shape for adapter objects that implement forking."""

    def fork(self, **kwargs: Any) -> ResultT:
        """Fork the target using adapter-specific arguments."""
        ...


def fork(target: SupportsFork[ResultT], **kwargs: Any) -> ResultT:
    """Delegate a fork request to a supported adapter target.

    Today this is intended for ``KitaruGraphRunner`` and objects exposing a
    compatible ``fork(...)`` method. The dispatcher does not look up stored
    executions or implement runtime-specific fork mechanics itself.
    """
    fork_method = getattr(target, "fork", None)
    if not callable(fork_method):
        raise KitaruUsageError(
            "kitaru.fork(...) currently supports KitaruGraphRunner or objects "
            "that expose a compatible `fork(...)` method. Pass the adapter "
            "runner as the first argument, for example `kitaru.fork(runner, ...)`."
        )
    return fork_method(**kwargs)
