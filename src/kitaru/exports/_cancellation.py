"""Shared cooperative cancellation checkpoints for export work."""

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar

CancellationCheckpoint = Callable[[], None]

_ACTIVE_CHECKPOINT: ContextVar[CancellationCheckpoint | None] = ContextVar(
    "kitaru_export_cancellation_checkpoint", default=None
)


def _noop_checkpoint() -> None:
    return None


def get_cancellation_checkpoint(
    checkpoint: CancellationCheckpoint | None,
) -> CancellationCheckpoint:
    """Resolve an explicit, inherited, or inert cancellation checkpoint."""
    return checkpoint or _ACTIVE_CHECKPOINT.get() or _noop_checkpoint


@contextmanager
def export_cancellation_scope(
    checkpoint: CancellationCheckpoint | None,
) -> Iterator[None]:
    """Make one checkpoint available to nested export helpers."""
    token = _ACTIVE_CHECKPOINT.set(get_cancellation_checkpoint(checkpoint))
    try:
        yield
    finally:
        _ACTIVE_CHECKPOINT.reset(token)
