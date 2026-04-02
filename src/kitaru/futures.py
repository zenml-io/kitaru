"""Kitaru-owned future wrappers for checkpoint concurrency.

These types decouple Kitaru's public API surface from the backend's native
future classes (currently ZenML), enabling backend-neutral evolution.  Each
wrapper delegates behavior to the underlying native future via duck typing —
no backend imports are needed in this module.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any, overload


class KitaruArtifactFuture:
    """Future for a single checkpoint output artifact.

    Wraps the backend's per-output artifact future.  Delegation is via duck
    typing so this class is backend-neutral.
    """

    __slots__ = ("_native",)

    def __init__(self, native: Any) -> None:
        self._native = native

    @property
    def invocation_id(self) -> str:
        """The checkpoint invocation ID that produced this artifact."""
        return self._native.invocation_id

    def running(self) -> bool:
        """Return ``True`` if the producing checkpoint is still executing."""
        return self._native.running()

    def result(self) -> Any:
        """Return the artifact handle (backend-specific representation)."""
        return self._native.result()

    def load(self, *, disable_cache: bool = False) -> Any:
        """Load and return the concrete artifact data."""
        return self._native.load(disable_cache=disable_cache)

    def chunk(self, index: int) -> Any:
        """Return a chunk of a chunked output artifact."""
        return self._native.chunk(index)


class KitaruStepFuture:
    """Future for a submitted checkpoint execution.

    Wraps the backend's step future.  Users receive this from
    ``checkpoint.submit()`` and call ``.result()`` to collect the outcome.
    """

    __slots__ = ("_native",)

    def __init__(self, native: Any) -> None:
        self._native = native

    @property
    def invocation_id(self) -> str:
        """The checkpoint invocation ID."""
        return self._native.invocation_id

    def running(self) -> bool:
        """Return ``True`` if the checkpoint is still executing."""
        return self._native.running()

    def wait(self) -> None:
        """Block until the checkpoint finishes without loading data."""
        self._native.wait()

    def result(self) -> Any:
        """Return the checkpoint output (artifact handles)."""
        return self._native.result()

    def load(self, *, disable_cache: bool = False) -> Any:
        """Load and return the concrete checkpoint return value(s)."""
        return self._native.load(disable_cache=disable_cache)

    def artifacts(self) -> Any:
        """Return raw artifact handles for advanced use."""
        return self._native.artifacts()

    def get_artifact(self, key: str) -> KitaruArtifactFuture:
        """Get a named artifact future from a multi-output checkpoint."""
        return KitaruArtifactFuture(self._native.get_artifact(key))

    @overload
    def __getitem__(self, key: int) -> KitaruArtifactFuture: ...

    @overload
    def __getitem__(self, key: slice) -> tuple[KitaruArtifactFuture, ...]: ...

    def __getitem__(
        self, key: int | slice
    ) -> KitaruArtifactFuture | tuple[KitaruArtifactFuture, ...]:
        """Index into the checkpoint's output artifacts."""
        native_result = self._native[key]
        if isinstance(native_result, tuple):
            return tuple(KitaruArtifactFuture(af) for af in native_result)
        return KitaruArtifactFuture(native_result)

    def __iter__(self) -> Iterator[KitaruArtifactFuture]:
        """Iterate over artifact futures for each checkpoint output."""
        for af in self._native:
            yield KitaruArtifactFuture(af)

    def __len__(self) -> int:
        """Return the number of output artifacts."""
        return len(self._native)


class KitaruMapFuture:
    """Future for a mapped or product checkpoint execution.

    Wraps the backend's map-results future.  Users receive this from
    ``checkpoint.map()`` or ``checkpoint.product()`` and iterate or call
    ``.result()`` to collect outcomes.
    """

    __slots__ = ("_futures", "_native")

    def __init__(self, native: Any) -> None:
        self._native = native
        self._futures = tuple(KitaruStepFuture(f) for f in native.futures)

    @property
    def futures(self) -> list[KitaruStepFuture]:
        """Wrapped child futures in submission order."""
        return list(self._futures)

    def running(self) -> bool:
        """Return ``True`` if any child checkpoint is still executing."""
        return self._native.running()

    def result(self) -> list[Any]:
        """Return output handles for each mapped invocation."""
        return self._native.result()

    def load(self, *, disable_cache: bool = False) -> list[Any]:
        """Load and return concrete values for each mapped invocation."""
        return self._native.load(disable_cache=disable_cache)

    def artifacts(self) -> list[Any]:
        """Return raw artifact handles for each mapped invocation."""
        return self._native.result()

    def unpack(self) -> tuple[list[KitaruArtifactFuture], ...]:
        """Unpack mapped results by output position.

        For multi-output checkpoints, returns a tuple of lists where each
        list contains artifact futures for one output across all mapped
        invocations.
        """
        native_unpacked = self._native.unpack()
        return tuple(
            [KitaruArtifactFuture(af) for af in artifact_list]
            for artifact_list in native_unpacked
        )

    @overload
    def __getitem__(self, key: int) -> KitaruStepFuture: ...

    @overload
    def __getitem__(self, key: slice) -> list[KitaruStepFuture]: ...

    def __getitem__(
        self, key: int | slice
    ) -> KitaruStepFuture | list[KitaruStepFuture]:
        """Index into the child step futures."""
        if isinstance(key, int):
            return self._futures[key]
        return list(self._futures[key])

    def __iter__(self) -> Iterator[KitaruStepFuture]:
        """Iterate over wrapped child step futures."""
        yield from self._futures

    def __len__(self) -> int:
        """Return the number of mapped invocations."""
        return len(self._futures)


def unwrap_kitaru_futures(value: Any) -> Any:
    """Recursively unwrap Kitaru futures to their native backend equivalents.

    Used internally before forwarding arguments to the backend, which only
    understands its own native future types.  Recurses into ``list``,
    ``tuple``, and ``dict`` structures but leaves all other values unchanged.
    """
    if isinstance(value, (KitaruStepFuture, KitaruArtifactFuture, KitaruMapFuture)):
        return value._native
    if isinstance(value, list):
        return [unwrap_kitaru_futures(item) for item in value]
    if isinstance(value, tuple):
        return tuple(unwrap_kitaru_futures(item) for item in value)
    if isinstance(value, dict):
        return {k: unwrap_kitaru_futures(v) for k, v in value.items()}
    return value


__all__ = [
    "KitaruArtifactFuture",
    "KitaruMapFuture",
    "KitaruStepFuture",
]
