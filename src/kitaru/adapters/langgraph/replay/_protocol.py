from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable


@dataclass(frozen=True)
class Caps:
    fork_granularity: str
    native_checkpoints: str
    resume: str


@runtime_checkable
class ReplayAdapter(Protocol):
    def seed(self, case: Any) -> str: ...
    def checkpoints(self, seed_exec_id: str) -> list[str]: ...
    def fork(self, seed_exec_id: str, *, from_: str, edits: list[Any], variant: Any) -> Any: ...
    def capabilities(self) -> Caps: ...


LANGGRAPH_CAPS = Caps(
    fork_granularity="call",
    native_checkpoints="reconstructed",
    resume="reconstruct",
)
