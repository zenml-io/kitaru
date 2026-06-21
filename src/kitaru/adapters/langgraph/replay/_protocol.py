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


# fork_granularity is "node": the reconstruction places one checkpoint per graph
# node and runs the node's whole callable (including any internal model/tool loop)
# live as one unit. Forking an individual call inside a node is not built.
LANGGRAPH_CAPS = Caps(
    fork_granularity="node",
    native_checkpoints="reconstructed",
    resume="reconstruct",
)
