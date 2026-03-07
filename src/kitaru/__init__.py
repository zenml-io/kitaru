"""Kitaru: durable execution for AI agents.

Kitaru provides primitives for making AI agent workflows persistent,
replayable, and observable. Decorate your orchestration function with
``@kitaru.flow`` and your work units with ``@kitaru.checkpoint`` to
get automatic durability.

Example::

    import kitaru

    @kitaru.checkpoint
    def fetch_data(url: str) -> str:
        return requests.get(url).text

    @kitaru.flow
    def my_agent(url: str) -> str:
        data = fetch_data(url)
        return data.upper()

Current status:

- Implemented: ``@kitaru.flow``, ``@kitaru.checkpoint``, ``kitaru.log()``,
  and ``connect()``
- In progress: ``wait()``, ``save()``, ``load()``, and ``llm()``

The CLI also supports global runtime log-store configuration via
``kitaru log-store set/show/reset``.
"""

from kitaru.artifacts import load, save
from kitaru.checkpoint import checkpoint
from kitaru.client import KitaruClient
from kitaru.config import configure, connect
from kitaru.flow import FlowHandle, flow
from kitaru.llm import llm
from kitaru.logging import log
from kitaru.wait import wait

__all__ = [
    "FlowHandle",
    "KitaruClient",
    "checkpoint",
    "configure",
    "connect",
    "flow",
    "llm",
    "load",
    "log",
    "save",
    "wait",
]
