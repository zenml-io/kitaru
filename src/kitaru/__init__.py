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

Note: most SDK primitives are still scaffolded and raise
``NotImplementedError``. The early connection primitive ``connect()`` is
implemented so you can authenticate with a Kitaru server before the rest of the
runtime lands.
"""

from kitaru.artifacts import load, save
from kitaru.checkpoint import checkpoint
from kitaru.client import KitaruClient
from kitaru.config import configure, connect
from kitaru.flow import flow
from kitaru.llm import llm
from kitaru.logging import log
from kitaru.wait import wait

__all__ = [
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
