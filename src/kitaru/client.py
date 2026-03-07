"""Kitaru client for programmatic execution management.

``KitaruClient`` provides a Python API for inspecting and managing
executions, artifacts, and server state. It wraps the ZenML client
with Kitaru-specific domain models and a simplified interface.

Example::

    from kitaru import KitaruClient

    client = KitaruClient()
    execution = client.executions.get("exec-123")
    print(execution.status)

Note: This is scaffolding. The client is not yet implemented.
"""

from __future__ import annotations

from kitaru.runtime import _not_implemented


class KitaruClient:
    """Client for managing Kitaru executions and artifacts.

    Provides namespaced access to executions and artifacts
    through ``client.executions`` and ``client.artifacts``.
    """

    def __init__(self, **kwargs: object) -> None:
        """Initialize a Kitaru client.

        Args:
            **kwargs: Connection options (server URL, credentials, etc.).
        """
        _not_implemented("KitaruClient")
