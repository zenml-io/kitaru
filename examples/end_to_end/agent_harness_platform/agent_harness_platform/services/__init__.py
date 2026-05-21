"""Typed service handlers — host-side dispatch via `exec_service`.

Stage 5 introduces this package. Services are typed Pydantic models
the LLM can call by name; the dispatcher validates args against a
discriminated union, then runs the handler in the host process and
returns a typed result.

Why host-side and not via the sandbox + proxy? Two reasons:

1. The result schema is structured. The agent doesn't need to parse
   `curl` output — `lookup_wiki` returns `WikiSnippet` objects directly.
2. Some services (publishing webhooks, hitting internal control planes)
   shouldn't run from the worker container at all; their credentials
   never touch the sandbox network.

The two credential paths come together here:

- **Sandboxed `exec`** (stages 2-4): the worker calls `curl wiki.local`,
  the proxy injects the bearer. Worker-side, indirect.
- **Host-side `exec_service`** (stage 5): the host process resolves
  the secret directly via `kitaru.get_secret(...)`, makes the HTTP
  call, returns typed data to the agent. Host-side, direct.
"""

from .registry import ALL_SERVICES, build_service_description
from .schemas import LookupWikiArgs, PublishSummaryArgs

__all__ = [
    "ALL_SERVICES",
    "LookupWikiArgs",
    "PublishSummaryArgs",
    "build_service_description",
]
