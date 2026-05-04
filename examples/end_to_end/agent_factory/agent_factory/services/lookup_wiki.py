"""`lookup_wiki` — typed wiki query, host-side.

Resolves the `wiki-token` secret directly via `kitaru.get_secret(...)`
and hits `http://wiki.local/snippets/<topic>` from the host process.
The proxy is NOT involved — this is the OTHER credential path.

In production, `wiki.local` would be the team's real wiki host and the
secret reference would point at the secret your platform team manages.
For the example, both live on the mock-services container.
"""

import json
import urllib.error
import urllib.request

import kitaru

from .schemas import LookupWikiArgs, LookupWikiResult, WikiSnippet

# Host-side `exec_service` reaches the mock via the host-bound port the
# runner publishes (Docker network aliases like `wiki.local` only resolve
# inside the `agent_factory` network). In production, this would point
# at the team's real wiki host.
from mocks.runner import HOST_PORT

_WIKI_BASE_URL = f"http://localhost:{HOST_PORT}"


def lookup_wiki(args: LookupWikiArgs) -> LookupWikiResult:
    token = kitaru.get_secret("wiki-token").values["value"]
    request = urllib.request.Request(
        f"{_WIKI_BASE_URL}/snippets/{args.topic}",
        headers={"Authorization": f"Bearer {token}"},
    )
    with urllib.request.urlopen(request, timeout=5) as response:
        body = json.loads(response.read())
    return LookupWikiResult(
        topic=body["topic"],
        snippets=[WikiSnippet(**s) for s in body["snippets"]],
    )
