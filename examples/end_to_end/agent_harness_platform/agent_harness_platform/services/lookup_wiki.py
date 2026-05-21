"""`lookup_wiki` — typed wiki query, host-side.

Resolves the `wiki-token` secret directly via `kitaru.get_secret(...)`
and hits `<mock-base-url>/snippets/<topic>` from the host process. The
proxy is NOT involved — this is the OTHER credential path.

In production, the base URL would be the team's real wiki host and the
secret reference would point at the secret your platform team manages.
For the example, both live on the mock-services container; the runner
publishes it on a host port and exports `AGENT_HARNESS_PLATFORM_MOCK_BASE_URL`
in the host process env.
"""

import json
import os
import urllib.error
import urllib.parse
import urllib.request

import kitaru

from .schemas import LookupWikiArgs, LookupWikiResult, WikiSnippet

_DEFAULT_BASE_URL = "http://localhost:8765"
_BASE_URL_ENV = "AGENT_HARNESS_PLATFORM_MOCK_BASE_URL"


def _base_url() -> str:
    return os.environ.get(_BASE_URL_ENV, _DEFAULT_BASE_URL)


def lookup_wiki(args: LookupWikiArgs) -> LookupWikiResult:
    token = kitaru.get_secret("wiki-token").values["value"]
    # Quote the topic so an LLM-supplied value with spaces / `?` / `#` /
    # path-traversal sequences can't escape the path segment.
    topic_path = urllib.parse.quote(args.topic, safe="")
    request = urllib.request.Request(
        f"{_base_url()}/snippets/{topic_path}",
        headers={"Authorization": f"Bearer {token}"},
    )
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            body = json.loads(response.read())
    except urllib.error.HTTPError as exc:
        # Surface non-2xx as a typed empty result so the LLM can
        # reason about the failure (e.g. "401 — token misconfigured")
        # rather than the whole turn dying with an unhandled exception.
        return LookupWikiResult(topic=f"<error {exc.code}: {exc.reason}>", snippets=[])
    return LookupWikiResult(
        topic=body["topic"],
        snippets=[WikiSnippet(**s) for s in body["snippets"]],
    )
