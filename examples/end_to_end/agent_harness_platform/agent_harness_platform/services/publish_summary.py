"""`publish_summary` — Discord-shaped webhook publish, host-side.

Resolves the `webhook-token` secret on the host and POSTs to
`<mock-base-url>/webhooks/<webhook_id>` with `Authorization: Bot <token>`.
Returns the typed `{message_id, posted_at}` envelope the mock issues.
"""

import json
import os
import urllib.error
import urllib.parse
import urllib.request

import kitaru

from .schemas import PublishSummaryArgs, PublishSummaryResult

_DEFAULT_BASE_URL = "http://localhost:8765"
_BASE_URL_ENV = "AGENT_HARNESS_PLATFORM_MOCK_BASE_URL"


def _base_url() -> str:
    return os.environ.get(_BASE_URL_ENV, _DEFAULT_BASE_URL)


def publish_summary(args: PublishSummaryArgs) -> PublishSummaryResult:
    token = kitaru.get_secret("webhook-token").values["value"]
    payload = json.dumps({"content": args.content}).encode("utf-8")
    # `webhook_id` is already pattern-validated on the args model
    # (`^[A-Za-z0-9._-]{1,64}$`); quote anyway for defense in depth.
    webhook_id_path = urllib.parse.quote(args.webhook_id, safe="")
    request = urllib.request.Request(
        f"{_base_url()}/webhooks/{webhook_id_path}",
        data=payload,
        method="POST",
        headers={
            "Authorization": f"Bot {token}",
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            body = json.loads(response.read())
    except urllib.error.HTTPError as exc:
        # Surface non-2xx as a typed result with a recognizable
        # error-shaped `message_id` so the agent can reason about
        # the failure.
        return PublishSummaryResult(
            message_id=f"<error {exc.code}: {exc.reason}>",
            posted_at=0,
        )
    return PublishSummaryResult(
        message_id=body["message_id"],
        posted_at=body["posted_at"],
    )
