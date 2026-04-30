"""Mock services for the agent_factory example.

A single FastAPI app multiplexed by `Host:` header so we can mock
multiple "external" services from one container on the Docker network.
Stage 4 ships ONE host (`wiki.local`); later chapters can extend to
notes.local, webhook.local, etc.

Each route validates an inbound `Authorization` header — without it,
returns 401 with a clear message. With it, returns sample data. The
401 is the visible chapter 4 payoff: without the proxy injecting auth,
the agent sees 401 and can't make progress; chapter 3's skill says
"curl wiki.local/<topic>" and the proxy's auth injection turns the
401 into a 200 transparently.

Run standalone for testing:

    uvicorn mocks.server:app --host 0.0.0.0 --port 80
"""

from fastapi import FastAPI, Header, HTTPException, Request

app = FastAPI()

# Tiny fixture set keyed by topic slug. A real fork would back this with
# whatever wiki / KB the team uses.
_WIKI_SNIPPETS: dict[str, list[dict[str, str]]] = {
    "durability": [
        {
            "url": "https://kitaru.ai/docs/durability",
            "excerpt": (
                "Durable execution means every checkpoint output is "
                "persisted; flows can pause for hours, the host can "
                "reboot, and on resume work picks up from the last "
                "completed checkpoint."
            ),
        },
        {
            "url": "https://kitaru.ai/blog/durable-agents",
            "excerpt": (
                "Without durability, a crashed agent loses every LLM "
                "token spent before the failure. Kitaru caches each "
                "checkpoint's output so the retry skips re-paying for "
                "the work that already succeeded."
            ),
        },
    ],
    "sandboxing": [
        {
            "url": "https://kitaru.ai/docs/sandbox",
            "excerpt": (
                "An agent's `exec` tool should run inside an isolated "
                "container — its own filesystem, its own network "
                "namespace — so prompt injection can't reach the host."
            ),
        },
    ],
    "replay": [
        {
            "url": "https://kitaru.ai/docs/replay",
            "excerpt": (
                "kitaru.replay() creates a new execution that starts "
                "from a chosen checkpoint, with optional output "
                "overrides. Cached upstream steps are reused; "
                "downstream steps re-execute against the new value."
            ),
        },
    ],
}

_WIKI_TOKEN_PREFIX = "Bearer "
_WIKI_TOKEN = "wiki-token"


def _check_wiki_auth(authorization: str | None) -> None:
    if not authorization:
        raise HTTPException(
            status_code=401,
            detail={
                "error": "unauthorized",
                "expected_prefix": "Bearer",
                "hint": "Set up the credential proxy and the Profile's "
                "sandbox_proxy_rules so wiki.local gets the bearer token "
                "auto-injected.",
            },
        )
    if not authorization.startswith(_WIKI_TOKEN_PREFIX):
        raise HTTPException(status_code=401, detail={"error": "bad_scheme"})
    token = authorization[len(_WIKI_TOKEN_PREFIX):]
    if token != _WIKI_TOKEN:
        raise HTTPException(status_code=401, detail={"error": "bad_token"})


def _redacted(value: str | None, max_chars: int = 8) -> str:
    if not value:
        return "<missing>"
    return value[:max_chars] + "…"


@app.middleware("http")
async def log_request(request: Request, call_next):
    """One-line per-request log so `docker logs mock-services` is useful."""
    response = await call_next(request)
    auth = request.headers.get("authorization")
    print(
        f"[mock-services] {request.method} "
        f"{request.url.path} (host={request.headers.get('host', '?')}, "
        f"auth={_redacted(auth)}) → {response.status_code}",
        flush=True,
    )
    return response


@app.get("/snippets/{topic}")
def get_snippets(
    topic: str, authorization: str | None = Header(None)
) -> dict[str, object]:
    _check_wiki_auth(authorization)
    snippets = _WIKI_SNIPPETS.get(topic.lower(), [])
    return {"topic": topic, "snippets": snippets}


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"status": "ok"}
