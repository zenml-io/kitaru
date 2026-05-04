"""Typed service schemas.

One Pydantic model per service-call kind (input args + result type).
The discriminated union over the args models lives in `registry.py`.
"""

from pydantic import BaseModel, Field


# === lookup_wiki =============================================================


class LookupWikiArgs(BaseModel):
    """Arguments for `lookup_wiki`."""

    topic: str = Field(
        description=(
            "Topic slug to look up. The mock recognizes "
            "'durability', 'sandboxing', and 'replay'. Unknown topics "
            "return an empty `snippets` list."
        )
    )


class WikiSnippet(BaseModel):
    url: str
    excerpt: str


class LookupWikiResult(BaseModel):
    topic: str
    snippets: list[WikiSnippet]


# === publish_summary =========================================================


class PublishSummaryArgs(BaseModel):
    """Arguments for `publish_summary`."""

    webhook_id: str = Field(
        description=(
            "ID of the webhook to publish to. Letters, digits, dot, "
            "dash, underscore only (1-64 chars). The mock returns a "
            "fresh `message_id`."
        ),
        pattern=r"^[A-Za-z0-9._-]{1,64}$",
    )
    content: str = Field(description="The summary text to post.")


class PublishSummaryResult(BaseModel):
    message_id: str
    posted_at: int
