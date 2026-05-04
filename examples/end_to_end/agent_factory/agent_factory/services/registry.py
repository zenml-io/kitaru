"""Service registry: name → (args model, handler).

The `exec_service` tool factory reads `ALL_SERVICES` to:

- Generate a Literal type for the LLM-facing `service_name` parameter
  (so the model can only call services that exist).
- Validate `args` against the right Pydantic model on each call.
- Dispatch to the handler.

The agent never sees the discriminated union directly. It sees a flat
`(service_name: Literal[...], args: dict)` shape — easier for LLMs to
emit reliably than a `oneOf` union schema. The body re-validates by
constructing `<ArgsModel>(**args)` based on the chosen `service_name`.
"""

from collections.abc import Callable
from typing import NamedTuple

from pydantic import BaseModel

from .lookup_wiki import lookup_wiki
from .publish_summary import publish_summary
from .schemas import LookupWikiArgs, PublishSummaryArgs


class ServiceCall(NamedTuple):
    args_model: type[BaseModel]
    # Handlers must accept a validated args model and return a Pydantic
    # model — `exec_service` calls `.model_dump()` on the result, so a
    # handler that returns a raw dict would AttributeError at LLM time.
    handler: Callable[[BaseModel], BaseModel]
    summary: str


ALL_SERVICES: dict[str, ServiceCall] = {
    "lookup_wiki": ServiceCall(
        args_model=LookupWikiArgs,
        handler=lookup_wiki,
        summary=(
            "Look up structured snippets for a topic from the internal "
            "wiki. Returns `{topic, snippets: [{url, excerpt}]}`."
        ),
    ),
    "publish_summary": ServiceCall(
        args_model=PublishSummaryArgs,
        handler=publish_summary,
        summary=(
            "Post a summary message to a webhook. Returns "
            "`{message_id, posted_at}`."
        ),
    ),
}


def _render_annotation(annotation: object) -> str:
    """Render a type annotation for the LLM-facing description.

    Plain `type` objects (e.g. `str`, `int`, custom classes) render via
    `__name__` — `str(str)` would yield ugly `"<class 'str'>"`. Generic
    aliases (`list[str]`, `dict[str, int]`), unions (`str | None`), and
    `Literal[...]` render via `str()` because that preserves type
    parameters that `__name__` would drop. Strip `typing.` and
    `__main__.` prefixes for readability.
    """
    if isinstance(annotation, type):
        return annotation.__name__
    rendered = str(annotation)
    for prefix in ("typing.", "__main__."):
        rendered = rendered.replace(prefix, "")
    return rendered


def build_service_description(allowed: set[str]) -> str:
    """Render the `exec_service` tool description from the allowed services.

    The result is a single string the LLM sees as the tool's description.
    Listing each service's args makes the LLM far more reliable at
    emitting the right `args` payload.

    Caller (`build_tools` in `tools.py`) is expected to have already
    rejected unknown service names; this function trusts `allowed`.
    """
    if not allowed:
        return "No services are currently enabled for this agent."
    lines = [
        "Dispatch a typed service call. Provide `service_name` (one of "
        "the values listed below) and `args` (a dict matching the listed "
        "schema for that service). Returns the service's typed result.",
        "",
        "Available services:",
    ]
    for name in sorted(allowed):
        call = ALL_SERVICES[name]
        fields = call.args_model.model_fields
        arg_lines = [
            f"      - {field_name}: {_render_annotation(field.annotation)} — {field.description or ''}".rstrip()
            for field_name, field in fields.items()
        ]
        lines.append(f"  - **{name}** — {call.summary}")
        if arg_lines:
            lines.append("    args:")
            lines.extend(arg_lines)
    return "\n".join(lines)
