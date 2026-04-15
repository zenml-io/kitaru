"""Serializable RunContext for Kitaru durable execution.

Whenever a PydanticAI ``RunContext`` must cross a Kitaru checkpoint boundary
that serializes its inputs (``@kitaru.checkpoint(runtime='isolated')``, remote
stacks, or any future cross-process step dispatch), the raw ``RunContext``
cannot round-trip — it holds live references (``agent``, toolset handles,
usage accumulators, event stream, etc.) that pickle would reject or corrupt.

``KitaruRunContext`` mirrors the pattern used by PydanticAI's Temporal and
DBOS integrations: a subclass that carries only a safe, JSON-serializable
subset of the run state, reattaches non-serializable fields (``agent``,
``deps``) on the other side of the boundary, and exposes class methods so
users can extend the carried fields in subclasses without touching the
adapter's wire protocol.

This class is dormant today — the adapter rejects ``runtime='isolated'`` via
:func:`kitaru.adapters.pydantic_ai._utils.reject_isolated_runtime`, so no
runtime path currently serializes a ``RunContext``. It is exported as a stable
extension point for when cross-process checkpoint dispatch is wired up; until
then, subclasses have no runtime effect.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from typing_extensions import TypeVar

from pydantic_ai.exceptions import UserError
from pydantic_ai.tools import RunContext

if TYPE_CHECKING:
    from pydantic_ai.agent.abstract import AbstractAgent

AgentDepsT = TypeVar('AgentDepsT', default=None, covariant=True)


_SERIALIZABLE_FIELDS: tuple[str, ...] = (
    'run_id',
    'metadata',
    'retries',
    'tool_call_id',
    'tool_name',
    'tool_call_approved',
    'tool_call_metadata',
    'retry',
    'max_retries',
    'run_step',
    'partial_output',
    'usage',
)


class KitaruRunContext(RunContext[AgentDepsT]):
    """RunContext subclass that survives Kitaru serialization boundaries.

    Only the fields listed in :data:`_SERIALIZABLE_FIELDS` are carried by
    default. To expose additional fields, subclass and override
    :meth:`serialize_run_context`, then pass the subclass to the part of the
    adapter that opens the boundary (e.g. an isolated-runtime checkpoint
    config in task 4).
    """

    def __init__(self, deps: AgentDepsT, **kwargs: Any):
        self.__dict__ = {**kwargs, 'deps': deps}
        self.__dict__.setdefault('agent', None)
        # Mirror the dataclass metadata used by RunContext so downstream code
        # that introspects fields sees a consistent subset.
        self.__dict__['__dataclass_fields__'] = {
            name: field
            for name, field in RunContext.__dataclass_fields__.items()
            if name in self.__dict__
        }

    def __getattr__(self, name: str) -> Any:
        if name in RunContext.__dataclass_fields__:
            raise UserError(
                f'{self.__class__.__name__!r} object has no attribute {name!r}. '
                'The attribute was not included in serialize_run_context; '
                'subclass KitaruRunContext and extend serialize_run_context to '
                'carry it across the Kitaru checkpoint boundary.'
            )
        raise AttributeError(name)

    @classmethod
    def serialize_run_context(cls, ctx: RunContext[Any]) -> dict[str, Any]:
        """Return the subset of ``ctx`` that survives checkpoint serialization."""
        return {name: getattr(ctx, name) for name in _SERIALIZABLE_FIELDS}

    @classmethod
    def deserialize_run_context(
        cls, data: dict[str, Any], deps: Any
    ) -> KitaruRunContext[Any]:
        """Rebuild a ``KitaruRunContext`` from a :meth:`serialize_run_context` payload."""
        return cls(**data, deps=deps)


def deserialize_run_context(
    run_context_type: type[KitaruRunContext[Any]],
    serialized: dict[str, Any],
    *,
    deps: Any,
    agent: AbstractAgent[Any, Any] | None,
) -> RunContext[Any]:
    """Helper that rebuilds a run context and reattaches the live ``agent`` ref."""

    ctx = run_context_type.deserialize_run_context(serialized, deps=deps)
    if agent is not None:
        ctx.__dict__['agent'] = agent
    return ctx
