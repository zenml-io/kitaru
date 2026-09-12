"""Accepted logical traces for importer normalization properties."""

from dataclasses import dataclass

from hypothesis import strategies as st
from hypothesis.strategies import DrawFn, SearchStrategy


@dataclass(frozen=True)
class LogicalNode:
    """One provider-independent source node and its measured usage."""

    node_id: str
    parent_id: str | None
    input_tokens: int | None
    output_tokens: int | None
    cost: str | None


@dataclass(frozen=True)
class LogicalTrace:
    """One source trace with a valid, connected node tree."""

    trace_id: str
    nodes: tuple[LogicalNode, ...]


@st.composite
def generate_logical_trace(draw: DrawFn, *, prefix: str = "trace") -> LogicalTrace:
    """Generate a small accepted trace with missing, zero, and positive usage."""
    trace_number = draw(st.integers(min_value=0, max_value=999))
    node_count = draw(st.integers(min_value=3, max_value=7))
    trace_id = f"{prefix}-{trace_number}"
    nodes: list[LogicalNode] = []
    for index in range(node_count):
        node_id = trace_id if index == 0 else f"{trace_id}-node-{index}"
        parent_id = (
            None
            if index == 0
            else nodes[
                0 if index == 2 else draw(st.integers(min_value=0, max_value=index - 1))
            ].node_id
        )
        if index % 3 == 0:
            input_tokens = output_tokens = None
            cost = None
        elif index % 3 == 1:
            input_tokens = output_tokens = 0
            cost = "0"
        else:
            input_tokens = draw(st.integers(min_value=1, max_value=10_000))
            output_tokens = draw(st.integers(min_value=1, max_value=10_000))
            coefficient = draw(st.integers(min_value=1, max_value=999_999))
            scale = draw(st.integers(min_value=0, max_value=6))
            if scale:
                divisor = 10**scale
                cost = f"{coefficient // divisor}.{coefficient % divisor:0{scale}d}"
            else:
                cost = str(coefficient)
        nodes.append(
            LogicalNode(
                node_id=node_id,
                parent_id=parent_id,
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                cost=cost,
            )
        )
    return LogicalTrace(trace_id=trace_id, nodes=tuple(nodes))


@st.composite
def generate_logical_forest(
    draw: DrawFn, *, min_size: int = 1
) -> tuple[LogicalTrace, ...]:
    """Generate unique traces whose records may be freely reordered."""
    size = draw(st.integers(min_value=min_size, max_value=3))
    traces = [
        draw(generate_logical_trace(prefix=f"trace-{index}")) for index in range(size)
    ]
    return tuple(traces)


def reorder_records(
    records: list[dict[str, object]],
) -> SearchStrategy[list[dict[str, object]]]:
    """Return bounded permutations of an accepted provider export."""
    return st.permutations(records).map(list)
