from kitaru.adapters.langgraph.replay import import_trace

_KIND_TO_TYPE = {
    "tool": "TOOL",
    "llm": "GENERATION",
}


def _row(obs_id, trace_id, node, name, started_at, kind="tool", parent="root"):
    return {
        "id": obs_id,
        "trace_id": trace_id,
        "type": _KIND_TO_TYPE.get(kind, "SPAN"),
        "name": name,
        "parent_observation_id": parent,
        "start_time": started_at,
        "metadata": {"langgraph_node": node},
        "input": {"args": {}},
        "output": {"ok": True},
    }


def _root_row(trace_id):
    return {
        "id": "root",
        "trace_id": trace_id,
        "type": "SPAN",
        "name": "agent",
        "parent_observation_id": None,
        "start_time": "2026-06-17T14:00:00Z",
        "input": {"user_request": "hi"},
        "output": {"decision": {"policy_label": "billing_policy"}},
        "metadata": {},
    }


def test_import_trace_keys_calls_by_node_and_index():
    trace_id = "t1"
    rows = [
        _root_row(trace_id),
        _row("o1", trace_id, "collect_evidence_with_tools", "lookup_customer", "2026-06-17T14:00:01Z"),
        _row("o2", trace_id, "collect_evidence_with_tools", "search_kb", "2026-06-17T14:00:02Z"),
        _row("o3", trace_id, "decide_action", "model_call", "2026-06-17T14:00:03Z", kind="llm"),
    ]
    case = import_trace(rows)

    by_name = {(c.node, c.call_index): c for c in case.recorded_calls}
    assert ("collect_evidence_with_tools", 0) in by_name
    assert ("collect_evidence_with_tools", 1) in by_name
    assert by_name[("collect_evidence_with_tools", 0)].name == "lookup_customer"
    assert by_name[("collect_evidence_with_tools", 1)].name == "search_kb"
    assert by_name[("decide_action", 0)].name == "model_call"


def test_import_trace_empty_input_raises_value_error():
    """Empty input raises ValueError."""
    import pytest

    with pytest.raises(ValueError, match="No cases could be imported"):
        import_trace([])


def test_import_trace_nonexistent_trace_id_raises_value_error():
    """trace_id that matches no trace raises ValueError."""
    import pytest

    trace_id = "t1"
    rows = [
        _root_row(trace_id),
        _row("o1", trace_id, "collect_evidence_with_tools", "lookup_customer", "2026-06-17T14:00:01Z"),
    ]

    with pytest.raises(ValueError, match="Trace id .* not found in rows"):
        import_trace(rows, trace_id="nonexistent_trace")


def test_import_trace_multiple_traces_without_trace_id_raises_value_error():
    """Multiple distinct traces without trace_id= raises ValueError."""
    import pytest

    rows = [
        _root_row("trace_1"),
        _row("o1", "trace_1", "collect_evidence_with_tools", "lookup_customer", "2026-06-17T14:00:01Z"),
        _root_row("trace_2"),
        _row("o2", "trace_2", "collect_evidence_with_tools", "lookup_customer", "2026-06-17T14:00:02Z"),
    ]

    with pytest.raises(ValueError, match="Multiple traces found"):
        import_trace(rows)


def test_import_trace_multiple_traces_with_matching_trace_id_selects_one():
    """Multiple traces WITH valid trace_id= selects exactly that one."""
    rows = [
        _root_row("trace_1"),
        _row("o1", "trace_1", "collect_evidence_with_tools", "lookup_customer", "2026-06-17T14:00:01Z"),
        _root_row("trace_2"),
        _row("o2", "trace_2", "collect_evidence_with_tools", "lookup_customer", "2026-06-17T14:00:02Z"),
    ]

    case = import_trace(rows, trace_id="trace_2")
    assert case.source_ref.source_id == "trace_2"
    assert len(case.recorded_calls) == 1
    assert case.recorded_calls[0].observation_id == "o2"
