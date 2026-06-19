from kitaru.adapters.langgraph.replay import import_trace

_KIND_TO_TYPE = {
    "tool": "TOOL",
    "llm": "GENERATION",
    "retrieval": "RETRIEVAL",
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
