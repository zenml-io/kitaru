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


def _camel_row(obs_id, trace_id, name, started_at, *, parent, type_, metadata=None, output=None):
    """A Langfuse-API-shaped (camelCase) observation row, like a real export."""
    return {
        "id": obs_id,
        "traceId": trace_id,
        "type": type_,
        "name": name,
        "parentObservationId": parent,
        "startTime": started_at,
        "metadata": metadata or {},
        "input": {"args": {}},
        "output": {"ok": True} if output is None else output,
    }


def _node_output_rows(trace_id):
    """Real-shape rows: node-level CHAIN spans carry the node's recorded output."""
    return [
        {
            "id": "root", "traceId": trace_id, "type": "SPAN", "name": "agent",
            "parentObservationId": None, "startTime": "2026-06-17T14:00:00Z",
            "input": {"user_request": "hi"},
            "output": {"decision": {"policy_label": "billing_policy", "risk_status": "safe"}},
            "metadata": {},
        },
        _camel_row("n_collect", trace_id, "collect_evidence_with_tools", "2026-06-17T14:00:01Z",
                   parent="root", type_="CHAIN",
                   metadata={"langgraph_node": "collect_evidence_with_tools"},
                   output={"tool_executions": [{"name": "lookup_customer", "kind": "db_read"}]}),
        _camel_row("n_sum", trace_id, "summarize_evidence", "2026-06-17T14:00:02Z",
                   parent="root", type_="CHAIN",
                   metadata={"langgraph_node": "summarize_evidence"},
                   output={"evidence_summary": "facts about Globex"}),
        # an inner call that inherits the node tag but is NOT the node span
        _camel_row("g1", trace_id, "ChatOpenAI", "2026-06-17T14:00:03Z",
                   parent="n_sum", type_="GENERATION", metadata={}),
    ]


def test_import_trace_extracts_per_node_outputs_from_node_spans():
    case = import_trace(_node_output_rows("tr-out"))
    node_outputs = case.raw_source_payload["langgraph_node_outputs"]
    assert node_outputs["summarize_evidence"] == {"evidence_summary": "facts about Globex"}
    assert node_outputs["collect_evidence_with_tools"]["tool_executions"][0]["name"] == "lookup_customer"
    # the inner ChatOpenAI call is NOT mistaken for a node-level output
    assert "ChatOpenAI" not in node_outputs


def test_node_outputs_from_case_seeds_skipped_nodes_with_recorded_outputs():
    from kitaru.adapters.langgraph.replay._agent import _node_outputs_from_case

    case = import_trace(_node_output_rows("tr-seed"))
    nodes = ["collect_evidence_with_tools", "summarize_evidence", "decide_action"]
    outputs = _node_outputs_from_case(case, nodes)
    # skipped upstream nodes now carry their real recorded state delta
    assert outputs["summarize_evidence"] == {"evidence_summary": "facts about Globex"}
    assert outputs["collect_evidence_with_tools"]["tool_executions"][0]["name"] == "lookup_customer"
    # the decision node still carries the observed decision
    assert outputs["decide_action"]["decision"]["risk_status"] == "safe"


def test_import_trace_attributes_nodes_via_tree_when_calls_lack_langgraph_node():
    """Real Langfuse shape: call observations (GENERATION/TOOL) carry NO
    langgraph_node on themselves — only the enclosing node-level span does.

    Regression guard for the verified upstream behavior: the reference agent
    invokes each model with an explicit config={'metadata': ...}, which drops
    the inherited langgraph_node from the LLM/tool run. The importer must
    attribute each call to its node via parentObservationId, NOT fall back to
    the call's own observation name (lookup_customer / ChatOpenAI).
    """
    tid = "tr-real"
    rows = [
        # trace root (the graph invoke); decision lives on its output
        {
            "id": "root", "traceId": tid, "type": "SPAN", "name": "agent",
            "parentObservationId": None, "startTime": "2026-06-17T14:00:00Z",
            "input": {"user_request": "hi"},
            "output": {"decision": {"policy_label": "billing_policy", "risk_status": "safe"}},
            "metadata": {},
        },
        # node-level spans carry langgraph_node (dropped from recorded_calls as "other")
        _camel_row("n_collect", tid, "collect_evidence_with_tools", "2026-06-17T14:00:01Z",
                   parent="root", type_="SPAN",
                   metadata={"langgraph_node": "collect_evidence_with_tools"}),
        _camel_row("n_decide", tid, "decide_action", "2026-06-17T14:00:05Z",
                   parent="root", type_="SPAN",
                   metadata={"langgraph_node": "decide_action"}),
        # the actual calls — NO langgraph_node on the call observation itself
        _camel_row("t1", tid, "lookup_customer", "2026-06-17T14:00:02Z",
                   parent="n_collect", type_="TOOL", metadata={}),
        _camel_row("g1", tid, "ChatOpenAI", "2026-06-17T14:00:06Z",
                   parent="n_decide", type_="GENERATION", metadata={"model": "gpt-5-mini"}),
    ]

    case = import_trace(rows)
    by_obs = {c.observation_id: c for c in case.recorded_calls}

    # attributed to the enclosing node, NOT to the call's own name
    assert by_obs["t1"].node == "collect_evidence_with_tools"
    assert by_obs["g1"].node == "decide_action"
    assert by_obs["t1"].call_index == 0
    assert by_obs["g1"].call_index == 0


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
