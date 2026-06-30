from kitaru._replay_verify_imported_models import RecordedCall


def test_recorded_call_carries_node_and_call_index():
    call = RecordedCall(
        kind="tool",
        name="lookup_customer",
        node="collect_evidence_with_tools",
        call_index=0,
    )
    assert call.node == "collect_evidence_with_tools"
    assert call.call_index == 0


def test_recorded_call_node_keying_defaults_to_none():
    call = RecordedCall(kind="llm", name="decide_action")
    assert call.node is None
    assert call.call_index is None
