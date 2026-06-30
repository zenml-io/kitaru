from kitaru.adapters.langgraph.replay._edits import Edit, edit, resolve_edits


def test_edit_factory_builds_model_edit():
    e = edit("model_call:decide_action", model="gpt-5-nano")
    assert e == Edit(target="model_call:decide_action", model="gpt-5-nano")


def test_call_edit_beats_variant_beats_recorded():
    edits = [edit("model_call:decide_action", model="gpt-5-nano")]
    effective = resolve_edits(
        node="decide_action",
        call_index=None,
        edits=edits,
        variant={"model": "gpt-5-mini"},
        recorded={"model": "gpt-5-pro"},
    )
    assert effective["model"] == "gpt-5-nano"


def test_variant_beats_recorded_when_no_call_edit():
    effective = resolve_edits(
        node="decide_action",
        call_index=None,
        edits=[],
        variant={"model": "gpt-5-mini"},
        recorded={"model": "gpt-5-pro"},
    )
    assert effective["model"] == "gpt-5-mini"


def test_unrelated_node_edit_is_ignored():
    edits = [edit("model_call:summarize_evidence", model="gpt-5-nano")]
    effective = resolve_edits(
        node="decide_action",
        call_index=None,
        edits=edits,
        variant=None,
        recorded={"model": "gpt-5-pro"},
    )
    assert effective["model"] == "gpt-5-pro"
