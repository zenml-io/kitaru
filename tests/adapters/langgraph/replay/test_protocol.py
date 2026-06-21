from kitaru.adapters.langgraph.replay._protocol import (
    LANGGRAPH_CAPS,
    Caps,
    ReplayAdapter,
)


def test_langgraph_caps_advertise_node_granularity():
    # The reconstruction checkpoints at NODE level: a node's whole callable runs
    # live as one unit. Call-level forking is not built, so Caps must not claim it.
    assert LANGGRAPH_CAPS.fork_granularity == "node"
    assert LANGGRAPH_CAPS.native_checkpoints == "reconstructed"


def test_minimal_adapter_satisfies_protocol():
    class Dummy:
        def seed(self, case):
            return "exec-1"

        def checkpoints(self, seed_exec_id):
            return ["receive_request"]

        def fork(self, seed_exec_id, *, from_, edits, variant):
            return object()

        def capabilities(self):
            return LANGGRAPH_CAPS

    adapter: ReplayAdapter = Dummy()
    assert isinstance(adapter.capabilities(), Caps)
