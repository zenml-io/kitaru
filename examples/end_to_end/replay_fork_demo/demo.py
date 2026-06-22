"""Replay & fork a recorded LangGraph run — the whole thing, at a glance.

    set -a && . ./.env && set +a
    uv run python demo.py langfuse:<trace_id>     # fetch a live trace, or
    uv run python demo.py obs.jsonl               # use a saved one

`glue` is the only domain-specific part (build the graph + rehydrate typed
state); a JSON-native agent wouldn't need it.
"""
import json
import sys

from kitaru.adapters.langgraph.replay import KitaruAdapter, import_langgraph_trace

import glue

# wrap your existing LangGraph agent
agent = KitaruAdapter(glue.graph(), cut=glue.CUT, rehydrate=glue.rehydrate)

# a case is a recorded run you can fork
ref = sys.argv[1] if len(sys.argv) > 1 else "obs.jsonl"
case = (
    import_langgraph_trace(ref)
    if ref.startswith("langfuse:")
    else import_langgraph_trace(rows=[json.loads(l) for l in open(ref) if l.strip()])
)

# REPLAY — reproduce from the cut: cached before, live after
replay = agent.replay(case)

# FORK — branch at the cut, change the model (+ a looser prompt), run forward
fork = agent.fork(case, model="gpt-5-nano", prompt_profile="trimmed_permissions").run()

# COMPARE — fork vs replay
print(fork.diff(replay))
