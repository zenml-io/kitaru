#!/usr/bin/env python
"""Replay & fork a recorded LangGraph→Langfuse trace of the reference agent.

End-to-end demo of `kitaru.adapters.langgraph.replay`:

  1. import a recorded trace (rich Langfuse observation rows) as a Case,
  2. reconstruct it as a native Kitaru execution (the "seed"),
  3. replay the tail live from a cut and compare to the trace  -> reproduction drift,
  4. fork the tail with an edited variant and compare to the replay -> fork drift.

First generate a trace and fetch its observation rows (needs OpenAI + Langfuse):

    uv run python examples/end_to_end/replay_verify_reference_agent/generate_traces.py \
        --scenario-set full --variants baseline
    uv run examples/replay_verify_imported_cases/fetch_langfuse_observations.py \
        --trace-id <TRACE_ID> --output obs.jsonl

Then run THIS demo from the repo root (uses your active Kitaru stack, so the
reconstruct/replay/fork executions show up in your dashboard; the live tail
re-runs the model, so OPENAI_API_KEY must be set):

    uv run python examples/end_to_end/replay_fork_demo.py obs.jsonl

It is run from `examples/end_to_end/` so the reference agent imports as a normal
package and Kitaru resolves as the installed library — no PYTHONPATH, no env vars.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

from replay_verify_reference_agent import db
from replay_verify_reference_agent.config import (
    EXAMPLE_DIR, load_scenarios, load_variant,
)
from replay_verify_reference_agent.graph import build_graph
from replay_verify_reference_agent.mock_api import MockApiServer
from replay_verify_reference_agent.tools import SupportTools, ToolExecution

from kitaru.adapters.langgraph.replay import KitaruReplayAgent, import_trace


def main(obs_path: str) -> int:
    rows = [json.loads(line) for line in Path(obs_path).read_text().splitlines() if line.strip()]
    case = import_trace(rows)
    cfg = case.trace_contract.raw_config
    scenario = {s.scenario_id: s for s in load_scenarios()}[cfg["scenario_id"]]
    variant = load_variant(cfg["variant_name"])
    print(f"imported {case.case_id}  scenario={cfg['scenario_id']}  variant={cfg['variant_name']}")

    # Rehydrate domain-typed node outputs from the trace's recorded per-node deltas,
    # so the live tail's cached head holds the objects the real nodes expect.
    stashed = case.raw_source_payload.get("langgraph_node_outputs", {})
    node_outputs: dict[str, dict] = {}
    if "collect_evidence_with_tools" in stashed:
        node_outputs["collect_evidence_with_tools"] = {
            "tool_executions": [
                ToolExecution.model_validate(te)
                for te in stashed["collect_evidence_with_tools"].get("tool_executions", [])
            ]
        }
    if "summarize_evidence" in stashed:
        node_outputs["summarize_evidence"] = {
            "evidence_summary": stashed["summarize_evidence"].get("evidence_summary", "")
        }

    with MockApiServer() as api:
        db.reset_database()
        tools = SupportTools(db_path=db.DEFAULT_DB_PATH, api_base_url=api.base_url,
                             kb_dir=EXAMPLE_DIR / "knowledge_base")
        graph = build_graph(tools=tools, callbacks=[], metadata={}, tags=[])
        agent = KitaruReplayAgent(graph, fanout_node="collect_evidence_with_tools")

        seed = agent.reconstruct(
            case,
            root_state={"scenario": scenario, "variant": variant},
            node_outputs=node_outputs or None,
        )
        replay = agent.replay(seed, from_="decide_action")
        fork = agent.fork(
            seed, from_="decide_action",
            variant={"model": "gpt-5-nano", "prompt_profile": "trimmed_permissions"},
        )
        report = agent.diff(case, replay, fork)

    decision = case.observed_output.get("decision", {})
    print("\n=== drift ===")
    print(f"  trace decision     : risk_status={decision.get('risk_status')} "
          f"required_action={decision.get('required_action')}")
    print(f"  reproduction_drift : {report.has_reproduction_drift}   (replay vs trace)")
    print(f"  fork_drift         : {report.has_fork_drift}   (fork vs replay)")
    changes = [(c.field, c.baseline_value, c.comparison_value) for c in report.fork if not c.matches]
    print(f"  fork field changes : {changes or 'none'}")
    return 0


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(__doc__)
        raise SystemExit(2)
    raise SystemExit(main(sys.argv[1]))
