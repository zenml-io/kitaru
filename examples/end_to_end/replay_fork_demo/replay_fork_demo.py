#!/usr/bin/env python
"""Replay & fork a recorded LangGraph→Langfuse trace of the reference agent.

Self-contained end-to-end demo of `kitaru.adapters.langgraph.replay`:

  1. import a recorded trace (rich Langfuse observation rows) as a Case,
  2. reconstruct it as a native Kitaru execution (the "seed"),
  3. replay the tail live from a cut and compare to the trace  -> reproduction drift,
  4. fork the tail with an edited variant and compare to the replay -> fork drift.

The agent it replays is bundled in `./reference_agent` — this example imports
only `kitaru` and its own bundled package, nothing from sibling examples.

To produce a trace + observation rows (needs OpenAI + Langfuse), use the
reference-agent example's generator, then fetch one trace's rows:

    uv run python examples/end_to_end/replay_verify_reference_agent/generate_traces.py \
        --scenario-set full --variants baseline
    uv run examples/replay_verify_imported_cases/fetch_langfuse_observations.py \
        --trace-id <TRACE_ID> --output examples/end_to_end/replay_fork_demo/obs.jsonl

Then run from this folder (uses your active Kitaru stack, so the
reconstruct/replay/fork executions show up in your dashboard; the live tail
re-runs the model, so OPENAI_API_KEY must be set):

    cd examples/end_to_end/replay_fork_demo
    set -a && . ./.env && set +a            # if you keep a local .env here
    uv run python replay_fork_demo.py obs.jsonl
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

from reference_agent import db
from reference_agent.config import EXAMPLE_DIR, load_scenarios, load_variant
from reference_agent.graph import build_graph
from reference_agent.mock_api import MockApiServer
from reference_agent.tools import SupportTools, ToolExecution

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
    obs = sys.argv[1] if len(sys.argv) == 2 else "obs.jsonl"
    if not Path(obs).exists():
        print(__doc__)
        print(f"\nobservation file not found: {obs}")
        raise SystemExit(2)
    raise SystemExit(main(obs))
