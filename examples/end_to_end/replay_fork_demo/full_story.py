#!/usr/bin/env python
"""The full replay & fork story, end to end, in one self-contained example.

  1. GENERATE — run the bundled LangGraph agent on a permission scenario; the run
     is traced to Langfuse.
  2. IMPORT   — fetch that trace's rich observation rows and import them as a Case.
  3. REPLAY & FORK — reconstruct the run as a native Kitaru execution, replay the
     tail UNCHANGED, then FORK it with a cheaper/looser variant.
  4. COMPARE  — reproduction drift (replay vs trace) and fork drift (fork vs replay).

Self-contained: imports only `kitaru` and the bundled `./reference_agent`.

Run it from this folder (needs OPENAI_API_KEY + Langfuse creds; keep them in a
local .env here). It runs on your active Kitaru stack, so the reconstruct /
replay / fork executions show up in your dashboard:

    cd examples/end_to_end/replay_fork_demo
    set -a && . ./.env && set +a
    uv run python full_story.py

Pass `--obs FILE` to skip generation and replay/fork an already-fetched trace:

    uv run python full_story.py --obs obs.jsonl
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path
from uuid import uuid4

from reference_agent import db
from reference_agent.config import (
    DEFAULT_AGENT_VERSION,
    EXAMPLE_DIR,
    load_scenarios,
    load_variant,
    missing_trace_environment,
)
from reference_agent.graph import build_graph, run_reference_agent
from reference_agent.mock_api import MockApiServer
from reference_agent.tools import SupportTools, ToolExecution

from kitaru.adapters.langgraph.replay import KitaruReplayAgent, import_trace

import comparison_html

# A permission-sensitive scenario: the kind where a cheaper/looser fork is most
# likely to drift in a way that matters.
SCENARIO_ID = "enterprise_permission_request"
VARIANT_NAME = "baseline"
FORK_VARIANT = {"model": "gpt-5-nano", "prompt_profile": "trimmed_permissions"}
# The reference agent's fixed node order; the fork point is decide_action.
NODES = ["receive_request", "collect_evidence_with_tools", "summarize_evidence",
         "decide_action", "final_response"]
CUT = "decide_action"


def _decision_of(result) -> dict:
    """Pull the decision dict out of a replay/fork result's node-output map."""
    outputs = getattr(result, "node_outputs", {}) or {}
    for node in ("decide_action", "final_response"):
        block = outputs.get(node) or {}
        if not isinstance(block, dict):
            continue
        dec = block.get("decision")
        if dec is None and isinstance(block.get("final_output"), dict):
            dec = block["final_output"].get("decision")
        if dec is not None:
            return dec.model_dump() if hasattr(dec, "model_dump") else dict(dec)
    return {}


def _generate_trace(scenario, variant) -> str:
    """Run the agent once with the Langfuse callback; return the trace id."""
    from langfuse import get_client
    from langfuse.langchain import CallbackHandler

    langfuse = get_client()
    trace_id = uuid4().hex
    metadata = {
        "scenario_id": scenario.scenario_id,
        "case_id": scenario.case_id,
        "variant_name": variant.name,
        "agent_version": DEFAULT_AGENT_VERSION,
        "model": variant.model,
        "prompt_profile": variant.prompt_profile,
        "tool_policy_name": variant.tool_policy_name,
        "tool_selection_mode": "llm_tool_calling",
    }
    tags = ["kitaru", "replay-fork-demo", variant.name, scenario.scenario_id]
    handler = CallbackHandler()
    with MockApiServer() as api:
        db.reset_database()
        with langfuse.start_as_current_observation(
            as_type="span",
            name="reference-agent-scenario",
            input={"scenario_id": scenario.scenario_id, "user_request": scenario.user_request},
            metadata={**metadata, "tags": tags},
            trace_context={"trace_id": trace_id},
        ) as root:
            output = run_reference_agent(
                scenario=scenario,
                variant=variant,
                db_path=db.DEFAULT_DB_PATH,
                api_base_url=api.base_url,
                kb_dir=EXAMPLE_DIR / "knowledge_base",
                callbacks=[handler],
                metadata=metadata,
                tags=tags,
            )
            root.update(output=output)
    langfuse.flush()
    return trace_id


def _fetch_rows(trace_id: str, *, timeout: float = 120.0) -> list[dict]:
    """Poll Langfuse until the trace is fully ingested.

    Langfuse ingestion is incremental, so we wait until the rows actually carry
    the node outputs the replay depends on (the skipped head: collect + summarize)
    and the observed decision — not just until some rows exist.
    """
    from langfuse import Langfuse

    needed_nodes = {"collect_evidence_with_tools", "summarize_evidence"}
    client = Langfuse()
    deadline = time.monotonic() + timeout
    rows: list[dict] = []
    while time.monotonic() < deadline:
        resp = client.api.observations.get_many(
            trace_id=trace_id,
            limit=100,
            fields="core,basic,io,metadata,model,usage",
        )
        rows = [json.loads(obs.model_dump_json()) for obs in resp.data]
        if rows:
            try:
                case = import_trace(rows)
                node_outputs = case.raw_source_payload.get("langgraph_node_outputs", {})
                has_decision = bool(case.observed_output.get("decision"))
                if needed_nodes <= set(node_outputs) and has_decision:
                    return rows
            except Exception:
                pass  # partial ingestion: keep polling
        time.sleep(3)
    raise SystemExit(
        f"Trace {trace_id} not fully ingested after {timeout:.0f}s "
        f"({len(rows)} rows so far)."
    )


def _typed_node_outputs(case) -> dict[str, dict]:
    """Rehydrate domain-typed node deltas so the live tail's cached head is real."""
    stashed = case.raw_source_payload.get("langgraph_node_outputs", {})
    out: dict[str, dict] = {}
    if "collect_evidence_with_tools" in stashed:
        out["collect_evidence_with_tools"] = {
            "tool_executions": [
                ToolExecution.model_validate(te)
                for te in stashed["collect_evidence_with_tools"].get("tool_executions", [])
            ]
        }
    if "summarize_evidence" in stashed:
        out["summarize_evidence"] = {
            "evidence_summary": stashed["summarize_evidence"].get("evidence_summary", "")
        }
    return out


def main(obs_file: str | None) -> int:
    scenarios = {s.scenario_id: s for s in load_scenarios()}

    # 1) GENERATE (or load a saved trace) ----------------------------------- #
    if obs_file:
        print(f"[1-2/4] import  — loading observation rows from {obs_file}")
        rows = [json.loads(l) for l in Path(obs_file).read_text().splitlines() if l.strip()]
    else:
        missing = missing_trace_environment()
        if missing:
            print("Generating a trace needs OpenAI + Langfuse creds. Missing: " + ", ".join(missing))
            return 2
        scenario, variant = scenarios[SCENARIO_ID], load_variant(VARIANT_NAME)
        print(f"[1/4]   generate — running the LangGraph agent "
              f"(scenario={SCENARIO_ID}, variant={VARIANT_NAME}) → Langfuse")
        trace_id = _generate_trace(scenario, variant)
        print(f"        trace_id={trace_id}")
        print("[2/4]   import   — waiting for Langfuse ingestion, then fetching rows…")
        rows = _fetch_rows(trace_id)

    # 2) IMPORT ------------------------------------------------------------- #
    case = import_trace(rows)
    cfg = case.trace_contract.raw_config
    scenario = scenarios[cfg["scenario_id"]]
    variant = load_variant(cfg["variant_name"])
    observed = case.observed_output.get("decision", {})
    print(f"        imported {case.case_id}: {len(case.recorded_calls)} recorded calls; "
          f"observed decision risk_status={observed.get('risk_status')} "
          f"required_action={observed.get('required_action')}")

    # 3) REPLAY & FORK ------------------------------------------------------ #
    print(f"[3/4]   replay & fork — reconstruct, replay the tail UNCHANGED, "
          f"then FORK ({FORK_VARIANT['model']} + {FORK_VARIANT['prompt_profile']})")
    with MockApiServer() as api:
        db.reset_database()
        tools = SupportTools(db_path=db.DEFAULT_DB_PATH, api_base_url=api.base_url,
                             kb_dir=EXAMPLE_DIR / "knowledge_base")
        graph = build_graph(tools=tools, callbacks=[], metadata={}, tags=[])
        agent = KitaruReplayAgent(graph, fanout_node="collect_evidence_with_tools")

        seed = agent.reconstruct(
            case,
            root_state={"scenario": scenario, "variant": variant},
            node_outputs=_typed_node_outputs(case) or None,
        )
        replay = agent.replay(seed, from_=CUT)
        fork = agent.fork(seed, from_=CUT, variant=FORK_VARIANT)
        report = agent.diff(case, replay, fork)

    # 4) COMPARE ------------------------------------------------------------ #
    print("[4/4]   compare")
    print(f"        reproduction  (replay vs trace): drift={report.has_reproduction_drift}")
    print(f"        fork          (fork  vs replay): drift={report.has_fork_drift}")
    changes = [(c.field, c.baseline_value, c.comparison_value) for c in report.fork if not c.matches]
    print(f"        fork changes : {changes or 'none'}")

    # HTML comparison report — the PRD "compare original vs fork" view.
    out_path = comparison_html.write(
        "replay_vs_fork.html",
        case_id=case.case_id,
        scenario=cfg["scenario_id"],
        cut=CUT,
        nodes=NODES,
        settings_changes=[
            (k, getattr(variant, k, None), v)
            for k, v in FORK_VARIANT.items()
            if getattr(variant, k, None) != v
        ],
        outcomes=[(c.field, c.baseline_value, c.comparison_value, c.matches) for c in report.fork],
        has_fork_drift=report.has_fork_drift,
        replay_summary=_decision_of(replay).get("summary", ""),
        fork_summary=_decision_of(fork).get("summary", ""),
    )
    print(f"        html report   : {out_path}")
    return 0


if __name__ == "__main__":
    obs = None
    if len(sys.argv) == 3 and sys.argv[1] == "--obs":
        obs = sys.argv[2]
    elif len(sys.argv) != 1:
        print(__doc__)
        raise SystemExit(2)
    raise SystemExit(main(obs))
