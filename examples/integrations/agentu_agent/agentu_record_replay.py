"""agentu + Kitaru: record an agent workflow, then fork it from any step.

agentu chains agents into workflows (`>>` sequential, `&` parallel) and its
sessions support `checkpoint(fork=True)` to A/B-test a conversation from a
decision point. Kitaru applies that fork idea to the *whole workflow*: every
step of a run is recorded as a durable checkpoint, and you can replay
("fork") a finished run from any step — across processes, machines, and
days — with edited step outputs or a different model, without re-running or
re-paying for the steps before it.

The scenario mirrors agentu's own workflow example: researcher agents fan
out over three topics in parallel, an analyst compares the findings, and a
writer drafts a report. On top of that, this example shows:

1. **Durable checkpoints** — each agentu call (`infer()` tool turns,
   `stream()` drafting) runs inside a `@kitaru.checkpoint`; researcher
   checkpoints run concurrently via `.submit()`.
2. **Inner-call tracing** — the `KitaruTrace` middleware plugs into agentu's
   standard middleware pipeline and saves every inner LLM prompt/response as
   Kitaru artifacts, with per-call latency metadata, on the enclosing
   checkpoint.
3. **Fork 1 — counterfactual** — replay only the writer step with the
   analyst's recorded conclusion swapped ("what would the report say if the
   analysis had gone the other way?").
4. **Fork 2 — model swap** (set `AGENTU_ALT_MODEL`) — replay the writer step
   on a different model with identical recorded inputs: a true A/B of models
   on one step, not two fresh runs that took different paths.

Run:
    cd examples/integrations/agentu_agent
    uv sync --extra local && uv pip install agentu
    uv run kitaru init      # one-time
    uv run python agentu_record_replay.py

Uses your local Ollama by default (same zero-config default as agentu).
Override with AGENTU_MODEL / AGENTU_API_BASE / AGENTU_API_KEY, and set
AGENTU_ALT_MODEL to enable the model-swap fork, e.g.:
    export AGENTU_API_BASE=https://api.openai.com/v1
    export AGENTU_API_KEY=sk-...
    export AGENTU_MODEL=gpt-4o
    export AGENTU_ALT_MODEL=gpt-4o-mini

Anthropic's OpenAI-compatible endpoint works too:
    export AGENTU_API_BASE=https://api.anthropic.com/v1
    export AGENTU_API_KEY=sk-ant-...
    export AGENTU_MODEL=claude-haiku-4-5-20251001
    export AGENTU_ALT_MODEL=claude-sonnet-4-5
"""

import asyncio
import os
import time

from agentu import Agent
from agentu.middleware.middleware import BaseMiddleware, CallContext

import kitaru
from kitaru import checkpoint, flow
from kitaru.client import ExecutionStatus

MODEL = os.getenv("AGENTU_MODEL")  # None -> agentu auto-detects from Ollama
ALT_MODEL = os.getenv("AGENTU_ALT_MODEL")
API_BASE = os.getenv("AGENTU_API_BASE", "http://localhost:11434/v1")
API_KEY = os.getenv("AGENTU_API_KEY")

TOPICS = ["AI agents", "ML infrastructure", "AI safety"]


class KitaruTrace(BaseMiddleware):
    """agentu middleware that records every inner LLM call in Kitaru.

    agentu invokes ``after`` on each model round-trip (both ``infer`` tool
    loops and ``stream`` calls). Because the agent runs inside a Kitaru
    checkpoint, ``kitaru.save``/``kitaru.log`` attach the prompt, response,
    and latency of each inner call to that checkpoint.
    """

    name = "kitaru_trace"

    def __init__(self, label: str):
        self.label = label
        self.calls = 0

    async def after(self, context: CallContext, response: str) -> str:
        self.calls += 1
        tag = f"{self.label}_llm_{self.calls}"
        kitaru.save(f"{tag}_prompt", context.prompt, type="prompt")
        kitaru.save(f"{tag}_response", response, type="response")
        kitaru.log(**{f"{tag}_latency_ms": round(context.elapsed_ms, 1)})
        return response


def research_notes(topic: str) -> dict:
    """Return raw notes for a topic (stand-in for a real search tool)."""
    notes = {
        "AI agents": "Agent frameworks multiplied in 2025; reliability is the gap.",
        "ML infrastructure": "Inference cost fell 10x; orchestration became the moat.",
        "AI safety": "Eval suites matured; production incident reviews are now common.",
    }
    return {"topic": topic, "notes": notes.get(topic, f"No notes on {topic}.")}


def make_agent(name: str, trace_label: str, model: str | None) -> Agent:
    """A plain agentu agent, with only the Kitaru trace middleware added."""
    agent = Agent(
        name,
        model=model,
        api_base=API_BASE,
        api_key=API_KEY,
        enable_memory=False,
    )
    return agent.use(KitaruTrace(trace_label))


@checkpoint(type="llm_call")
def research(topic: str, model: str | None) -> dict:
    """One researcher turn: the agent picks and calls the research tool."""

    async def turn() -> dict:
        agent = make_agent("researcher", "research", model).with_tools([research_notes])
        try:
            return await agent.infer(
                f"Research the topic '{topic}' with the research_notes tool."
            )
        finally:
            await agent.close()

    result = turn_result = asyncio.run(turn())
    if result.get("history"):
        turn_result = result["history"][-1]
    kitaru.log(topic=topic, tool_used=str(turn_result.get("tool_used")))
    findings = result.get("text_response") or str(turn_result.get("result", result))
    return {"topic": topic, "findings": findings}


@checkpoint(type="llm_call")
def analyze(findings: list[dict], model: str | None) -> str:
    """The analyst compares all researcher findings (fan-in)."""
    bullet_list = "\n".join(f"- {f['topic']}: {f['findings']}" for f in findings)
    prompt = (
        f"Compare these research findings and state the single most important "
        f"trend in two sentences:\n{bullet_list}"
    )

    async def turn() -> str:
        agent = make_agent("analyst", "analyze", model)
        try:
            return "".join([chunk async for chunk in agent.stream(prompt)])
        finally:
            await agent.close()

    return asyncio.run(turn())


@checkpoint(type="llm_call")
def write_report(analysis: str, model: str | None) -> str:
    """The writer drafts the report from the analyst's conclusion."""
    prompt = (
        f"Analysis: {analysis}\n"
        "Write a three-sentence executive report based on this analysis."
    )

    async def turn() -> str:
        agent = make_agent("writer", "write_report", model)
        try:
            return "".join([chunk async for chunk in agent.stream(prompt)])
        finally:
            await agent.close()

    return asyncio.run(turn())


@flow(cache=False)  # agent turns are non-deterministic; always run them fresh
def content_pipeline(topics: list[str], model: str | None = None) -> str:
    """Researcher fan-out, analyst fan-in, writer — every step recorded."""
    futures = [research.submit(topic, model) for topic in topics]
    findings = [future.result() for future in futures]
    analysis = analyze(findings, model)
    return write_report(analysis, model)


def wait_for(client: kitaru.KitaruClient, exec_id: str, timeout: float = 180.0):
    """Poll an execution until it reaches a terminal state."""
    deadline = time.time() + timeout
    while time.time() <= deadline:
        execution = client.executions.get(exec_id)
        if execution.status in {
            ExecutionStatus.COMPLETED,
            ExecutionStatus.FAILED,
            ExecutionStatus.CANCELLED,
        }:
            return execution
        time.sleep(1)
    raise TimeoutError(f"Execution {exec_id} did not finish within {timeout}s.")


def load_report(client: kitaru.KitaruClient, exec_id: str) -> str:
    """Fetch the write_report checkpoint output of a finished execution."""
    execution = wait_for(client, exec_id)
    if execution.status != ExecutionStatus.COMPLETED:
        raise RuntimeError(f"Replay finished with status '{execution.status.value}'.")
    report_cp = next(cp for cp in execution.checkpoints if cp.name == "write_report")
    # The checkpoint's own output is named `<flow>::<checkpoint>::output`;
    # KitaruTrace's saved prompts/responses sit alongside it as extra outputs.
    output = next(a for a in report_cp.artifacts if a.name.endswith("::output"))
    return str(output.load())


def fork_with_counterfactual(client: kitaru.KitaruClient, exec_id: str) -> str:
    """Replay the writer step with the analyst's recorded conclusion swapped."""
    counter_analysis = (
        "Contrary take: agent frameworks are consolidating, and the real "
        "bottleneck is trust in autonomous runs, not infrastructure cost."
    )
    replayed = client.executions.replay(
        exec_id,
        at="write_report",
        checkpoint_overrides={"analyze": {"output": counter_analysis}},
    )
    return replayed.results[0].replay_exec_id


def fork_with_model(client: kitaru.KitaruClient, exec_id: str, model: str) -> str:
    """Replay the writer step on a different model with identical inputs."""
    replayed = client.executions.replay(
        exec_id,
        at="write_report",
        flow_overrides={"model": model},
    )
    return replayed.results[0].replay_exec_id


def run_workflow(topics: list[str] = TOPICS) -> dict[str, str]:
    """Record the pipeline, then fork it from the writer step.

    Returns:
        Mapping of run label to execution ID.
    """
    executions: dict[str, str] = {}

    handle = content_pipeline.run(topics, MODEL)
    original_report = str(handle.wait())
    executions["source"] = handle.exec_id
    print(f"\nOriginal report ({MODEL or 'auto'}):\n  {original_report}\n")

    client = kitaru.KitaruClient()

    fork_id = fork_with_counterfactual(client, handle.exec_id)
    executions["counterfactual"] = fork_id
    print("Fork 1 - counterfactual analysis (researchers + analyst NOT re-run):")
    print(f"  {load_report(client, fork_id)}\n")

    if ALT_MODEL:
        fork_id = fork_with_model(client, handle.exec_id, ALT_MODEL)
        executions["model_swap"] = fork_id
        print(f"Fork 2 - writer swapped to {ALT_MODEL} (same recorded inputs):")
        print(f"  {load_report(client, fork_id)}\n")
    else:
        print("Set AGENTU_ALT_MODEL to also fork the writer onto another model.\n")

    return executions


def main() -> None:
    """Run the example as a script."""
    executions = run_workflow()
    for label, exec_id in executions.items():
        print(f"{label:>16}: {exec_id}")
    print("\nInspect the runs with: kitaru executions list")
    print("Each checkpoint carries the traced inner LLM prompts and responses.")


if __name__ == "__main__":
    main()
