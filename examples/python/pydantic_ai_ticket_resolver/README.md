# Investigate a PydanticAI agent with Kitaru

This directory is Kitaru's canonical investigation example. It contains a PydanticAI returns agent, ten checked-in Langfuse traces with model-generated reasoning summaries, and deterministic tests. All customers, orders, shipments, and actions are synthetic. Refund and replacement tools modify only an in-memory store.

Use this README to prepare the example and import its starting evidence. Continue with the [complete returns-agent tutorial](../../../docs/book/tutorials/returns-agent/README.md) for the investigation, evaluator, replay, and comparison method.

## Watch the guided tour

Want to see the complete Kitaru loop without setting anything up first? [Watch the 26-minute guided tour](https://youtu.be/aYLfzXEr2Rk). Alex starts with the Kitaru Quickstart, then uses the `kitaru-guided-tour` skill on this example to review recorded sessions, define an evaluator and cohort, replay one improvement, and compare the result.

<p align="center">
  <a href="https://youtu.be/aYLfzXEr2Rk"><img src="assets/kitaru-guided-tour.webp" alt="Watch the Kitaru guided tour on YouTube"></a>
</p>

## Requirements

- [Git](https://git-scm.com/)
- [uv](https://docs.astral.sh/uv/)
- Docker, only if you use the optional local Kitaru server
- Node.js and `npx`, for installing the optional coding-agent skills

No model-provider or Langfuse credentials are needed for the checked-in import.

## Prepare the example

Clone Kitaru, enter the example directory, and install its frozen environment:

```bash
git clone https://github.com/zenml-io/kitaru.git
cd kitaru/examples/python/pydantic_ai_ticket_resolver
uv sync --frozen
```

Check the currently selected Kitaru server:

```bash
uv run kitaru status
```

If the selected server is healthy, keep using it. It can be local or cloud. Check whether this template is already set up there:

```bash
uv run kitaru agent get returns-resolver
uv run kitaru session list \
  --agent returns-resolver \
  --tag returns-baseline \
  --origin imported \
  --size 20
```

If the agent and its ten imported sessions already exist, skip to [Continue with a coding agent](#continue-with-a-coding-agent). The guided tour will inspect and resume that state before it creates anything. If neither exists, continue with the registration below. If only part of the setup exists, or `returns-resolver` belongs to another project, select a different server so the fixed example names do not collide.

If no usable server is selected and you want an isolated local server for the example, start and select one with Docker:

```bash
uv run kitaru login --local
uv run kitaru status
```

Register the included agent from the repository root:

<!-- e2e:register -->

```bash
uv run kitaru agent register \
  returns-resolver \
  --command "python -m returns_agent.agent" \
  --description "Resolve one synthetic returns or delivery request, execute one mock action, and draft the customer reply." \
  --display-version baseline-v1 \
  --working-dir . \
  --timeout-seconds 180 \
  --tool lookup_order \
  --tool get_return_policy \
  --tool check_shipping \
  --tool issue_refund \
  --tool create_replacement \
  --tool escalate_to_human
```

Open a second terminal in this directory and start a worker. No model-provider credentials are needed to import the checked-in traces:

<!-- e2e:worker -->

```bash
uv run kitaru worker start --name canonical-example-worker --concurrency 10
```

Leave the worker running while you import and investigate. Return to the first terminal for the remaining commands.

Import the checked-in Langfuse traces:

<!-- e2e:import -->

```bash
uv run kitaru session import \
  traces/langfuse-traces.jsonl \
  --importer kitaru/langfuse@latest \
  --agent returns-resolver@1 \
  --tag returns-baseline \
  --params '{"source_instance":"kitaru-canonical-example"}' \
  --media-type application/x-ndjson \
  --wait
```

Confirm that all ten sessions are available:

<!-- e2e:list -->

```bash
uv run kitaru session list \
  --tag returns-baseline \
  --origin imported \
  --size 20
```

## Continue with a coding agent

Install the Kitaru skills:

```bash
npx skills add zenml-io/kitaru-skills
```

Then give your coding agent this prompt:

```text
Use the kitaru-guided-tour skill with the registered returns-resolver agent
and the sessions tagged returns-baseline. Walk me through the prepared review
and show me the relevant trace evidence before asking for each judgment. Once
we agree on a behavior to improve, help me turn it into an evaluator and test
one small change. Show me the full run plan and ask before changing code or
starting paid model work.
```

The skill stores investigation state in Kitaru and can resume from existing agents, import jobs, tags, and sessions. The [complete tutorial](../../../docs/book/tutorials/returns-agent/README.md) explains the five-step method and the commands behind it.

### Already familiar with Kitaru?

If you have read the [Kitaru quickstart](https://docs.zenml.io/kitaru/getting-started/quickstart) and understand how Kitaru moves from recorded evidence to an evaluator and replay, you can use the less scripted investigation skill instead:

```text
Use kitaru-investigation with the registered returns-resolver agent and the
sessions tagged returns-baseline. Start from the recorded evidence and help
me decide what is worth investigating. Once I accept a finding, help me turn
it into an evaluator and test one bounded change. Ask before creating
resources, changing code, or starting paid replay.
```

If the investigation points to agent behavior, change `returns_agent/agent.py` and register the new implementation as another agent version before running the experiment.

When you finish investigating, press `Ctrl-C` in the worker terminal. If you selected the temporary local server for this example, disconnect from it with `uv run kitaru logout`.

When you are ready to investigate your own agent, open its project and start with `kitaru-investigation`.
