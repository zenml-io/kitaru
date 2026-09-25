# Kitaru ATIF importer

Development package for importing [Agent Trajectory Interchange Format (ATIF)](https://www.harborframework.com/docs/agents/trajectory-format) JSON and local Harbor rollout outputs. It is not yet published or part of Kitaru's default importer catalog.

## Prepare Harbor outputs locally

From the Kitaru checkout, collect a Harbor job or trial directory into one upload:

```bash
uv run --project plugins python -m kitaru_atif_importer.harbor /path/to/harbor/job --output /tmp/harbor-import.json
```

The helper collects `agent/trajectory.json` and `steps/<name>/agent/trajectory.json`, with selected outcome and identity fields from each trial's `result.json`. It excludes Harbor configurations and agent-result metadata, which may contain credentials. Trajectory text is preserved: review it before uploading. Symlinked directories are skipped, symlinked input files are rejected, and referenced media or external trajectories are never read. Output must be a new file. The helper limits input and output to 90 MiB and 10,000 trajectories; select smaller job directories for larger collections.

## Register and test the development importer

After logging into your intended test server, register the self-contained parser script:

```bash
uv run kitaru importer register dev-atif --server SERVER_URL --provider atif --script plugins/packages/atif-importer/src/kitaru_atif_importer/importer.py --entrypoint parse --display-version dev
uv run kitaru session import /tmp/harbor-import.json --server SERVER_URL --importer dev-atif@1 --agent AGENT@VERSION --wait
```

An importer-capable worker must be running for that server. Registration uploads this script only; it does not publish a package. Register a new importer version after changing the script. A standalone ATIF JSON file can replace the bundle in the import command.

## Payload contract

`parse(payload: bytes, params: dict)` yields normalized sessions or isolated record failures. Besides a single ATIF trajectory, it accepts this envelope:

```json
{
  "trajectories": [
    {
      "source_id": "trial-uuid/agent/trajectory.json",
      "trajectory": {"schema_version": "ATIF-v1.7", "agent": {"name": "example", "version": "1"}, "steps": [{"step_id": 1, "source": "user", "message": "Solve the task"}]},
      "harbor_result": {"id": "trial-uuid", "verifier_result": {"rewards": {"reward": 1}}}
    }
  ]
}
```

Each envelope entry can alternatively be a plain ATIF trajectory. The optional `namespace` parser parameter separates independent collections; use it when source IDs are only unique within a job. The helper prefers Harbor trial IDs and includes the trajectory's relative segment path. It falls back to paths relative to the selected input directory when trial IDs are absent. Reimporting an existing source identity skips it rather than updating it.

## Fidelity and limits

- Each root trajectory becomes a session. Multi-step Harbor trials remain separate sessions with their trial identity and step outcome. Continuation files are not automatically stitched together.
- Explicit reasoning, tool calls/results, model identity, per-step tokens and recorded costs use Kitaru fields. Structured content, source extras, final metrics and training-specific data remain available as source data.
- Aggregate or deterministic steps remain spans. Kitaru's native model-call count therefore counts represented individual calls, not every underlying inference in an aggregate step. Copied context retains its evidence without adding historical calls, tokens, or costs again.
- If no descendant reports a cost and there is no copied context, the recorded final cost is assigned to the trajectory span. Otherwise final metrics remain metadata, so partial or subagent costs are not double-counted. Missing per-step token usage is not reconstructed from totals.
- Point timestamps do not establish durations. Missing or timezone-naive timestamps do not get invented timezone information.
- Harbor execution errors are separate from verifier rewards. A zero reward does not mark an otherwise completed execution as failed. Rewards are metadata, not native Kitaru evaluation results.
- Embedded subagent relationships are retained where supplied. External references and media paths are preserved without fetching their content. Imported transcripts alone do not make a Harbor sandbox replayable.

Local parser tests and clean-wheel checks precede live-server verification. See [plugin development](../../DEVELOPMENT.md) for clean worker and candidate-wheel setup.
