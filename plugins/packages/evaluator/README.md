# Kitaru built-in evaluators

Run offline evaluations over recorded and imported Kitaru sessions. This package contains the evaluator implementations registered under the `kitaru/` namespace by the default server and resolved by Kitaru workers.

Most users do not install or call this package directly. Select one or more registered evaluators from the CLI:

```bash
kitaru session evaluate "$SESSION_ID" \
  --evaluator kitaru/session-diagnostics@latest \
  --evaluator kitaru/tool-health@latest \
  --wait
```

The evaluators read stored session evidence. They do not run the agent, call a model provider, invoke a live tool, replay a session, or query an external service. Descriptive evaluators report findings without forcing a pass/fail judgment; configured policy evaluators can pass, fail, or hold when evidence is insufficient.

See the [deterministic evaluations guide](https://docs.zenml.io/kitaru/guides/deterministic-evaluations) for the evaluator catalog, parameters, evidence semantics, and versioning guidance.

## Links

- [Kitaru documentation](https://docs.zenml.io/kitaru)
- [Source code](https://github.com/zenml-io/kitaru)
- [Issue tracker](https://github.com/zenml-io/kitaru/issues)

Licensed under Apache-2.0.
