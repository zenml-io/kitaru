# Evaluation, Replay, and Experiment Contracts

Use this reference when changing evaluation, replay, or experiment behavior. Cross-check the current DTOs in `src/kitaru/api_models/v1/{evaluation,replay,experiment_run,ui}.py` and CLI specs in `src/kitaru/cli/app.py` before editing these contracts.

Evaluator selections carry per-version parameters. On CLI commands that select evaluators, repeat `--evaluator-params 'EVALUATOR@VERSION=JSON_OBJECT'` for each configured evaluator that needs parameters; the token must match one of the exact `--evaluator` tokens. Preserve the parameters in examples and reviewer reproduction steps because evaluator-produced rows record them and baseline reuse compares them.

Replay creation and experiment-run start use `--baseline-evaluation-mode none|if_missing|force`, defaulting to `if_missing`. `none` does not score baselines, `if_missing` adopts the latest existing evaluation only when the session, evaluator version, and parameters all match and otherwise schedules a fresh baseline evaluation, and `force` always schedules a fresh baseline evaluation. The old `evaluate_baselines` boolean survives only on REST requests and responses for compatibility; do not expose it in new SDK or CLI guidance, and never send it together with `baseline_evaluation_mode`.

`EvaluationResult` accepts optional `min_score`, `max_score`, and `target_score` only for float results. Evaluator-produced evaluation responses include the producing evaluator version and the parameters it ran with; manual rows have neither provenance value. Manual evaluations are append-only per session and name, and a duplicate returns HTTP 409.

Experiment-run aggregates are pinned to the evaluations linked to each replay. Later evaluations of the same sessions do not change an existing run's statistics. Groups are keyed by evaluation name, data type, and evaluator version; manual evaluations are excluded. Keep these fields and the linked-evaluation semantics intact when changing aggregate DTOs or frontend mappings.
