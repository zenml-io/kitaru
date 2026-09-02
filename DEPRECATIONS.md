# Deprecations

Deprecated surfaces that are still served for backwards compatibility. One entry per surface: what is deprecated, what replaces it, and when it can be removed.

## `evaluate_baselines` on replay and experiment run create requests and responses

- **Deprecated:** The `evaluate_baselines` boolean on `POST /api/v1/replays`, `POST /api/v1/experiments/{experiment_id}/runs`, and their responses. Requests still accept it, mapping `false` to `none` and `true` to `if_missing`. Responses still emit it, derived as `baseline_evaluation_mode != none`.
- **Replaced by:** `baseline_evaluation_mode` with values `none`, `if_missing`, and `force`.
- **Removable:** Once the dashboard and generated clients read `baseline_evaluation_mode`.
