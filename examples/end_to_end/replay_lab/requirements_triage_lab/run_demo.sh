#!/usr/bin/env bash
#
# Replayability demo — engineering requirements triage, via CLI (no dashboard).
#
# Beat 1: seed a cohort of "production" requirements-triage executions.
# Beat 2: REPLAY one case from a checkpoint with a DIFFERENT model, and show the
#         cheaper model silently drop a mandatory safety sign-off.
# Beat 3: run Replay Lab over the whole cohort -> ship/caution/HOLD verdict.
#
# Deterministic: identical every run, no API keys, no network beyond the Kitaru
# server. Run from the repo root:  bash <this script>

set -euo pipefail
cd "$(git rev-parse --show-toplevel)"
export ZENML_DISABLE_CLIENT_SERVER_MISMATCH_WARNING=True
RL=examples/end_to_end/replay_lab/requirements_triage_lab

echo
echo "########## BEAT 1: seed production requirements-triage executions ##########"
uv run python $RL/seed_observed.py 2>/dev/null | grep -E "Seeding|execution:|Wrote manifest"

# Grab the safety-critical case's execution id (the load-bearing bracket).
BRACKET=$(uv run python -c "
import json,sys
m=json.load(open('$RL/manifests/requirements_triage.json'))
print(next(c['exec_id'] for c in m['cases'] if c['case_id']=='bracket-load-signoff'))
")
echo "Bracket-load-signoff execution: $BRACKET"

echo
echo "########## BEAT 2: REPLAY that case from 'draft_response' with the cheaper model ##########"
echo "\$ kitaru executions replay $BRACKET --from draft_response --args '{\"agent_profile\":\"candidate\"}'"
REPLAY=$(uv run kitaru executions replay "$BRACKET" \
  --from draft_response --args '{"agent_profile":"candidate"}' --output json 2>/dev/null \
  | grep '^{' | tail -1 \
  | python3 -c "import json,sys; print(json.load(sys.stdin)['item']['exec_id'])")
echo "Replayed execution: $REPLAY"

echo
echo "########## The divergence (original vs replayed) ##########"
uv run python $RL/show_divergence.py "$BRACKET" "$REPLAY" 2>/dev/null

echo
echo "########## BEAT 3: Replay Lab over the whole cohort -> verdict ##########"
uv run python $RL/run_replay_lab.py >/dev/null 2>&1
uv run python $RL/render_panel.py 2>/dev/null

echo
echo "Done. Everything above ran via the CLI/SDK — no dashboard required."
