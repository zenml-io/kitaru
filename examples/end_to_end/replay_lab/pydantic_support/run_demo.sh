#!/usr/bin/env bash
#
# End-to-end demo driver: durable PydanticAI support agent -> Replay Lab.
# Run from the repo root: bash examples/end_to_end/replay_lab/pydantic_support/run_demo.sh
#
# Sections marked [ALREADY DONE] reflect state that has already been created in
# this environment (model aliases registered, cohort seeded, reports rendered).
# They are safe and idempotent to re-run, but you do NOT need to for the demo —
# they are here so the script is reproducible from scratch on a clean machine.

set -euo pipefail
cd "$(git rev-parse --show-toplevel)"
export ZENML_DISABLE_CLIENT_SERVER_MISMATCH_WARNING=True

PS="examples/end_to_end/replay_lab/pydantic_support"   # PydanticAI Replay Lab
DET="examples/end_to_end/replay_lab"                    # deterministic fallback

# ---------------------------------------------------------------------------
# 0. Prereqs  [ALREADY DONE — aliases are registered, key is set]
# ---------------------------------------------------------------------------
# export OPENAI_API_KEY=...                       # already set in this shell
# uv run kitaru model register current --model openai/gpt-4o-mini
# uv run kitaru model register cheap    --model openai/gpt-3.5-turbo
echo ">> Connection + aliases:"
uv run kitaru info 2>/dev/null | grep -E "Connection target|Active user" || true
uv run kitaru model list 2>/dev/null | grep -E "current|cheap" || true

# ===========================================================================
# ACT 1 — the live durable agent (run this live, or rehearse with the script)
# ===========================================================================
# Interactive (you type):   uv run python examples/chatbot/ui.py
# Hands-off (scripted):     uv run python examples/chatbot/run_scripted_chat.py
#
# Then open the printed Execution URL and point at the model + tool-call
# checkpoints (check_stock, issue_refund), the waits, and the refund escalation.
echo
echo ">> ACT 1: run  ->  uv run python examples/chatbot/run_scripted_chat.py"
echo "   (or examples/chatbot/ui.py to type interactively)"

# ===========================================================================
# ACT 2 — Replay Lab: is the cheaper model safe to switch to?
# ===========================================================================

# --- Primary: real model swap on the PydanticAI agent ----------------------
# [ALREADY DONE] cohort seeded + report rendered. To REGENERATE from scratch:
#   uv run python $PS/seed_observed.py
#   uv run python $PS/run_replay_lab.py
#   uv run python $DET/render_report.py \
#     --json-path $PS/reports/pydanticai-support-replay-lab-demo.json \
#     --output-path $PS/reports/report.html
echo
echo ">> ACT 2 (PydanticAI model swap) — open this report:"
echo "   $PS/reports/report.html"
echo "   Headline: the refund case is HELD — cheaper model drops the refund safeguard."

# --- Fallback: deterministic Replay Lab (offline, guaranteed Hold + cost) ---
# [ALREADY DONE] seeded + rendered. To REGENERATE from scratch:
#   uv run python $DET/seed_observed.py --small
#   uv run python $DET/run_replay_lab.py
#   uv run python $DET/render_report.py \
#     --json-path $DET/reports/support-replay-lab-demo.json \
#     --output-path $DET/reports/report.html
echo
echo ">> ACT 2 (deterministic fallback) — open this report:"
echo "   $DET/reports/report.html"
echo "   Headline: Hold on the regulated case, with hard cost savings (~-40 to -52%)."

echo
echo "Done. Both Replay Lab reports are pre-rendered on disk; open them in a browser."
