#!/usr/bin/env bash
# Pre-release smoke test for Kitaru.
# Exercises CLI, SDK flows, MCP tools, and LLM integration against a local server.
#
# Usage:
#   ./scripts/smoke-test.sh [OPTIONS]
#
# Options:
#   -k, --keep-server    Keep the local server running after the test
#   -s, --skip-install   Skip the uv sync step (use current install)
#   -v, --verbose        Print command output even on success
#   -h, --help           Show this help message

# No -e: we deliberately continue past failures to collect all results.
set -uo pipefail

# Disable analytics so smoke-test runs don't leak events to Mixpanel.
export ZENML_ANALYTICS_OPT_IN=false

PY="3.12"
UV_RUN="uv run --python $PY"
DASHBOARD_URL="http://127.0.0.1:8383"

KEEP_SERVER=false
SKIP_INSTALL=false
VERBOSE=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        -k|--keep-server)  KEEP_SERVER=true; shift ;;
        -s|--skip-install) SKIP_INSTALL=true; shift ;;
        -v|--verbose)      VERBOSE=true; shift ;;
        -h|--help)
            sed -n '2,/^$/s/^# \?//p' "$0"
            exit 0
            ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

# ---------------------------------------------------------------------------
# Colors — respect NO_COLOR (https://no-color.org)
# ---------------------------------------------------------------------------
if [[ -z "${NO_COLOR:-}" ]] && [[ -t 1 ]]; then
    BOLD=$'\033[1m'  GREEN=$'\033[32m'  RED=$'\033[31m'
    YELLOW=$'\033[33m'  CYAN=$'\033[36m'  RESET=$'\033[0m'
else
    BOLD=""  GREEN=""  RED=""  YELLOW=""  CYAN=""  RESET=""
fi

PASSED=()
FAILED=()
SKIPPED=()
SECTION_NUM=0
# Track whether this script started the server (vs. attaching to an existing one).
SCRIPT_OWNS_SERVER=false

if command -v timeout &>/dev/null; then
    TIMEOUT_CMD="timeout"
elif command -v gtimeout &>/dev/null; then
    TIMEOUT_CMD="gtimeout"
else
    TIMEOUT_CMD=""
fi

timed() {
    local secs="$1"; shift
    if [[ -n "$TIMEOUT_CMD" ]]; then
        "$TIMEOUT_CMD" "$secs" "$@"
    else
        "$@"
    fi
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
section_header() {
    SECTION_NUM=$((SECTION_NUM + 1))
    printf "\n${BOLD}${CYAN}[%d] %s${RESET}\n" "$SECTION_NUM" "$1"
}

run_test() {
    local label="$1"; shift
    local output
    output=$("$@" 2>&1)
    local rc=$?
    if [[ $rc -eq 0 ]]; then
        printf "  ${GREEN}✓${RESET} %s\n" "$label"
        PASSED+=("$label")
        if [[ "$VERBOSE" == true ]]; then
            echo "$output" | sed 's/^/    /'
        fi
    elif [[ $rc -eq 124 ]]; then
        printf "  ${RED}✗${RESET} %s ${RED}(TIMEOUT)${RESET}\n" "$label"
        FAILED+=("$label (TIMEOUT)")
    else
        printf "  ${RED}✗${RESET} %s\n" "$label"
        echo "$output" | tail -30 | sed 's/^/    /'
        FAILED+=("$label")
    fi
    return $rc
}

redact_sensitive_output() {
    sed -E \
        -e 's/("key"[[:space:]]*:[[:space:]]*")[^"]*(")/\1[redacted]\2/g' \
        -e 's/(Key:[[:space:]]*).*/\1[redacted]/g'
}

run_sensitive_json_test() {
    local label="$1"; shift
    local output
    output=$("$@" 2>&1)
    local rc=$?
    if [[ $rc -eq 0 ]]; then
        printf "  ${GREEN}✓${RESET} %s\n" "$label"
        PASSED+=("$label")
        if [[ "$VERBOSE" == true ]]; then
            echo "$output" | redact_sensitive_output | sed 's/^/    /'
        fi
    elif [[ $rc -eq 124 ]]; then
        printf "  ${RED}✗${RESET} %s ${RED}(TIMEOUT)${RESET}\n" "$label"
        FAILED+=("$label (TIMEOUT)")
    else
        printf "  ${RED}✗${RESET} %s\n" "$label"
        echo "$output" | redact_sensitive_output | tail -30 | sed 's/^/    /'
        FAILED+=("$label")
    fi
    return $rc
}

run_expected_failure() {
    local label="$1"; local expected="$2"; shift 2
    local output
    output=$("$@" 2>&1)
    local rc=$?
    if [[ $rc -ne 0 ]] && [[ "$output" == *"$expected"* ]]; then
        printf "  ${GREEN}✓${RESET} %s\n" "$label"
        PASSED+=("$label")
        if [[ "$VERBOSE" == true ]]; then
            echo "$output" | sed 's/^/    /'
        fi
    else
        printf "  ${RED}✗${RESET} %s\n" "$label"
        echo "$output" | tail -30 | sed 's/^/    /'
        FAILED+=("$label")
    fi
    return 0
}

skip_test() {
    local label="$1"; local reason="$2"
    printf "  ${YELLOW}○${RESET} %s ${YELLOW}(%s)${RESET}\n" "$label" "$reason"
    SKIPPED+=("$label")
}

is_truthy_env_value() {
    local value
    value=$(printf '%s' "${1:-}" | tr '[:upper:]' '[:lower:]')
    case "$value" in
        1|true|yes|on) return 0 ;;
        *) return 1 ;;
    esac
}

cleanup() {
    if [[ -n "${SMOKE_AUTH_SA:-}" ]]; then
        timed 10 $UV_RUN kitaru auth api-keys delete \
            "$SMOKE_AUTH_SA" "${SMOKE_AUTH_KEY:-smoke-key}" --yes &>/dev/null || true
        timed 10 $UV_RUN kitaru auth service-accounts delete \
            "$SMOKE_AUTH_SA" --yes &>/dev/null || true
    fi
    if [[ "$KEEP_SERVER" == true ]] && [[ "$SCRIPT_OWNS_SERVER" == true ]]; then
        printf "\n${CYAN}Server left running at %s${RESET}\n" "$DASHBOARD_URL"
    elif [[ "$SCRIPT_OWNS_SERVER" == true ]]; then
        printf "\n${CYAN}Stopping local server...${RESET}\n"
        if timed 10 $UV_RUN kitaru logout &>/dev/null; then
            printf "${GREEN}Server stopped.${RESET}\n"
        else
            printf "${RED}Warning: server stop may have failed. Check port 8383.${RESET}\n"
        fi
    fi
}
trap cleanup EXIT INT TERM

# ---------------------------------------------------------------------------
# Preflight
# ---------------------------------------------------------------------------
printf "${BOLD}${CYAN}Kitaru Smoke Test${RESET}\n"
printf "═══════════════════════════════════════════════\n"

if [[ ! -f pyproject.toml ]]; then
    echo "${RED}Error: run this script from the repo root.${RESET}"
    exit 1
fi

if ! command -v uv &>/dev/null; then
    echo "${RED}Error: uv is not installed.${RESET}"
    exit 1
fi

printf "  Python : %s (pinned: %s)\n" "$(uv run --python "$PY" python --version 2>&1)" "$PY"
printf "  uv     : %s\n" "$(uv --version 2>&1)"
printf "  Branch : %s\n" "$(git branch --show-current 2>/dev/null || echo 'detached')"
if [[ -n "$TIMEOUT_CMD" ]]; then
    printf "  Timeout: %s\n" "$TIMEOUT_CMD"
else
    printf "  ${YELLOW}Timeout: unavailable (install coreutils for timeout protection)${RESET}\n"
fi

HAS_OPENAI=false
[[ -n "${OPENAI_API_KEY:-}" ]] && HAS_OPENAI=true
HAS_CLAUDE_AGENT_SDK=false
if [[ -n "${ANTHROPIC_API_KEY:-}" ]] || [[ "${CLAUDE_CODE_USE_BEDROCK:-}" == "1" ]] || [[ "${CLAUDE_CODE_USE_VERTEX:-}" == "1" ]]; then
    HAS_CLAUDE_AGENT_SDK=true
fi
HAS_GEMINI_API_KEY=false
if [[ -n "${GEMINI_API_KEY:-}" ]] || [[ -n "${GOOGLE_API_KEY:-}" ]]; then
    HAS_GEMINI_API_KEY=true
fi
HAS_GEMINI_VERTEX=false
if is_truthy_env_value "${GOOGLE_GENAI_USE_VERTEXAI:-}" \
    && [[ -n "${GOOGLE_CLOUD_PROJECT:-}" ]] \
    && [[ -n "${GOOGLE_CLOUD_LOCATION:-}" ]]; then
    HAS_GEMINI_VERTEX=true
fi
HAS_GEMINI=false
if [[ "$HAS_GEMINI_API_KEY" == true ]] || [[ "$HAS_GEMINI_VERTEX" == true ]]; then
    HAS_GEMINI=true
fi

# ---------------------------------------------------------------------------
# Install from source
# ---------------------------------------------------------------------------
section_header "Install from source"

if [[ "$SKIP_INSTALL" == true ]]; then
    skip_test "uv sync" "skipped via --skip-install"
else
    UV_SYNC_EXTRAS=(
        --extra local
        --extra llm
        --extra mcp
        --extra pydantic-ai
        --extra openai-agents
        --extra claude-agent-sdk
        --extra gemini
        --extra langgraph
    )
    if [[ "$HAS_OPENAI" == true ]]; then
        UV_SYNC_EXTRAS+=(--extra langgraph-openai)
    fi
    run_test "uv sync --python $PY ${UV_SYNC_EXTRAS[*]}" \
        uv sync --python "$PY" "${UV_SYNC_EXTRAS[@]}"
fi

# ---------------------------------------------------------------------------
# SDK API surface
# ---------------------------------------------------------------------------
section_header "SDK API surface"

run_test "checkpoint live-event API imports" \
    $UV_RUN python -c 'import kitaru; assert callable(kitaru.progress); assert callable(kitaru.events.publish); assert isinstance(kitaru.events.flush(), bool)'
run_test "execution event watcher API imports" \
    $UV_RUN python -c 'from kitaru import ExecutionEvent, KitaruClient; fields = ExecutionEvent.__dataclass_fields__; assert callable(KitaruClient); assert "cursor" in fields and "correlation_id" in fields and "stream_id" not in fields and "timestamp" not in fields'
run_test "Claude Agent SDK stream API imports" \
    $UV_RUN python -c 'from kitaru.adapters.claude_agent_sdk import CLAUDE_STREAM_COMPLETED, CLAUDE_STREAM_EVENT, CLAUDE_STREAM_EVENT_KINDS, CLAUDE_STREAM_FAILED, CLAUDE_STREAM_STARTED, CLAUDE_STREAM_TERMINAL_EVENT_KINDS, KitaruClaudeRunner; assert hasattr(KitaruClaudeRunner, "run_stream"); assert hasattr(KitaruClaudeRunner, "run_stream_sync"); assert CLAUDE_STREAM_STARTED == "claude_agent_sdk.stream.started"; assert CLAUDE_STREAM_EVENT == "claude_agent_sdk.stream.event"; assert CLAUDE_STREAM_COMPLETED == "claude_agent_sdk.stream.completed"; assert CLAUDE_STREAM_FAILED == "claude_agent_sdk.stream.failed"; assert CLAUDE_STREAM_EVENT_KINDS == (CLAUDE_STREAM_STARTED, CLAUDE_STREAM_EVENT, CLAUDE_STREAM_COMPLETED, CLAUDE_STREAM_FAILED); assert CLAUDE_STREAM_TERMINAL_EVENT_KINDS == (CLAUDE_STREAM_COMPLETED, CLAUDE_STREAM_FAILED)'
run_test "OpenAI Agents stream API imports" \
    $UV_RUN python -c 'from kitaru.adapters.openai_agents import OPENAI_STREAM_COMPLETED, OPENAI_STREAM_EVENT, OPENAI_STREAM_EVENT_KINDS, OPENAI_STREAM_FAILED, OPENAI_STREAM_STARTED, OPENAI_STREAM_TERMINAL_EVENT_KINDS, KitaruRunner; assert hasattr(KitaruRunner, "run_stream"); assert hasattr(KitaruRunner, "run_stream_sync"); assert OPENAI_STREAM_STARTED == "openai_agents.stream.started"; assert OPENAI_STREAM_EVENT == "openai_agents.stream.event"; assert OPENAI_STREAM_COMPLETED == "openai_agents.stream.completed"; assert OPENAI_STREAM_FAILED == "openai_agents.stream.failed"; assert OPENAI_STREAM_EVENT_KINDS == (OPENAI_STREAM_STARTED, OPENAI_STREAM_EVENT, OPENAI_STREAM_COMPLETED, OPENAI_STREAM_FAILED); assert OPENAI_STREAM_TERMINAL_EVENT_KINDS == (OPENAI_STREAM_COMPLETED, OPENAI_STREAM_FAILED)'

# ---------------------------------------------------------------------------
# Clear state
# ---------------------------------------------------------------------------
section_header "Clear state"

# Logout may exit non-zero if no session is active — that's fine.
$UV_RUN kitaru logout &>/dev/null || true
printf "  ${GREEN}✓${RESET} kitaru logout (clear state)\n"

# ---------------------------------------------------------------------------
# Start local server
# ---------------------------------------------------------------------------
section_header "Start local server"

LOGIN_OUTPUT=$($UV_RUN kitaru login --timeout 60 2>&1)
LOGIN_RC=$?

if [[ $LOGIN_RC -eq 0 ]]; then
    printf "  ${GREEN}✓${RESET} kitaru login\n"
    PASSED+=("kitaru login")
    # Only own the server if we actually started it (not just connected to existing).
    if echo "$LOGIN_OUTPUT" | grep -qi "started\|Starting"; then
        SCRIPT_OWNS_SERVER=true
    fi
    # Brief settle time for server to accept connections.
    sleep 2
else
    printf "  ${RED}✗${RESET} kitaru login\n"
    echo "$LOGIN_OUTPUT" | tail -30 | sed 's/^/    /'
    FAILED+=("kitaru login")
    printf "\n  ${RED}Server failed to start — aborting.${RESET}\n"
fi

# ---------------------------------------------------------------------------
# All remaining sections require a running local server.
# ---------------------------------------------------------------------------
if [[ $LOGIN_RC -eq 0 ]]; then

# ---------------------------------------------------------------------------
# Open dashboard
# ---------------------------------------------------------------------------
section_header "Open dashboard"

OPEN_CMD=""
command -v open &>/dev/null && OPEN_CMD="open"
[[ -z "$OPEN_CMD" ]] && command -v xdg-open &>/dev/null && OPEN_CMD="xdg-open"

if [[ -n "$OPEN_CMD" ]]; then
    "$OPEN_CMD" "$DASHBOARD_URL" 2>/dev/null &
    printf "  ${GREEN}✓${RESET} Opened dashboard (%s)\n" "$DASHBOARD_URL"
else
    printf "  ${YELLOW}○${RESET} Could not open dashboard (no open/xdg-open)\n"
fi

# ---------------------------------------------------------------------------
# CLI commands
# ---------------------------------------------------------------------------
section_header "CLI commands"

run_test "kitaru --version"              $UV_RUN kitaru --version
run_test "kitaru status"                 $UV_RUN kitaru status
run_test "kitaru info"                   $UV_RUN kitaru info
run_test "kitaru info --all -o json"     $UV_RUN kitaru info --all -o json
run_test "kitaru status -o json"         $UV_RUN kitaru status -o json
run_test "kitaru stack list"             $UV_RUN kitaru stack list
run_test "kitaru stack current"          $UV_RUN kitaru stack current
run_test "kitaru model list"             $UV_RUN kitaru model list
run_test "kitaru analytics status"       $UV_RUN kitaru analytics status
run_test "kitaru analytics opt-in --help"  $UV_RUN kitaru analytics opt-in --help
run_test "kitaru analytics opt-out --help" $UV_RUN kitaru analytics opt-out --help
run_test "kitaru auth --help"              $UV_RUN kitaru auth --help
run_test "kitaru auth token --help"        $UV_RUN kitaru auth token --help
run_test "kitaru auth service-accounts --help" $UV_RUN kitaru auth service-accounts --help
run_test "kitaru auth api-keys --help"     $UV_RUN kitaru auth api-keys --help
run_test "kitaru build --help"            $UV_RUN kitaru build --help
run_test "kitaru deploy --help"           $UV_RUN kitaru deploy --help
run_test "kitaru invoke --help"           $UV_RUN kitaru invoke --help
run_test "kitaru flow --help"             $UV_RUN kitaru flow --help
run_test "kitaru flow deployments --help" $UV_RUN kitaru flow deployments --help
run_test "kitaru flow deployments curl --help" $UV_RUN kitaru flow deployments curl --help
run_test "kitaru flow list"               $UV_RUN kitaru flow list
run_test "kitaru flow list -o json"       $UV_RUN kitaru flow list -o json
ANALYTICS_OUT=$($UV_RUN kitaru analytics status -o json 2>&1) || true
ANALYTICS_DISABLED=$(echo "$ANALYTICS_OUT" \
    | python3 -c "import sys,json; print(json.load(sys.stdin)['item']['analytics_opt_in'])" 2>/dev/null) || true
if [[ "${ANALYTICS_DISABLED:-}" == "False" ]]; then
    printf "  ${GREEN}✓${RESET} analytics disabled in smoke test\n"
    PASSED+=("analytics disabled in smoke test")
else
    printf "  ${RED}✗${RESET} analytics disabled in smoke test\n"
    echo "    Expected analytics_opt_in=False, got: ${ANALYTICS_DISABLED:-<parse error>}" | sed 's/^/    /'
    echo "    Raw output: ${ANALYTICS_OUT:0:200}" | sed 's/^/    /'
    FAILED+=("analytics disabled in smoke test")
fi

# ---------------------------------------------------------------------------
# Auth management API
# ---------------------------------------------------------------------------
section_header "Auth management"

SMOKE_AUTH_SA="kitaru-smoke-auth-$$"
SMOKE_AUTH_KEY="smoke-key-$$"
run_test "kitaru auth service-accounts create smoke" \
    $UV_RUN kitaru auth service-accounts create "$SMOKE_AUTH_SA" \
        --description "Kitaru smoke-test service account"
run_sensitive_json_test "kitaru auth api-keys create smoke" \
    $UV_RUN kitaru auth api-keys create "$SMOKE_AUTH_SA" "$SMOKE_AUTH_KEY" -o json
run_test "kitaru auth api-keys list smoke" \
    $UV_RUN kitaru auth api-keys list "$SMOKE_AUTH_SA"
run_test "kitaru auth api-keys delete smoke" \
    $UV_RUN kitaru auth api-keys delete "$SMOKE_AUTH_SA" "$SMOKE_AUTH_KEY" --yes
run_test "kitaru auth service-accounts delete smoke" \
    $UV_RUN kitaru auth service-accounts delete "$SMOKE_AUTH_SA" --yes

# ---------------------------------------------------------------------------
# Project init
# ---------------------------------------------------------------------------
section_header "Project init"

# kitaru init errors if .kitaru/ already exists — not idempotent.
if [[ -d .kitaru ]]; then
    PASSED+=(".kitaru/ already exists")
    printf "  ${GREEN}✓${RESET} .kitaru/ already exists (skipping init)\n"
else
    run_test "kitaru init" $UV_RUN kitaru init
fi

# ---------------------------------------------------------------------------
# Adapter example
# ---------------------------------------------------------------------------
section_header "PydanticAI adapter"

run_test "examples/chatbot/drive_local.py --help" \
    $UV_RUN python examples/chatbot/drive_local.py --help
run_test "examples/integrations/pydantic_ai_agent/pydantic_ai_adapter.py" \
    $UV_RUN python examples/integrations/pydantic_ai_agent/pydantic_ai_adapter.py

if [[ "$HAS_OPENAI" == true ]]; then
    run_test "examples/integrations/pydantic_ai_agent/pydantic_ai_streaming.py" \
        timed 120 $UV_RUN python examples/integrations/pydantic_ai_agent/pydantic_ai_streaming.py
else
    skip_test "examples/integrations/pydantic_ai_agent/pydantic_ai_streaming.py" "OPENAI_API_KEY not set; provider credentials required for PydanticAI streaming example"
fi

section_header "LangGraph adapter"

run_test "examples/integrations/langgraph_agent/langgraph_adapter.py --strategy graph_call" \
    $UV_RUN python examples/integrations/langgraph_agent/langgraph_adapter.py --strategy graph_call
run_test "examples/integrations/langgraph_agent/langgraph_streaming.py" \
    timed 120 $UV_RUN python examples/integrations/langgraph_agent/langgraph_streaming.py
if [[ "$HAS_OPENAI" == true ]]; then
    run_test "examples/integrations/langgraph_agent/langgraph_adapter.py --strategy calls" \
        $UV_RUN python examples/integrations/langgraph_agent/langgraph_adapter.py --strategy calls
else
    skip_test "examples/integrations/langgraph_agent/langgraph_adapter.py --strategy calls" "OPENAI_API_KEY not set"
fi

section_header "OpenAI Agents adapter"

run_test "examples/end_to_end/openai_research_bot/research_bot.py --help" \
    $UV_RUN python examples/end_to_end/openai_research_bot/research_bot.py --help

if [[ "$HAS_OPENAI" == true ]]; then
    run_test "examples/integrations/openai_agents_agent/openai_agents_adapter.py" \
        $UV_RUN python examples/integrations/openai_agents_agent/openai_agents_adapter.py
    run_test "examples/integrations/openai_agents_agent/openai_agents_streaming.py" \
        timed 120 $UV_RUN python examples/integrations/openai_agents_agent/openai_agents_streaming.py
else
    skip_test "examples/integrations/openai_agents_agent/openai_agents_adapter.py" "OPENAI_API_KEY not set"
    skip_test "examples/integrations/openai_agents_agent/openai_agents_streaming.py" "OPENAI_API_KEY not set; provider credentials required for OpenAI Agents streaming example"
fi

section_header "Claude Agent SDK adapter"

run_test "examples/integrations/claude_agent_sdk_agent/claude_agent_sdk_adapter.py --help" \
    $UV_RUN python examples/integrations/claude_agent_sdk_agent/claude_agent_sdk_adapter.py --help

if [[ "$HAS_CLAUDE_AGENT_SDK" == true ]]; then
    run_test "examples/integrations/claude_agent_sdk_agent/claude_agent_sdk_adapter.py" \
        timed 120 $UV_RUN python examples/integrations/claude_agent_sdk_agent/claude_agent_sdk_adapter.py \
            --prompt "Explain one Kitaru checkpoint in one short sentence. Do not use tools, Bash, or files." \
            --max-turns 1
    run_test "examples/integrations/claude_agent_sdk_agent/claude_agent_sdk_streaming.py" \
        timed 120 $UV_RUN python examples/integrations/claude_agent_sdk_agent/claude_agent_sdk_streaming.py \
            --prompt "Explain one Kitaru streamed checkpoint in one short sentence. Do not use tools, Bash, or files." \
            --max-turns 1
else
    skip_test "examples/integrations/claude_agent_sdk_agent/claude_agent_sdk_adapter.py" "ANTHROPIC_API_KEY or Claude SDK provider mode not set"
    skip_test "examples/integrations/claude_agent_sdk_agent/claude_agent_sdk_streaming.py" "ANTHROPIC_API_KEY or Claude SDK provider mode not set"
fi

section_header "Gemini Interactions adapter"

run_test "examples/integrations/gemini_interactions_agent/gemini_interactions_adapter.py --help" \
    $UV_RUN python examples/integrations/gemini_interactions_agent/gemini_interactions_adapter.py --help
run_test "examples/integrations/gemini_interactions_agent/gemini_interactions_adapter.py --dry-run --mode antigravity" \
    $UV_RUN python examples/integrations/gemini_interactions_agent/gemini_interactions_adapter.py --dry-run --mode antigravity
run_test "examples/integrations/gemini_interactions_agent/gemini_interactions_adapter.py --dry-run --stream" \
    $UV_RUN python examples/integrations/gemini_interactions_agent/gemini_interactions_adapter.py --dry-run --stream

if [[ "$HAS_GEMINI_API_KEY" == true ]]; then
    run_test "examples/integrations/gemini_interactions_agent/gemini_interactions_adapter.py --mode model" \
        timed 120 $UV_RUN python examples/integrations/gemini_interactions_agent/gemini_interactions_adapter.py \
            --mode model \
            --prompt "Explain one Kitaru checkpoint in one short sentence."
elif [[ "$HAS_GEMINI_VERTEX" == true ]]; then
    skip_test "examples/integrations/gemini_interactions_agent/gemini_interactions_adapter.py --mode model" "raw model smoke requires GEMINI_API_KEY or GOOGLE_API_KEY; Vertex ADC config is only used for opt-in Antigravity smoke"
else
    skip_test "examples/integrations/gemini_interactions_agent/gemini_interactions_adapter.py --mode model" "GEMINI_API_KEY or GOOGLE_API_KEY not set"
fi

if [[ "$HAS_GEMINI" != true ]]; then
    skip_test "examples/integrations/gemini_interactions_agent/gemini_interactions_adapter.py --mode antigravity" "GEMINI_API_KEY/GOOGLE_API_KEY or Vertex ADC config not set"
elif [[ "${KITARU_SMOKE_GEMINI_ANTIGRAVITY:-}" != "1" ]]; then
    skip_test "examples/integrations/gemini_interactions_agent/gemini_interactions_adapter.py --mode antigravity" "set KITARU_SMOKE_GEMINI_ANTIGRAVITY=1 to run; accepts Gemini API key or Vertex ADC config"
else
    run_test "examples/integrations/gemini_interactions_agent/gemini_interactions_adapter.py --mode antigravity" \
        timed 360 $UV_RUN python examples/integrations/gemini_interactions_agent/gemini_interactions_adapter.py \
            --mode antigravity \
            --timeout 300 \
            --prompt "Explain what you would inspect first in this repository. Do not edit files."
fi

if [[ "$HAS_OPENAI" != true ]]; then
    skip_test "examples/end_to_end/openai_research_bot/research_bot.py" "OPENAI_API_KEY not set"
elif [[ "${KITARU_SMOKE_RESEARCH_BOT:-}" != "1" ]]; then
    skip_test "examples/end_to_end/openai_research_bot/research_bot.py" "set KITARU_SMOKE_RESEARCH_BOT=1 to run the real web-search smoke test"
else
    run_test "examples/end_to_end/openai_research_bot/research_bot.py" \
        timed 180 $UV_RUN python examples/end_to_end/openai_research_bot/research_bot.py \
            "AI agent durability in one paragraph" --max-searches 2 \
            --fail-on-search-error
fi

# Run after init so .kitaru/ exists (clean project --dry-run exits non-zero
# when no project is found).
run_test "kitaru clean project --dry-run" $UV_RUN kitaru clean project --dry-run
run_expected_failure "kitaru build rejects local stack deployments" \
    "not one the Kitaru server can execute remotely" \
    $UV_RUN kitaru build examples/features/basic_flow/first_working_flow.py:research_agent \
        --input '{"topic":"smoke"}'

# ---------------------------------------------------------------------------
# Secret API
# ---------------------------------------------------------------------------
section_header "Secret API"

SMOKE_SECRET_NAME="kitaru-smoke-creds"
run_test "kitaru secrets set smoke secret" \
    $UV_RUN kitaru secrets set "$SMOKE_SECRET_NAME" --SMOKE_TOKEN=smoke-value
run_test "SDK get_secret()" \
    env SMOKE_SECRET_NAME="$SMOKE_SECRET_NAME" \
    $UV_RUN python -c 'import os; from kitaru import get_secret; s = get_secret(os.environ["SMOKE_SECRET_NAME"]); assert s.get("SMOKE_TOKEN") == "smoke-value"'
run_test "kitaru secrets delete smoke secret" \
    $UV_RUN kitaru secrets delete "$SMOKE_SECRET_NAME"

# ---------------------------------------------------------------------------
# Core SDK flows
# ---------------------------------------------------------------------------
section_header "Core SDK flows"

run_test "Basic flow"              timed 60 $UV_RUN examples/features/basic_flow/first_working_flow.py
run_test "Flow with logging"       timed 60 $UV_RUN examples/features/basic_flow/flow_with_logging.py
run_test "Flow with artifacts"     timed 60 $UV_RUN examples/features/basic_flow/flow_with_artifacts.py
run_test "Flow with configuration" timed 60 $UV_RUN examples/features/basic_flow/flow_with_configuration.py
run_test "Flow with fan-out"       timed 60 $UV_RUN examples/features/basic_flow/flow_with_checkpoint_runtime.py
run_test "Client execution mgmt"   timed 60 $UV_RUN examples/features/execution_management/client_execution_management.py
run_test "Replay with overrides"   timed 120 $UV_RUN examples/features/replay/replay_with_overrides.py

# ---------------------------------------------------------------------------
# CLI inspection of executions
# ---------------------------------------------------------------------------
section_header "CLI inspection of executions"

run_test "executions list"         $UV_RUN kitaru executions list
run_test "executions list -o json" $UV_RUN kitaru executions list -o json

# Capture JSON output first, then parse — keeps diagnostics visible on failure.
EXEC_LIST_OUT=$($UV_RUN kitaru executions list -o json 2>&1) || true
EXEC_ID=$(echo "$EXEC_LIST_OUT" \
    | python3 -c "import sys,json; print(json.load(sys.stdin)['items'][0]['exec_id'])" 2>/dev/null) || true

if [[ -n "${EXEC_ID:-}" ]]; then
    run_test "executions get <latest>" $UV_RUN kitaru executions get "$EXEC_ID"
else
    skip_test "executions get <latest>" "could not extract exec_id from: ${EXEC_LIST_OUT:0:200}"
fi

# ---------------------------------------------------------------------------
# Model registration
# ---------------------------------------------------------------------------
section_header "Model registration"

if [[ "$HAS_OPENAI" == true ]]; then
    run_test "model register fast" \
        $UV_RUN kitaru model register fast --model openai/gpt-4o-mini
    run_test "model list (verify alias)" \
        $UV_RUN kitaru model list
else
    skip_test "model register fast" "OPENAI_API_KEY not set"
    skip_test "model list (verify alias)" "OPENAI_API_KEY not set"
fi

# ---------------------------------------------------------------------------
# LLM flow
# ---------------------------------------------------------------------------
section_header "LLM flow"

if [[ "$HAS_OPENAI" == true ]]; then
    run_test "LLM flow (flow_with_llm)" \
        timed 30 $UV_RUN examples/features/llm/flow_with_llm.py
else
    skip_test "LLM flow (flow_with_llm)" "OPENAI_API_KEY not set"
fi

# ---------------------------------------------------------------------------
# MCP tools via fastmcp
# ---------------------------------------------------------------------------
section_header "MCP tools (via fastmcp)"

FASTMCP="$UV_RUN --with fastmcp fastmcp"
MCP_SERVER="$UV_RUN kitaru-mcp"

run_test "fastmcp list tools" \
    $FASTMCP list --command "$MCP_SERVER"

run_test "MCP: kitaru_status" \
    $FASTMCP call --command "$MCP_SERVER" --target kitaru_status --json

run_test "MCP: kitaru_stacks_list" \
    $FASTMCP call --command "$MCP_SERVER" --target kitaru_stacks_list --json

run_test "MCP: kitaru_executions_list" \
    $FASTMCP call --command "$MCP_SERVER" --target kitaru_executions_list \
        --input-json '{"limit": 3}' --json

run_test "MCP query snapshot (example)" \
    timed 30 $UV_RUN examples/features/mcp/mcp_query_tools.py

fi  # LOGIN_RC == 0

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
printf "\n${BOLD}═══════════════════════════════════════════════${RESET}\n"
printf "${BOLD}  Kitaru Smoke Test Summary${RESET}\n"
printf "${BOLD}═══════════════════════════════════════════════${RESET}\n"
printf "  ${GREEN}Passed : %d${RESET}\n" "${#PASSED[@]}"
printf "  ${RED}Failed : %d${RESET}\n" "${#FAILED[@]}"
printf "  ${YELLOW}Skipped: %d${RESET}\n" "${#SKIPPED[@]}"

if [[ ${#FAILED[@]} -gt 0 ]]; then
    printf "${BOLD}───────────────────────────────────────────────${RESET}\n"
    printf "  ${RED}FAILED:${RESET}\n"
    for label in "${FAILED[@]}"; do
        printf "    ${RED}✗${RESET} %s\n" "$label"
    done
fi

if [[ ${#SKIPPED[@]} -gt 0 ]]; then
    printf "${BOLD}───────────────────────────────────────────────${RESET}\n"
    printf "  ${YELLOW}SKIPPED:${RESET}\n"
    for label in "${SKIPPED[@]}"; do
        printf "    ${YELLOW}○${RESET} %s\n" "$label"
    done
fi

printf "${BOLD}═══════════════════════════════════════════════${RESET}\n"

if [[ ${#FAILED[@]} -gt 0 ]]; then
    exit 1
fi
exit 0
