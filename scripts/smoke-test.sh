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

skip_test() {
    local label="$1"; local reason="$2"
    printf "  ${YELLOW}○${RESET} %s ${YELLOW}(%s)${RESET}\n" "$label" "$reason"
    SKIPPED+=("$label")
}

cleanup() {
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

# ---------------------------------------------------------------------------
# Install from source
# ---------------------------------------------------------------------------
section_header "Install from source"

if [[ "$SKIP_INSTALL" == true ]]; then
    skip_test "uv sync" "skipped via --skip-install"
else
    run_test "uv sync --python $PY --extra local --extra llm --extra mcp" \
        uv sync --python "$PY" --extra local --extra llm --extra mcp
fi

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

# Run after init so .kitaru/ exists (clean project --dry-run exits non-zero
# when no project is found).
run_test "kitaru clean project --dry-run" $UV_RUN kitaru clean project --dry-run

# ---------------------------------------------------------------------------
# Core SDK flows
# ---------------------------------------------------------------------------
section_header "Core SDK flows"

run_test "Basic flow"              timed 60 $UV_RUN examples/basic_flow/first_working_flow.py
run_test "Flow with logging"       timed 60 $UV_RUN examples/basic_flow/flow_with_logging.py
run_test "Flow with artifacts"     timed 60 $UV_RUN examples/basic_flow/flow_with_artifacts.py
run_test "Flow with configuration" timed 60 $UV_RUN examples/basic_flow/flow_with_configuration.py
run_test "Flow with fan-out"       timed 60 $UV_RUN examples/basic_flow/flow_with_checkpoint_runtime.py
run_test "Client execution mgmt"   timed 60 $UV_RUN examples/execution_management/client_execution_management.py
run_test "Replay with overrides"   timed 120 $UV_RUN examples/replay/replay_with_overrides.py

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
        timed 30 $UV_RUN examples/llm/flow_with_llm.py
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
    timed 30 $UV_RUN examples/mcp/mcp_query_tools.py

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
