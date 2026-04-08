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

set -uo pipefail

# ---------------------------------------------------------------------------
# Flags
# ---------------------------------------------------------------------------
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
# Colors (respect NO_COLOR — https://no-color.org)
# ---------------------------------------------------------------------------
if [[ -z "${NO_COLOR:-}" ]] && [[ -t 1 ]]; then
    BOLD=$'\033[1m'  GREEN=$'\033[32m'  RED=$'\033[31m'
    YELLOW=$'\033[33m'  CYAN=$'\033[36m'  RESET=$'\033[0m'
else
    BOLD=""  GREEN=""  RED=""  YELLOW=""  CYAN=""  RESET=""
fi

# ---------------------------------------------------------------------------
# State tracking
# ---------------------------------------------------------------------------
PASSED=()
FAILED=()
SKIPPED=()
SECTION_NUM=0

# ---------------------------------------------------------------------------
# Timeout command (GNU coreutils `timeout` or macOS `gtimeout`)
# ---------------------------------------------------------------------------
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
    else
        printf "  ${RED}✗${RESET} %s\n" "$label"
        echo "$output" | tail -10 | sed 's/^/    /'
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
    if [[ "$KEEP_SERVER" == true ]]; then
        printf "\n${CYAN}Server left running at http://127.0.0.1:8383${RESET}\n"
    else
        printf "\n${CYAN}Stopping local server...${RESET}\n"
        uv run kitaru logout 2>/dev/null || true
    fi
}
trap cleanup EXIT INT TERM

# ---------------------------------------------------------------------------
# [0] Preflight
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

printf "  Python : %s\n" "$(python3 --version 2>&1)"
printf "  uv     : %s\n" "$(uv --version 2>&1)"
printf "  Branch : %s\n" "$(git branch --show-current 2>/dev/null || echo 'detached')"

# ---------------------------------------------------------------------------
# [1] Install from source
# ---------------------------------------------------------------------------
section_header "Install from source"

if [[ "$SKIP_INSTALL" == true ]]; then
    skip_test "uv sync" "skipped via --skip-install"
else
    run_test "uv sync --extra local --extra llm --extra mcp" \
        uv sync --extra local --extra llm --extra mcp
fi

# ---------------------------------------------------------------------------
# [2] Logout (clear state)
# ---------------------------------------------------------------------------
section_header "Clear state"

# Allow logout to fail — may already be logged out
uv run kitaru logout 2>/dev/null || true
printf "  ${GREEN}✓${RESET} kitaru logout (clear state)\n"

# ---------------------------------------------------------------------------
# [3] Login (start local server)
# ---------------------------------------------------------------------------
section_header "Start local server"

run_test "kitaru login" \
    uv run kitaru login --timeout 60

# Give the server a moment to be fully ready
sleep 2

# ---------------------------------------------------------------------------
# [4] Open dashboard
# ---------------------------------------------------------------------------
section_header "Open dashboard"

DASHBOARD_URL="http://127.0.0.1:8383"
if command -v open &>/dev/null; then
    open "$DASHBOARD_URL" 2>/dev/null &
    printf "  ${GREEN}✓${RESET} Opened dashboard (%s)\n" "$DASHBOARD_URL"
elif command -v xdg-open &>/dev/null; then
    xdg-open "$DASHBOARD_URL" 2>/dev/null &
    printf "  ${GREEN}✓${RESET} Opened dashboard (%s)\n" "$DASHBOARD_URL"
else
    printf "  ${YELLOW}○${RESET} Could not open dashboard (no open/xdg-open)\n"
fi

# ---------------------------------------------------------------------------
# [5] CLI commands
# ---------------------------------------------------------------------------
section_header "CLI commands"

run_test "kitaru --version"       uv run kitaru --version
run_test "kitaru status"          uv run kitaru status
run_test "kitaru info"            uv run kitaru info
run_test "kitaru status -o json"  uv run kitaru status -o json
run_test "kitaru stack list"      uv run kitaru stack list
run_test "kitaru stack current"   uv run kitaru stack current
run_test "kitaru model list"      uv run kitaru model list
run_test "kitaru executions list" uv run kitaru executions list

# ---------------------------------------------------------------------------
# [6] Project init
# ---------------------------------------------------------------------------
section_header "Project init"

if [[ -d .kitaru ]]; then
    printf "  ${GREEN}✓${RESET} .kitaru/ already exists (skipping init)\n"
else
    run_test "kitaru init" uv run kitaru init
fi

# ---------------------------------------------------------------------------
# [7] Core SDK flows
# ---------------------------------------------------------------------------
section_header "Core SDK flows"

run_test "Basic flow"              timed 60 uv run examples/basic_flow/first_working_flow.py
run_test "Flow with logging"       timed 60 uv run examples/basic_flow/flow_with_logging.py
run_test "Flow with artifacts"     timed 60 uv run examples/basic_flow/flow_with_artifacts.py
run_test "Flow with configuration" timed 60 uv run examples/basic_flow/flow_with_configuration.py
run_test "Flow with fan-out"       timed 60 uv run examples/basic_flow/flow_with_checkpoint_runtime.py
run_test "Client execution mgmt"   timed 60 uv run examples/execution_management/client_execution_management.py
run_test "Replay with overrides"   timed 60 uv run examples/replay/replay_with_overrides.py

# ---------------------------------------------------------------------------
# [8] CLI inspection of executions
# ---------------------------------------------------------------------------
section_header "CLI inspection of executions"

run_test "executions list"        uv run kitaru executions list
run_test "executions list -o json" uv run kitaru executions list -o json

# Extract latest exec_id and inspect it
EXEC_ID=$(uv run kitaru executions list -o json 2>/dev/null \
    | python3 -c "import sys,json; print(json.load(sys.stdin)['items'][0]['exec_id'])" 2>/dev/null) || true

if [[ -n "${EXEC_ID:-}" ]]; then
    run_test "executions get <latest>" uv run kitaru executions get "$EXEC_ID"
else
    skip_test "executions get <latest>" "could not extract exec_id"
fi

# ---------------------------------------------------------------------------
# [9] Model registration
# ---------------------------------------------------------------------------
section_header "Model registration"

if [[ -n "${OPENAI_API_KEY:-}" ]]; then
    run_test "model register fast" \
        uv run kitaru model register fast --model openai/gpt-4o-mini
    run_test "model list (verify alias)" \
        uv run kitaru model list
else
    skip_test "model register fast" "OPENAI_API_KEY not set"
    skip_test "model list (verify alias)" "OPENAI_API_KEY not set"
fi

# ---------------------------------------------------------------------------
# [10] LLM flow
# ---------------------------------------------------------------------------
section_header "LLM flow"

if [[ -n "${OPENAI_API_KEY:-}" ]]; then
    run_test "LLM flow (flow_with_llm)" \
        timed 30 uv run examples/llm/flow_with_llm.py
else
    skip_test "LLM flow (flow_with_llm)" "OPENAI_API_KEY not set"
fi

# ---------------------------------------------------------------------------
# [11] MCP tools via fastmcp
# ---------------------------------------------------------------------------
section_header "MCP tools (via fastmcp)"

FASTMCP="uv run --with fastmcp fastmcp"
MCP_SERVER="uv run kitaru-mcp"

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
    timed 30 uv run examples/mcp/mcp_query_tools.py

# ---------------------------------------------------------------------------
# [12] Summary
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
