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
#   --release            Enforce release-grade preflight and required provider skips
#   --required-provider-area AREA
#                        Mark a provider area as required for this release.
#                        Repeatable. Areas: openai, anthropic, gemini-model,
#                        gemini-sandbox-function, gemini-antigravity,
#                        research-bot
#   --json-out PATH      Write structured smoke results to PATH
#   -h, --help           Show this help message

# No -e: we deliberately continue past failures to collect all results.
set -uo pipefail

# Disable analytics so smoke-test runs don't leak events to Mixpanel.
export ZENML_ANALYTICS_OPT_IN=false

print_help() {
    cat <<'EOF'
Pre-release smoke test for Kitaru.
Exercises CLI, SDK flows, MCP tools, and LLM integration against a local server.

Usage:
  ./scripts/smoke-test.sh [OPTIONS]

Options:
  -k, --keep-server    Keep the local server running after the test
  -s, --skip-install   Skip the uv sync step (use current install)
  -v, --verbose        Print command output even on success
  --release            Enforce release-grade preflight and required provider skips
  --required-provider-area AREA
                       Mark a provider area as required for this release.
                       Repeatable. Areas: openai, anthropic, gemini-model,
                       gemini-sandbox-function, gemini-antigravity,
                       research-bot
  --json-out PATH      Write structured smoke results to PATH
  -h, --help           Show this help message
EOF
}

PY="3.12"
UV_RUN="uv run --python $PY"
DASHBOARD_URL="http://127.0.0.1:8383"

KEEP_SERVER=false
SKIP_INSTALL=false
VERBOSE=false
RELEASE_MODE=false
JSON_OUT=""
REQUIRED_PROVIDER_AREAS=()

find_json_out_arg() {
    while [[ $# -gt 0 ]]; do
        if [[ "$1" == "--json-out" ]] && [[ $# -ge 2 ]] && [[ "$2" != --* ]]; then
            printf '%s\n' "$2"
            return 0
        fi
        shift
    done
    return 0
}

write_early_json_result() {
    local json_out="$1"
    local label="$2"
    local reason="$3"
    local release_mode="${4:-false}"
    [[ -z "$json_out" ]] && return 0

    local branch
    local sha
    branch=$(git branch --show-current 2>/dev/null || echo "detached")
    sha=$(git rev-parse HEAD 2>/dev/null || echo "")
    mkdir -p "$(dirname "$json_out")" || return 1

    python3 - "$json_out" "$label" "$reason" "$release_mode" "$branch" "$sha" <<'PY'
import json
import sys

out_path, label, reason, release_mode, branch, sha = sys.argv[1:]
record = {
    "label": label,
    "status": "failed",
    "section": "Preflight",
    "reason": reason,
    "provider_area": "none",
    "required_env": [],
    "release_relevant": False,
    "duration_seconds": 0,
}
payload = {
    "invocation": {"release": release_mode == "true", "required_provider_areas": []},
    "git": {"branch": branch, "sha": sha},
    "timeout": {"command": None},
    "sections": ["Preflight"],
    "checks": [record],
    "provider_attestation": {
        "required_provider_areas": [],
        "credentials": {
            "openai": False,
            "anthropic": False,
            "gemini_api_key": False,
            "gemini_vertex": False,
            "gemini_any": False,
        },
        "required_area_status": {},
    },
    "counts": {"passed": 0, "failed": 1, "skipped": 0, "release_relevant_skipped": 0},
}
with open(out_path, "w", encoding="utf-8") as handle:
    json.dump(payload, handle, indent=2, sort_keys=True)
    handle.write("\n")
PY
}

DISCOVERED_JSON_OUT=$(find_json_out_arg "$@")

while [[ $# -gt 0 ]]; do
    case "$1" in
        -k|--keep-server)  KEEP_SERVER=true; shift ;;
        -s|--skip-install) SKIP_INSTALL=true; shift ;;
        -v|--verbose)      VERBOSE=true; shift ;;
        --release)         RELEASE_MODE=true; shift ;;
        --json-out)
            if [[ $# -lt 2 ]]; then
                echo "Error: --json-out requires a path" >&2
                write_early_json_result "$DISCOVERED_JSON_OUT" "smoke option parsing" "--json-out requires a path" "$RELEASE_MODE" || true
                exit 1
            fi
            JSON_OUT="$2"
            shift 2
            ;;
        --required-provider-area)
            if [[ $# -lt 2 ]]; then
                echo "Error: --required-provider-area requires one of: openai, anthropic, gemini-model, gemini-sandbox-function, gemini-antigravity, research-bot" >&2
                write_early_json_result "${JSON_OUT:-$DISCOVERED_JSON_OUT}" "smoke option parsing" "--required-provider-area requires a value" "$RELEASE_MODE" || true
                exit 1
            fi
            case "$2" in
                openai|anthropic|gemini-model|gemini-sandbox-function|gemini-antigravity|research-bot)
                    REQUIRED_PROVIDER_AREAS+=("$2")
                    ;;
                *)
                    echo "Error: unsupported provider area '$2'. Expected one of: openai, anthropic, gemini-model, gemini-sandbox-function, gemini-antigravity, research-bot" >&2
                    write_early_json_result "${JSON_OUT:-$DISCOVERED_JSON_OUT}" "smoke option parsing" "unsupported provider area '$2'" "$RELEASE_MODE" || true
                    exit 1
                    ;;
            esac
            shift 2
            ;;
        -h|--help)
            print_help
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            write_early_json_result "${JSON_OUT:-$DISCOVERED_JSON_OUT}" "smoke option parsing" "unknown option '$1'" "$RELEASE_MODE" || true
            exit 1
            ;;
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
RELEASE_RELEVANT_SKIPPED=()
SECTIONS=()
PROVIDER_AREAS_RECORDED=()
SECTION_NUM=0
CURRENT_SECTION="Preflight"
RESULT_RECORDS_FILE=$(mktemp "${TMPDIR:-/tmp}/kitaru-smoke-results.XXXXXX")
RECORDING_FAILED=false
GEMINI_SANDBOX_FUNCTION_SMOKE_STACK=""
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
    CURRENT_SECTION="$1"
    SECTIONS+=("$1")
    printf "\n${BOLD}${CYAN}[%d] %s${RESET}\n" "$SECTION_NUM" "$CURRENT_SECTION"
}

is_required_provider_area() {
    local area="$1"
    local required
    [[ ${#REQUIRED_PROVIDER_AREAS[@]} -eq 0 ]] && return 1
    for required in "${REQUIRED_PROVIDER_AREAS[@]}"; do
        [[ "$required" == "$area" ]] && return 0
    done
    return 1
}

provider_area_has_record() {
    local area="$1"
    local recorded
    [[ ${#PROVIDER_AREAS_RECORDED[@]} -eq 0 ]] && return 1
    for recorded in "${PROVIDER_AREAS_RECORDED[@]}"; do
        [[ "$recorded" == "$area" ]] && return 0
    done
    return 1
}

record_check() {
    local label="$1"
    local status="$2"
    local reason="${3:-}"
    local provider_area="${4:-none}"
    local required_env="${5:-}"
    local duration_seconds="${6:-0}"
    local release_relevant=false

    if [[ "$RELEASE_MODE" == true ]] && is_required_provider_area "$provider_area"; then
        release_relevant=true
    fi
    if [[ "$provider_area" != "none" ]]; then
        PROVIDER_AREAS_RECORDED+=("$provider_area")
    fi

    case "$status" in
        passed) PASSED+=("$label") ;;
        failed)
            if [[ -n "$reason" ]]; then
                FAILED+=("$label ($reason)")
            else
                FAILED+=("$label")
            fi
            ;;
        skipped)
            if [[ -n "$reason" ]]; then
                SKIPPED+=("$label — $reason")
            else
                SKIPPED+=("$label")
            fi
            if [[ "$release_relevant" == true ]]; then
                RELEASE_RELEVANT_SKIPPED+=("$label — $reason")
            fi
            ;;
    esac

    python3 - "$RESULT_RECORDS_FILE" \
        "$label" "$status" "$CURRENT_SECTION" "$reason" "$provider_area" \
        "$required_env" "$release_relevant" "$duration_seconds" <<'PY'
import json
import sys

path, label, status, section, reason, provider_area, required_env, release_relevant, duration = sys.argv[1:]
record = {
    "label": label,
    "status": status,
    "section": section,
    "reason": reason,
    "provider_area": provider_area,
    "required_env": [item for item in required_env.split(",") if item],
    "release_relevant": release_relevant == "true",
    "duration_seconds": int(duration or 0),
}
with open(path, "a", encoding="utf-8") as handle:
    handle.write(json.dumps(record, sort_keys=True) + "\n")
PY
    local record_rc=$?
    if [[ $record_rc -ne 0 ]]; then
        RECORDING_FAILED=true
        printf "  ${RED}Warning: failed to record structured result for %s${RESET}\n" "$label" >&2
    fi
}

record_pass() {
    local label="$1"
    local provider_area="${2:-none}"
    local required_env="${3:-}"
    local duration_seconds="${4:-0}"
    record_check "$label" "passed" "" "$provider_area" "$required_env" "$duration_seconds"
}

record_failure() {
    local label="$1"
    local reason="${2:-}"
    local provider_area="${3:-none}"
    local required_env="${4:-}"
    local duration_seconds="${5:-0}"
    record_check "$label" "failed" "$reason" "$provider_area" "$required_env" "$duration_seconds"
}

skip_test() {
    local label="$1"
    local reason="$2"
    local provider_area="${3:-none}"
    local required_env="${4:-}"
    printf "  ${YELLOW}○${RESET} %s ${YELLOW}(%s)${RESET}\n" "$label" "$reason"
    record_check "$label" "skipped" "$reason" "$provider_area" "$required_env" 0
}

write_json_results() {
    [[ -z "$JSON_OUT" ]] && return 0

    local branch
    local sha
    branch=$(git branch --show-current 2>/dev/null || echo "detached")
    sha=$(git rev-parse HEAD 2>/dev/null || echo "")
    mkdir -p "$(dirname "$JSON_OUT")" || return 1

    python3 - "$RESULT_RECORDS_FILE" "$JSON_OUT" \
        "$RELEASE_MODE" "$branch" "$sha" "$TIMEOUT_CMD" \
        "$HAS_OPENAI" "$HAS_CLAUDE_AGENT_SDK" "$HAS_GEMINI_API_KEY" \
        "$HAS_GEMINI_VERTEX" "$HAS_GEMINI" "$RECORDING_FAILED" \
        "${REQUIRED_PROVIDER_AREAS[*]-}" <<'PY'
import json
import sys

(
    records_path,
    out_path,
    release_mode,
    branch,
    sha,
    timeout_cmd,
    has_openai,
    has_anthropic,
    has_gemini_api_key,
    has_gemini_vertex,
    has_gemini,
    recording_failed,
    required_provider_areas,
) = sys.argv[1:]

records = []
try:
    with open(records_path, encoding="utf-8") as handle:
        records = [json.loads(line) for line in handle if line.strip()]
except FileNotFoundError:
    records = []

if recording_failed == "true":
    records.append(
        {
            "label": "structured result recording",
            "status": "failed",
            "section": "Smoke result integrity",
            "reason": "one or more checks could not be written to structured result records",
            "provider_area": "none",
            "required_env": [],
            "release_relevant": False,
            "duration_seconds": 0,
        }
    )

counts = {
    "passed": sum(1 for record in records if record["status"] == "passed"),
    "failed": sum(1 for record in records if record["status"] == "failed"),
    "skipped": sum(1 for record in records if record["status"] == "skipped"),
    "release_relevant_skipped": sum(
        1
        for record in records
        if record["status"] == "skipped" and record["release_relevant"]
    ),
}

provider_areas = [area for area in required_provider_areas.split() if area]
attestation = {
    "required_provider_areas": provider_areas,
    "credentials": {
        "openai": has_openai == "true",
        "anthropic": has_anthropic == "true",
        "gemini_api_key": has_gemini_api_key == "true",
        "gemini_vertex": has_gemini_vertex == "true",
        "gemini_any": has_gemini == "true",
    },
    "required_area_status": {},
}
for area in provider_areas:
    area_records = [record for record in records if record["provider_area"] == area]
    attestation["required_area_status"][area] = {
        "passed": sum(1 for record in area_records if record["status"] == "passed"),
        "failed": sum(1 for record in area_records if record["status"] == "failed"),
        "skipped": sum(1 for record in area_records if record["status"] == "skipped"),
        "skip_reasons": [
            {"label": record["label"], "reason": record["reason"]}
            for record in area_records
            if record["status"] == "skipped"
        ],
    }

payload = {
    "invocation": {
        "release": release_mode == "true",
        "required_provider_areas": provider_areas,
    },
    "git": {"branch": branch, "sha": sha},
    "timeout": {"command": timeout_cmd or None},
    "sections": list(dict.fromkeys(record["section"] for record in records)),
    "checks": records,
    "provider_attestation": attestation,
    "counts": counts,
}
with open(out_path, "w", encoding="utf-8") as handle:
    json.dump(payload, handle, indent=2, sort_keys=True)
    handle.write("\n")
PY
}

validate_required_provider_area_records() {
    [[ "$RELEASE_MODE" != true ]] && return 0
    [[ ${#REQUIRED_PROVIDER_AREAS[@]} -eq 0 ]] && return 0
    local area
    for area in "${REQUIRED_PROVIDER_AREAS[@]}"; do
        if ! provider_area_has_record "$area"; then
            printf "  ${RED}✗${RESET} required provider area %s has no recorded checks\n" "$area"
            record_failure "required provider area $area coverage" \
                "no checks recorded for required provider area" "$area" "" 0
        fi
    done
}

run_test() {
    local label="$1"; shift
    local output
    local start=$SECONDS
    output=$("$@" 2>&1)
    local rc=$?
    local duration=$((SECONDS - start))
    if [[ $rc -eq 0 ]]; then
        printf "  ${GREEN}✓${RESET} %s\n" "$label"
        record_pass "$label" "none" "" "$duration"
        if [[ "$VERBOSE" == true ]]; then
            echo "$output" | sed 's/^/    /'
        fi
    elif [[ $rc -eq 124 ]]; then
        printf "  ${RED}✗${RESET} %s ${RED}(TIMEOUT)${RESET}\n" "$label"
        record_failure "$label" "TIMEOUT" "none" "" "$duration"
    else
        printf "  ${RED}✗${RESET} %s\n" "$label"
        echo "$output" | tail -30 | sed 's/^/    /'
        record_failure "$label" "exit status $rc" "none" "" "$duration"
    fi
    return $rc
}

run_provider_test() {
    local provider_area="$1"
    local required_env="$2"
    local label="$3"
    shift 3
    local output
    local start=$SECONDS
    output=$("$@" 2>&1)
    local rc=$?
    local duration=$((SECONDS - start))
    if [[ $rc -eq 0 ]]; then
        printf "  ${GREEN}✓${RESET} %s\n" "$label"
        record_pass "$label" "$provider_area" "$required_env" "$duration"
        if [[ "$VERBOSE" == true ]]; then
            echo "$output" | sed 's/^/    /'
        fi
    elif [[ $rc -eq 124 ]]; then
        printf "  ${RED}✗${RESET} %s ${RED}(TIMEOUT)${RESET}\n" "$label"
        record_failure "$label" "TIMEOUT" "$provider_area" "$required_env" "$duration"
    else
        printf "  ${RED}✗${RESET} %s\n" "$label"
        echo "$output" | tail -30 | sed 's/^/    /'
        record_failure "$label" "exit status $rc" "$provider_area" "$required_env" "$duration"
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
    local start=$SECONDS
    output=$("$@" 2>&1)
    local rc=$?
    local duration=$((SECONDS - start))
    if [[ $rc -eq 0 ]]; then
        printf "  ${GREEN}✓${RESET} %s\n" "$label"
        record_pass "$label" "none" "" "$duration"
        if [[ "$VERBOSE" == true ]]; then
            echo "$output" | redact_sensitive_output | sed 's/^/    /'
        fi
    elif [[ $rc -eq 124 ]]; then
        printf "  ${RED}✗${RESET} %s ${RED}(TIMEOUT)${RESET}\n" "$label"
        record_failure "$label" "TIMEOUT" "none" "" "$duration"
    else
        printf "  ${RED}✗${RESET} %s\n" "$label"
        echo "$output" | redact_sensitive_output | tail -30 | sed 's/^/    /'
        record_failure "$label" "exit status $rc" "none" "" "$duration"
    fi
    return $rc
}

run_expected_failure() {
    local label="$1"; local expected="$2"; shift 2
    local output
    local start=$SECONDS
    output=$("$@" 2>&1)
    local rc=$?
    local duration=$((SECONDS - start))
    if [[ $rc -ne 0 ]] && [[ "$output" == *"$expected"* ]]; then
        printf "  ${GREEN}✓${RESET} %s\n" "$label"
        record_pass "$label" "none" "" "$duration"
        if [[ "$VERBOSE" == true ]]; then
            echo "$output" | sed 's/^/    /'
        fi
    else
        printf "  ${RED}✗${RESET} %s\n" "$label"
        echo "$output" | tail -30 | sed 's/^/    /'
        record_failure "$label" "expected failure did not match" "none" "" "$duration"
    fi
    return 0
}

is_truthy_env_value() {
    local value
    value=$(printf '%s' "${1:-}" | tr '[:upper:]' '[:lower:]')
    case "$value" in
        1|true|yes|on) return 0 ;;
        *) return 1 ;;
    esac
}

active_stack_has_single_sandbox() {
    local current_json
    local stack_name
    local stack_json
    current_json=$($UV_RUN kitaru stack current -o json 2>/dev/null) || return 1
    stack_name=$(echo "$current_json" | python3 -c 'import json,sys; print(json.load(sys.stdin)["item"]["name"])' 2>/dev/null) || return 1
    [[ -n "$stack_name" ]] || return 1
    stack_json=$($UV_RUN kitaru stack show "$stack_name" -o json 2>/dev/null) || return 1
    echo "$stack_json" | python3 -c 'import json,sys; components=json.load(sys.stdin)["item"].get("components", []); sandboxes=[component for component in components if component.get("role") == "sandbox"]; sys.exit(0 if len(sandboxes) == 1 else 1)'
}

cleanup() {
    rm -f "$RESULT_RECORDS_FILE"

    if [[ -n "${SMOKE_AUTH_SA:-}" ]]; then
        timed 10 $UV_RUN kitaru auth api-keys delete \
            "$SMOKE_AUTH_SA" "${SMOKE_AUTH_KEY:-smoke-key}" --yes &>/dev/null || true
        timed 10 $UV_RUN kitaru auth service-accounts delete \
            "$SMOKE_AUTH_SA" --yes &>/dev/null || true
    fi
    if [[ -n "${GEMINI_SANDBOX_FUNCTION_SMOKE_STACK:-}" ]]; then
        timed 60 $UV_RUN kitaru stack delete \
            "$GEMINI_SANDBOX_FUNCTION_SMOKE_STACK" --recursive &>/dev/null || true
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
    write_early_json_result "$JSON_OUT" "repo root preflight" "run this script from the repo root" "$RELEASE_MODE" || true
    exit 1
fi

if ! command -v uv &>/dev/null; then
    echo "${RED}Error: uv is not installed.${RESET}"
    write_early_json_result "$JSON_OUT" "uv preflight" "uv is not installed" "$RELEASE_MODE" || true
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

if [[ "$RELEASE_MODE" == true ]]; then
    printf "  Release: enabled\n"
    if [[ ${#REQUIRED_PROVIDER_AREAS[@]} -gt 0 ]]; then
        printf "  Required provider areas: %s\n" "${REQUIRED_PROVIDER_AREAS[*]}"
    else
        printf "  Required provider areas: none\n"
    fi
    if [[ -z "$TIMEOUT_CMD" ]]; then
        printf "  ${RED}Release mode requires timeout or gtimeout. Install coreutils before running release smoke.${RESET}\n"
        record_failure "release preflight timeout command" "timeout/gtimeout missing" "none" "" 0
        if [[ -n "$JSON_OUT" ]] && ! write_json_results; then
            printf "  ${RED}Failed to write structured results: %s${RESET}\n" "$JSON_OUT" >&2
        fi
        exit 1
    fi
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
run_test "Claude Agent SDK sandbox MCP API imports" \
    $UV_RUN python -c 'from kitaru.adapters.claude_agent_sdk import DEFAULT_CLAUDE_SANDBOX_COMMAND_MAX_CHARS, KITARU_SANDBOX_COMMAND_ALLOWED_TOOL_NAME, create_kitaru_sandbox_mcp_server; assert callable(create_kitaru_sandbox_mcp_server); assert DEFAULT_CLAUDE_SANDBOX_COMMAND_MAX_CHARS > 0; assert KITARU_SANDBOX_COMMAND_ALLOWED_TOOL_NAME == "mcp__kitaru__run_command"'
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
    record_pass "kitaru login"
    # Only own the server if we actually started it (not just connected to existing).
    if echo "$LOGIN_OUTPUT" | grep -qi "started\|Starting"; then
        SCRIPT_OWNS_SERVER=true
    fi
    # Brief settle time for server to accept connections.
    sleep 2
else
    printf "  ${RED}✗${RESET} kitaru login\n"
    echo "$LOGIN_OUTPUT" | tail -30 | sed 's/^/    /'
    record_failure "kitaru login" "exit status $LOGIN_RC"
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
    record_pass "Opened dashboard"
else
    printf "  ${YELLOW}○${RESET} Could not open dashboard (no open/xdg-open)\n"
    skip_test "Open dashboard" "no open/xdg-open command available"
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
run_test "kitaru stack create --help"    $UV_RUN kitaru stack create --help
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
    record_pass "analytics disabled in smoke test"
else
    printf "  ${RED}✗${RESET} analytics disabled in smoke test\n"
    echo "    Expected analytics_opt_in=False, got: ${ANALYTICS_DISABLED:-<parse error>}" | sed 's/^/    /'
    echo "    Raw output: ${ANALYTICS_OUT:0:200}" | sed 's/^/    /'
    record_failure "analytics disabled in smoke test" "analytics status was not disabled"
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
    printf "  ${GREEN}✓${RESET} .kitaru/ already exists (skipping init)\n"
    record_pass ".kitaru/ already exists"
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
    run_provider_test "openai" "OPENAI_API_KEY" \
        "examples/integrations/pydantic_ai_agent/pydantic_ai_streaming.py" \
        timed 120 $UV_RUN python examples/integrations/pydantic_ai_agent/pydantic_ai_streaming.py
    if active_stack_has_single_sandbox; then
        run_provider_test "openai" "OPENAI_API_KEY" \
            "examples/integrations/pydantic_ai_agent/pydantic_ai_sandbox_toolset.py" \
            timed 120 $UV_RUN python examples/integrations/pydantic_ai_agent/pydantic_ai_sandbox_toolset.py
    else
        skip_test "examples/integrations/pydantic_ai_agent/pydantic_ai_sandbox_toolset.py" "active stack does not have exactly one sandbox component; provider credentials alone are not enough for this example; --release --required-provider-area openai makes this prerequisite skip fail release smoke" "openai" "OPENAI_API_KEY"
    fi
else
    skip_test "examples/integrations/pydantic_ai_agent/pydantic_ai_streaming.py" "OPENAI_API_KEY not set; provider credentials required for PydanticAI streaming example" "openai" "OPENAI_API_KEY"
    skip_test "examples/integrations/pydantic_ai_agent/pydantic_ai_sandbox_toolset.py" "OPENAI_API_KEY not set; provider credentials and one active-stack sandbox required for PydanticAI sandbox toolset example" "openai" "OPENAI_API_KEY"
fi

section_header "LangGraph adapter"

run_test "examples/integrations/langgraph_agent/langgraph_adapter.py --strategy graph_call" \
    $UV_RUN python examples/integrations/langgraph_agent/langgraph_adapter.py --strategy graph_call
run_test "examples/integrations/langgraph_agent/langgraph_streaming.py" \
    timed 120 $UV_RUN python examples/integrations/langgraph_agent/langgraph_streaming.py
run_test "LangGraph sandbox command tool factory" \
    $UV_RUN python -c 'from kitaru.adapters.langgraph import DEFAULT_SANDBOX_COMMAND_TOOL_NAME, create_sandbox_command_tool; tool = create_sandbox_command_tool(); assert tool.name == DEFAULT_SANDBOX_COMMAND_TOOL_NAME; assert set(tool.args_schema.model_fields) == {"command", "cwd"}'
if [[ "$HAS_OPENAI" == true ]]; then
    run_provider_test "openai" "OPENAI_API_KEY" \
        "examples/integrations/langgraph_agent/langgraph_adapter.py --strategy calls" \
        timed 120 $UV_RUN python examples/integrations/langgraph_agent/langgraph_adapter.py --strategy calls
else
    skip_test "examples/integrations/langgraph_agent/langgraph_adapter.py --strategy calls" "OPENAI_API_KEY not set" "openai" "OPENAI_API_KEY"
fi

section_header "OpenAI Agents adapter"

run_test "examples/end_to_end/openai_research_bot/research_bot.py --help" \
    $UV_RUN python examples/end_to_end/openai_research_bot/research_bot.py --help
run_test "examples/end_to_end/coding_agent import/CLI contract" \
    $UV_RUN python -c 'import ast, sys; from pathlib import Path; root = Path("examples/end_to_end/coding_agent"); sys.path.insert(0, str(root)); from models import FollowUp, LLMResponse; import tools; assert FollowUp(is_finished=True).is_finished; assert LLMResponse(role="assistant", content="ok").to_message()["role"] == "assistant"; assert any(tool["function"]["name"] == "read_file" for tool in tools.ALL_TOOLS); tree = ast.parse((root / "agent.py").read_text()); funcs = {node.name: node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef)}; assert {"coding_agent", "main"} <= set(funcs); assert any(isinstance(dec, ast.Call) and getattr(dec.func, "attr", getattr(dec.func, "id", "")) == "command" for dec in funcs["main"].decorator_list)'

if [[ "$HAS_OPENAI" == true ]]; then
    run_provider_test "openai" "OPENAI_API_KEY" \
        "examples/integrations/openai_agents_agent/openai_agents_adapter.py" \
        timed 120 $UV_RUN python examples/integrations/openai_agents_agent/openai_agents_adapter.py
    run_provider_test "openai" "OPENAI_API_KEY" \
        "examples/integrations/openai_agents_agent/openai_agents_streaming.py" \
        timed 120 $UV_RUN python examples/integrations/openai_agents_agent/openai_agents_streaming.py
else
    skip_test "examples/integrations/openai_agents_agent/openai_agents_adapter.py" "OPENAI_API_KEY not set" "openai" "OPENAI_API_KEY"
    skip_test "examples/integrations/openai_agents_agent/openai_agents_streaming.py" "OPENAI_API_KEY not set; provider credentials required for OpenAI Agents streaming example" "openai" "OPENAI_API_KEY"
fi

section_header "Claude Agent SDK adapter"

run_test "examples/integrations/claude_agent_sdk_agent/claude_agent_sdk_adapter.py --help" \
    $UV_RUN python examples/integrations/claude_agent_sdk_agent/claude_agent_sdk_adapter.py --help
run_test "examples/integrations/claude_agent_sdk_agent/claude_agent_sdk_sandbox_tool.py --help" \
    $UV_RUN python examples/integrations/claude_agent_sdk_agent/claude_agent_sdk_sandbox_tool.py --help

if [[ "$HAS_CLAUDE_AGENT_SDK" == true ]]; then
    run_provider_test "anthropic" "ANTHROPIC_API_KEY,CLAUDE_CODE_USE_BEDROCK,CLAUDE_CODE_USE_VERTEX" \
        "examples/integrations/claude_agent_sdk_agent/claude_agent_sdk_adapter.py" \
        timed 120 $UV_RUN python examples/integrations/claude_agent_sdk_agent/claude_agent_sdk_adapter.py \
            --prompt "Explain one Kitaru checkpoint in one short sentence. Do not use tools, Bash, or files." \
            --max-turns 1
    run_provider_test "anthropic" "ANTHROPIC_API_KEY,CLAUDE_CODE_USE_BEDROCK,CLAUDE_CODE_USE_VERTEX" \
        "examples/integrations/claude_agent_sdk_agent/claude_agent_sdk_streaming.py" \
        timed 120 $UV_RUN python examples/integrations/claude_agent_sdk_agent/claude_agent_sdk_streaming.py \
            --prompt "Explain one Kitaru streamed checkpoint in one short sentence. Do not use tools, Bash, or files." \
            --max-turns 1
else
    skip_test "examples/integrations/claude_agent_sdk_agent/claude_agent_sdk_adapter.py" "ANTHROPIC_API_KEY or Claude SDK provider mode not set" "anthropic" "ANTHROPIC_API_KEY,CLAUDE_CODE_USE_BEDROCK,CLAUDE_CODE_USE_VERTEX"
    skip_test "examples/integrations/claude_agent_sdk_agent/claude_agent_sdk_streaming.py" "ANTHROPIC_API_KEY or Claude SDK provider mode not set" "anthropic" "ANTHROPIC_API_KEY,CLAUDE_CODE_USE_BEDROCK,CLAUDE_CODE_USE_VERTEX"
fi

section_header "Gemini Interactions adapter"

run_test "examples/integrations/gemini_interactions_agent/gemini_interactions_adapter.py --help" \
    $UV_RUN python examples/integrations/gemini_interactions_agent/gemini_interactions_adapter.py --help
run_test "examples/integrations/gemini_interactions_agent/gemini_interactions_adapter.py --dry-run --mode antigravity" \
    $UV_RUN python examples/integrations/gemini_interactions_agent/gemini_interactions_adapter.py --dry-run --mode antigravity
run_test "examples/integrations/gemini_interactions_agent/gemini_interactions_adapter.py --dry-run --stream" \
    $UV_RUN python examples/integrations/gemini_interactions_agent/gemini_interactions_adapter.py --dry-run --stream
run_test "examples/integrations/gemini_interactions_agent/gemini_interactions_adapter.py --dry-run --mode sandbox-function" \
    $UV_RUN python examples/integrations/gemini_interactions_agent/gemini_interactions_adapter.py --dry-run --mode sandbox-function

if [[ "$HAS_GEMINI_API_KEY" == true ]]; then
    run_provider_test "gemini-model" "GEMINI_API_KEY,GOOGLE_API_KEY" \
        "examples/integrations/gemini_interactions_agent/gemini_interactions_adapter.py --mode model" \
        timed 120 $UV_RUN python examples/integrations/gemini_interactions_agent/gemini_interactions_adapter.py \
            --mode model \
            --prompt "Explain one Kitaru checkpoint in one short sentence."
elif [[ "$HAS_GEMINI_VERTEX" == true ]]; then
    skip_test "examples/integrations/gemini_interactions_agent/gemini_interactions_adapter.py --mode model" "raw model smoke requires GEMINI_API_KEY or GOOGLE_API_KEY; Vertex ADC config is only used for opt-in Antigravity smoke" "gemini-model" "GEMINI_API_KEY,GOOGLE_API_KEY"
else
    skip_test "examples/integrations/gemini_interactions_agent/gemini_interactions_adapter.py --mode model" "GEMINI_API_KEY or GOOGLE_API_KEY not set" "gemini-model" "GEMINI_API_KEY,GOOGLE_API_KEY"
fi

if [[ "$HAS_GEMINI_API_KEY" != true ]]; then
    skip_test "examples/integrations/gemini_interactions_agent/gemini_interactions_adapter.py --mode sandbox-function" "sandbox-function smoke requires GEMINI_API_KEY or GOOGLE_API_KEY; Vertex ADC does not serve raw model interactions" "gemini-sandbox-function" "GEMINI_API_KEY,GOOGLE_API_KEY,KITARU_SMOKE_GEMINI_SANDBOX_FUNCTION"
elif [[ "${KITARU_SMOKE_GEMINI_SANDBOX_FUNCTION:-}" != "1" ]]; then
    skip_test "examples/integrations/gemini_interactions_agent/gemini_interactions_adapter.py --mode sandbox-function" "set KITARU_SMOKE_GEMINI_SANDBOX_FUNCTION=1 to run the real Gemini custom-function plus Kitaru sandbox smoke" "gemini-sandbox-function" "KITARU_SMOKE_GEMINI_SANDBOX_FUNCTION"
else
    GEMINI_SANDBOX_FUNCTION_SMOKE_STACK="kitaru-gemini-sandbox-function-smoke-$$"
    run_test "Create Gemini sandbox function smoke stack" \
        timed 60 $UV_RUN kitaru stack create "$GEMINI_SANDBOX_FUNCTION_SMOKE_STACK" \
            --type local \
            --sandbox local \
            --no-activate
    run_provider_test "gemini-sandbox-function" "GEMINI_API_KEY,GOOGLE_API_KEY,KITARU_SMOKE_GEMINI_SANDBOX_FUNCTION" \
        "examples/integrations/gemini_interactions_agent/gemini_interactions_adapter.py --mode sandbox-function" \
        timed 180 env \
            KITARU_STACK="$GEMINI_SANDBOX_FUNCTION_SMOKE_STACK" \
            $UV_RUN python examples/integrations/gemini_interactions_agent/gemini_interactions_adapter.py \
                --mode sandbox-function \
                --prompt "Call the sandbox_python_version function, then answer in one short sentence."
fi

if [[ "$HAS_GEMINI" != true ]]; then
    skip_test "examples/integrations/gemini_interactions_agent/gemini_interactions_adapter.py --mode antigravity" "GEMINI_API_KEY/GOOGLE_API_KEY or Vertex ADC config not set" "gemini-antigravity" "GEMINI_API_KEY,GOOGLE_API_KEY,GOOGLE_GENAI_USE_VERTEXAI,GOOGLE_CLOUD_PROJECT,GOOGLE_CLOUD_LOCATION,KITARU_SMOKE_GEMINI_ANTIGRAVITY"
elif [[ "${KITARU_SMOKE_GEMINI_ANTIGRAVITY:-}" != "1" ]]; then
    skip_test "examples/integrations/gemini_interactions_agent/gemini_interactions_adapter.py --mode antigravity" "set KITARU_SMOKE_GEMINI_ANTIGRAVITY=1 to run; accepts Gemini API key or Vertex ADC config" "gemini-antigravity" "KITARU_SMOKE_GEMINI_ANTIGRAVITY"
else
    run_provider_test "gemini-antigravity" "GEMINI_API_KEY,GOOGLE_API_KEY,GOOGLE_GENAI_USE_VERTEXAI,GOOGLE_CLOUD_PROJECT,GOOGLE_CLOUD_LOCATION,KITARU_SMOKE_GEMINI_ANTIGRAVITY" \
        "examples/integrations/gemini_interactions_agent/gemini_interactions_adapter.py --mode antigravity" \
        timed 360 $UV_RUN python examples/integrations/gemini_interactions_agent/gemini_interactions_adapter.py \
            --mode antigravity \
            --timeout 300 \
            --prompt "Explain what you would inspect first in this repository. Do not edit files."
fi

if [[ "$HAS_OPENAI" != true ]]; then
    skip_test "examples/end_to_end/openai_research_bot/research_bot.py" "OPENAI_API_KEY not set" "research-bot" "OPENAI_API_KEY,KITARU_SMOKE_RESEARCH_BOT"
elif [[ "${KITARU_SMOKE_RESEARCH_BOT:-}" != "1" ]]; then
    skip_test "examples/end_to_end/openai_research_bot/research_bot.py" "set KITARU_SMOKE_RESEARCH_BOT=1 to run the real web-search smoke test" "research-bot" "KITARU_SMOKE_RESEARCH_BOT"
else
    run_provider_test "research-bot" "OPENAI_API_KEY,KITARU_SMOKE_RESEARCH_BOT" \
        "examples/end_to_end/openai_research_bot/research_bot.py" \
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
run_test "Checkpoint streaming example" timed 60 $UV_RUN examples/features/checkpoint_streaming/checkpoint_streaming.py
SANDBOX_SMOKE_TMP=$(mktemp -d "${TMPDIR:-/tmp}/kitaru-sandbox-smoke.XXXXXX")
SANDBOX_SMOKE_CONFIG="$SANDBOX_SMOKE_TMP/config"
SANDBOX_SMOKE_REPO="$SANDBOX_SMOKE_TMP/repo"
SANDBOX_SMOKE_STACK="kitaru-smoke-sandbox-$$"
mkdir -p "$SANDBOX_SMOKE_CONFIG" "$SANDBOX_SMOKE_REPO/.kitaru"
printf '{}\n' > "$SANDBOX_SMOKE_REPO/.kitaru/config.yaml"
# Keep this example on an isolated local ZenML config. The smoke script is
# connected to a local server above, and that server may not have the local
# sandbox flavor registered. The isolated config mirrors the deterministic
# pytest path and guarantees later smoke checks keep using the main active stack.
run_test "Create sandbox example stack" \
    timed 60 env \
        -u ZENML_SERVER \
        -u ZENML_ACTIVE_PROJECT_ID \
        -u ZENML_ACTIVE_STACK_ID \
        -u ZENML_LOCAL_STORES_PATH \
        -u KITARU_STACK \
        STACK_NAME="$SANDBOX_SMOKE_STACK" \
        ZENML_CONFIG_PATH="$SANDBOX_SMOKE_CONFIG" \
        ZENML_REPOSITORY_PATH="$SANDBOX_SMOKE_REPO" \
        ZENML_ANALYTICS_OPT_IN=false \
        $UV_RUN python -c 'import os, kitaru; kitaru.create_stack(os.environ["STACK_NAME"])'
run_test "Active stack sandbox command" \
    timed 60 env \
        -u ZENML_SERVER \
        -u ZENML_ACTIVE_PROJECT_ID \
        -u ZENML_ACTIVE_STACK_ID \
        -u ZENML_LOCAL_STORES_PATH \
        -u KITARU_STACK \
        ZENML_CONFIG_PATH="$SANDBOX_SMOKE_CONFIG" \
        ZENML_REPOSITORY_PATH="$SANDBOX_SMOKE_REPO" \
        ZENML_ANALYTICS_OPT_IN=false \
        $UV_RUN python examples/features/sandbox/active_stack_sandbox_command.py
if [[ "$HAS_OPENAI" == true ]]; then
    LANGGRAPH_SANDBOX_SMOKE_CONFIG=$(mktemp -d "${TMPDIR:-/tmp}/kitaru-langgraph-sandbox-smoke.XXXXXX")
    LANGGRAPH_SANDBOX_SMOKE_STACK="kitaru-langgraph-sandbox-smoke-$$"
    run_test "Create LangGraph sandbox example stack" \
        timed 60 env \
            -u ZENML_SERVER \
            -u ZENML_ACTIVE_PROJECT_ID \
            -u ZENML_ACTIVE_STACK_ID \
            -u ZENML_LOCAL_STORES_PATH \
            -u KITARU_STACK \
            STACK_NAME="$LANGGRAPH_SANDBOX_SMOKE_STACK" \
            ZENML_CONFIG_PATH="$LANGGRAPH_SANDBOX_SMOKE_CONFIG" \
            ZENML_REPOSITORY_PATH="$PWD" \
            ZENML_ANALYTICS_OPT_IN=false \
            $UV_RUN python -c 'import os, kitaru; kitaru.create_stack(os.environ["STACK_NAME"])'
    run_provider_test "openai" "OPENAI_API_KEY" \
        "examples/integrations/langgraph_agent/langgraph_adapter.py --strategy sandbox" \
        timed 180 env \
            -u ZENML_SERVER \
            -u ZENML_ACTIVE_PROJECT_ID \
            -u ZENML_ACTIVE_STACK_ID \
            -u ZENML_LOCAL_STORES_PATH \
            KITARU_STACK="$LANGGRAPH_SANDBOX_SMOKE_STACK" \
            LANGGRAPH_SANDBOX_AGENT_MODEL="${LANGGRAPH_SANDBOX_AGENT_MODEL:-gpt-5-nano}" \
            ZENML_CONFIG_PATH="$LANGGRAPH_SANDBOX_SMOKE_CONFIG" \
            ZENML_REPOSITORY_PATH="$PWD" \
            ZENML_ANALYTICS_OPT_IN=false \
            $UV_RUN python examples/integrations/langgraph_agent/langgraph_adapter.py --strategy sandbox
else
    skip_test "examples/integrations/langgraph_agent/langgraph_adapter.py --strategy sandbox" "OPENAI_API_KEY not set" "openai" "OPENAI_API_KEY"
fi
run_test "Client execution mgmt"   timed 60 $UV_RUN examples/features/execution_management/client_execution_management.py
run_test "Wait/resume example import contract" \
    $UV_RUN python -c 'from importlib.util import module_from_spec, spec_from_file_location; from pathlib import Path; path = Path("examples/features/execution_management/wait_and_resume.py"); spec = spec_from_file_location("wait_and_resume_smoke", path); assert spec and spec.loader; module = module_from_spec(spec); spec.loader.exec_module(module); details = module.ReleaseDetails(notes="Bug fixes", major_version=2); assert details.major_version == 2; source = path.read_text(); assert "approve_release" in source and "release_details" in source and "timeout=3600" in source and "timeout=60" in source'
run_test "Replay with overrides"   timed 120 $UV_RUN examples/features/replay/replay_with_overrides.py

# ---------------------------------------------------------------------------
# CLI inspection of executions
# ---------------------------------------------------------------------------
section_header "CLI inspection of executions"

run_test "executions list"         $UV_RUN kitaru executions list
run_test "executions list -o json" $UV_RUN kitaru executions list -o json
run_test "SDK execution statistics" \
    $UV_RUN python -c 'from kitaru import KitaruClient; stats = KitaruClient().executions.statistics(group_by=["status"], metrics=["duration_avg:duration:avg"], max_groups=5); assert hasattr(stats, "groups"); assert all(hasattr(group, "metrics") for group in stats.groups)'
run_test "executions statistics -o json" \
    $UV_RUN kitaru executions statistics --group-by status --metric duration_avg:duration:avg -o json

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
    run_provider_test "openai" "OPENAI_API_KEY" "model register fast" \
        timed 30 $UV_RUN kitaru model register fast --model openai/gpt-4o-mini
    run_provider_test "openai" "OPENAI_API_KEY" "model list (verify alias)" \
        timed 30 $UV_RUN kitaru model list
else
    skip_test "model register fast" "OPENAI_API_KEY not set" "openai" "OPENAI_API_KEY"
    skip_test "model list (verify alias)" "OPENAI_API_KEY not set" "openai" "OPENAI_API_KEY"
fi

# ---------------------------------------------------------------------------
# LLM flow
# ---------------------------------------------------------------------------
section_header "LLM flow"

if [[ "$HAS_OPENAI" == true ]]; then
    run_provider_test "openai" "OPENAI_API_KEY" "LLM flow (flow_with_llm)" \
        timed 30 $UV_RUN examples/features/llm/flow_with_llm.py
else
    skip_test "LLM flow (flow_with_llm)" "OPENAI_API_KEY not set" "openai" "OPENAI_API_KEY"
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

run_test "MCP: kitaru_executions_statistics" \
    $FASTMCP call --command "$MCP_SERVER" --target kitaru_executions_statistics \
        --input-json '{"group_by": ["status"], "metrics": ["duration_avg:duration:avg"]}' --json

run_test "MCP query snapshot (example)" \
    timed 30 $UV_RUN examples/features/mcp/mcp_query_tools.py

fi  # LOGIN_RC == 0

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
printf "\n${BOLD}═══════════════════════════════════════════════${RESET}\n"
printf "${BOLD}  Kitaru Smoke Test Summary${RESET}\n"
printf "${BOLD}═══════════════════════════════════════════════${RESET}\n"

validate_required_provider_area_records

if [[ "$RECORDING_FAILED" == true ]]; then
    FAILED+=("structured result recording failed")
fi

if [[ -n "$JSON_OUT" ]]; then
    if write_json_results; then
        printf "  Structured results: %s\n" "$JSON_OUT"
    else
        printf "  ${RED}✗${RESET} Failed to write structured results: %s\n" "$JSON_OUT" >&2
        FAILED+=("structured results write failed")
    fi
fi

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

if [[ "$RELEASE_MODE" == true ]]; then
    printf "${BOLD}───────────────────────────────────────────────${RESET}\n"
    printf "  ${CYAN}PROVIDER ATTESTATION:${RESET}\n"
    printf "    OpenAI credentials: %s\n" "$HAS_OPENAI"
    printf "    Anthropic/Claude credentials: %s\n" "$HAS_CLAUDE_AGENT_SDK"
    printf "    Gemini direct API credentials: %s\n" "$HAS_GEMINI_API_KEY"
    printf "    Gemini Vertex config: %s\n" "$HAS_GEMINI_VERTEX"
    if [[ ${#REQUIRED_PROVIDER_AREAS[@]} -gt 0 ]]; then
        printf "    Required provider areas: %s\n" "${REQUIRED_PROVIDER_AREAS[*]}"
    else
        printf "    Required provider areas: none\n"
    fi
    if [[ ${#RELEASE_RELEVANT_SKIPPED[@]} -gt 0 ]]; then
        printf "  ${RED}RELEASE-RELEVANT SKIPS:${RESET}\n"
        for label in "${RELEASE_RELEVANT_SKIPPED[@]}"; do
            printf "    ${RED}✗${RESET} %s\n" "$label"
        done
    fi
fi

printf "${BOLD}═══════════════════════════════════════════════${RESET}\n"

if [[ ${#FAILED[@]} -gt 0 ]] || [[ ${#RELEASE_RELEVANT_SKIPPED[@]} -gt 0 ]]; then
    exit 1
fi
exit 0
