#!/usr/bin/env bash
# install.sh — one-line Kitaru installer.
#
#   curl -fsSL https://kitaru.ai/install | bash
#   curl -fsSL https://kitaru.ai/install | bash -s -- --pre
#
# What it does, in order:
#   1. Makes sure `uv` is available (installs it from astral.sh if not).
#   2. Installs kitaru[cli,mcp,worker]. Where depends on where you run it:
#        - inside a Python project (pyproject.toml or uv.lock in the current
#          directory): `uv add` into that project's environment, so its worker
#          can replay your agent alongside your agent's own dependencies;
#        - anywhere else: `uv tool install` into an isolated environment, with
#          `kitaru` and `kitaru-mcp` on PATH (~/.local/bin). Good for the CLI,
#          MCP server, imports and evaluators; replays need the project form.
#      --project / --global force either.
#   3. Installs the Kitaru agent skills (zenml-io/kitaru-skills) from the
#      repository tarball into ~/.agents/skills, plus ~/.claude/skills and
#      ~/.codex/skills when those CLIs are installed. No Node needed.
#   4. Registers the Kitaru MCP server with Claude Code and Codex if their
#      CLIs are installed; prints the JSON for everything else.
#   5. Stops there and prints the two ways to get a server: local in Docker
#      (`kitaru login --local`) or the managed cloud. Login is a decision, so
#      the script does not make it for you.
#
# Nothing here needs sudo. Everything lands under $HOME. Re-running upgrades.
#
# Design borrowed from raindrop.sh/install (step output, tty handling,
# --no-* escape hatches) and astral.sh/uv/install.sh (no root, no assumptions
# about the system Python). Kitaru ships as a Python package, not a binary, so
# uv is the one dependency this script will install for you.

# Bash-only, but fail politely under sh/dash/zsh-as-sh. This line is POSIX so
# it runs before the shell reaches any bash syntax below.
if [ -z "${BASH_VERSION:-}" ]; then echo "Kitaru's installer needs bash. Run: curl -fsSL https://kitaru.ai/install | bash" >&2; exit 1; fi

set -euo pipefail

# ---------------------------------------------------------------------------
# Options
# ---------------------------------------------------------------------------
KITARU_VERSION="${KITARU_VERSION:-}"          # pin, e.g. 0.24.0
KITARU_PRE="${KITARU_PRE:-0}"                 # allow pre-releases
KITARU_SERVER="${KITARU_SERVER:-}"            # point the MCP server at a team server
KITARU_SKIP_SKILLS="${KITARU_SKIP_SKILLS:-0}"
KITARU_SKIP_MCP="${KITARU_SKIP_MCP:-0}"
KITARU_QUIET="${KITARU_QUIET:-0}"
KITARU_VERBOSE="${KITARU_VERBOSE:-0}"
KITARU_WITH=()                                # extra packages, e.g. kitaru-pydantic-ai
KITARU_PYTHON="${KITARU_PYTHON:-3.12}"        # uv-managed Python if none suitable (global mode)
KITARU_SCOPE="${KITARU_SCOPE:-auto}"          # auto | project | global
KITARU_SKILLS_REPO="${KITARU_SKILLS_REPO:-zenml-io/kitaru-skills}"
KITARU_LOCAL_URL="${KITARU_LOCAL_URL:-http://localhost:8000}"
KITARU_MCP_MODE="${KITARU_MCP_MODE:-standard}"
KITARU_NO_MODIFY_PATH="${KITARU_NO_MODIFY_PATH:-0}"    # leave rc files alone
KITARU_MIN_UV="0.5.0"                                  # older uv lacks the tool flags we use

usage() {
  cat <<'USAGE'
Usage: install.sh [options]

  --version=X.Y.Z     Install a specific Kitaru version (default: latest)
  --pre               Allow pre-release versions
  --with=PKG          Also install PKG into the same environment (repeatable),
                      e.g. --with=kitaru-pydantic-ai --with=kitaru-langgraph
  --project           Install into the Python project in the current directory
                      (uv add). Default when a pyproject.toml or uv.lock is here.
  --global            Install into an isolated uv tool environment on PATH.
                      Default anywhere else.
  --server=URL        Point the MCP server at a team/self-hosted server
                      (default: the local server, http://localhost:8000)
  --no-skills         Skip installing the coding-agent skills
  --no-mcp            Skip registering the MCP server with Claude Code / Codex
  --no-modify-path    Do not edit shell rc files; you add ~/.local/bin yourself
  --quiet             Only print errors
  --verbose           Print every command
  -h, --help          This text

Environment equivalents: KITARU_VERSION, KITARU_PRE=1, KITARU_SERVER,
KITARU_SCOPE=project|global, KITARU_SKIP_SKILLS=1, KITARU_SKIP_MCP=1,
KITARU_NO_MODIFY_PATH=1, KITARU_QUIET=1, KITARU_VERBOSE=1,
KITARU_PYTHON (default 3.12), NO_COLOR.
USAGE
}

while [ $# -gt 0 ]; do
  case "$1" in
    --version=*) KITARU_VERSION="${1#*=}" ;;
    --version) shift; KITARU_VERSION="${1:-}" ;;
    --pre) KITARU_PRE=1 ;;
    --with=*) KITARU_WITH+=("${1#*=}") ;;
    --with) shift; KITARU_WITH+=("${1:-}") ;;
    --server=*) KITARU_SERVER="${1#*=}" ;;
    --server) shift; KITARU_SERVER="${1:-}" ;;
    --project) KITARU_SCOPE=project ;;
    --global) KITARU_SCOPE=global ;;
    --no-skills) KITARU_SKIP_SKILLS=1 ;;
    --no-mcp) KITARU_SKIP_MCP=1 ;;
    --no-modify-path) KITARU_NO_MODIFY_PATH=1 ;;
    --quiet) KITARU_QUIET=1 ;;
    --verbose) KITARU_VERBOSE=1 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown option: $1" >&2; usage >&2; exit 2 ;;
  esac
  shift
done

# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------
C_ORANGE=""; C_GREEN=""; C_RED=""; C_DIM=""; C_BOLD=""; C_RESET=""
if [ -z "${NO_COLOR:-}" ] && [ -t 1 ]; then
  C_ORANGE=$'\033[38;5;208m'; C_GREEN=$'\033[32m'; C_RED=$'\033[31m'
  C_DIM=$'\033[2m'; C_BOLD=$'\033[1m'; C_RESET=$'\033[0m'
fi

say()  { [ "$KITARU_QUIET" = "1" ] || printf '%s\n' "$*"; }
step() { [ "$KITARU_QUIET" = "1" ] || printf '%s◇%s %s\n' "$C_ORANGE" "$C_RESET" "$*"; }
ok()   { [ "$KITARU_QUIET" = "1" ] || printf '%s✓%s %s\n' "$C_GREEN" "$C_RESET" "$*"; }
note() { [ "$KITARU_QUIET" = "1" ] || printf '  %s%s%s\n' "$C_DIM" "$*" "$C_RESET"; }
warn() { printf '%s!%s %s\n' "$C_ORANGE" "$C_RESET" "$*" >&2; }
die()  { printf '%s✕%s %s\n' "$C_RED" "$C_RESET" "$*" >&2; exit 1; }
run()  { [ "$KITARU_VERBOSE" = "1" ] && printf '  %s$ %s%s\n' "$C_DIM" "$*" "$C_RESET" >&2; "$@"; }
quiet() {
  # Run a command, showing its output only on failure or --verbose.
  # stdin is /dev/null so a child can never swallow the rest of this script
  # when it is being piped in from curl.
  if [ "$KITARU_VERBOSE" = "1" ]; then run "$@" </dev/null; return $?; fi
  local out status=0; out="$(mktemp)"
  "$@" >"$out" 2>&1 </dev/null || status=$?
  [ "$status" -eq 0 ] || cat "$out" >&2
  rm -f "$out"; return "$status"
}
have() { command -v "$1" >/dev/null 2>&1; }

main() {
# PATH as the user had it, before this script adds anything to it.
ORIG_PATH="$PATH"
# ---------------------------------------------------------------------------
# Guards
# ---------------------------------------------------------------------------
if [ "$(id -u)" -eq 0 ] && [ -n "${SUDO_USER:-}" ] && [ "$SUDO_USER" != "root" ]; then
  die "Do not run this installer with sudo. It installs into your home directory and needs no root access."
fi

case "$(uname -s)" in
  Darwin|Linux) ;;
  MINGW*|MSYS*|CYGWIN*)
    # Git Bash / MSYS. uv, the CLI and the MCP server install fine; the local
    # server still needs Docker Desktop. WSL is the smoother path.
    warn "Windows (Git Bash) detected. Installing; WSL is recommended for the local server." ;;
  *) die "Unsupported OS: $(uname -s)" ;;
esac

if ! have curl && ! have wget; then die "Need curl or wget."; fi

fetch_to_stdout() {
  if have curl; then curl -fsSL --proto '=https' --tlsv1.2 "$1"
  else wget -q --https-only -O- "$1"; fi
}

say ""
say "${C_ORANGE}◆${C_RESET} ${C_BOLD}Installing Kitaru${C_RESET}"
say ""

# ---------------------------------------------------------------------------
# 1. uv
# ---------------------------------------------------------------------------
ensure_path() {
  case ":$PATH:" in *":$1:"*) ;; *) export PATH="$1:$PATH" ;; esac
}
ensure_path "$HOME/.local/bin"
ensure_path "$HOME/.cargo/bin"

version_ge() {
  # version_ge A B → true if A >= B (dotted numerics only)
  [ "$(printf '%s\n%s\n' "$2" "$1" | sort -t. -k1,1n -k2,2n -k3,3n | head -1)" = "$2" ]
}

uv_version_of() { "$1" --version 2>/dev/null | awk '{print $2}'; }

install_uv() {
  step "Installing uv (Python package manager, from astral.sh)"
  # Download to a file first: `quiet` closes stdin, so piping into `sh -s`
  # would hand the installer an empty script.
  UV_INSTALLER="$(mktemp)"
  trap 'rm -f "$UV_INSTALLER"' EXIT
  fetch_to_stdout https://astral.sh/uv/install.sh >"$UV_INSTALLER" \
    || die "Could not download the uv installer from https://astral.sh/uv/install.sh"
  quiet sh "$UV_INSTALLER" --quiet \
    || die "uv install failed. Install it from https://docs.astral.sh/uv/ and re-run."
  rm -f "$UV_INSTALLER"; trap - EXIT
  # Use the binary we just installed by absolute path, and put its directory
  # in front so an older uv earlier on PATH (or bash's hashed path) can't win.
  UV="$HOME/.local/bin/uv"
  export PATH="$HOME/.local/bin:$PATH"; hash -r
  [ -x "$UV" ] || die "uv installed but $UV is missing. Install it from https://docs.astral.sh/uv/ and re-run."
  version_ge "$(uv_version_of "$UV")" "$KITARU_MIN_UV" \
    || die "uv at $UV is $(uv_version_of "$UV"), older than $KITARU_MIN_UV. Install a current uv and re-run."
  ok "uv $(uv_version_of "$UV") installed"
}

# $UV is the uv binary used for everything below (absolute path).
UV=""
if have uv; then
  UV="$(command -v uv)"
  UV_VER="$(uv_version_of "$UV")"
  if version_ge "$UV_VER" "$KITARU_MIN_UV"; then
    ok "uv $UV_VER found"
  else
    note "uv $UV_VER is older than $KITARU_MIN_UV; installing a current one alongside it."
    install_uv
  fi
else
  install_uv
fi

persist_path() {
  # `uv tool update-shell` handles bash/zsh/fish when it recognizes the login
  # shell. Cover the rest (Alpine sh, containers, CI images) by appending to
  # ~/.profile when no rc file mentions the directory yet.
  local dir="$1" f
  for f in "$HOME/.bashrc" "$HOME/.zshrc" "$HOME/.profile" "$HOME/.bash_profile" "$HOME/.config/fish/config.fish"; do
    [ -f "$f" ] && grep -qs "$dir\|\.local/bin" "$f" && return 0
  done
  printf '\n# Added by the Kitaru installer\nexport PATH="%s:$PATH"\n' "$dir" >> "$HOME/.profile"
}

# ---------------------------------------------------------------------------
# 2. kitaru CLI + MCP server: into this project, or an isolated tool env
# ---------------------------------------------------------------------------
SPEC="kitaru[cli,mcp,worker]"
[ -n "$KITARU_VERSION" ] && SPEC="${SPEC}==${KITARU_VERSION}"
EXTRA_PKGS=()
for pkg in "${KITARU_WITH[@]:-}"; do [ -n "$pkg" ] && EXTRA_PKGS+=("$pkg"); done

PROJECT_DIR="$PWD"
if [ "$KITARU_SCOPE" = "auto" ]; then
  if [ -f "$PROJECT_DIR/pyproject.toml" ] || [ -f "$PROJECT_DIR/uv.lock" ]; then
    KITARU_SCOPE=project
  else
    KITARU_SCOPE=global
  fi
fi

if [ "$KITARU_SCOPE" = "project" ]; then
  # ---- into the project in the current directory ---------------------------
  # The worker runs your agent, so it has to live where your agent's
  # dependencies live. `uv add` puts kitaru into this project's environment
  # (creating .venv if needed) and records it in pyproject.toml.
  [ -f "$PROJECT_DIR/pyproject.toml" ] || die "--project needs a pyproject.toml in $PROJECT_DIR (run \`uv init\` first, or use --global)."
  step "Installing $SPEC into this project ($PROJECT_DIR)"
  UV_ADD=(add --quiet)
  [ "$KITARU_PRE" = "1" ] && UV_ADD+=(--prerelease allow)
  quiet "$UV" "${UV_ADD[@]}" "$SPEC" "${EXTRA_PKGS[@]:+${EXTRA_PKGS[@]}}" \
    || die "uv add failed (uv's message is above; Kitaru needs Python >= 3.11). To install outside this project instead: --global"
  VENV_DIR="${UV_PROJECT_ENVIRONMENT:-$PROJECT_DIR/.venv}"
  case "$(uname -s)" in
    MINGW*|MSYS*|CYGWIN*) TOOL_BIN="$VENV_DIR/Scripts" ;;
    *) TOOL_BIN="$VENV_DIR/bin" ;;
  esac
  KITARU_BIN="$TOOL_BIN/kitaru"
  [ -x "$KITARU_BIN" ] || [ -x "$KITARU_BIN.exe" ] || die "uv reported success but $KITARU_BIN is missing. Re-run with --verbose."
  ok "kitaru $("$KITARU_BIN" --version 2>/dev/null) installed into this project's environment"
  note "environment: $VENV_DIR (recorded in pyproject.toml)"
  note "run it as: uv run kitaru ...   (or activate the environment)"
  # MCP: run through uv so the assistant gets this project's environment.
  MCP_CMD=("$UV" run --directory "$PROJECT_DIR" kitaru-mcp)
  CLAUDE_SCOPE=project
else
  # ---- isolated tool environment on PATH -----------------------------------
  UV_ARGS=(tool install --upgrade --quiet --python "$KITARU_PYTHON")
  [ "$KITARU_PRE" = "1" ] && UV_ARGS+=(--prerelease allow)
  for pkg in "${EXTRA_PKGS[@]:+${EXTRA_PKGS[@]}}"; do UV_ARGS+=(--with "$pkg"); done

  step "Installing $SPEC (isolated, on PATH)"
  quiet "$UV" "${UV_ARGS[@]}" "$SPEC" || die "uv tool install failed. Re-run with --verbose for details."

  # uv puts tool executables in its tool bin dir; make sure future shells see it.
  TOOL_BIN="$("$UV" tool dir --bin 2>/dev/null || echo "$HOME/.local/bin")"
  TOOL_ENV="$("$UV" tool dir 2>/dev/null || echo "$HOME/.local/share/uv/tools")/kitaru"
  case "$(uname -s)" in MINGW*|MSYS*|CYGWIN*) TOOL_BIN="$(cygpath -u "$TOOL_BIN" 2>/dev/null || echo "$TOOL_BIN")" ;; esac
  ensure_path "$TOOL_BIN"
  if [ "$KITARU_NO_MODIFY_PATH" = "1" ]; then
    note "Not editing shell rc files (--no-modify-path). Make sure $TOOL_BIN is on your PATH."
  else
    quiet "$UV" tool update-shell || true
    persist_path "$TOOL_BIN"
  fi

  KITARU_BIN="$TOOL_BIN/kitaru"
  [ -x "$KITARU_BIN" ] || [ -x "$KITARU_BIN.exe" ] || die "uv reported success but $KITARU_BIN is missing. Re-run with --verbose."
  ok "kitaru $("$KITARU_BIN" --version 2>/dev/null) installed"
  note "isolated environment: $TOOL_ENV   (uv tool; no other Python touched)"
  note "commands: $TOOL_BIN/kitaru, $TOOL_BIN/kitaru-mcp"
  # Warn if a different kitaru was already reachable on the PATH the user
  # started with (a pip or pipx install, say): depending on their rc order it
  # may keep winning in new terminals.
  hash -r
  RESOLVED_KITARU="$(PATH="$ORIG_PATH" command -v kitaru 2>/dev/null || true)"
  if [ -n "$RESOLVED_KITARU" ] && [ "$RESOLVED_KITARU" != "$KITARU_BIN" ] && [ "$RESOLVED_KITARU" != "$KITARU_BIN.exe" ]; then
    warn "Another kitaru at $RESOLVED_KITARU ($("$RESOLVED_KITARU" --version 2>/dev/null || echo unknown)) shadows the one just installed. Remove it or put $TOOL_BIN first on PATH."
  fi
  MCP_CMD=("$TOOL_BIN/kitaru-mcp")
  CLAUDE_SCOPE=user
fi

# ---------------------------------------------------------------------------
# 3. Coding-agent skills
# ---------------------------------------------------------------------------
# Destinations: ~/.agents/skills is the cross-agent location; ~/.claude and
# ~/.codex get their own copy when that CLI is installed or the dir exists.
skill_destinations() {
  echo "$HOME/.agents/skills"
  if have claude || [ -d "$HOME/.claude" ]; then echo "$HOME/.claude/skills"; fi
  if have codex || [ -d "$HOME/.codex" ]; then echo "$HOME/.codex/skills"; fi
}

install_skills() {
  # Fetch the repository tarball (no git, no Node, no auth, nothing executed
  # from the current directory) and copy every skill under skills/ into each
  # destination. Every mutation is checked; a partial copy is a failure.
  local tmp url src dest skill name
  tmp="$(mktemp -d)" || return 1
  url="https://github.com/${KITARU_SKILLS_REPO}/archive/refs/heads/main.tar.gz"
  if ! fetch_to_stdout "$url" | tar -xzf - -C "$tmp" 2>/dev/null; then
    rm -rf "$tmp"; return 1
  fi
  src="$(find "$tmp" -mindepth 2 -maxdepth 2 -type d -name skills | head -1)"
  if [ -z "$src" ]; then rm -rf "$tmp"; return 1; fi
  SKILL_NAMES=()
  for skill in "$src"/*/; do
    [ -f "$skill/SKILL.md" ] && SKILL_NAMES+=("$(basename "$skill")")
  done
  if [ "${#SKILL_NAMES[@]}" -eq 0 ]; then rm -rf "$tmp"; return 1; fi
  SKILL_DESTS=()
  while IFS= read -r dest; do SKILL_DESTS+=("$dest"); done < <(skill_destinations)
  for dest in "${SKILL_DESTS[@]}"; do
    mkdir -p "$dest" || { rm -rf "$tmp"; return 1; }
    for name in "${SKILL_NAMES[@]}"; do
      rm -rf "$dest/$name" || { rm -rf "$tmp"; return 1; }
      cp -R "$src/$name" "$dest/$name" || { rm -rf "$tmp"; return 1; }
    done
  done
  rm -rf "$tmp"
  skills_complete
}

# Postcondition: every skill from the tarball is present in every destination.
skills_complete() {
  local dest name
  for dest in "${SKILL_DESTS[@]}"; do
    for name in "${SKILL_NAMES[@]}"; do
      [ -f "$dest/$name/SKILL.md" ] || return 1
    done
  done
  return 0
}

SKILL_NAMES=(); SKILL_DESTS=()
if [ "$KITARU_SKIP_SKILLS" = "1" ]; then
  note "Skipping skills (--no-skills)"
else
  step "Installing Kitaru agent skills ($KITARU_SKILLS_REPO)"
  if install_skills; then
    ok "${#SKILL_NAMES[@]} skills installed into ${SKILL_DESTS[*]}"
  else
    warn "Could not install skills. Later: npx skills add $KITARU_SKILLS_REPO"
  fi
fi

# ---------------------------------------------------------------------------
# 4. MCP server registration
# ---------------------------------------------------------------------------
MCP_SERVER_URL="${KITARU_SERVER:-$KITARU_LOCAL_URL}"
MCP_ARGS=(--server "$MCP_SERVER_URL" --mode "$KITARU_MCP_MODE")

if [ "$KITARU_SKIP_MCP" = "1" ]; then
  note "Skipping MCP registration (--no-mcp)"
else
  step "Registering the Kitaru MCP server"
  registered=0
  if have claude; then
    # User-scope MCP servers live in ~/.claude.json, project-scope ones in
    # ./.mcp.json. Snapshot the file so a failed replace can put the previous
    # entry back instead of leaving none.
    if [ "$CLAUDE_SCOPE" = "project" ]; then CLAUDE_CFG="$PROJECT_DIR/.mcp.json"; else CLAUDE_CFG="$HOME/.claude.json"; fi
    CLAUDE_CFG_BAK=""
    if [ -f "$CLAUDE_CFG" ]; then
      CLAUDE_CFG_BAK="$(mktemp)"; cp "$CLAUDE_CFG" "$CLAUDE_CFG_BAK"
    fi
    if claude mcp get kitaru >/dev/null 2>&1; then
      quiet claude mcp remove --scope "$CLAUDE_SCOPE" kitaru || true
    fi
    if quiet claude mcp add --scope "$CLAUDE_SCOPE" kitaru -- "${MCP_CMD[@]}" "${MCP_ARGS[@]}" \
       && claude mcp get kitaru >/dev/null 2>&1; then
      ok "Claude Code: MCP server 'kitaru' ($CLAUDE_SCOPE scope)"; registered=1
    else
      if [ -n "$CLAUDE_CFG_BAK" ]; then cp "$CLAUDE_CFG_BAK" "$CLAUDE_CFG"; fi
      warn "Claude Code: could not register MCP server; previous config left as it was."
    fi
    [ -n "$CLAUDE_CFG_BAK" ] && rm -f "$CLAUDE_CFG_BAK"
  fi
  if have codex; then
    # `codex mcp add` overwrites an existing name, so no remove first.
    if quiet codex mcp add kitaru -- "${MCP_CMD[@]}" "${MCP_ARGS[@]}"; then
      ok "Codex: MCP server 'kitaru'"; registered=1
    else
      warn "Codex: could not register MCP server"
    fi
  fi
  if [ "$registered" = "0" ]; then
    note "No Claude Code or Codex CLI found. For Cursor or any MCP client, add:"
    MCP_JSON_ARGS=""
    for a in "${MCP_CMD[@]:1}" "${MCP_ARGS[@]}"; do MCP_JSON_ARGS="$MCP_JSON_ARGS${MCP_JSON_ARGS:+,}\"$a\""; done
    note "  {\"mcpServers\":{\"kitaru\":{\"command\":\"${MCP_CMD[0]}\",\"args\":[$MCP_JSON_ARGS]}}}"
  fi
fi

# ---------------------------------------------------------------------------
# Done
# ---------------------------------------------------------------------------
say ""
say "${C_GREEN}◆${C_RESET} ${C_BOLD}Kitaru is installed.${C_RESET}"
say ""
if [ "$KITARU_SCOPE" = "project" ]; then
  say "  Installed into this project's environment. Run it as ${C_BOLD}uv run kitaru ...${C_RESET}"
  say "  (or activate $VENV_DIR)."
  say ""
elif [ "$(PATH="$ORIG_PATH" command -v kitaru 2>/dev/null || true)" != "$KITARU_BIN" ]; then
  say "  Open a new terminal so 'kitaru' is on your PATH."
  say ""
fi
if [ -n "$KITARU_SERVER" ]; then
  say "  Next, log in to your server:"
  say ""
  say "    ${C_BOLD}kitaru login $KITARU_SERVER${C_RESET}"
else
  say "  Next, pick where your Kitaru server lives:"
  say ""
  say "    ${C_BOLD}kitaru login --local${C_RESET}       local, in Docker. Free, open source."
  say "    ${C_BOLD}https://cloud.kitaru.ai${C_RESET}    managed cloud. 14-day trial, no credit card required."
  say "                               then: kitaru login <your workspace URL>"
fi
say ""
say "  Then, in your agent's repo, tell your coding agent:"
say "    ${C_BOLD}Use kitaru-investigation to investigate this agent.${C_RESET}"
if [ "$KITARU_SCOPE" = "global" ]; then
  say ""
  say "  Replays run your agent, so they need Kitaru inside the agent's own project:"
  say "  re-run this installer from that repo, or ${C_BOLD}uv add \"kitaru[cli,worker]\" kitaru-<adapter>${C_RESET} there."
fi
say ""
say "  No agent yet?  ${C_BOLD}Use kitaru-guided-tour to show me Kitaru on the example agent.${C_RESET}"
say "  Check setup:   kitaru doctor"
say "  Docs:          https://docs.zenml.io/kitaru"
say ""
}

main "$@"
