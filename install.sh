#!/usr/bin/env bash
# install.sh — one-line Kitaru installer.
#
#   curl -fsSL https://kitaru.ai/install | bash
#   curl -fsSL https://kitaru.ai/install | bash -s -- --pre
#   curl -fsSL https://kitaru.ai/install | bash -s -- --server https://team.kitaru.ai
#
# What it does, in order:
#   1. Makes sure `uv` is available (installs it from astral.sh if not).
#   2. `uv tool install kitaru[cli,mcp,worker]` into an isolated environment,
#      with a uv-managed Python if the machine has none. Puts `kitaru` and
#      `kitaru-mcp` on PATH (~/.local/bin) for future terminals.
#   3. Installs the Kitaru agent skills (zenml-io/kitaru-skills) for every
#      coding agent found on the machine, user scope. Uses `npx skills` when
#      Node is present, otherwise copies them from git.
#   4. Registers the Kitaru MCP server with Claude Code and Codex if their
#      CLIs are installed; prints the JSON for everything else.
#   5. If Docker is running and there is a terminal, runs `kitaru login --local`
#      to start the local server. Otherwise tells you the one command to run.
#
# Nothing here needs sudo. Everything lands under $HOME. Re-running upgrades.
#
# Design borrowed from raindrop.sh/install (step output, tty handling,
# --no-* escape hatches) and astral.sh/uv/install.sh (no root, no assumptions
# about the system Python). Kitaru ships as a Python package, not a binary, so
# uv is the one dependency this script will install for you.

set -euo pipefail

# ---------------------------------------------------------------------------
# Options
# ---------------------------------------------------------------------------
KITARU_VERSION="${KITARU_VERSION:-}"          # pin, e.g. 0.24.0
KITARU_PRE="${KITARU_PRE:-0}"                 # allow pre-releases
KITARU_SERVER="${KITARU_SERVER:-}"            # team server URL instead of --local
KITARU_SKIP_SKILLS="${KITARU_SKIP_SKILLS:-0}"
KITARU_SKIP_MCP="${KITARU_SKIP_MCP:-0}"
KITARU_SKIP_LOGIN="${KITARU_SKIP_LOGIN:-0}"
KITARU_QUIET="${KITARU_QUIET:-0}"
KITARU_VERBOSE="${KITARU_VERBOSE:-0}"
KITARU_WITH=()                                # extra packages, e.g. kitaru-pydantic-ai
KITARU_PYTHON="${KITARU_PYTHON:-3.12}"        # uv-managed Python if none suitable
KITARU_SKILLS_REPO="${KITARU_SKILLS_REPO:-zenml-io/kitaru-skills}"
KITARU_LOCAL_URL="${KITARU_LOCAL_URL:-http://localhost:8000}"
KITARU_MCP_MODE="${KITARU_MCP_MODE:-standard}"

usage() {
  cat <<'USAGE'
Usage: install.sh [options]

  --version=X.Y.Z     Install a specific Kitaru version (default: latest)
  --pre               Allow pre-release versions
  --with=PKG          Also install PKG into the same environment (repeatable),
                      e.g. --with=kitaru-pydantic-ai --with=kitaru-langgraph
  --server=URL        Log in to a team/self-hosted server instead of starting
                      a local one
  --no-skills         Skip installing the coding-agent skills
  --no-mcp            Skip registering the MCP server with Claude Code / Codex
  --no-login          Skip `kitaru login` (just install the CLI)
  --quiet             Only print errors
  --verbose           Print every command
  -h, --help          This text

Environment equivalents: KITARU_VERSION, KITARU_PRE=1, KITARU_SERVER,
KITARU_SKIP_SKILLS=1, KITARU_SKIP_MCP=1, KITARU_SKIP_LOGIN=1, KITARU_QUIET=1,
KITARU_VERBOSE=1, KITARU_PYTHON (default 3.12), NO_COLOR.
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
    --no-skills) KITARU_SKIP_SKILLS=1 ;;
    --no-mcp) KITARU_SKIP_MCP=1 ;;
    --no-login) KITARU_SKIP_LOGIN=1 ;;
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
  local out; out="$(mktemp)"
  if "$@" >"$out" 2>&1 </dev/null; then rm -f "$out"; return 0; fi
  local status=$?
  cat "$out" >&2; rm -f "$out"; return $status
}
have() { command -v "$1" >/dev/null 2>&1; }

main() {
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
  else wget -qO- "$1"; fi
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

if have uv; then
  ok "uv $(uv --version 2>/dev/null | awk '{print $2}') found"
else
  step "Installing uv (Python package manager, from astral.sh)"
  # Download to a file first: `quiet` closes stdin, so piping into `sh -s`
  # would hand the installer an empty script.
  UV_INSTALLER="$(mktemp)"
  fetch_to_stdout https://astral.sh/uv/install.sh >"$UV_INSTALLER" \
    || die "Could not download the uv installer from https://astral.sh/uv/install.sh"
  quiet sh "$UV_INSTALLER" --quiet \
    || die "uv install failed. Install it from https://docs.astral.sh/uv/ and re-run."
  rm -f "$UV_INSTALLER"
  ensure_path "$HOME/.local/bin"
  have uv || die "uv installed but not found on PATH. Open a new terminal and re-run."
  ok "uv $(uv --version | awk '{print $2}') installed"
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
# 2. kitaru CLI + MCP server, in an isolated tool environment
# ---------------------------------------------------------------------------
SPEC="kitaru[cli,mcp,worker]"
[ -n "$KITARU_VERSION" ] && SPEC="${SPEC}==${KITARU_VERSION}"

UV_ARGS=(tool install --upgrade --quiet --python "$KITARU_PYTHON")
[ "$KITARU_PRE" = "1" ] && UV_ARGS+=(--prerelease allow)
for pkg in "${KITARU_WITH[@]:-}"; do
  [ -n "$pkg" ] && UV_ARGS+=(--with "$pkg")
done

step "Installing $SPEC"
quiet uv "${UV_ARGS[@]}" "$SPEC" || die "uv tool install failed. Re-run with --verbose for details."

# uv puts tool executables in its tool bin dir; make sure future shells see it.
TOOL_BIN="$(uv tool dir --bin 2>/dev/null || echo "$HOME/.local/bin")"
case "$(uname -s)" in MINGW*|MSYS*|CYGWIN*) TOOL_BIN="$(cygpath -u "$TOOL_BIN" 2>/dev/null || echo "$TOOL_BIN")" ;; esac
ensure_path "$TOOL_BIN"
quiet uv tool update-shell || true
persist_path "$TOOL_BIN"

have kitaru || die "kitaru installed to $TOOL_BIN but is not on PATH. Add it and re-run."
ok "kitaru $(kitaru --version 2>/dev/null) installed"
note "$TOOL_BIN/kitaru, $TOOL_BIN/kitaru-mcp"

# ---------------------------------------------------------------------------
# 3. Coding-agent skills
# ---------------------------------------------------------------------------
skills_present() {
  local d
  for d in "$HOME/.agents/skills" "$HOME/.claude/skills" "$HOME/.codex/skills"; do
    [ -f "$d/kitaru-investigation/SKILL.md" ] && return 0
  done
  return 1
}

install_skills_from_tarball() {
  # Fallback without Node: fetch the repo tarball (no git, no auth) and copy
  # each skill into the user-scope skills folder of every agent host kitaru
  # looks in (~/.agents always; ~/.claude and ~/.codex when present).
  local tmp; tmp="$(mktemp -d)"
  local url="https://github.com/${KITARU_SKILLS_REPO}/archive/refs/heads/main.tar.gz"
  if ! fetch_to_stdout "$url" | tar -xzf - -C "$tmp" 2>/dev/null; then
    rm -rf "$tmp"; return 1
  fi
  local src; src="$(find "$tmp" -maxdepth 2 -type d -name skills | head -1)"
  [ -n "$src" ] || { rm -rf "$tmp"; return 1; }
  local host
  for host in "$HOME/.agents" "$HOME/.claude" "$HOME/.codex"; do
    if [ "$host" != "$HOME/.agents" ] && [ ! -d "$host" ]; then continue; fi
    mkdir -p "$host/skills"
    local skill
    for skill in "$src"/*/; do
      [ -f "$skill/SKILL.md" ] || continue
      rm -rf "$host/skills/$(basename "$skill")"
      cp -R "$skill" "$host/skills/$(basename "$skill")"
    done
  done
  rm -rf "$tmp"
  return 0
}

if [ "$KITARU_SKIP_SKILLS" = "1" ]; then
  note "Skipping skills (--no-skills)"
else
  step "Installing Kitaru agent skills ($KITARU_SKILLS_REPO)"
  if have npx; then
    # The skills CLI exits 0 even when it installed nothing, so check the
    # filesystem rather than the exit code.
    quiet npx -y skills add "$KITARU_SKILLS_REPO" -g -y -a '*' -s '*' || true
    if skills_present; then
      ok "Skills installed for every coding agent found (user scope)"
    elif install_skills_from_tarball; then
      ok "Skills copied into ~/.agents/skills (and ~/.claude, ~/.codex where present)"
    else
      warn "Could not install skills. Later: npx skills add $KITARU_SKILLS_REPO"
    fi
  elif install_skills_from_tarball; then
    ok "Skills copied into ~/.agents/skills (and ~/.claude, ~/.codex where present)"
  else
    warn "Could not install skills. Later: npx skills add $KITARU_SKILLS_REPO"
  fi
fi

# ---------------------------------------------------------------------------
# 4. MCP server registration
# ---------------------------------------------------------------------------
MCP_SERVER_URL="${KITARU_SERVER:-$KITARU_LOCAL_URL}"
MCP_BIN="$TOOL_BIN/kitaru-mcp"

if [ "$KITARU_SKIP_MCP" = "1" ]; then
  note "Skipping MCP registration (--no-mcp)"
else
  step "Registering the Kitaru MCP server"
  registered=0
  if have claude; then
    if claude mcp get kitaru >/dev/null 2>&1; then
      quiet claude mcp remove --scope user kitaru || true
    fi
    if quiet claude mcp add --scope user kitaru -- "$MCP_BIN" --server "$MCP_SERVER_URL" --mode "$KITARU_MCP_MODE"; then
      ok "Claude Code: MCP server 'kitaru' (user scope)"; registered=1
    else
      warn "Claude Code: could not register MCP server"
    fi
  fi
  if have codex; then
    quiet codex mcp remove kitaru 2>/dev/null || true
    if quiet codex mcp add kitaru -- "$MCP_BIN" --server "$MCP_SERVER_URL" --mode "$KITARU_MCP_MODE"; then
      ok "Codex: MCP server 'kitaru'"; registered=1
    else
      warn "Codex: could not register MCP server"
    fi
  fi
  if [ "$registered" = "0" ]; then
    note "No Claude Code or Codex CLI found. For Cursor or any MCP client, add:"
    note "  {\"mcpServers\":{\"kitaru\":{\"command\":\"$MCP_BIN\",\"args\":[\"--server\",\"$MCP_SERVER_URL\",\"--mode\",\"$KITARU_MCP_MODE\"]}}}"
  fi
fi

# ---------------------------------------------------------------------------
# 5. Login (local server via Docker, or a team server)
# ---------------------------------------------------------------------------
docker_running() { have docker && docker info >/dev/null 2>&1; }
has_tty() { [ -t 0 ] || [ -r /dev/tty ]; }

login_cmd() {
  if [ -n "$KITARU_SERVER" ]; then printf 'kitaru login %s' "$KITARU_SERVER"
  else printf 'kitaru login --local'; fi
}

LOGGED_IN=0
if [ "$KITARU_SKIP_LOGIN" = "1" ]; then
  note "Skipping login (--no-login)"
elif [ -z "$KITARU_SERVER" ] && ! docker_running; then
  warn "Docker is not running, so the local Kitaru server was not started."
  note "Start Docker, then run: kitaru login --local"
  note "Or use a team server:   kitaru login https://<your-team>.kitaru.ai"
elif ! has_tty; then
  note "No terminal attached, so login was not run. Next: $(login_cmd)"
else
  step "Logging in ($(login_cmd))"
  if [ -n "$KITARU_SERVER" ]; then
    kitaru login "$KITARU_SERVER" </dev/tty && LOGGED_IN=1 || warn "Login did not complete. Run: $(login_cmd)"
  else
    kitaru login --local </dev/tty && LOGGED_IN=1 || warn "Local server did not start. Run: kitaru login --local"
  fi
fi

# ---------------------------------------------------------------------------
# Done
# ---------------------------------------------------------------------------
say ""
say "${C_GREEN}◆${C_RESET} ${C_BOLD}Kitaru is installed.${C_RESET}"
say ""
if ! command -v kitaru >/dev/null 2>&1 || [ "${TOOL_BIN}" != "$(dirname "$(command -v kitaru)")" ]; then
  say "  Open a new terminal so 'kitaru' is on your PATH."
fi
if [ "$LOGGED_IN" = "0" ]; then
  say "  1. $(login_cmd)"
  say "  2. Open your coding agent in your agent's repo and say:"
else
  say "  Open your coding agent in your agent's repo and say:"
fi
say ""
say "     ${C_BOLD}Use kitaru-investigation to investigate this agent.${C_RESET}"
say ""
say "  No agent yet?  ${C_BOLD}Use kitaru-guided-tour to show me Kitaru on the example agent.${C_RESET}"
say "  Check setup:   kitaru doctor"
say "  Docs:          https://docs.zenml.io/kitaru"
say ""
}

main "$@"
