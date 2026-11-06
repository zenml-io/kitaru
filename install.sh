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
#   3. Runs `kitaru setup`, which installs the Kitaru agent skills
#      (zenml-io/kitaru-skills) into ~/.agents/skills plus ~/.claude/skills
#      when Claude Code is installed, and registers the
#      Kitaru MCP server with Claude Code, Codex, Cursor, and Windsurf when
#      found (printing the JSON for everything else). Re-run `kitaru setup`
#      after installing a new coding agent. Kitaru releases before `setup`
#      existed get the same steps done by this script instead.
#   4. Stops there and prints the two ways to get a server: local in Docker or Podman
#      (`kitaru login --local`) or the managed cloud (`kitaru login`). Login
#      is a decision, so the script does not make it for you.
#
# Nothing here needs sudo. Everything lands under $HOME. Re-running upgrades.
#
# Design follows astral.sh/uv/install.sh (no root, no assumptions about the
# system Python, --no-* escape hatches). Kitaru ships as a Python package, not
# a binary, so uv is the one dependency this script will install for you.

# Bash-only, but fail politely under sh/dash/zsh-as-sh. This line is POSIX so
# it runs before the shell reaches any bash syntax below.
if [ -z "${BASH_VERSION:-}" ]; then echo "Kitaru's installer needs bash. Run: curl -fsSL https://kitaru.ai/install | bash" >&2; exit 1; fi

# macOS sh sets BASH_VERSION but rejects the Bash syntax below in POSIX mode.
case ":${SHELLOPTS:-}:" in *:posix:*) echo "Kitaru's installer needs bash, not sh. Run: curl -fsSL https://kitaru.ai/install | bash" >&2; exit 1 ;; esac

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
KITARU_PYTHON_EXPLICIT="${KITARU_PYTHON:+1}"
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
  --python=VERSION    Select Python for a global install (default: 3.12)
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

require_value() {
  case "${2:-}" in
    ""|-*) echo "$1 needs a value." >&2; exit 2 ;;
  esac
}

while [ $# -gt 0 ]; do
  case "$1" in
    --version=*) require_value --version "${1#*=}"; KITARU_VERSION="${1#*=}" ;;
    --version) require_value "$1" "${2:-}"; shift; KITARU_VERSION="$1" ;;
    --python=*) require_value --python "${1#*=}"; KITARU_PYTHON="${1#*=}"; KITARU_PYTHON_EXPLICIT=1 ;;
    --python) require_value "$1" "${2:-}"; shift; KITARU_PYTHON="$1"; KITARU_PYTHON_EXPLICIT=1 ;;
    --pre) KITARU_PRE=1 ;;
    --with=*) require_value --with "${1#*=}"; KITARU_WITH+=("${1#*=}") ;;
    --with) require_value "$1" "${2:-}"; shift; KITARU_WITH+=("$1") ;;
    --server=*) require_value --server "${1#*=}"; KITARU_SERVER="${1#*=}" ;;
    --server) require_value "$1" "${2:-}"; shift; KITARU_SERVER="$1" ;;
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

have curl || die "Need curl with HTTPS support. Install curl and re-run this installer."

fetch_to_stdout() {
  curl -fsSL --proto '=https' --proto-redir '=https' --tlsv1.2 "$1"
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
UV_BIN_DIR="${UV_INSTALL_DIR:-${CARGO_DIST_FORCE_INSTALL_DIR:-${UV_UNMANAGED_INSTALL:-${XDG_BIN_HOME:-${XDG_DATA_HOME:+$XDG_DATA_HOME/../bin}}}}}"
UV_BIN_DIR="${UV_BIN_DIR:-$HOME/.local/bin}"
if [ "$UV_BIN_DIR" = "${CARGO_HOME:-$HOME/.cargo}" ]; then UV_BIN_DIR="$UV_BIN_DIR/bin"; fi
for candidate in "$UV_BIN_DIR" "$HOME/.local/bin" "$HOME/.cargo/bin"; do
  if [ -x "$candidate/uv" ] && "$candidate/uv" --version >/dev/null 2>&1; then
    ensure_path "$candidate"
    break
  fi
done

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
  if [ "$KITARU_NO_MODIFY_PATH" = "1" ]; then
    export UV_NO_MODIFY_PATH=1
  fi
  quiet env PATH="$ORIG_PATH" UV_INSTALL_DIR="$UV_BIN_DIR" sh "$UV_INSTALLER" --quiet \
    || die "uv install failed. Install it from https://docs.astral.sh/uv/ and re-run."
  rm -f "$UV_INSTALLER"; trap - EXIT
  # Use the binary we just installed by absolute path, and put its directory
  # in front so an older uv earlier on PATH (or bash's hashed path) can't win.
  UV="$UV_BIN_DIR/uv"
  export PATH="$UV_BIN_DIR:$PATH"; hash -r
  [ -x "$UV" ] || die "uv installed but $UV is missing. Install it from https://docs.astral.sh/uv/ and re-run."
  version_ge "$(uv_version_of "$UV")" "$KITARU_MIN_UV" \
    || die "uv at $UV is $(uv_version_of "$UV"), older than $KITARU_MIN_UV. Install a current uv and re-run."
  ok "uv $(uv_version_of "$UV") installed"
}

# $UV is the uv binary used for everything below (absolute path).
UV=""
if have uv; then
  UV="$(command -v uv)"
  UV_VER="$(uv_version_of "$UV" || true)"
  if [ -n "$UV_VER" ] && version_ge "$UV_VER" "$KITARU_MIN_UV"; then
    ok "uv $UV_VER found"
  else
    note "uv at $UV is unavailable or older than $KITARU_MIN_UV; installing a current one."
    install_uv
  fi
else
  install_uv
fi

persist_path() {
  # `uv tool update-shell` handles bash/zsh/fish when it recognizes the login
  # shell. Cover the rest (Alpine sh, containers, CI images) by appending to
  # ~/.profile when no rc file mentions the directory yet.
  local dir="$1" f display="$1"
  case "$dir" in "$HOME"/*) display="\$HOME${dir#"$HOME"}" ;; esac
  for f in "$HOME/.bashrc" "$HOME/.zshenv" "$HOME/.zshrc" "$HOME/.profile" "$HOME/.bash_profile" "$HOME/.config/fish/config.fish"; do
    [ -f "$f" ] && grep -Fqs -e "$dir" -e "$display" "$f" && return 0
  done
  local shell="${SHELL:-}"
  if [ "${shell##*/}" = fish ]; then
    warn "Add $dir to fish's PATH: fish_add_path '$dir'"
    return 0
  fi
  # shellcheck disable=SC2016  # Expand PATH when the shell starts.
  if ! printf '\n# Added by the Kitaru installer\nexport PATH="%s:$PATH"\n' "$dir" >> "$HOME/.profile"; then
    warn "Could not update ~/.profile. Add this to your shell: export PATH=\"$dir:\$PATH\""
  fi
  return 0
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
  if [ -f "$PROJECT_DIR/pyproject.toml" ] || [ -f "$PROJECT_DIR/uv.lock" ] || [ -f "$PROJECT_DIR/Pipfile" ] || [ -f "$PROJECT_DIR/poetry.lock" ] || [ -f "$PROJECT_DIR/pdm.lock" ] || [ -f "$PROJECT_DIR/Pipfile.lock" ]; then
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
  for foreign in poetry.lock pdm.lock Pipfile Pipfile.lock; do
    [ ! -e "$PROJECT_DIR/$foreign" ] || die "Found $foreign. Add Kitaru with that project's package manager, or use --global for an isolated CLI."
  done
  if grep -Eq '^[[:space:]]*\[[[:space:]]*tool[[:space:]]*\.[[:space:]]*(poetry|pdm)[[:space:]]*(\.|\])' "$PROJECT_DIR/pyproject.toml"; then
    die "Found Poetry/PDM project configuration. Add Kitaru with that package manager, or use --global for an isolated CLI."
  fi
  grep -Eq '^[[:space:]]*\[[[:space:]]*project[[:space:]]*\][[:space:]]*(#.*)?$' "$PROJECT_DIR/pyproject.toml" \
    || die "pyproject.toml has no [project] metadata. Initialize a Python project first, or use --global for an isolated CLI."
  step "Installing $SPEC into this project ($PROJECT_DIR)"
  UV_ADD=(add --quiet --upgrade-package kitaru)
  [ "$KITARU_PRE" = "1" ] && UV_ADD+=(--prerelease allow)
  quiet "$UV" "${UV_ADD[@]}" "$SPEC" "${EXTRA_PKGS[@]:+${EXTRA_PKGS[@]}}" \
    || die "uv add failed (uv's message is above; Kitaru needs Python >= 3.11). To install outside this project instead: --global"
  VENV_DIR="$("$UV" run --no-sync --offline --no-python-downloads python -c 'import sys; print(sys.prefix)')" \
    || die "Could not locate the project environment after uv add. Re-run with --verbose."
  case "$(uname -s)" in MINGW*|MSYS*|CYGWIN*) VENV_DIR="$(cygpath -u "$VENV_DIR")" ;; esac
  case "$(uname -s)" in
    MINGW*|MSYS*|CYGWIN*) TOOL_BIN="$VENV_DIR/Scripts" ;;
    *) TOOL_BIN="$VENV_DIR/bin" ;;
  esac
  KITARU_BIN="$TOOL_BIN/kitaru"
  [ -x "$KITARU_BIN" ] || [ -x "$KITARU_BIN.exe" ] || die "uv reported success but $KITARU_BIN is missing. Re-run with --verbose."
  CLI_VERSION="$("$KITARU_BIN" --version)" || die "Kitaru was installed but its CLI could not start. See the error above and re-run with --verbose."
  ok "$CLI_VERSION installed into this project's environment"
  note "environment: $VENV_DIR (recorded in pyproject.toml)"
  note "run it as: uv run kitaru ...   (or activate the environment)"
  # MCP: run through uv so the assistant gets this project's environment.
  MCP_CMD=("$UV" run --directory "$PROJECT_DIR" kitaru-mcp)
  CLAUDE_SCOPE=project
else
  # ---- isolated tool environment on PATH -----------------------------------
  TOOL_ROOT="$("$UV" tool dir)" || die "Could not locate uv's tool directory."
  if [ -f "$TOOL_ROOT/kitaru/uv-receipt.toml" ] && [ -z "$KITARU_VERSION" ] && [ "$KITARU_PRE" != "1" ] && [ -z "$KITARU_PYTHON_EXPLICIT" ] && [ "${#EXTRA_PKGS[@]}" -eq 0 ]; then
    step "Upgrading Kitaru (preserving installed Python and add-on packages)"
    quiet "$UV" tool upgrade --quiet kitaru || die "uv tool upgrade failed. See uv's error above and re-run with --verbose."
  else
    UV_ARGS=(tool install --upgrade --quiet --python "$KITARU_PYTHON")
    [ "$KITARU_PRE" = "1" ] && UV_ARGS+=(--prerelease allow)
    for pkg in "${EXTRA_PKGS[@]:+${EXTRA_PKGS[@]}}"; do UV_ARGS+=(--with "$pkg"); done
    step "Installing $SPEC (isolated, on PATH)"
    quiet "$UV" "${UV_ARGS[@]}" "$SPEC" || die "uv tool install failed. If an unmanaged kitaru executable already exists, remove it with its original package manager or choose UV_TOOL_BIN_DIR; this installer will not overwrite it. Re-run with --verbose for details."
  fi

  # uv puts tool executables in its tool bin dir; make sure future shells see it.
  TOOL_BIN="$("$UV" tool dir --bin 2>/dev/null || echo "$HOME/.local/bin")"
  TOOL_ENV="$("$UV" tool dir 2>/dev/null || echo "$HOME/.local/share/uv/tools")/kitaru"
  case "$(uname -s)" in MINGW*|MSYS*|CYGWIN*) TOOL_BIN="$(cygpath -u "$TOOL_BIN" 2>/dev/null || echo "$TOOL_BIN")" ;; esac
  ensure_path "$TOOL_BIN"
  if [ "$KITARU_NO_MODIFY_PATH" = "1" ]; then
    note "Not editing shell rc files (--no-modify-path). Make sure $TOOL_BIN is on your PATH."
  else
    quiet env PATH="$ORIG_PATH" "$UV" tool update-shell || true
    persist_path "$TOOL_BIN"
  fi

  KITARU_BIN="$TOOL_BIN/kitaru"
  [ -x "$KITARU_BIN" ] || [ -x "$KITARU_BIN.exe" ] || die "uv reported success but $KITARU_BIN is missing. Re-run with --verbose."
  CLI_VERSION="$("$KITARU_BIN" --version)" || die "Kitaru was installed but its CLI could not start. See the error above and re-run with --verbose."
  ok "$CLI_VERSION installed"
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
# 3. Skills and MCP registration, via `kitaru setup`
# ---------------------------------------------------------------------------
# The CLI owns client detection so it can be re-run after installing a new
# coding agent. `schema setup` is offline and only succeeds on versions that
# have the command; older ones fall through to the bash implementation below.
SETUP_DONE=0
if [ "$KITARU_SKIP_SKILLS" = "1" ] && [ "$KITARU_SKIP_MCP" = "1" ]; then
  note "Skipping skills and MCP registration (--no-skills --no-mcp)"
  SETUP_DONE=1
elif "$KITARU_BIN" schema setup >/dev/null 2>&1; then
  step "Running kitaru setup (skills and MCP registration)"
  SETUP_ARGS=(--mode "$KITARU_MCP_MODE")
  [ "$KITARU_SKIP_SKILLS" = "1" ] && SETUP_ARGS+=(--no-skills)
  [ "$KITARU_SKIP_MCP" = "1" ] && SETUP_ARGS+=(--no-mcp)
  SETUP_SERVER_ARGS=()
  # Older releases with setup may predate the global output option.
  if "$KITARU_BIN" --help 2>/dev/null | grep -q -- '--output'; then
    SETUP_SERVER_ARGS+=(--output text)
  fi
  [ -n "$KITARU_SERVER" ] && SETUP_SERVER_ARGS+=(--server "$KITARU_SERVER")
  if [ "$KITARU_QUIET" = "1" ]; then
    quiet "$KITARU_BIN" "${SETUP_SERVER_ARGS[@]:+${SETUP_SERVER_ARGS[@]}}" setup "${SETUP_ARGS[@]}" || warn "kitaru setup failed; run it again later: $KITARU_BIN setup"
  else
    "$KITARU_BIN" "${SETUP_SERVER_ARGS[@]:+${SETUP_SERVER_ARGS[@]}}" setup "${SETUP_ARGS[@]}" </dev/null || warn "kitaru setup failed; run it again later: $KITARU_BIN setup"
  fi
  SETUP_DONE=1
fi

if [ "$SETUP_DONE" = "0" ]; then
# ---------------------------------------------------------------------------
# 3a. Coding-agent skills (Kitaru releases without `kitaru setup`)
# ---------------------------------------------------------------------------
# Codex reads ~/.agents/skills directly; only Claude needs another copy.
skill_destinations() {
  echo "$HOME/.agents/skills"
  if have claude || [ -d "$HOME/.claude" ]; then echo "$HOME/.claude/skills"; fi
}

install_skills() {
  (
    # Bash 3 clears function locals before EXIT; keep cleanup state in this subshell.
    tmp=""; staging=""; target=""
    SKILL_NAMES=(); SKILL_DESTS=()
    # shellcheck disable=SC2329 # Invoked by the scoped EXIT trap.
    cleanup_skills() {
      trap '' INT TERM
      if [ -n "$staging" ] && { [ -e "$staging/previous" ] || [ -L "$staging/previous" ]; }; then
        if [ ! -e "$target" ] && [ ! -L "$target" ]; then
          mv "$staging/previous" "$target" || { warn "Previous skill remains at $staging/previous"; rm -rf "$tmp"; return; }
        fi
      fi
      [ -z "$staging" ] || rm -rf "$staging"
      [ -z "$tmp" ] || rm -rf "$tmp"
    }
    trap cleanup_skills EXIT
    trap 'exit 130' INT
    trap 'exit 143' TERM
    tmp="$(mktemp -d)" || exit 1
    url="https://github.com/${KITARU_SKILLS_REPO}/archive/refs/heads/main.tar.gz"
    fetch_to_stdout "$url" | tar -xzf - -C "$tmp" 2>/dev/null || exit 1
    src="$(find "$tmp" -mindepth 2 -maxdepth 2 -type d -name skills | head -1)"
    [ -n "$src" ] || exit 1
    SKILL_NAMES=()
    for skill in "$src"/*/; do
      [ -f "$skill/SKILL.md" ] && SKILL_NAMES+=("$(basename "$skill")")
    done
    [ "${#SKILL_NAMES[@]}" -gt 0 ] || exit 1
    SKILL_DESTS=()
    while IFS= read -r dest; do SKILL_DESTS+=("$dest"); done < <(skill_destinations)
    for dest in "${SKILL_DESTS[@]}"; do
      mkdir -p "$dest" || exit 1
      for name in "${SKILL_NAMES[@]}"; do
        target="$dest/$name"
        staging="$(mktemp -d "$dest/.kitaru-skills.XXXXXX")" || exit 1
        cp -R "$src/$name" "$staging/$name" || exit 1
        if [ -e "$target" ] || [ -L "$target" ]; then
          mv "$target" "$staging/previous" || exit 1
        fi
        mv "$staging/$name" "$target" || exit 1
        rm -rf "$staging"
        staging=""
      done
    done
    for dest in "${SKILL_DESTS[@]}"; do
      for name in "${SKILL_NAMES[@]}"; do
        [ -f "$dest/$name/SKILL.md" ] || exit 1
      done
    done
  )
}

if [ "$KITARU_SKIP_SKILLS" = "1" ]; then
  note "Skipping skills (--no-skills)"
else
  step "Installing Kitaru agent skills ($KITARU_SKILLS_REPO)"
  if install_skills; then
    ok "Skills installed into ~/.agents/skills and detected Claude Code directories"
  else
    warn "Could not install skills. Later: npx skills add $KITARU_SKILLS_REPO"
  fi
fi

# ---------------------------------------------------------------------------
# 3b. MCP server registration (Kitaru releases without `kitaru setup`)
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

fi  # SETUP_DONE

# ---------------------------------------------------------------------------
# Done
# ---------------------------------------------------------------------------
say ""
say "${C_GREEN}◆${C_RESET} ${C_BOLD}Kitaru is installed.${C_RESET}"
say ""
# Print the PATH export needed by the first word of the suggested commands.
if [ "$KITARU_SCOPE" = "project" ]; then
  K="uv run kitaru"; NEEDED_DIR="$(dirname "$UV")"
  case "$(PATH="$ORIG_PATH" command -v uv 2>/dev/null || true)" in
    "$UV"|"$UV.exe") NEEDED_DIR="" ;;
  esac
  say "  Installed into this project's environment, so run it as ${C_BOLD}uv run kitaru ...${C_RESET}"
  say "  (or activate $VENV_DIR)."
  say ""
else
  K="kitaru"; NEEDED_DIR="$TOOL_BIN"
  case "$(PATH="$ORIG_PATH" command -v kitaru 2>/dev/null || true)" in
    "$KITARU_BIN"|"$KITARU_BIN.exe") NEEDED_DIR="" ;;
  esac
fi
if [ -n "$NEEDED_DIR" ]; then
  case "$NEEDED_DIR" in "$HOME"/*) NEEDED_DIR="\$HOME${NEEDED_DIR#"$HOME"}" ;; esac
  say "  '${K%% *}' is not on this shell's PATH yet. Run:"
  say "    ${C_BOLD}export PATH=\"$NEEDED_DIR:\$PATH\"${C_RESET}"
  say ""
fi
if [ -n "$KITARU_SERVER" ]; then
  say "  Next, log in to your server:"
  say ""
  say "    ${C_BOLD}$K login $KITARU_SERVER${C_RESET}"
else
  say "  Next, pick where your Kitaru server lives:"
  say ""
  say "    ${C_BOLD}$K login --local${C_RESET}    local, with Docker or Podman. Free, open source."
  say "    ${C_BOLD}$K login${C_RESET}            managed cloud. 14-day trial, no credit card required."
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
say "  New editor?    $K setup   (wires skills and MCP into it)"
say "  Check setup:   $K doctor"
say "  Docs:          https://docs.zenml.io/kitaru"
say ""
}

main "$@"
