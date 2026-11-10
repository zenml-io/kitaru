#!/usr/bin/env bash
# Offline installer regressions using real uv and a minimal fixture wheel.
set -euo pipefail
INSTALLER="${1:-$(cd "$(dirname "$0")/.." && pwd)/install.sh}"
INSTALLER="$(cd "$(dirname "$INSTALLER")" && pwd)/$(basename "$INSTALLER")"
command -v uv >/dev/null
PYTHON="$(uv python find --no-python-downloads ">=3.11")"
PYTHON_INSTALL_DIR="$(uv python dir)"
TMP="$(mktemp -d /tmp/kitaru-installer.XXXXXX)"
TMP="$(cd "$TMP" && pwd -P)"
trap 'rm -rf "$TMP"' EXIT
export TMPDIR="$TMP"
export UV_CACHE_DIR="$TMP/cache"
export UV_TOOL_DIR="$TMP/tools" UV_TOOL_BIN_DIR="$TMP/bin"
export XDG_CONFIG_HOME="$TMP/config" XDG_DATA_HOME="$TMP/data"
export XDG_CACHE_HOME="$TMP/xdg-cache"
export UV_OFFLINE=1 UV_PYTHON_DOWNLOADS=never UV_NO_CONFIG=1
export UV_PYTHON_INSTALL_DIR="$PYTHON_INSTALL_DIR"
export UV_NO_INDEX=1 UV_FIND_LINKS="$TMP/wheels"
unset VIRTUAL_ENV UV_PROJECT_ENVIRONMENT UV_PYTHON UV_PROJECT UV_WORKING_DIR UV_CONSTRAINT UV_FROZEN UV_NO_SYNC KITARU_PYTHON
mkdir -p "$TMP/wheels"
# This is an installer fixture, not a test of the real Kitaru package.
"$PYTHON" - "$TMP/wheels" <<'PY'
import pathlib
import sys
import zipfile

wheel = pathlib.Path(sys.argv[1]) / "kitaru-0.0.0-py3-none-any.whl"
files = {
    "kitaru_fixture.py": 'def main():\n    print("kitaru, version 0.0.0")\n',
    "kitaru-0.0.0.dist-info/METADATA": "Metadata-Version: 2.1\nName: kitaru\nVersion: 0.0.0\nProvides-Extra: cli\nProvides-Extra: mcp\nProvides-Extra: worker\n",
    "kitaru-0.0.0.dist-info/WHEEL": "Wheel-Version: 1.0\nGenerator: installer-regression\nRoot-Is-Purelib: true\nTag: py3-none-any\n",
    "kitaru-0.0.0.dist-info/entry_points.txt": "[console_scripts]\nkitaru = kitaru_fixture:main\n",
}
files["kitaru-0.0.0.dist-info/RECORD"] = "".join(f"{name},,\n" for name in files)
with zipfile.ZipFile(wheel, "w") as archive:
    for name, content in files.items():
        archive.writestr(name, content)

# A Python payload with interpreter-specific wheel tags models an adapter
# whose transitive dependency has wheels through Python 3.14 but no sdist.
adapter_files = {
    "adapter_fixture.py": "VALUE = 1\n",
    "kitaru_adapter_fixture-1.0.0.dist-info/METADATA": "Metadata-Version: 2.1\nName: kitaru-adapter-fixture\nVersion: 1.0.0\nRequires-Python: >=3.11\nRequires-Dist: kitaru\n",
    "kitaru_adapter_fixture-1.0.0.dist-info/WHEEL": "Wheel-Version: 1.0\nRoot-Is-Purelib: true\n" + "".join(f"Tag: cp3{minor}-none-any\n" for minor in range(11, 15)),
    "kitaru_adapter_fixture-1.0.0.dist-info/RECORD": "",
}
with zipfile.ZipFile(wheel.parent / "kitaru_adapter_fixture-1.0.0-cp311.cp312.cp313.cp314-none-any.whl", "w") as archive:
    for name, content in adapter_files.items():
        archive.writestr(name, content)
PY

run_project() {
  local label="$1" environment="$2" workspace="$TMP/$1"
  mkdir -p "$workspace/member"
  cat > "$workspace/pyproject.toml" <<'TOML'
[project]
name = "installer-workspace"
version = "0.0.0"
requires-python = ">=3.11"
[tool.uv.workspace]
members = ["member"]
TOML
  cat > "$workspace/member/pyproject.toml" <<'TOML'
[project]
name = "installer-member"
version = "0.0.0"
requires-python = ">=3.11"
TOML
  # Pin at the workspace root, not the current member directory.
  if [ "${3:-pin}" = pin ]; then
    printf '%s\n' "$PYTHON" > "$workspace/.python-version"
  else
    local existing="$workspace/.venv"
    if [ -n "$environment" ]; then
      case "$environment" in /*) existing="$environment" ;; *) existing="$workspace/$environment" ;; esac
    fi
    uv venv --python "$PYTHON" "$existing" >/dev/null 2>&1
  fi
  (
    cd "$workspace/member"
    if [ -n "$environment" ]; then export UV_PROJECT_ENVIRONMENT="$environment"; fi
    bash "$INSTALLER" --project --no-skills --no-modify-path --no-mcp --verbose
  ) > "$TMP/$label.log" 2>&1 || { cat "$TMP/$label.log"; return 1; }
  local expected="$workspace/.venv"
  if [ -n "$environment" ]; then
    case "$environment" in /*) expected="$environment" ;; *) expected="$workspace/$environment" ;; esac
  fi
  [ -x "$expected/bin/kitaru" ]
  grep -F "environment: $expected " "$TMP/$label.log" >/dev/null
  [ "$("$expected/bin/python" -c 'import sys; print(sys.base_prefix)')" = "$("$PYTHON" -c 'import sys; print(sys.base_prefix)')" ]
  if [ "${3:-pin}" = pin ]; then
    [ "$(cat "$workspace/.python-version")" = "$PYTHON" ]
  else
    [ ! -e "$workspace/.python-version" ]
  fi
  echo "PASS: $label"
}

run_project workspace ""
run_project existing-env "" existing
run_project relative-env custom-env
run_project absolute-env "$TMP/absolute-venv"
run_project relative-existing-env custom-env existing
run_project absolute-existing-env "$TMP/absolute-existing-venv" existing

# The environment can remain usable after its base interpreter disappears
# from PATH and uv's configured installation directory. Do not download a
# metadata parser when the project's interpreter already works.
mkdir "$TMP/uv-only" "$TMP/undiscoverable-pythons"
ln -s "$(command -v uv)" "$TMP/uv-only/uv"
UV_INSTALL_DIR="$TMP/uv-only" UV_PYTHON_INSTALL_DIR="$TMP/undiscoverable-pythons" PATH="$TMP/uv-only:/usr/bin:/bin" \
  run_project undiscoverable-existing-python "" existing
UV_INSTALL_DIR="$TMP/uv-only" UV_PYTHON_INSTALL_DIR="$TMP/undiscoverable-pythons" PATH="$TMP/uv-only:/usr/bin:/bin" \
  run_project undiscoverable-custom-python custom-env existing

run_unpinned_project() {
  local label="$1" requirement="$2" expected="$3" project="$TMP/$1"
  shift 3
  mkdir -p "$project"
  cat > "$project/pyproject.toml" <<TOML
[project]
name = "installer-$label"
version = "0.0.0"
requires-python = "$requirement"
TOML
  if [ -n "${PROJECT_PIN:-}" ]; then printf '%s\n' "$PROJECT_PIN" > "$project/.python-version"; fi
  (
    cd "$project"
    bash "$INSTALLER" --project --no-skills --no-mcp --no-modify-path --with=kitaru-adapter-fixture "$@"
  ) > "$TMP/$label.log" 2>&1 || { cat "$TMP/$label.log"; return 1; }
  "$project/.venv/bin/python" -c 'import adapter_fixture; assert adapter_fixture.VALUE == 1'
  local actual_python expected_python
  actual_python="$("$project/.venv/bin/python" -c 'import sys; print(sys.base_prefix)')"
  expected_python="$("$expected" -c 'import sys; print(sys.base_prefix)')"
  if [ "$actual_python" != "$expected_python" ]; then
    echo "$label: expected Python from $expected_python, got $actual_python" >&2
    return 1
  fi
  if [ -n "${PROJECT_PIN:-}" ]; then
    [ "$(cat "$project/.python-version")" = "$PROJECT_PIN" ]
  else
    [ ! -e "$project/.python-version" ]
  fi
  echo "PASS: $label"
}

# This project only accepts the locally available minor, so the check works
# offline on every supported CI interpreter, including jobs without 3.12.
PYTHON_MINOR="$("$PYTHON" -c 'import sys; print(sys.version_info.minor)')"
run_unpinned_project constrained-python ">=3.$PYTHON_MINOR,<3.$((PYTHON_MINOR + 1))" "$PYTHON"
run_unpinned_project explicit-python '>=3.11' "$PYTHON" --python "$PYTHON"
KITARU_PYTHON="$PYTHON" run_unpinned_project environment-python '>=3.11' "$PYTHON"

mkdir -p "$TMP/unpinned-workspace/member"
cat > "$TMP/unpinned-workspace/pyproject.toml" <<TOML
[project]
name = "installer-unpinned-workspace"
version = "0.0.0"
requires-python = ">=3.$PYTHON_MINOR,<3.$((PYTHON_MINOR + 1))"
[tool.uv.workspace]
members = ["member"]
TOML
cat > "$TMP/unpinned-workspace/member/pyproject.toml" <<'TOML'
[project]
name = "installer-unpinned-member"
version = "0.0.0"
requires-python = ">=3.11"
TOML
(
  cd "$TMP/unpinned-workspace/member"
  bash "$INSTALLER" --project --no-skills --no-mcp --no-modify-path --with=kitaru-adapter-fixture
) > "$TMP/unpinned-workspace.log" 2>&1 || { cat "$TMP/unpinned-workspace.log"; exit 1; }
"$TMP/unpinned-workspace/.venv/bin/python" -c 'import adapter_fixture; assert adapter_fixture.VALUE == 1'
[ "$("$TMP/unpinned-workspace/.venv/bin/python" -c 'import sys; print(sys.base_prefix)')" = "$("$PYTHON" -c 'import sys; print(sys.base_prefix)')" ]
[ ! -e "$TMP/unpinned-workspace/.python-version" ]
[ ! -e "$TMP/unpinned-workspace/member/.python-version" ]
echo 'PASS: unpinned workspace honors root Python requirements'

PYTHON_312="$(uv python find --no-project --no-python-downloads 3.12 2>/dev/null || true)"
if [ -n "$PYTHON_312" ]; then
  run_unpinned_project preferred-python '>=3.11' "$PYTHON_312"
  PROJECT_PIN="$PYTHON_312" run_unpinned_project explicit-pinned-python '>=3.11' "$PYTHON_312" --python 3.12
  if [ "$("$PYTHON" -c 'import sys; print(sys.base_prefix)')" != "$("$PYTHON_312" -c 'import sys; print(sys.base_prefix)')" ]; then
    mkdir "$TMP/conflicting-pin"
    cp "$TMP/explicit-pinned-python/pyproject.toml" "$TMP/conflicting-pin/pyproject.toml"
    cp "$TMP/conflicting-pin/pyproject.toml" "$TMP/conflicting-pin/original.toml"
    printf '%s\n' "$PYTHON" > "$TMP/conflicting-pin/.python-version"
    if (
      cd "$TMP/conflicting-pin"
      bash "$INSTALLER" --project --no-skills --no-mcp --no-modify-path --python 3.12
    ) > "$TMP/conflicting-pin.log" 2>&1; then
      echo 'installer accepted a Python override that later uv run commands cannot retain' >&2
      exit 1
    fi
    cmp "$TMP/conflicting-pin/pyproject.toml" "$TMP/conflicting-pin/original.toml"
    [ "$(cat "$TMP/conflicting-pin/.python-version")" = "$PYTHON" ]
    [ ! -e "$TMP/conflicting-pin/.venv" ]
    grep -F 'Update that pin explicitly' "$TMP/conflicting-pin.log" >/dev/null
    echo 'PASS: conflicting Python override leaves project and pin unchanged'
  fi
else
  echo 'SKIP: Python 3.12 preference (interpreter not installed)'
fi

# A request outside the project's requirements must fail before uv add can
# change the project. It must not be silently ignored or replace an existing pin.
mkdir "$TMP/incompatible-python"
cat > "$TMP/incompatible-python/pyproject.toml" <<'TOML'
[project]
name = "installer-incompatible-python"
version = "0.0.0"
requires-python = ">=3.11"
TOML
cp "$TMP/incompatible-python/pyproject.toml" "$TMP/incompatible-python/original.toml"
printf '%s\n' "$PYTHON" > "$TMP/incompatible-python/.python-version"
if (
  cd "$TMP/incompatible-python"
  bash "$INSTALLER" --project --no-skills --no-mcp --no-modify-path --python=3.10
) > "$TMP/incompatible-python.log" 2>&1; then
  echo 'installer ignored an incompatible --python request' >&2
  cat "$TMP/incompatible-python.log" >&2
  exit 1
fi
cmp "$TMP/incompatible-python/pyproject.toml" "$TMP/incompatible-python/original.toml"
[ "$(cat "$TMP/incompatible-python/.python-version")" = "$PYTHON" ]
[ ! -e "$TMP/incompatible-python/.venv" ]
grep -Ei 'Python|interpreter' "$TMP/incompatible-python.log" >/dev/null
echo 'PASS: incompatible Python request leaves project and pin unchanged'

# Exercise the production bootstrap function, stopping before its HOME-based
# binary lookup. The stub cannot install uv or edit shell configuration.
awk '/^install_uv\(\) \{/ { copy=1 } copy { print } copy && /^\}/ { exit }' "$INSTALLER" > "$TMP/bootstrap-function.sh"
for flag in 0 1; do
  # The sourced production function calls these test doubles.
  # shellcheck disable=SC2329,SC2016
  (
    step() { :; }
    fetch_to_stdout() { printf 'printf "%%s" "${UV_NO_MODIFY_PATH:-unset}" > "$BOOTSTRAP_OBSERVED"\nexit 42\n'; }
    quiet() { "$@"; }
    die() { rm -f "$UV_INSTALLER"; exit 42; }
    export BOOTSTRAP_OBSERVED="$TMP/bootstrap-$flag"
    unset UV_NO_MODIFY_PATH
    export KITARU_NO_MODIFY_PATH="$flag"
    export ORIG_PATH="$PATH" UV_BIN_DIR="$TMP/bootstrap-bin"
    # shellcheck source=/dev/null
    source "$TMP/bootstrap-function.sh"
    install_uv
  ) && exit 1
  expected="unset"
  [ "$flag" = 1 ] && expected=1
  [ "$(cat "$TMP/bootstrap-$flag")" = "$expected" ]
  echo "PASS: bootstrap no-modify-path=$flag"
done

mkdir "$TMP/project"
cat > "$TMP/project/pyproject.toml" <<'TOML'
[project]
name = "installer-project"
version = "0.0.0"
requires-python = ">=3.11"
TOML
printf '%s\n' "$PYTHON" > "$TMP/project/.python-version"
(
  cd "$TMP/project"
  bash "$INSTALLER" --no-skills --no-mcp --no-modify-path
) > "$TMP/project.log" 2>&1 || { cat "$TMP/project.log"; exit 1; }
[ -x "$TMP/project/.venv/bin/kitaru" ]
echo 'PASS: compatible standalone project'

# Upgrade a previously pinned dependency using the same real-uv fixture.
(
  cd "$TMP/workspace/member"
  "$PYTHON" - "$TMP/wheels" <<'PY'
import pathlib
import sys
import zipfile

root = pathlib.Path(sys.argv[1])
with zipfile.ZipFile(root / "kitaru-0.0.0-py3-none-any.whl") as old:
    files = {name.replace("0.0.0", "0.0.1"): old.read(name).replace(b"0.0.0", b"0.0.1") for name in old.namelist()}
with zipfile.ZipFile(root / "kitaru-0.0.1-py3-none-any.whl", "w") as new:
    for name, content in files.items():
        new.writestr(name, content)
PY
  bash "$INSTALLER" --project --no-skills --no-mcp --no-modify-path
) > "$TMP/upgrade.log" 2>&1 || { cat "$TMP/upgrade.log"; exit 1; }
grep -F 'kitaru 0.0.1 installed' "$TMP/upgrade.log" >/dev/null
echo 'PASS: rerun upgrades project dependency'

if bash --posix "$INSTALLER" --help > "$TMP/posix.log" 2>&1; then exit 1; fi
grep -F 'installer needs bash' "$TMP/posix.log" >/dev/null
echo 'PASS: POSIX Bash rejection'

# Extract production functions to isolate profile and skill writes. HOME is
# never reassigned; only these copies substitute a temporary test destination.
extract_function() {
  # shellcheck disable=SC2016
  awk -v name="$1" '$0 == name "() {" { copy=1 } copy { print } copy && /^}/ { exit }' "$INSTALLER" \
    | sed 's/\$HOME/\$TEST_HOME/g'
}
extract_function persist_path > "$TMP/functions.sh"
extract_function install_skills >> "$TMP/functions.sh"
# shellcheck source=/dev/null
source "$TMP/functions.sh"
TEST_HOME="$TMP/test-home"
mkdir -p "$TEST_HOME"
# shellcheck disable=SC2016
printf 'export PATH="%s:$PATH"\n' "$TEST_HOME/.local/bin" > "$TEST_HOME/.zshenv"
persist_path "$TEST_HOME/.local/bin"
[ ! -e "$TEST_HOME/.profile" ]
echo 'PASS: zshenv persistence detected'

mkdir -p "$TMP/archive/repo/skills/kitaru-fixture" "$TMP/skills"
printf 'new\n' > "$TMP/archive/repo/skills/kitaru-fixture/SKILL.md"
tar -czf "$TMP/skills.tar.gz" -C "$TMP/archive" repo
export KITARU_SKILLS_REPO=fixture
# shellcheck disable=SC2329
fetch_to_stdout() { cat "$TMP/skills.tar.gz"; }
# shellcheck disable=SC2329
skill_destinations() { echo "$TMP/skills"; }
# shellcheck disable=SC2329
warn() { echo "$*" >&2; }
for prior in directory file symlink; do
  rm -rf "$TMP/skills/kitaru-fixture"
  case "$prior" in
    directory) mkdir "$TMP/skills/kitaru-fixture"; echo original > "$TMP/skills/kitaru-fixture/SKILL.md" ;;
    file) echo original > "$TMP/skills/kitaru-fixture" ;;
    symlink) ln -s missing-target "$TMP/skills/kitaru-fixture" ;;
  esac
  for failure in copy publish; do
    (
      # Fail either staging the replacement or publishing it after retirement.
      # shellcheck disable=SC2329
      cp() { [ "$failure" != copy ] && command cp "$@"; }
      # shellcheck disable=SC2329
      mv() {
        case "$1" in */.kitaru-skills.*/kitaru-fixture) [ "$failure" != publish ] || return 1 ;; esac
        command mv "$@"
      }
      if install_skills; then exit 1; fi
    )
    case "$prior" in
      directory) [ "$(cat "$TMP/skills/kitaru-fixture/SKILL.md")" = original ] ;;
      file) [ "$(cat "$TMP/skills/kitaru-fixture")" = original ] ;;
      symlink) [ "$(readlink "$TMP/skills/kitaru-fixture")" = missing-target ] ;;
    esac
  done
  install_skills
  [ "$(cat "$TMP/skills/kitaru-fixture/SKILL.md")" = new ]
done
echo 'PASS: legacy skills update and copy/publish rollback for directories, files and symlinks'

# Inspect the commands suggested to a shell whose PATH lacks the tool bin.
(
  cd "$TMP"
  KITARU_PYTHON="$PYTHON" bash "$INSTALLER" --global --no-skills --no-mcp --no-modify-path --version=0.0.1
) > "$TMP/global.log" 2>&1 || { cat "$TMP/global.log"; exit 1; }
grep -F "export PATH=\"$TMP/bin:\$PATH\"" "$TMP/global.log" >/dev/null
grep -F 'kitaru login --local' "$TMP/global.log" >/dev/null
if grep -F 'uvx kitaru' "$TMP/global.log"; then exit 1; fi
echo 'PASS: usable global shell hint'

# The real installer can discover uv in its usual home directory even though
# the caller cannot. Substitute only the script's HOME references, never the
# environment inherited by uv or any user's configuration.
REAL_UV="$(command -v uv)"
mkdir -p "$TEST_HOME/.local/bin" "$TMP/stubs"
ln -s "$REAL_UV" "$TEST_HOME/.local/bin/uv"
# shellcheck disable=SC2016
sed 's/\$HOME/\$TEST_HOME/g' "$INSTALLER" > "$TMP/installer-isolated.sh"
export TEST_HOME
(
  cd "$TMP/workspace/member"
  PATH=/usr/bin:/bin bash "$TMP/installer-isolated.sh" --project --no-skills --no-mcp --no-modify-path
) > "$TMP/project-hint.log" 2>&1 || { cat "$TMP/project-hint.log"; exit 1; }

# shellcheck disable=SC2016
grep -F 'export PATH="$TEST_HOME/.local/bin:$PATH"' "$TMP/project-hint.log" >/dev/null
echo 'PASS: usable project hint for newly available uv'

cat > "$TMP/stubs/uv" <<'STUB'
#!/usr/bin/env bash
if [ "${1:-}" = --version ]; then echo 'uv 0.0.0'; exit 0; fi
exit 96
STUB
chmod +x "$TMP/stubs/uv"
(
  cd "$TMP/workspace/member"
  PATH="$TMP/stubs:/usr/bin:/bin" bash "$TMP/installer-isolated.sh" --project --no-skills --no-mcp --no-modify-path
) > "$TMP/outdated-project-hint.log" 2>&1 || { cat "$TMP/outdated-project-hint.log"; exit 1; }
# shellcheck disable=SC2016
grep -F 'export PATH="$TEST_HOME/.local/bin:$PATH"' "$TMP/outdated-project-hint.log" >/dev/null
echo 'PASS: project hint selects the current uv ahead of an outdated one'

# Intercept update-shell so real uv never edits a profile. Other operations
# still use real uv and the local fixture wheel.
cat > "$TMP/stubs/uv" <<'STUB'
#!/usr/bin/env bash
if [ "${1:-} ${2:-}" = 'tool update-shell' ]; then
  printf '%s' "$PATH" > "$OBSERVED_PATH"
  exit 0
fi
exec "$REAL_UV" "$@"
STUB
chmod +x "$TMP/stubs/uv"
rm "$TEST_HOME/.local/bin/uv"
export REAL_UV OBSERVED_PATH="$TMP/observed-path"
ORIGINAL_TEST_PATH="$TMP/stubs:$PATH"
(
  cd "$TMP"
  PATH="$ORIGINAL_TEST_PATH" KITARU_PYTHON="$PYTHON" bash "$TMP/installer-isolated.sh" --global --no-skills --no-mcp --version=0.0.1
) > "$TMP/persistence.log" 2>&1 || { cat "$TMP/persistence.log"; exit 1; }
[ "$(cat "$OBSERVED_PATH")" = "$ORIGINAL_TEST_PATH" ]
echo 'PASS: update-shell receives original PATH'

# A normal rerun must upgrade the package while retaining the existing tool's
# add-ons and interpreter. Inspect uv's real receipt, not just command flags.
"$PYTHON" - "$TMP/wheels" <<'PY'
import pathlib
import sys
import zipfile
root = pathlib.Path(sys.argv[1])
files = {
    "fixture_extra.py": "VALUE = 1\n",
    "fixture_extra-1.0.0.dist-info/METADATA": "Metadata-Version: 2.1\nName: fixture-extra\nVersion: 1.0.0\n",
    "fixture_extra-1.0.0.dist-info/WHEEL": "Wheel-Version: 1.0\nRoot-Is-Purelib: true\nTag: py3-none-any\n",
    "fixture_extra-1.0.0.dist-info/RECORD": "",
}
with zipfile.ZipFile(root / "fixture_extra-1.0.0-py3-none-any.whl", "w") as archive:
    for name, content in files.items():
        archive.writestr(name, content)
PY
(
  cd "$TMP"
  export UV_TOOL_DIR="$TMP/preserve-tools" UV_TOOL_BIN_DIR="$TMP/preserve-bin"
  uv tool install --python "$PYTHON" --with fixture-extra 'kitaru[cli,mcp,worker]' >/dev/null 2>&1
  before="$("$UV_TOOL_DIR/kitaru/bin/python" -c 'import sys; print(sys.base_prefix)')"
  "$PYTHON" - "$TMP/wheels" <<'PY'
import pathlib
import sys
import zipfile
root = pathlib.Path(sys.argv[1])
with zipfile.ZipFile(root / "kitaru-0.0.1-py3-none-any.whl") as old:
    files = {name.replace("0.0.1", "0.0.2"): old.read(name).replace(b"0.0.1", b"0.0.2") for name in old.namelist()}
with zipfile.ZipFile(root / "kitaru-0.0.2-py3-none-any.whl", "w") as new:
    for name, content in files.items():
        new.writestr(name, content)
PY
  bash "$INSTALLER" --global --no-skills --no-mcp --no-modify-path
  [ "$("$UV_TOOL_DIR/kitaru/bin/python" -c 'import sys; print(sys.base_prefix)')" = "$before" ]
  "$UV_TOOL_DIR/kitaru/bin/python" -c 'import fixture_extra; assert fixture_extra.VALUE == 1'
  "$PYTHON" - "$UV_TOOL_DIR/kitaru/uv-receipt.toml" "$PYTHON" <<'PY'
import pathlib
import sys
import tomllib
receipt = tomllib.loads(pathlib.Path(sys.argv[1]).read_text())["tool"]
assert any(item["name"] == "fixture-extra" for item in receipt["requirements"]), receipt
assert receipt["python"] == sys.argv[2], receipt
PY
  "$UV_TOOL_BIN_DIR/kitaru" --version | grep -F 'version 0.0.2'
) > "$TMP/global-preservation.log" 2>&1 || { cat "$TMP/global-preservation.log"; exit 1; }
echo 'PASS: real global upgrade preserves receipt, add-on package and Python'
