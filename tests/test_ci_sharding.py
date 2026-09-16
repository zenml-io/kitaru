#  Copyright (c) ZenML GmbH 2026. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at:
#
#       https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
#  or implied. See the License for the specific language governing
#  permissions and limitations under the License.
"""CI shards retain the full collection without splitting test files."""

import os
import subprocess
import sys
from pathlib import Path
from unittest.mock import Mock

import pytest

import conftest


def _collect(root: Path, index: str | None = None) -> list[str]:
    env = os.environ.copy()
    env.pop("KITARU_TEST_SHARD_INDEX", None)
    env.pop("KITARU_TEST_SHARD_COUNT", None)
    if index is not None:
        env["KITARU_TEST_SHARD_INDEX"] = index
        env["KITARU_TEST_SHARD_COUNT"] = "2"
    script = (
        "import sys; from types import SimpleNamespace; "
        f"sys.path.insert(0, {str(Path(conftest.__file__).parent)!r}); "
        "import conftest; import pytest; "
        "raise SystemExit(pytest.main(['--collect-only', '-q', "
        "'--rootdir=.', '--confcutdir=.'], "
        "plugins=[SimpleNamespace(pytest_collection_modifyitems="
        "conftest.pytest_collection_modifyitems)]))"
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=root,
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    return [line for line in result.stdout.splitlines() if "::test_" in line]


def test_shards_partition_collection_and_include_new_files(tmp_path: Path) -> None:
    """New files are included once, with each file's original test order intact."""
    source = (
        "import pytest\n"
        "@pytest.mark.parametrize('value', [1, 2])\n"
        "def test_example(value): pass\n"
        "def test_other(): pass\n"
    )
    for index in range(8):
        (tmp_path / f"test_example_{index}.py").write_text(source)
    original = _collect(tmp_path)
    (tmp_path / "test_new.py").write_text(source)
    full = _collect(tmp_path)
    shards = [_collect(tmp_path, index) for index in ("0", "1")]

    assert len(full) == len(original) + 3
    assert all(shards)
    assert set(shards[0]).isdisjoint(shards[1])
    assert set(shards[0]) | set(shards[1]) == set(full)
    assert sum(map(len, shards)) == len(full)
    for shard in shards:
        files = {node.split("::")[0] for node in shard}
        assert shard == [node for node in full if node.split("::")[0] in files]


@pytest.mark.parametrize(
    ("index", "count"),
    [
        ("0", None),
        (None, "2"),
        ("", "2"),
        ("x", "2"),
        ("0", "x"),
        ("0", "0"),
        ("0", "-1"),
        ("-1", "2"),
        ("2", "2"),
    ],
)
def test_invalid_shard_configuration_fails(
    monkeypatch: pytest.MonkeyPatch, index: str | None, count: str | None
) -> None:
    """Invalid shard configuration cannot silently omit tests."""
    for name, value in (("INDEX", index), ("COUNT", count)):
        key = f"KITARU_TEST_SHARD_{name}"
        if value is None:
            monkeypatch.delenv(key, raising=False)
        else:
            monkeypatch.setenv(key, value)
    with pytest.raises(pytest.UsageError, match="KITARU_TEST_SHARD"):
        conftest.pytest_collection_modifyitems(Mock(spec=pytest.Config), [])


@pytest.mark.parametrize("value", [None, ""])
def test_unconfigured_sharding_preserves_collection(
    monkeypatch: pytest.MonkeyPatch, value: str | None
) -> None:
    """Local runs and matrix rows with empty values keep every collected item."""
    for name in ("KITARU_TEST_SHARD_INDEX", "KITARU_TEST_SHARD_COUNT"):
        if value is None:
            monkeypatch.delenv(name, raising=False)
        else:
            monkeypatch.setenv(name, value)
    config = Mock(spec=pytest.Config)
    item = Mock(spec=pytest.Item)
    items: list[pytest.Item] = [item]

    conftest.pytest_collection_modifyitems(config, items)

    assert items == [item]
    assert config.mock_calls == []
